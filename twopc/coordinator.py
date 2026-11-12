"""两阶段提交 —— 协调者进程。

职责与规则：
  * /submit 收到 JSON 后先在本地 SQLite 持久登记事务，再发起任何网络通信；
  * 多事务可交错 prepare；分库反对/锁冲突 -> 持久决定 ABORT；全赞成 -> COMMIT；
  * 决定落盘后永不改变，通知双方可无限重试（断连、丢响应、重复投递均安全）；
  * 仅双方确认 commit 后该事务才返回 committed；abort 返回 aborted；
  * 重启恢复：无决定的事务撤销，已有 commit 继续完成，绝不以超时推翻决定；
  * /data 一致性查询：等待所有在途事务结束，并在提交互斥锁内读取两库，
    保证与“整批发布”串行等价（准备值不可见、半批不可见）。
"""

import argparse
import json
import os
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (  # noqa: E402
    BadRequest,
    Conflict,
    SHARDS,
    canonical_writes,
    post_json,
    get_json,
    validate_transaction,
)

PENDING, COMMITTED, ABORTED = "PENDING", "COMMITTED", "ABORTED"

# 通信故障时的重试节奏（秒）。决定落盘后会一直重试到最终可达。
RETRY_DELAYS = [0.05, 0.1, 0.2, 0.4, 0.8, 1.0, 1.0]


class Coordinator:
    def __init__(self, dbpath, shard_urls):
        self.dbpath = dbpath
        # shard_urls: {"db1": url, "db2": url}
        self.shard_urls = shard_urls
        self.conn = sqlite3.connect(dbpath, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self._init_schema()
        # txid -> 工作线程，用于重复提交时等待同一事务
        self.inflight = {}
        self.inflight_lock = threading.Lock()
        # 提交互斥锁：保证 commit 决定对两库的发布对 /data 呈现为整批
        self.commit_lock = threading.RLock()
        # 故障注入开关
        self.crash_after_first_prepare = False
        self.crash_after_first_commit = False

    def _init_schema(self):
        with self.conn:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS txn ("
                " txid TEXT PRIMARY KEY, writes BLOB NOT NULL,"
                " state TEXT NOT NULL, decided_at TEXT)"
            )

    # ---------------------------------------------------------- 提交入口
    def submit(self, obj):
        txid, norm = validate_transaction(obj)  # 非法输入：不建事务
        blob = canonical_writes(norm)

        with self.inflight_lock:
            row = self.conn.execute(
                "SELECT state, writes FROM txn WHERE txid=?", (txid,)
            ).fetchone()
            if row is not None:
                # 同 txid 异内容：先于一切锁/协议判断直接拒绝
                if row[1] != blob:
                    raise Conflict(
                        "txid %s: 提交内容与首次登记不一致（异内容拒绝）" % txid
                    )
                state = row[0]
                worker = self.inflight.get(txid)
            else:
                # 先持久登记事务，再进行任何通信
                with self.conn:
                    self.conn.execute(
                        "INSERT INTO txn(txid,writes,state) VALUES(?,?,?)",
                        (txid, blob, PENDING),
                    )
                t = threading.Thread(
                    target=self._worker, args=(txid, norm), daemon=True
                )
                self.inflight[txid] = t
                t.start()
                state, worker = PENDING, t

        if worker is not None and worker.is_alive():
            worker.join()  # 同 txid 同 JSON 重提：等待并返回原状态
        final = self.conn.execute(
            "SELECT state FROM txn WHERE txid=?", (txid,)
        ).fetchone()[0]
        return final

    # ---------------------------------------------------------- 2PC 工作流
    def _worker(self, txid, norm):
        try:
            votes = {}
            for shard in SHARDS:
                votes[shard] = self._prepare_until_answer(txid, norm, shard)
                # 故障注入点 1：单库 prepare 完成后强退协调者（决定尚未落盘）
                if self.crash_after_first_prepare:
                    self.crash_after_first_prepare = False
                    sys.stderr.write(
                        "[fault] crash after first prepare (txid=%s)\n" % txid
                    )
                    os._exit(77)

            if all(v == "PREPARED" for v in votes.values()):
                decision = COMMITTED
            else:
                decision = ABORTED
            self._persist_decision(txid, decision)

            if decision == COMMITTED:
                self._commit_both(txid)
            else:
                self._abort_both(txid)
        finally:
            with self.inflight_lock:
                self.inflight.pop(txid, None)

    def _prepare_until_answer(self, txid, norm, shard):
        """向一个分库反复 prepare，直到拿到明确投票；重复投递天然幂等。"""
        body = {"txid": txid, "writes": norm}
        i = 0
        while True:
            try:
                code, data = post_json(self.shard_urls[shard], "/prepare", body)
            except OSError:
                code, data = None, None
            if isinstance(data, dict) and data.get("vote") in (
                "PREPARED",
                "ABORTED",
                "COMMITTED",
            ):
                return data["vote"]
            # 409 且无 vote（如同 txid 异内容）是分库的永久性拒绝，
            # 不可能靠重试恢复，按反对票处理以推动协调者作出 ABORT 决定。
            if code == 409:
                return "ABORTED"
            # 断连/超时/5xx 等：恢复后最终可达，继续重试
            time.sleep(RETRY_DELAYS[min(i, len(RETRY_DELAYS) - 1)])
            i += 1

    def _persist_decision(self, txid, decision):
        with self.conn:
            self.conn.execute(
                "UPDATE txn SET state=?, decided_at=datetime('now') "
                "WHERE txid=? AND state=?",
                (decision, txid, PENDING),
            )
        # 落盘后永不改变：后续只重发通知，不重新决策

    def _commit_both(self, txid):
        with self.commit_lock:  # 整批发布对 /data 串行等价
            for shard in SHARDS:
                self._decision_call(txid, shard, "/commit")
                # 故障注入点 2：commit 已落盘、仅一库应用后强退
                if self.crash_after_first_commit:
                    self.crash_after_first_commit = False
                    sys.stderr.write(
                        "[fault] crash after commit applied on first shard "
                        "(txid=%s)\n" % txid
                    )
                    os._exit(78)

    def _abort_both(self, txid):
        for shard in SHARDS:
            self._decision_call(txid, shard, "/abort")

    def _decision_call(self, txid, shard, path):
        """决定通知：无限重试到双方确认；分库侧幂等，重复投递不重复应用。"""
        body = {"txid": txid}
        i = 0
        while True:
            try:
                code, data = post_json(self.shard_urls[shard], path, body)
            except OSError:
                data = None
            # 200 明确确认；409 表示对侧已是同一终态（幂等重投/恢复竞态），
            # 均视为确认。终态反转类 409 理论不会出现，出现则继续安全重试。
            if code == 200 and isinstance(data, dict) and data.get("state"):
                return
            if code == 409:
                return
            time.sleep(RETRY_DELAYS[min(i, len(RETRY_DELAYS) - 1)])
            i += 1

    # ---------------------------------------------------------- 重启恢复
    def recover(self):
        """启动时同步恢复：无决定事务撤销；已有 commit 继续完成。"""
        rows = self.conn.execute("SELECT txid, writes, state FROM txn").fetchall()
        for txid, blob, state in rows:
            norm = json.loads(blob.decode("utf-8"))
            # 恢复期间重新登记 inflight，供 /status 观测
            t = threading.Thread(
                target=self._recover_one, args=(txid, norm, state), daemon=True
            )
            with self.inflight_lock:
                self.inflight[txid] = t
            t.start()
        # 全部恢复完成后才对外服务，保证 /data 不读到恢复中间态
        for t in list(self.inflight.values()):
            t.join()

    def _recover_one(self, txid, norm, state):
        try:
            if state == PENDING:
                # 无决定的事务撤销（即使某分库已 prepare，abort 亦安全）
                self._persist_decision(txid, ABORTED)
                self._abort_both(txid)
            elif state == COMMITTED:
                # 决定不可推翻：继续把 commit 通知补发完成
                self._commit_both(txid)
            else:  # ABORTED：补发 abort 直到双方确认
                self._abort_both(txid)
        finally:
            with self.inflight_lock:
                self.inflight.pop(txid, None)

    # ---------------------------------------------------------- 查询
    def status(self, txid):
        row = self.conn.execute(
            "SELECT state FROM txn WHERE txid=?", (txid,)
        ).fetchone()
        if row is None:
            return None
        return row[0]

    def consistent_data(self):
        """双库一致查询。

        1) 等待所有在途事务到达终态；
        2) 在提交互斥锁内顺序读取两库 —— 不可能读到“一库已提交一库未提交”。
        """
        with self.inflight_lock:
            waiters = list(self.inflight.values())
        for t in waiters:
            t.join()
        with self.commit_lock:
            out = {}
            for shard in SHARDS:
                code, data = get_json(self.shard_urls[shard], "/data")
                if code != 200 or not isinstance(data, dict):
                    raise RuntimeError("分库 %s 数据查询失败: %r" % (shard, data))
                out[shard] = data["data"]
            return out


def make_handler(coord):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            sys.stderr.write("[coord] %s\n" % (fmt % args))

        def _send(self, code, obj):
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self):
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n) if n else b""
            if not raw:
                raise BadRequest("body: 缺失，需要 JSON 对象")
            try:
                return json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                raise BadRequest("body: 不是合法 JSON")

        def do_GET(self):
            try:
                if self.path == "/health":
                    self._send(200, {"coordinator": True, "ok": True})
                elif self.path == "/data":
                    self._send(200, {"data": coord.consistent_data()})
                elif self.path.startswith("/txn/") and self.path.endswith("/status"):
                    txid = self.path[len("/txn/"):-len("/status")]
                    st = coord.status(txid)
                    if st is None:
                        self._send(404, {"error": "未知 txid", "txid": txid})
                    else:
                        self._send(200, {"txid": txid, "state": st})
                else:
                    self._send(404, {"error": "未知路径", "path": self.path})
            except Exception as e:
                self._send(500, {"error": str(e)})

        def do_POST(self):
            try:
                obj = self._read_json()
                if self.path == "/submit":
                    state = coord.submit(obj)
                    self._send(200, {
                        "txid": validate_transaction(obj)[0],
                        "state": state,
                        "result": "committed" if state == COMMITTED else "aborted",
                    })
                elif self.path == "/control/crash-after-first-prepare":
                    coord.crash_after_first_prepare = True
                    self._send(200, {"ok": True, "arm": "crash_after_first_prepare"})
                elif self.path == "/control/crash-after-first-commit":
                    coord.crash_after_first_commit = True
                    self._send(200, {"ok": True, "arm": "crash_after_first_commit"})
                else:
                    self._send(404, {"error": "未知路径", "path": self.path})
            except BadRequest as e:
                self._send(400, {"error": e.msg})
            except Conflict as e:
                self._send(409, {"error": e.msg})
            except Exception as e:
                self._send(500, {"error": str(e)})

    return Handler


def main():
    ap = argparse.ArgumentParser(description="2PC 协调者进程")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--db1-url", required=True)
    ap.add_argument("--db2-url", required=True)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    coord = Coordinator(args.db, {"db1": args.db1_url, "db2": args.db2_url})

    print("coordinator recovering...", flush=True)
    coord.recover()
    print("coordinator recovery done", flush=True)

    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(coord))
    print("coordinator listening on %d, db=%s" % (args.port, args.db), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
