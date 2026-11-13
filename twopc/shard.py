"""两阶段提交 —— 分库（参与者）进程。

每个分库是独立进程、独立 SQLite 文件，仅通过 HTTP 与协调者通信。

持久状态（SQLite，单库内一个线程锁串行化所有写事务）：
  kv(k)            可见数据：仅已 commit 的写集可被 /data 看到
  txn(txid)        事务记录：状态 + 首次写集（PREPARED / COMMITTED / ABORTED）
  locks(txid,k)    PREPARED 事务持有的键锁
  stats            应用次数等幂等观测计数

关键规则：
  * prepare 不改动可见数据，只持久写集并独占其键；锁冲突立即反对，不等待；
  * txid 首次内容固定，异内容一律拒绝，且内容判断先于锁冲突判断；
  * abort 即使先于 prepare 到达也持久记终态 ABORTED，迟到 prepare 不复活；
  * 终态 COMMITTED/ABORTED 永不反转；重复 prepare/commit/abort 幂等；
  * commit 原子应用写集（含 null 写入/替换）并释放锁。

fenced（代）模式（--fenced 开启，未开启时行为与旧协议完全一致）：
  * meta 表持久保存已安装的最大代 max_epoch；/install 同代幂等、低代拒绝、
    高代原子安装；安装绝不触碰准备、锁与终态；
  * prepare/commit/abort 与 /data 必须携带与已安装代一致的 epoch：
    先做类型校验（非法 epoch 为 400），再判代号（不符 409 stale_epoch，
    不改任何状态），最后才沿用内容、锁及终态规则；
  * 未安装任何代的 fenced 分库拒绝一切协议请求（409），等待接管方安装。
"""

import argparse
import json
import os
import sqlite3
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (  # noqa: E402
    BadRequest,
    Conflict,
    SHARDS,
    StaleEpoch,
    canonical_writes,
    validate_epoch,
    validate_transaction,
)

PREPARED, COMMITTED, ABORTED = "PREPARED", "COMMITTED", "ABORTED"

# 撤销先于 prepare 到达时的写集占位标记（规范化写集不可能为空字节串）
EARLY_ABORT_BLOB = b""


class ShardStore:
    def __init__(self, name, dbpath, fenced=False):
        self.name = name
        self.dbpath = dbpath
        self.fenced = fenced
        self.lock = threading.RLock()  # 进程内串行化，保证判断+落盘原子
        self.conn = sqlite3.connect(dbpath, check_same_thread=False, timeout=30,
                                    isolation_level=None)  # 永久自动提交，事务全显式
        self.conn.execute("PRAGMA busy_timeout=10000")
        # DELETE 回滚日志：跨进程/文件系统语义最稳，配合 FULL 同步保证落盘
        self._set_journal_mode_with_retry("DELETE")
        self.conn.execute("PRAGMA synchronous=FULL")
        self._init_schema()
        # 故障注入：下一次对应接口的应答在“落盘之后”丢弃
        self.drop_next = {"prepare": False, "commit": False, "abort": False}
        # 故障注入：下一次对应接口落盘后先延迟再应答（用于制造接管竞态）
        self.delay_next = {"prepare": 0.0, "commit": 0.0, "abort": 0.0}

    def _set_journal_mode_with_retry(self, mode):
        i = 0
        delays = [0.05, 0.1, 0.2, 0.4, 0.8, 1.0, 1.0]
        while True:
            try:
                self.conn.execute("PRAGMA journal_mode=%s" % mode)
                return
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() or "busy" in str(e).lower():
                    time.sleep(delays[min(i, len(delays) - 1)])
                    i += 1
                    continue
                raise

    def _init_schema(self):
        with self.conn:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS kv ("
                " k TEXT PRIMARY KEY, v TEXT)"  # NULL 值以 SQL NULL 存储
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS txn ("
                " txid TEXT PRIMARY KEY, state TEXT NOT NULL,"
                " writes BLOB NOT NULL)"
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS locks ("
                " txid TEXT NOT NULL, k TEXT NOT NULL,"
                " PRIMARY KEY (txid, k))"
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS stats ("
                " name TEXT PRIMARY KEY, val INTEGER NOT NULL DEFAULT 0)"
            )
            # 代（epoch）持久化：单行 meta，已安装的最大代
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS meta ("
                " key TEXT PRIMARY KEY, val INTEGER NOT NULL)"
            )

    # ------------------------------------------------------------ 代管理
    def installed_epoch(self):
        """返回已安装的最大代；从未安装返回 None。"""
        with self.lock:
            row = self.conn.execute(
                "SELECT val FROM meta WHERE key='epoch'"
            ).fetchone()
        return row[0] if row else None

    def install_epoch(self, epoch):
        """安装代：同代幂等、低代拒绝（不改状态）、高代原子提升。

        安装只动 meta，绝不清除准备、锁或终态。返回 (epoch, note)。
        """
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT val FROM meta WHERE key='epoch'"
            ).fetchone()
            current = row[0] if row else None
            if current is not None:
                if epoch == current:
                    return current, "代 %d 已安装，幂等返回" % epoch
                if epoch < current:
                    # 低代拒绝：不更新任何状态
                    raise StaleEpoch(
                        "代 %d 低于已安装代 %d，拒绝安装" % (epoch, current),
                        installed=current, request_epoch=epoch,
                    )
            self.conn.execute(
                "INSERT INTO meta(key,val) VALUES('epoch',?) "
                "ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                (epoch,),
            )
            self._bump("epoch_install")
            return epoch, ("首次安装代 %d" % epoch if current is None
                           else "代由 %d 提升至 %d" % (current, epoch))

    def check_epoch(self, epoch):
        """协议代号守卫。

        调用方须已完成 epoch 类型校验（非法类型在 HTTP 层判 400）。
        未携带代、与已安装代不符一律 StaleEpoch（409）且不改状态。
        非 fenced 模式不启用该守卫。
        """
        if not self.fenced:
            return
        current = self.installed_epoch()
        if epoch is None or current is None or epoch != current:
            raise StaleEpoch(
                "stale_epoch: 请求代 %r 与已安装代 %r 不符" % (epoch, current),
                installed=current, request_epoch=epoch,
            )

    def _bump(self, stat):
        self.conn.execute(
            "INSERT INTO stats(name,val) VALUES(?,1) "
            "ON CONFLICT(name) DO UPDATE SET val=val+1", (stat,)
        )

    # ------------------------------------------------------------ prepare
    def prepare(self, txid, norm, epoch=None):
        """返回 (state, note)。state ∈ PREPARED/ABORTED/COMMITTED。"""
        self.check_epoch(epoch)  # 代号不符：StaleEpoch，不触碰任何状态
        writes_blob = canonical_writes(norm)
        keys = sorted(norm[self.name].keys())
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT state, writes FROM txn WHERE txid=?", (txid,)
            ).fetchone()

            if row is not None:
                state, blob = row
                # 撤销先到：从未登记过写集，任何迟到 prepare 都不得复活，
                # 直接返回终态票（内容比对对该占位行不适用）
                if state == ABORTED and blob == EARLY_ABORT_BLOB:
                    return ABORTED, "撤销先于准备到达，迟到 prepare 不得复活"
                # 内容判断先于锁冲突判断，且对任何已有记录生效
                if blob != writes_blob:
                    raise Conflict(
                        "txid %s: 写集内容与首次登记不一致（异内容拒绝）" % txid
                    )
                if state == PREPARED:
                    return PREPARED, "重复 prepare，幂等返回"
                if state == COMMITTED:
                    return COMMITTED, "事务已提交，重复 prepare 幂等"
                # ABORTED：终态不可逆，迟到 prepare 不得复活
                return ABORTED, "事务已撤销，终态不可反转"

            # 新事务：先检查全部键锁，任一被其他事务持有 -> 立即反对，不等待
            for k in keys:
                holder = self.conn.execute(
                    "SELECT txid FROM locks WHERE k=? LIMIT 1", (k,)
                ).fetchone()
                if holder is not None:
                    # 反对也持久登记终态（记录本次写集），不持锁
                    self.conn.execute(
                        "INSERT INTO txn(txid,state,writes) VALUES(?,?,?)",
                        (txid, ABORTED, writes_blob),
                    )
                    self._bump("prepare_conflict")
                    return ABORTED, "键 %s 被事务 %s 持锁，立即反对" % (k, holder[0])

            # 全赞成路径：持久保存写集并独占其键（准备不改可见数据）
            self.conn.execute(
                "INSERT INTO txn(txid,state,writes) VALUES(?,?,?)",
                (txid, PREPARED, writes_blob),
            )
            for k in keys:
                self.conn.execute(
                    "INSERT INTO locks(txid,k) VALUES(?,?)", (txid, k)
                )
            self._bump("prepare_yes")
            return PREPARED, "已准备"

    # ------------------------------------------------------------ commit
    def commit(self, txid, epoch=None):
        self.check_epoch(epoch)  # 代号不符：StaleEpoch，不触碰任何状态
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT state, writes FROM txn WHERE txid=?", (txid,)
            ).fetchone()
            if row is None:
                raise Conflict("txid %s: 未准备即提交，拒绝" % txid)
            state, blob = row
            if state == COMMITTED:
                return COMMITTED, "重复 commit，幂等（不重复应用）"
            if state == ABORTED:
                raise Conflict("txid %s: 已撤销，终态不可反转为提交" % txid)

            # PREPARED -> 原子应用写集并释放锁
            writes = json.loads(blob.decode("utf-8"))[self.name]
            for k in sorted(writes.keys()):
                # 新增或替换；值为 None 时写 SQL NULL，区别于键不存在
                self.conn.execute(
                    "INSERT INTO kv(k,v) VALUES(?,?) "
                    "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                    (k, writes[k]),
                )
            self.conn.execute("DELETE FROM locks WHERE txid=?", (txid,))
            self.conn.execute(
                "UPDATE txn SET state=? WHERE txid=?", (COMMITTED, txid)
            )
            self._bump("commit_applied")
            return COMMITTED, "写集已原子应用，锁已释放"

    # ------------------------------------------------------------ abort
    def abort(self, txid, epoch=None):
        self.check_epoch(epoch)  # 代号不符：StaleEpoch，不触碰任何状态
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT state FROM txn WHERE txid=?", (txid,)
            ).fetchone()
            if row is None:
                # abort 先于 prepare 到达：持久记终态（占位写集），不持锁
                self.conn.execute(
                    "INSERT INTO txn(txid,state,writes) VALUES(?,?,?)",
                    (txid, ABORTED, EARLY_ABORT_BLOB),
                )
                self._bump("abort_early")
                return ABORTED, "撤销先于准备到达，已持久终态"
            state = row[0]
            if state == ABORTED:
                return ABORTED, "重复 abort，幂等"
            if state == COMMITTED:
                raise Conflict("txid %s: 已提交，终态不可反转为撤销" % txid)
            # PREPARED：丢弃准备并释放锁
            self.conn.execute("DELETE FROM locks WHERE txid=?", (txid,))
            self.conn.execute(
                "UPDATE txn SET state=? WHERE txid=?", (ABORTED, txid)
            )
            self._bump("abort_after_prepare")
            return ABORTED, "准备已丢弃，锁已释放"

    # ------------------------------------------------------------ 只读视图
    def data(self):
        with self.lock:
            rows = self.conn.execute("SELECT k,v FROM kv ORDER BY k").fetchall()
        # 按 key 字节序返回；None 即 JSON null
        rows.sort(key=lambda r: r[0].encode("utf-8"))
        return {k: v for k, v in rows}

    def txn_state(self, txid):
        with self.lock:
            row = self.conn.execute(
                "SELECT state FROM txn WHERE txid=?", (txid,)
            ).fetchone()
        return row[0] if row else None

    def held_locks(self):
        with self.lock:
            rows = self.conn.execute(
                "SELECT txid,k FROM locks ORDER BY txid,k"
            ).fetchall()
        return [{"txid": t, "key": k} for t, k in rows]

    def stats_view(self):
        with self.lock:
            rows = self.conn.execute("SELECT name,val FROM stats").fetchall()
        return dict(rows)


def make_handler(store):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):  # 安静日志
            sys.stderr.write("[%s] %s\n" % (store.name, fmt % args))

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

        def _query_epoch(self):
            """从 query 提取 epoch；缺失返回 None；非法（含布尔）抛 BadRequest。

            注意 bool 是 int 子类，必须显式拒绝；代只允许正整数。
            """
            qs = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(qs)
            if "epoch" not in params:
                return None
            raw = params["epoch"][-1]
            try:
                value = int(raw)
            except ValueError:
                raise BadRequest(
                    "epoch: 必须是正整数（禁止布尔/浮点/字符串，实际: %r）" % raw
                )
            return validate_epoch(value)

        def _body_epoch(self, obj):
            """从 JSON body 提取 epoch；缺失返回 None；非法抛 BadRequest。"""
            if not isinstance(obj, dict) or "epoch" not in obj:
                return None
            return validate_epoch(obj["epoch"])

        def _request_epoch(self, obj):
            """协议请求的代：query 优先，其次 body；二者均缺失为 None。"""
            q = self._query_epoch()
            if q is not None:
                return q
            return self._body_epoch(obj)

        def _maybe_drop(self, phase):
            """落盘成功后模拟响应丢失：丢弃应答并关闭连接。"""
            if store.drop_next.get(phase):
                store.drop_next[phase] = False
                self.close_connection = True
                try:
                    self.wfile.close()
                except OSError:
                    pass
                return True
            return False

        def _maybe_delay(self, phase):
            """落盘成功后延迟应答，制造接管与旧代重试的竞态窗口。"""
            delay = store.delay_next.get(phase) or 0.0
            if delay > 0:
                store.delay_next[phase] = 0.0
                time.sleep(delay)

        def do_GET(self):
            try:
                path = urllib.parse.urlparse(self.path).path
                if path == "/health":
                    self._send(200, {"shard": store.name, "ok": True,
                                     "fenced": store.fenced})
                elif path == "/debug/epoch":
                    # 观测接口本身不参与协议，不设代号守卫
                    self._send(200, {"shard": store.name,
                                     "fenced": store.fenced,
                                     "installed_epoch": store.installed_epoch()})
                elif path == "/data":
                    # 一致查询也要与接管线性一致：fenced 下必须带当前代
                    epoch = self._query_epoch()
                    store.check_epoch(epoch)
                    self._send(200, {"shard": store.name, "data": store.data()})
                elif path == "/debug/locks":
                    self._send(200, {"shard": store.name,
                                     "locks": store.held_locks()})
                elif path == "/debug/stats":
                    self._send(200, {"shard": store.name,
                                     "stats": store.stats_view()})
                elif path.startswith("/txn/") and path.endswith("/status"):
                    txid = path[len("/txn/"):-len("/status")]
                    st = store.txn_state(txid)
                    if st is None:
                        self._send(404, {"error": "未知 txid", "txid": txid})
                    else:
                        self._send(200, {"txid": txid, "state": st})
                else:
                    self._send(404, {"error": "未知路径", "path": self.path})
            except BadRequest as e:
                self._send(400, {"error": e.msg})
            except StaleEpoch as e:
                self._send(409, {"error": e.msg, "reason": "stale_epoch",
                                 "installed_epoch": e.installed,
                                 "request_epoch": e.request_epoch})
            except Exception as e:  # 只读接口兜底
                self._send(500, {"error": str(e)})

        def do_POST(self):
            try:
                obj = self._read_json()
                path = urllib.parse.urlparse(self.path).path
                if path == "/install":
                    # 安装请求自身携带的代：先类型校验，再由 store 判高低
                    epoch = self._body_epoch(obj)
                    installed, note = store.install_epoch(epoch)
                    self._send(200, {"ok": True, "epoch": installed, "note": note})
                elif path == "/prepare":
                    txid, norm = validate_transaction(obj)
                    # 校验顺序：类型 -> 代号 -> 内容/锁/终态
                    epoch = self._request_epoch(obj)
                    state, note = store.prepare(txid, norm, epoch=epoch)
                    if self._maybe_drop("prepare"):
                        return
                    self._maybe_delay("prepare")
                    code = 200 if state == PREPARED else 409
                    self._send(code, {"txid": txid, "vote": state, "note": note})
                elif path == "/commit":
                    self._require_txid(obj)
                    epoch = self._request_epoch(obj)
                    state, note = store.commit(obj["txid"], epoch=epoch)
                    if self._maybe_drop("commit"):
                        return
                    self._maybe_delay("commit")
                    self._send(200, {"txid": obj["txid"], "state": state,
                                     "note": note})
                elif path == "/abort":
                    self._require_txid(obj)
                    epoch = self._request_epoch(obj)
                    state, note = store.abort(obj["txid"], epoch=epoch)
                    if self._maybe_drop("abort"):
                        return
                    self._maybe_delay("abort")
                    self._send(200, {"txid": obj["txid"], "state": state,
                                     "note": note})
                elif path == "/control/drop-next":
                    phase = obj.get("phase")
                    if phase not in store.drop_next:
                        raise BadRequest("phase: 必须是 prepare/commit/abort 之一")
                    store.drop_next[phase] = True
                    self._send(200, {"ok": True, "drop_next": store.drop_next})
                elif path == "/control/delay-next":
                    phase = obj.get("phase")
                    if phase not in store.delay_next:
                        raise BadRequest("phase: 必须是 prepare/commit/abort 之一")
                    delay = obj.get("seconds", 0)
                    if isinstance(delay, bool) or not isinstance(delay, (int, float)) \
                            or delay < 0 or delay > 30:
                        raise BadRequest(
                            "seconds: 必须是 0~30 的非负数值（实际: %r）" % delay
                        )
                    store.delay_next[phase] = float(delay)
                    self._send(200, {"ok": True, "delay_next": store.delay_next})
                else:
                    self._send(404, {"error": "未知路径", "path": self.path})
            except BadRequest as e:
                self._send(400, {"error": e.msg})
            except StaleEpoch as e:
                # stale_epoch：不改任何状态，也不是反对票
                self._send(409, {"error": e.msg, "reason": "stale_epoch",
                                 "installed_epoch": e.installed,
                                 "request_epoch": e.request_epoch})
            except Conflict as e:
                self._send(409, {"error": e.msg})
            except Exception as e:
                self._send(500, {"error": str(e)})

        @staticmethod
        def _require_txid(obj):
            if not isinstance(obj, dict) or "txid" not in obj:
                raise BadRequest("txid: 字段缺失")
            if not isinstance(obj["txid"], str) or not obj["txid"]:
                raise BadRequest("txid: 必须是非空字符串")

    return Handler


def main():
    ap = argparse.ArgumentParser(description="2PC 分库进程")
    ap.add_argument("--name", required=True, choices=SHARDS)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--db", required=True, help="SQLite 文件路径")
    ap.add_argument("--fenced", action="store_true",
                    help="启用 fenced 代模式：协议请求须携带已安装代")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    store = ShardStore(args.name, args.db, fenced=args.fenced)
    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(store))
    print("shard %s listening on %d, db=%s, fenced=%s"
          % (args.name, args.port, args.db, args.fenced), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
