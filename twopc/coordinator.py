"""两阶段提交 —— 协调者进程。

职责与规则：
  * /submit 收到 JSON 后先在本地 SQLite 持久登记事务，再发起任何网络通信；
  * 多事务可交错 prepare；分库反对/锁冲突 -> 持久决定 ABORT；全赞成 -> COMMIT；
  * 决定落盘后永不改变，通知双方可无限重试（断连、丢响应、重复投递均安全）；
  * 仅双方确认 commit 后该事务才返回 committed；abort 返回 aborted；
  * 重启恢复：无决定的事务撤销，已有 commit 继续完成，绝不以超时推翻决定；
  * /data 一致性查询：等待所有在途事务结束，并在提交互斥锁内读取两库，
    保证与“整批发布”串行等价（准备值不可见、半批不可见）。

fenced（代）模式（--fenced 开启，未开启时与旧行为完全兼容）：
  * 多个协调进程共享原协调库，其中 epoch_ledger 是跨进程代账：
    BEGIN IMMEDIATE 串行化原子领号，代从 1 递增、只增不复用；
  * 启动序列：领号 -> 先向两个分库安装本代（安装不清准备/锁/终态，
    同代幂等、低代拒绝）-> 再恢复旧事务（有决定继续通知、无决定撤销）
    -> 全部恢复完成才开 HTTP 接受提交与一致查询；
  * 一切分库协议请求携带本代；分库先判类型再判代号，代号不符回 409
    stale_epoch 且不改状态。stale_epoch 不是反对票，绝不促成新决定：
    收到即自我 fence-out，停止一切写动作，新客户端请求一律 409；
  * 落决定前再核对代账最大值，只有当前代能登记事务或持久决定；
  * 接管沿用原 txid 及内容；旧代的任何响应/错误都不能促成新决定。
"""

import argparse
import contextlib
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
    post_json,
    get_json,
    validate_epoch,
    validate_transaction,
)

PENDING, COMMITTED, ABORTED = "PENDING", "COMMITTED", "ABORTED"

# 通信故障时的重试节奏（秒）。决定落盘后会一直重试到最终可达。
RETRY_DELAYS = [0.05, 0.1, 0.2, 0.4, 0.8, 1.0, 1.0]


class FencedOut(Exception):
    """本协调进程已被更新代挤掉：停止一切写动作，不再产生任何决定。"""


class Coordinator:
    def __init__(self, dbpath, shard_urls, fenced=False,
                 crash_after_first_install=False, epoch_file=None):
        self.dbpath = dbpath
        # shard_urls: {"db1": url, "db2": url}
        self.shard_urls = shard_urls
        self.fenced = fenced
        self.crash_after_first_install = crash_after_first_install
        # 本进程代身份文件：每个协调进程必须各自独立（默认按 dbpath 派生）。
        # 多进程共享同一协调库时由调用方经 --epoch-file 显式区分，
        # 否则并发首启会因读到彼此的身份文件而错误短路领号。
        self.epoch_file = epoch_file or (dbpath + ".epoch")
        self.conn = sqlite3.connect(dbpath, check_same_thread=False, timeout=30,
                                    isolation_level=None)  # 永久自动提交，事务全显式
        # busy_timeout 先行：写事务遇锁先等待而非立刻 OperationalError
        self.conn.execute("PRAGMA busy_timeout=10000")
        # 采用 DELETE 回滚日志（而非 WAL）：在网络/映射文件系统（如容器
        # overlayfs）上 WAL 的共享内存在跨进程并发首启时不可靠；DELETE 模式
        # 靠文件锁串行化写者，对本实验的低负载场景跨进程语义最稳。
        self._set_journal_mode_with_retry("DELETE")
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
        # fenced 状态
        self.epoch = None           # 本领到的代；非 fenced 恒为 None
        self.fenced_out = False     # 被更新代挤掉后置 True，停止一切写动作
        self.fence_lock = threading.RLock()

    def _set_journal_mode_with_retry(self, mode):
        i = 0
        while True:
            try:
                self.conn.execute("PRAGMA journal_mode=%s" % mode)
                return
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() or "busy" in str(e).lower():
                    time.sleep(RETRY_DELAYS[min(i, len(RETRY_DELAYS) - 1)])
                    i += 1
                    continue
                raise

    def _init_schema(self):
        with self.conn:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS txn ("
                " txid TEXT PRIMARY KEY, writes BLOB NOT NULL,"
                " state TEXT NOT NULL, decided_at TEXT)"
            )
            # 跨进程代账：所有协调进程共享本库，代只增不复用。
            # 这是代的唯一权威；进程本地代身份另存独立文件，不放进共享库，
            # 避免共享单行元数据在并发领号时的唯一约束竞争。
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS epoch_ledger ("
                " epoch INTEGER PRIMARY KEY,"
                " claimed_at TEXT NOT NULL DEFAULT (datetime('now')))"
            )

    # ---------------------------------------------------------- 代管理
    def _read_local_epoch(self):
        """读取本进程本地代身份；不存在或损坏返回 None。"""
        try:
            with open(self.epoch_file, "r", encoding="utf-8") as f:
                value = int(f.read().strip())
            return value if value > 0 else None
        except (OSError, ValueError):
            return None

    def _write_local_epoch(self, epoch):
        tmp = self.epoch_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(str(epoch))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.epoch_file)

    def claim_epoch(self):
        """fenced 启动：从共享代账原子领取一个从 1 递增、不复用的代。

        * 本地代身份文件存在（同进程普通重启）：沿用该号，不向代账插新号；
        * 全新进程接管：在 BEGIN IMMEDIATE 写事务内对 epoch_ledger 做
          原子 max+1 插入（多进程下严格串行、不重号），再持久本地身份。
        """
        local = self._read_local_epoch()
        if local is not None:
            self.epoch = local
            return local

        epoch = self._insert_ledger_epoch()
        self._write_local_epoch(epoch)
        self.epoch = epoch
        return epoch

    def _insert_ledger_epoch(self):
        """在 BEGIN IMMEDIATE 写事务内对代账做原子 max+1 插入。

        连接处于永久自动提交模式，事务边界完全显式，杜绝 Python sqlite3
        隐式事务残留对手动 BEGIN 的污染。
        """
        i = 0
        while True:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                try:
                    mx = self.conn.execute(
                        "SELECT COALESCE(MAX(epoch),0) FROM epoch_ledger"
                    ).fetchone()[0]
                    epoch = mx + 1
                    self.conn.execute(
                        "INSERT INTO epoch_ledger(epoch) VALUES(?)", (epoch,)
                    )
                    self.conn.execute("COMMIT")
                except Exception:
                    with contextlib.suppress(sqlite3.Error):
                        self.conn.execute("ROLLBACK")
                    raise
                return epoch
            except sqlite3.OperationalError as e:
                # locked/busy：另一进程正在领号，退避后重试，绝不复用
                msg = str(e).lower()
                if "locked" in msg or "busy" in msg:
                    time.sleep(RETRY_DELAYS[min(i, len(RETRY_DELAYS) - 1)])
                    i += 1
                    continue
                raise

    def ledger_max_epoch(self):
        row = self.conn.execute(
            "SELECT COALESCE(MAX(epoch),0) FROM epoch_ledger"
        ).fetchone()
        return row[0]

    def _mark_fenced_out(self, reason):
        """收到 stale_epoch 等证据，确认本代已失效：自我隔离，不再写。"""
        with self.fence_lock:
            if not self.fenced_out:
                self.fenced_out = True
                sys.stderr.write(
                    "[fence] 代 %s 已失效，自我 fence-out：%s\n"
                    % (self.epoch, reason)
                )

    def _ensure_active(self):
        """任何写动作/客户端入口前调用：已被挤掉则抛 FencedOut。"""
        if self.fenced and self.fenced_out:
            raise FencedOut("本协调进程所属代 %s 已失效" % self.epoch)

    def _check_leadership(self):
        """落决定前核对代账：只有当前代（最大值保持者）能持久决定。"""
        if not self.fenced:
            return
        if self.ledger_max_epoch() != self.epoch:
            self._mark_fenced_out("代账最大值已不是本代")
            raise FencedOut("代 %s 不再是当前代" % self.epoch)

    def install_on_shards(self):
        """先向两个分库安装本代；同代幂等、低代由分库拒绝。

        安装不清除任何准备、锁或终态。任一分库持续不可达则重试等待，
        全部安装成功前不恢复事务、不开放服务。
        """
        for shard in SHARDS:
            self._install_one(shard)
            if self.crash_after_first_install:
                self.crash_after_first_install = False
                sys.stderr.write(
                    "[fault] crash after epoch installed on first shard "
                    "(epoch=%s)\n" % self.epoch
                )
                os._exit(79)

    def _install_one(self, shard):
        body = {"epoch": self.epoch}
        i = 0
        while True:
            self._ensure_active()
            try:
                code, data = post_json(
                    self.shard_urls[shard], "/install", body, timeout=5.0
                )
            except OSError:
                code, data = None, None
            if code == 200 and isinstance(data, dict) and data.get("epoch") == self.epoch:
                return
            if code == 409 and isinstance(data, dict) \
                    and data.get("reason") == "stale_epoch":
                # 分库已装更高代 -> 本代不可能成为当前代
                self._mark_fenced_out(
                    "分库 %s 已装代 %r" % (shard, data.get("installed_epoch"))
                )
                raise FencedOut("分库 %s 拒绝本代安装" % shard)
            time.sleep(RETRY_DELAYS[min(i, len(RETRY_DELAYS) - 1)])
            i += 1

    # ---------------------------------------------------------- 提交入口
    def submit(self, obj, epoch=None):
        self._ensure_active()
        txid, norm = validate_transaction(obj)  # 非法输入：不建事务
        blob = canonical_writes(norm)

        with self.inflight_lock:
            worker = None
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
                if worker is None and state == PENDING:
                    # 接管场景：旧代留下的无决定事务，本代按恢复语义撤销，
                    # 沿用原 txid 与内容（不新建事务、不改写内容）
                    self._check_leadership()
                    worker = threading.Thread(
                        target=self._recover_one,
                        args=(txid, norm, PENDING), daemon=True,
                    )
                    self.inflight[txid] = worker
                    worker.start()
            else:
                # 只有当前代能登记新事务
                self._check_leadership()
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
        self._ensure_active()  # 等待期间可能已被更新代挤掉
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
        except FencedOut:
            # 本代已失效：停下，不登记任何新决定（旧代错误不促成新决定）
            sys.stderr.write("[fence] worker(txid=%s) 因代失效中止\n" % txid)
        finally:
            with self.inflight_lock:
                self.inflight.pop(txid, None)

    def _prepare_until_answer(self, txid, norm, shard):
        """向一个分库反复 prepare，直到拿到明确投票；重复投递天然幂等。"""
        body = {"txid": txid, "writes": norm}
        i = 0
        while True:
            self._ensure_active()
            try:
                code, data = post_json(
                    self.shard_urls[shard], "/prepare", body,
                    epoch=self.epoch,
                )
            except OSError:
                code, data = None, None
            # stale_epoch：不是反对票，不参与决定；本代立即失效并中止
            if code == 409 and isinstance(data, dict) \
                    and data.get("reason") == "stale_epoch":
                self._mark_fenced_out(
                    "prepare 被分库 %s 判 stale（%r）"
                    % (shard, data.get("installed_epoch"))
                )
                raise FencedOut("prepare 时代不符")
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
        # 只有当前代能持久决定：再核对一次共享代账
        self._check_leadership()
        with self.conn:
            cur = self.conn.execute(
                "UPDATE txn SET state=?, decided_at=datetime('now') "
                "WHERE txid=? AND state=?",
                (decision, txid, PENDING),
            )
            if cur.rowcount == 0:
                # 已有决定（可能由接管方落盘）：绝不覆盖
                row = self.conn.execute(
                    "SELECT state FROM txn WHERE txid=?", (txid,)
                ).fetchone()
                raise Conflict(
                    "txid %s: 决定已存在为 %s，不得改为 %s"
                    % (txid, row[0] if row else "?", decision)
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
            self._ensure_active()
            try:
                code, data = post_json(
                    self.shard_urls[shard], path, body, epoch=self.epoch
                )
            except OSError:
                code, data = None, None
            # stale_epoch：本代已失效，中止且不把它当作任何一种确认/反对
            if code == 409 and isinstance(data, dict) \
                    and data.get("reason") == "stale_epoch":
                self._mark_fenced_out(
                    "%s 被分库 %s 判 stale（%r）"
                    % (path, shard, data.get("installed_epoch"))
                )
                raise FencedOut("决定通知时代不符")
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
        """启动时同步恢复：无决定事务撤销；已有 commit 继续完成。

        fenced 模式下必须在两库安装本代之后调用；恢复全部完成才对外服务。
        """
        rows = self.conn.execute("SELECT txid, writes, state FROM txn").fetchall()
        for txid, blob, state in rows:
            if isinstance(blob, str):  # 兼容被外部以 TEXT 写入的记录
                blob = blob.encode("utf-8")
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
        except FencedOut:
            sys.stderr.write(
                "[fence] recover(txid=%s) 因代失效中止，等待更新代处理\n" % txid
            )
        except Conflict:
            # 决定已被接管方落盘：本代不再插手该事务
            sys.stderr.write("[fence] recover(txid=%s) 决定已存在，退出\n" % txid)
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

    def consistent_data(self, epoch=None):
        """双库一致查询。

        1) fenced 下先校验代并确认本代仍有效；
        2) 等待所有在途事务到达终态；
        3) 在提交互斥锁内顺序读取两库（读请求同样带代，旧代读不到半批），
           不可能读到“一库已提交一库未提交”，与接管及整批发布线性一致。
        """
        self._ensure_active()
        with self.inflight_lock:
            waiters = list(self.inflight.values())
        for t in waiters:
            t.join()
        self._ensure_active()
        with self.commit_lock:
            out = {}
            for shard in SHARDS:
                code, data = get_json(
                    self.shard_urls[shard], "/data", epoch=epoch
                )
                if code == 409 and isinstance(data, dict) \
                        and data.get("reason") == "stale_epoch":
                    self._mark_fenced_out(
                        "查询被分库 %s 判 stale" % shard
                    )
                    raise FencedOut("一致查询时代不符")
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

        def _query_epoch(self):
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
            if not isinstance(obj, dict) or "epoch" not in obj:
                return None
            return validate_epoch(obj["epoch"])

        def _request_epoch(self, obj):
            q = self._query_epoch()
            if q is not None:
                return q
            return self._body_epoch(obj)

        def _guard_epoch(self, epoch):
            """fenced 模式客户端入口守卫：类型先于代号，不符 409 且不改状态。

            代号匹配后再核对共享代账最大值：协调进程虽仍存活，一旦代账出现
            更新代即视为旧实例失效，新收到的客户端请求一律 409。
            """
            if not coord.fenced:
                return
            if epoch is None or epoch != coord.epoch:
                raise StaleEpoch(
                    "stale_epoch: 请求代 %r 与当前协调代 %r 不符"
                    % (epoch, coord.epoch),
                    installed=coord.epoch, request_epoch=epoch,
                )
            if coord.ledger_max_epoch() != coord.epoch:
                coord._mark_fenced_out("代账已出现更新代")
                raise FencedOut("代 %s 已不是当前代" % coord.epoch)

        def do_GET(self):
            try:
                parsed = urllib.parse.urlparse(self.path)
                path = parsed.path
                if path == "/health":
                    self._send(200, {
                        "coordinator": True, "ok": True,
                        "fenced": coord.fenced, "epoch": coord.epoch,
                        "fenced_out": coord.fenced_out,
                    })
                elif path == "/debug/epoch":
                    # 观测接口不设代号守卫，便于测试确认代际状态
                    self._send(200, {
                        "fenced": coord.fenced, "epoch": coord.epoch,
                        "ledger_max": coord.ledger_max_epoch(),
                        "fenced_out": coord.fenced_out,
                    })
                elif path == "/data":
                    epoch = self._query_epoch()
                    self._guard_epoch(epoch)
                    self._send(200, {"data": coord.consistent_data(epoch=epoch)})
                elif path.startswith("/txn/") and path.endswith("/status"):
                    epoch = self._query_epoch()
                    self._guard_epoch(epoch)
                    txid = path[len("/txn/"):-len("/status")]
                    st = coord.status(txid)
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
            except FencedOut as e:
                self._send(409, {"error": str(e), "reason": "stale_epoch",
                                 "installed_epoch": coord.epoch,
                                 "fenced_out": True})
            except Exception as e:
                self._send(500, {"error": str(e)})

        def do_POST(self):
            try:
                obj = self._read_json()
                path = urllib.parse.urlparse(self.path).path
                if path == "/submit":
                    # 先类型校验、再判代号，最后才做事务内容校验
                    epoch = self._request_epoch(obj)
                    self._guard_epoch(epoch)
                    state = coord.submit(obj, epoch=epoch)
                    self._send(200, {
                        "txid": validate_transaction(obj)[0],
                        "state": state,
                        "result": "committed" if state == COMMITTED else "aborted",
                    })
                elif path == "/control/crash-after-first-prepare":
                    self._guard_epoch(self._request_epoch(obj))
                    coord.crash_after_first_prepare = True
                    self._send(200, {"ok": True, "arm": "crash_after_first_prepare"})
                elif path == "/control/crash-after-first-commit":
                    self._guard_epoch(self._request_epoch(obj))
                    coord.crash_after_first_commit = True
                    self._send(200, {"ok": True, "arm": "crash_after_first_commit"})
                else:
                    self._send(404, {"error": "未知路径", "path": self.path})
            except BadRequest as e:
                self._send(400, {"error": e.msg})
            except StaleEpoch as e:
                self._send(409, {"error": e.msg, "reason": "stale_epoch",
                                 "installed_epoch": e.installed,
                                 "request_epoch": e.request_epoch})
            except FencedOut as e:
                # 旧实例失效后新收到的客户端请求一律 409
                self._send(409, {"error": str(e), "reason": "stale_epoch",
                                 "installed_epoch": coord.epoch,
                                 "fenced_out": True})
            except Conflict as e:
                self._send(409, {"error": e.msg})
            except Exception as e:
                self._send(500, {"error": str(e)})

    return Handler


def main():
    ap = argparse.ArgumentParser(description="2PC 协调者进程")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--db", required=True,
                    help="协调库路径；fenced 模式下多代协调进程共享同一文件")
    ap.add_argument("--db1-url", required=True)
    ap.add_argument("--db2-url", required=True)
    ap.add_argument("--fenced", action="store_true",
                    help="启用 fenced 代模式：领号、安装分库、代校验")
    ap.add_argument("--epoch-file", default=None,
                    help="本进程代身份文件；多个协调进程共享协调库时必须各自不同")
    ap.add_argument("--crash-after-first-install", action="store_true",
                    help="故障注入：fenced 启动时仅首个分库安装成功后强退")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    if args.epoch_file:
        os.makedirs(os.path.dirname(os.path.abspath(args.epoch_file)), exist_ok=True)
    coord = Coordinator(
        args.db,
        {"db1": args.db1_url, "db2": args.db2_url},
        fenced=args.fenced,
        crash_after_first_install=args.crash_after_first_install,
        epoch_file=args.epoch_file,
    )

    if args.fenced:
        # 启动序列：原子领号 -> 先向两分库安装本代 -> 再恢复旧事务，
        # 全部恢复完成才开放 HTTP（提交与一致查询）。
        epoch = coord.claim_epoch()
        print("coordinator fenced: claimed epoch=%d, installing on shards..."
              % epoch, flush=True)
        coord.install_on_shards()
        print("coordinator fenced: epoch %d installed on both shards" % epoch,
              flush=True)

    print("coordinator recovering...", flush=True)
    coord.recover()
    print("coordinator recovery done", flush=True)

    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(coord))
    print("coordinator listening on %d, db=%s, fenced=%s, epoch=%s"
          % (args.port, args.db, args.fenced, coord.epoch), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
