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

整批续采（changefeed）模式（--changefeed 开启，未开启时与旧行为完全兼容）：
  * changelog 仅记录 COMMITTED 事务：COMMIT 决定落盘的同一 SQLite 写事务内
    原子分配从 1 连续的 seq，撤销不占号；空写集、同值覆盖批次也原样保留；
  * 提交在 commit_lock 临界区内“决定+seq → 两库按 seq 顺序应用 → 推进持久
    发布水位”，水位在双方确认后、成功回执前推进，单调只增；
  * GET /snapshot 返回与水位前缀严格对应的双库状态（可等待发布完成）；
    POST /changes {"after":N} 返回 after 之后至当前水位的全部已发布批次，
    按 seq 排序、无批次为 []；after 须非负整数（禁布尔），超水位 409、
    非法 400 并定位 after；fenced 下先代号守卫再验 after；
  * 半批或“双库已应用但水位未落盘”强退，恢复沿用原 seq 幂等补齐并推进
    水位，不漏批、不重复；fenced 接管保留水位，旧代响应不得推进它。
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
    validate_nonneg_integer,
    validate_transaction,
)

PENDING, COMMITTED, ABORTED = "PENDING", "COMMITTED", "ABORTED"

# 通信故障时的重试节奏（秒）。决定落盘后会一直重试到最终可达。
RETRY_DELAYS = [0.05, 0.1, 0.2, 0.4, 0.8, 1.0, 1.0]


class FencedOut(Exception):
    """本协调进程已被更新代挤掉：停止一切写动作，不再产生任何决定。"""


class BeyondCursor(Exception):
    """续采 after 超过当前持久发布水位（409），请求不改变任何状态。"""

    def __init__(self, msg, after=None, cursor=None):
        super().__init__(msg)
        self.msg = msg
        self.after = after
        self.cursor = cursor


class Coordinator:
    def __init__(self, dbpath, shard_urls, fenced=False,
                 crash_after_first_install=False, epoch_file=None,
                 changefeed=False):
        self.dbpath = dbpath
        # shard_urls: {"db1": url, "db2": url}
        self.shard_urls = shard_urls
        self.fenced = fenced
        # 整批续采（changefeed）模式：发布水位 + 连续 seq 的变更日志
        self.changefeed = changefeed
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
        # 数据库写锁：所有线程共享同一 SQLite 连接，多语句显式事务
        # （BEGIN IMMEDIATE ... COMMIT）必须串行，否则一个线程的 COMMIT
        # 会提前提交另一线程仍在进行中的事务，造成决定/序号错乱。
        self.db_write_lock = threading.RLock()
        # 故障注入开关
        self.crash_after_first_prepare = False
        self.crash_after_first_commit = False
        # 故障注入：双库均已应用 commit、但发布水位尚未落盘时强退
        self.crash_after_both_commit = False
        # 续采发布水位的内存缓存；None 表示尚未读取（或 changefeed 未启用）
        self._cursor_cache = None
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
            # 整批续采：仅 COMMITTED 事务进入变更日志。
            # seq 从 1 连续、只增不复用；ABORT 不占号（无对应行）。
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS changelog ("
                " seq INTEGER PRIMARY KEY,"
                " txid TEXT NOT NULL UNIQUE,"
                " writes BLOB NOT NULL)"
            )
            # 整批续采：持久发布水位。仅在两库均确认应用后推进；
            # 水位 N 表示 seq<=N 的批次均已在两库应用并对外发布。
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS cf_meta ("
                " key TEXT PRIMARY KEY, val INTEGER NOT NULL)"
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
                with self.db_write_lock:
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

    # ---------------------------------------------------------- 整批续采
    def publish_cursor(self):
        """持久发布水位（未启用 changefeed 或空态为 0）。

        水位 N 表示 seq<=N 的提交批次都已在两库应用并完成发布；
        seq 大于水位的变更日志行可能已分配序号但尚未发布（半批/在途）。
        """
        if not self.changefeed:
            return 0
        if self._cursor_cache is not None:
            return self._cursor_cache
        row = self.conn.execute(
            "SELECT val FROM cf_meta WHERE key='publish_cursor'"
        ).fetchone()
        value = row[0] if row else 0
        self._cursor_cache = value
        return value

    def _max_changelog_seq(self):
        row = self.conn.execute(
            "SELECT COALESCE(MAX(seq),0) FROM changelog"
        ).fetchone()
        return row[0]

    def _persist_commit_decision(self, txid, blob):
        """落 COMMIT 决定并在同一事务内原子分配连续 seq。

        在 BEGIN IMMEDIATE 写事务内完成 txn 决定更新与 changelog 插入：
        seq = max(seq)+1 从 1 连续，只增不复用；撤销事务不写 changelog、
        不占号。并发提交由 SQLite 文件锁串行化，绝不重号/跳号。
        """
        i = 0
        while True:
            try:
                with self.db_write_lock:
                    self.conn.execute("BEGIN IMMEDIATE")
                    try:
                        cur = self.conn.execute(
                            "UPDATE txn SET state=?, decided_at=datetime('now') "
                            "WHERE txid=? AND state=?",
                            (COMMITTED, txid, PENDING),
                        )
                        if cur.rowcount == 0:
                            # 已有决定（可能由接管方落盘）：绝不覆盖
                            self.conn.execute("ROLLBACK")
                            row = self.conn.execute(
                                "SELECT state FROM txn WHERE txid=?", (txid,)
                            ).fetchone()
                            raise Conflict(
                                "txid %s: 决定已存在为 %s，不得改为 %s"
                                % (txid, row[0] if row else "?", COMMITTED)
                            )
                        seq = self._max_changelog_seq() + 1
                        self.conn.execute(
                            "INSERT INTO changelog(seq,txid,writes) VALUES(?,?,?)",
                            (seq, txid, blob),
                        )
                        self.conn.execute("COMMIT")
                    except Conflict:
                        raise
                    except Exception:
                        with contextlib.suppress(sqlite3.Error):
                            self.conn.execute("ROLLBACK")
                        raise
                return seq
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "locked" in msg or "busy" in msg:
                    time.sleep(RETRY_DELAYS[min(i, len(RETRY_DELAYS) - 1)])
                    i += 1
                    continue
                raise

    def _advance_cursor_to(self, seq):
        """发布水位推进到 seq（单调）：仅在两库确认应用后调用。

        同一 SQLite 事务内读改写，天然只增不减；推进成功后更新内存缓存。
        """
        i = 0
        while True:
            try:
                with self.db_write_lock:
                    self.conn.execute("BEGIN IMMEDIATE")
                    try:
                        row = self.conn.execute(
                            "SELECT val FROM cf_meta WHERE key='publish_cursor'"
                        ).fetchone()
                        current = row[0] if row else 0
                        if seq > current:
                            current = seq
                            self.conn.execute(
                                "INSERT INTO cf_meta(key,val) "
                                "VALUES('publish_cursor',?) "
                                "ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                                (current,),
                            )
                        self.conn.execute("COMMIT")
                    except Exception:
                        with contextlib.suppress(sqlite3.Error):
                            self.conn.execute("ROLLBACK")
                        raise
                self._cursor_cache = current
                return current
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "locked" in msg or "busy" in msg:
                    time.sleep(RETRY_DELAYS[min(i, len(RETRY_DELAYS) - 1)])
                    i += 1
                    continue
                raise

    def snapshot(self, epoch=None):
        """GET /snapshot：返回与发布水位前缀严格对应的双库状态。

        先等待在途事务结束（发布未完成则等待），再在提交互斥锁内读两库，
        保证快照恰好对应 cursor 前缀：不含任何 seq>cursor 的半批/在途写入。
        """
        self._ensure_active()
        with self.inflight_lock:
            waiters = list(self.inflight.values())
        for t in waiters:
            t.join()
        self._ensure_active()
        with self.commit_lock:
            data = self._read_both_shards(epoch)
            return {"cursor": self.publish_cursor(), "data": data}

    def changes(self, after, epoch=None):
        """POST /changes：返回 after 之后至当前水位的全部批次（按 seq 排序）。

        只返回已发布（seq<=发布水位）的批次；无批次为 []。after 必须是非负
        整数；超过当前水位返回 409（由 HTTP 层映射 BeyondCursor）。
        """
        self._ensure_active()
        validate_nonneg_integer(after)
        with self.commit_lock:  # 与发布互斥，读取的是同一发布视图
            cursor = self.publish_cursor()
            if after > cursor:
                raise BeyondCursor(
                    "after=%d 超过当前发布水位 %d" % (after, cursor),
                    after=after, cursor=cursor,
                )
            rows = self.conn.execute(
                "SELECT seq, txid, writes FROM changelog "
                "WHERE seq>? AND seq<=? ORDER BY seq",
                (after, cursor),
            ).fetchall()
            batches = [
                {"seq": seq, "txid": txid,
                 "writes": json.loads(blob.decode("utf-8"))}
                for seq, txid, blob in rows
            ]
            return {"cursor": cursor, "batches": batches}

    def _read_both_shards(self, epoch=None):
        """在调用方持有的 commit_lock 内顺序读取两库已提交数据。"""
        out = {}
        for shard in SHARDS:
            code, data = get_json(
                self.shard_urls[shard], "/data", epoch=epoch
            )
            if code == 409 and isinstance(data, dict) \
                    and data.get("reason") == "stale_epoch":
                self._mark_fenced_out(
                    "快照/查询被分库 %s 判 stale" % shard
                )
                raise FencedOut("读两库时代不符")
            if code != 200 or not isinstance(data, dict):
                raise RuntimeError("分库 %s 数据查询失败: %r" % (shard, data))
            out[shard] = data["data"]
        return out

    # ---------------------------------------------------------- 提交发布
    def _publish_committed(self, txid, seq, blob):
        """在提交互斥锁内完成整批发布：两库按 seq 顺序应用，再推进水位。

        两库确认 commit 之后、向上层返回成功之前持久推进发布水位；
        分库侧 commit 幂等，任何阶段强退都由恢复沿用同一 seq 补齐，
        不漏批、不重复应用。返回发布后的水位。
        """
        norm = json.loads(blob.decode("utf-8"))
        with self.commit_lock:
            for shard in SHARDS:
                self._decision_call(txid, shard, "/commit")
                # 故障注入点：commit 已落盘、仅一库应用后强退（半批现场）
                if self.crash_after_first_commit:
                    self.crash_after_first_commit = False
                    sys.stderr.write(
                        "[fault] crash after commit applied on first shard "
                        "(txid=%s, seq=%s)\n" % (txid, seq)
                    )
                    os._exit(78)
            # 故障注入点：两库均应用、发布水位落盘前强退（水位落后现场）
            if self.crash_after_both_commit:
                self.crash_after_both_commit = False
                sys.stderr.write(
                    "[fault] crash after both shards committed but before "
                    "cursor durable (txid=%s, seq=%s)\n" % (txid, seq)
                )
                os._exit(80)
            return self._advance_cursor_to(seq)

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
                with self.db_write_lock, self.conn:
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

            if decision == ABORTED:
                # 撤销不占号：仅落 ABORT 决定，不写变更日志
                self._persist_decision(txid, ABORTED)
                self._abort_both(txid)
            elif self.changefeed:
                # 整批续采：在发布互斥段内原子落 COMMIT 决定+连续 seq，
                # 两库按 seq 顺序应用，双方确认后才推进持久发布水位
                self._check_leadership()
                blob = canonical_writes(norm)
                seq = self._persist_commit_decision(txid, blob)
                self._publish_committed(txid, seq, blob)
            else:
                self._persist_decision(txid, COMMITTED)
                self._commit_both(txid)
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
        with self.db_write_lock, self.conn:
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
        changefeed 模式下 COMMITTED 批次必须严格按 seq 顺序补齐发布并推进
        水位，因此采用单线程顺序恢复（服务尚未开放，无并发提交）。
        """
        if self.changefeed:
            self._recover_changefeed()
            return
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

    def _recover_changefeed(self):
        """整批续采恢复：沿用原序号补齐，不漏批、不重复。

        1) 变更日志中的 COMMITTED 批次按 seq 升序：向两库幂等补发 commit
           （已应用的分库不重复应用），再把发布水位推进到该 seq；
           半批或“双库已应用但水位未落盘”现场都由此收敛；
        2) 无决定（PENDING）事务补 ABORT 并通知撤销（本就不占号）；
        3) ABORTED 事务补发 abort。
        """
        # 先读持久水位，缓存失效重建
        self._cursor_cache = None
        rows = self.conn.execute(
            "SELECT c.seq, c.txid, c.writes FROM changelog c "
            "JOIN txn t ON t.txid=c.txid "
            "WHERE t.state=? ORDER BY c.seq", (COMMITTED,)
        ).fetchall()
        for seq, txid, blob in rows:
            if isinstance(blob, str):
                blob = blob.encode("utf-8")
            # 沿用原 seq 发布；分库幂等，水位只增，强退任意阶段都安全
            self._publish_committed(txid, seq, blob)

        others = self.conn.execute(
            "SELECT txid, writes, state FROM txn "
            "WHERE state IN (?, ?)", (PENDING, ABORTED)
        ).fetchall()
        for txid, blob, state in others:
            if isinstance(blob, str):
                blob = blob.encode("utf-8")
            norm = json.loads(blob.decode("utf-8"))
            self._recover_one(txid, norm, state)

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
            return self._read_both_shards(epoch=epoch)


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
                        "changefeed": coord.changefeed,
                        "cursor": coord.publish_cursor(),
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
                elif path == "/snapshot":
                    if not coord.changefeed:
                        self._send(404, {"error": "未启用 --changefeed，接口不可用"})
                        return
                    # 先沿用代号守卫，再进入快照视图
                    epoch = self._query_epoch()
                    self._guard_epoch(epoch)
                    self._send(200, coord.snapshot(epoch=epoch))
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
                elif path == "/changes":
                    if not coord.changefeed:
                        self._send(404, {"error": "未启用 --changefeed，接口不可用"})
                        return
                    # 先沿用代号守卫，再验 after
                    epoch = self._request_epoch(obj)
                    self._guard_epoch(epoch)
                    if not isinstance(obj, dict) or "after" not in obj:
                        raise BadRequest("after: 字段缺失")
                    after = validate_nonneg_integer(obj["after"])
                    self._send(200, coord.changes(after, epoch=epoch))
                elif path == "/control/crash-after-first-prepare":
                    self._guard_epoch(self._request_epoch(obj))
                    coord.crash_after_first_prepare = True
                    self._send(200, {"ok": True, "arm": "crash_after_first_prepare"})
                elif path == "/control/crash-after-first-commit":
                    self._guard_epoch(self._request_epoch(obj))
                    coord.crash_after_first_commit = True
                    self._send(200, {"ok": True, "arm": "crash_after_first_commit"})
                elif path == "/control/crash-after-both-commit":
                    self._guard_epoch(self._request_epoch(obj))
                    coord.crash_after_both_commit = True
                    self._send(200, {"ok": True, "arm": "crash_after_both_commit"})
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
            except BeyondCursor as e:
                # after 超过发布水位：409 且定位 after，不改任何状态
                self._send(409, {"error": e.msg, "reason": "after_beyond_cursor",
                                 "after": e.after, "cursor": e.cursor})
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
    ap.add_argument("--changefeed", action="store_true",
                    help="启用整批续采：连续 seq 变更日志、发布水位、"
                         "GET /snapshot 与 POST /changes")
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
        changefeed=args.changefeed,
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
    print("coordinator listening on %d, db=%s, fenced=%s, epoch=%s, changefeed=%s"
          % (args.port, args.db, args.fenced, coord.epoch, coord.changefeed),
          flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
