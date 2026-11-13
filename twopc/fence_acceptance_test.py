#!/usr/bin/env python3
"""fenced（代）模式端到端验收：双协调进程 + 双分库，滚动接管与整批发布。

覆盖任务要求的全部场景：
  F1 全新集群三进程 fenced 启用：领号=1、两库安装、正常整批提交与查询
  F2 代协议：请求必带代；类型先于代号；非法 epoch 定位且禁布尔；
     缺失/旧代 -> 409 stale_epoch 且不改状态；stale_epoch 不是反对票
  F3 只安装一库时再次强退：重启幂等安装（不领新号、不升代），随后恢复
  F4 无决定撤销：接管把旧代 PENDING 事务（两库已 PREPARED 持锁）撤销，
     准备值不可见、锁释放
  F5 题面示例：第1代 T 写 k="a"，已决定提交且仅 db1 应用；第2代接管
     补齐双库；迟到的第1代 abort 必须拒绝（终态/数据/锁/应用次数核对）
  F6 迟到 prepare / 提交应答丢失：旧代请求 409 不改状态；重试不重复应用
  F7 旧协调进程仍存活：新代领取更高号并安装后，旧代新收请求一律 409；
     旧代进行中的事务遇到 stale 响应自我 fence-out，stale 不促成任何决定
  F8 接管与整批发布的线性一致：旧代/错代查询 409，绝无半批；代账严格递增
"""

import http.client
import contextlib
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from common import canonical_request  # noqa: E402
PY = sys.executable
WORK = os.path.join(HERE, "run_fence")

DB1_PORT, DB2_PORT = 17011, 17012
COORD_PORTS = {"g1": 17010, "g2": 17013, "g3": 17014}
DB1_URL = "http://127.0.0.1:%d" % DB1_PORT
DB2_URL = "http://127.0.0.1:%d" % DB2_PORT
COORD_URLS = {g: "http://127.0.0.1:%d" % p for g, p in COORD_PORTS.items()}
SHARED_COORD_DB = os.path.join(WORK, "coord_shared.sqlite")


def epoch_file_for(gen):
    """每个协调代各自独立的代身份文件（共享协调库，不共享身份）。"""
    return os.path.join(WORK, "epoch_%s.id" % gen)


# 当前活跃代身份文件：随 start_coord / forget_local_epoch 推进
ACTIVE_EPOCH_FILE = {"path": epoch_file_for("g1")}

PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  \033[32mPASS\033[0m %s" % name)
    else:
        FAIL += 1
        print("  \033[31mFAIL\033[0m %s %s" % (name, detail))


# ---------------------------------------------------------------- HTTP 工具

def _req(base, method, path, body=None, raw=None, timeout=15):
    u = urllib.parse.urlparse(base)
    conn = http.client.HTTPConnection(u.hostname, u.port, timeout=timeout)
    try:
        headers = {}
        data = None
        if raw is not None:
            data = raw
            headers["Content-Type"] = "application/json"
        elif body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        conn.request(method, path, body=data, headers=headers)
        r = conn.getresponse()
        txt = r.read().decode()
        try:
            return r.status, json.loads(txt) if txt else None
        except ValueError:
            return r.status, txt
    finally:
        conn.close()


def post(base, path, body=None, **kw):
    return _req(base, "POST", path, body=body, **kw)


def get(base, path, **kw):
    return _req(base, "GET", path, **kw)


def wait_health(url, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        try:
            code, _ = get(url, "/health", timeout=2)
            if code == 200:
                return True
        except OSError:
            pass
        time.sleep(0.1)
    return False


def wait_exit(gen, timeout=10):
    p = procs.get(("coord", gen))
    if p is None:
        return True
    end = time.time() + timeout
    while time.time() < end:
        if p.poll() is not None:
            return True
        time.sleep(0.05)
    return p.poll() is not None


def W(db1=None, db2=None):
    return {"db1": db1 or {}, "db2": db2 or {}}


def shard_epoch(shard):
    return get(shard, "/debug/epoch")[1]["installed_epoch"]


def shard_state(shard, txid):
    code, data = get(shard, "/txn/%s/status" % txid)
    return data.get("state") if code == 200 else None


def shard_locks(shard):
    return get(shard, "/debug/locks")[1]["locks"]


def shard_stats(shard):
    return get(shard, "/debug/stats")[1]["stats"]


def coord_debug(gen):
    return get(COORD_URLS[gen], "/debug/epoch")[1]


def ledger_rows():
    conn = sqlite3.connect(SHARED_COORD_DB, timeout=10)
    try:
        rows = conn.execute("SELECT epoch FROM epoch_ledger ORDER BY epoch").fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


def forget_local_epoch(gen=None):
    """模拟一个全新协调进程接管：删除指定代（默认当前代）的身份文件，
    但保留共享代账（代只增不复用）。"""
    path = epoch_file_for(gen) if gen else ACTIVE_EPOCH_FILE["path"]
    with contextlib.suppress(OSError):
        os.remove(path)


def insert_pending(txid, writes):
    """以与协调者完全一致的规范化 BLOB 登记一个 PENDING 事务（guards 全空）。"""
    conn = sqlite3.connect(SHARED_COORD_DB, timeout=10)
    try:
        with conn:
            conn.execute(
                "INSERT INTO txn(txid,writes,state) VALUES(?,?,?)",
                (txid, canonical_request(writes, {"db1": [], "db2": []}),
                 "PENDING"),
            )
    finally:
        conn.close()


def insert_committed(txid, writes):
    """登记一个已落盘 COMMIT 决定（模拟旧代强退现场，guards 全空）。"""
    conn = sqlite3.connect(SHARED_COORD_DB, timeout=10)
    try:
        with conn:
            conn.execute(
                "INSERT INTO txn(txid,writes,state,decided_at) "
                "VALUES(?,?,?,datetime('now'))",
                (txid, canonical_request(writes, {"db1": [], "db2": []}),
                 "COMMITTED"),
            )
    finally:
        conn.close()


def txn_row(txid):
    conn = sqlite3.connect(SHARED_COORD_DB, timeout=10)
    try:
        return conn.execute(
            "SELECT state, decided_at FROM txn WHERE txid=?", (txid,)
        ).fetchone()
    finally:
        conn.close()


def log_contains(gen, needle):
    path = os.path.join(WORK, "coord_%s.log" % gen)
    try:
        with open(path, encoding="utf-8") as f:
            return needle in f.read()
    except OSError:
        return False


# ---------------------------------------------------------------- 进程管理

procs = {}
logfiles = {}


def start_shard(name, port):
    db = os.path.join(WORK, "%s.sqlite" % name)
    log = open(os.path.join(WORK, "%s.log" % name), "a")
    logfiles[name] = log
    p = subprocess.Popen(
        [PY, "-I", os.path.join(HERE, "shard.py"),
         "--name", name, "--port", str(port), "--db", db, "--fenced"],
        stdout=log, stderr=subprocess.STDOUT,
    )
    procs[("shard", name)] = p
    url = DB1_URL if name == "db1" else DB2_URL
    assert wait_health(url), "%s 启动失败" % name


def start_coord(gen, crash_after_first_install=False, expect_crash=False):
    port = COORD_PORTS[gen]
    log = open(os.path.join(WORK, "coord_%s.log" % gen), "a")
    logfiles[("coord", gen)] = log
    ef = epoch_file_for(gen)
    ACTIVE_EPOCH_FILE["path"] = ef
    cmd = [PY, "-I", os.path.join(HERE, "coordinator.py"),
           "--port", str(port), "--db", SHARED_COORD_DB,
           "--epoch-file", ef,
           "--db1-url", DB1_URL, "--db2-url", DB2_URL, "--fenced"]
    if crash_after_first_install:
        cmd.append("--crash-after-first-install")
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    procs[("coord", gen)] = p
    if expect_crash:
        # 该进程按故障注入设计会在开放 HTTP 前退出
        assert wait_exit(gen), "协调者 %s 未按预期强退" % gen
        return
    assert wait_health(COORD_URLS[gen]), "协调者 %s 启动失败" % gen


def kill(key):
    p = procs.get(key)
    if p and p.poll() is None:
        p.send_signal(signal.SIGKILL)
        p.wait(timeout=10)


def stop_all():
    for key in list(procs.keys()):
        kill(key)
    for f in logfiles.values():
        try:
            f.close()
        except OSError:
            pass
    logfiles.clear()


# ================================================================= 测试用例

def f01_fresh_cluster_and_basic():
    print("[F1] 全新 fenced 集群：领号=1、两库安装、整批提交与一致查询")
    start_shard("db1", DB1_PORT)
    start_shard("db2", DB2_PORT)

    # 未安装任何代前：协议请求一律 409，且不建任何状态
    code, r = post(DB1_URL, "/prepare",
                   {"txid": "ghost", "writes": W({"g": "1"}, {})})
    check("未安装代 prepare 拒绝", code == 409 and r.get("reason") == "stale_epoch", r)
    check("拒绝不留事务痕迹", shard_state(DB1_URL, "ghost") is None)
    check("未安装时分库代为 None", shard_epoch(DB1_URL) is None)

    start_coord("g1")
    check("g1 领到第 1 代", coord_debug("g1")["epoch"] == 1, coord_debug("g1"))
    check("g1 未被 fence-out", coord_debug("g1")["fenced_out"] is False)
    check("db1 已安装代 1", shard_epoch(DB1_URL) == 1)
    check("db2 已安装代 1", shard_epoch(DB2_URL) == 1)
    check("代账为 [1]", ledger_rows() == [1], ledger_rows())

    code, r = post(COORD_URLS["g1"], "/submit?epoch=1",
                   {"txid": "f1a", "writes": W({"k": "a"}, {"k": "a"})})
    check("f1a 整批提交", code == 200 and r["state"] == "COMMITTED", r)
    code, data = get(COORD_URLS["g1"], "/data?epoch=1")
    check("一致查询双库 k=a",
          code == 200 and data["data"]["db1"].get("k") == "a"
          and data["data"]["db2"].get("k") == "a", data)
    check("提交后无残留锁",
          shard_locks(DB1_URL) == [] and shard_locks(DB2_URL) == [])


def f02_epoch_protocol_validation():
    print("[F2] 代协议：类型先于代号；非法定位、禁布尔；缺失/旧代 409 不改状态")
    # 协调者侧
    code, r = post(COORD_URLS["g1"], "/submit",
                   {"txid": "f2", "writes": W({"x": "1"}, {})})
    check("协调者缺失代 -> 409 stale", code == 409
          and r.get("reason") == "stale_epoch", r)
    code, r = post(COORD_URLS["g1"], "/submit?epoch=2",
                   {"txid": "f2", "writes": W({"x": "1"}, {})})
    check("协调者旧/高代不符 -> 409 stale", code == 409
          and r.get("reason") == "stale_epoch", r)
    for bad in ("true", "1.5", "0", "-1", "abc"):
        code, r = post(COORD_URLS["g1"], "/submit?epoch=%s" % bad,
                       {"txid": "f2", "writes": W({"x": "1"}, {})})
        check("非法 query epoch=%r 定位 400" % bad,
              code == 400 and "epoch" in r.get("error", ""), r)
    code, r = post(COORD_URLS["g1"], "/submit",
                   {"txid": "f2", "epoch": True,
                    "writes": W({"x": "1"}, {})})
    check("布尔 epoch 拒绝（禁布尔）", code == 400 and "epoch" in r["error"], r)
    code, r = post(COORD_URLS["g1"], "/submit",
                   {"txid": "f2", "epoch": 1.0,
                    "writes": W({"x": "1"}, {})})
    check("浮点 epoch 拒绝", code == 400 and "epoch" in r["error"], r)

    # 分库侧：类型先于代号；stale 不改状态
    code, r = post(DB1_URL, "/prepare?epoch=true",
                   {"txid": "f2x", "writes": W({"x": "1"}, {})})
    check("分库布尔代 400", code == 400 and "epoch" in r["error"], r)
    code, r = post(DB1_URL, "/prepare?epoch=9",
                   {"txid": "f2x", "writes": W({"x": "1"}, {})})
    check("分库错代 409 stale 且不留痕",
          code == 409 and r.get("reason") == "stale_epoch"
          and shard_state(DB1_URL, "f2x") is None, r)
    code, r = post(DB1_URL, "/commit?epoch=1", {"txid": "never"})
    check("当代但未准备仍按内容/终态规则 409（非 stale）",
          code == 409 and r.get("reason") is None, r)
    code, r = get(DB1_URL, "/data")
    check("分库 /data 缺代 409", code == 409 and r.get("reason") == "stale_epoch", r)
    code, r = get(COORD_URLS["g1"], "/data?epoch=1")
    check("当代 /data 正常", code == 200 and "data" in r, r)

    # 安装接口：低代拒绝、同代幂等
    code, r = post(DB1_URL, "/install", {"epoch": 0})
    check("安装非法代 400", code == 400 and "epoch" in r["error"], r)
    code, r = post(DB1_URL, "/install", {"epoch": 1})
    check("同代安装幂等", code == 200 and r["epoch"] == 1 and "幂等" in r["note"], r)
    code, r = post(DB1_URL, "/install", {"epoch": 0})
    check("安装代必须正整数（0 拒绝）", code == 400, r)


def f03_crash_after_only_one_shard_install():
    print("[F3] 只安装一库时再次强退：重启幂等安装、不领新号，随后正常恢复")
    # 先在 g1 造一个已提交事务，制造共享库历史
    post(COORD_URLS["g1"], "/submit?epoch=1",
         {"txid": "f3old", "writes": W({"k3": "old"}, {"k3": "old"})})

    kill(("coord", "g1"))
    # g2 领号 2，安装完 db1 即强退（db2 仍停在代 1）
    forget_local_epoch()
    start_coord("g2", crash_after_first_install=True, expect_crash=True)
    check("g2 确已强退", wait_exit("g2"))
    check("仅 db1 升到代 2", shard_epoch(DB1_URL) == 2
          and shard_epoch(DB2_URL) == 1,
          (shard_epoch(DB1_URL), shard_epoch(DB2_URL)))
    check("代账已登记 [1,2]", ledger_rows() == [1, 2], ledger_rows())
    # 安装不清数据：旧提交数据原样保留
    check("安装未动已提交数据",
          get(DB1_URL, "/data?epoch=2")[1]["data"].get("k3") == "old")

    # g2 重启：沿用本代 2（不领新号），对 db1 幂等、给 db2 补装
    start_coord("g2")
    check("g2 重启仍为代 2（不复用/不跳号）",
          coord_debug("g2")["epoch"] == 2, coord_debug("g2"))
    check("代账仍为 [1,2]", ledger_rows() == [1, 2], ledger_rows())
    check("两库均安装代 2", shard_epoch(DB1_URL) == 2
          and shard_epoch(DB2_URL) == 2)
    code, r = post(COORD_URLS["g2"], "/submit?epoch=2",
                   {"txid": "f3new", "writes": W({"k3": "new"}, {})})
    check("恢复后新代可提交", code == 200 and r["state"] == "COMMITTED", r)
    # 旧代请求现在到处被拒
    code, r = post(COORD_URLS["g2"], "/submit?epoch=1",
                   {"txid": "f3stale", "writes": W({"z": "1"}, {})})
    check("旧代提交 409", code == 409 and r.get("reason") == "stale_epoch", r)


def f04_no_decision_abort_on_takeover():
    print("[F4] 无决定撤销：两库已 PREPARED 持锁的 PENDING 事务被新代撤销")
    # 直接在两分库准备一个事务（绕过协调者，模拟旧代留下的在途准备）
    body = {"txid": "f4a", "writes": W({"k4": "v4"}, {"k4": "v4"})}
    c1, _ = post(DB1_URL, "/prepare?epoch=2", body)
    c2, _ = post(DB2_URL, "/prepare?epoch=2", body)
    check("两库手动 prepare 成功", c1 == 200 and c2 == 200)
    check("两库持锁",
          any(l["txid"] == "f4a" for l in shard_locks(DB1_URL))
          and any(l["txid"] == "f4a" for l in shard_locks(DB2_URL)))
    # 在共享协调库登记 PENDING（无决定），模拟旧代强退现场
    insert_pending("f4a", W({"k4": "v4"}, {"k4": "v4"}))

    kill(("coord", "g2"))
    forget_local_epoch()
    start_coord("g3")  # 领号 3 -> 安装 -> 恢复：PENDING 应撤销
    check("g3 领到代 3", coord_debug("g3")["epoch"] == 3)
    check("代账严格递增 [1,2,3]", ledger_rows() == [1, 2, 3], ledger_rows())
    end = time.time() + 10
    while time.time() < end and txn_row("f4a") and txn_row("f4a")[0] != "ABORTED":
        time.sleep(0.1)
    row = txn_row("f4a")
    check("协调库决定为 ABORTED", row is not None and row[0] == "ABORTED", row)
    check("db1 终态 ABORTED", shard_state(DB1_URL, "f4a") == "ABORTED")
    check("db2 终态 ABORTED", shard_state(DB2_URL, "f4a") == "ABORTED")
    check("锁全部释放",
          shard_locks(DB1_URL) == [] and shard_locks(DB2_URL) == [])
    data = get(COORD_URLS["g3"], "/data?epoch=3")[1]["data"]
    check("准备值不可见（两库均无 k4）",
          "k4" not in data["db1"] and "k4" not in data["db2"], data)

    # 同 txid 同内容向新代重提：按恢复语义返回原 ABORTED，不复活
    code, r = post(COORD_URLS["g3"], "/submit?epoch=3",
                   {"txid": "f4a", "writes": W({"k4": "v4"}, {"k4": "v4"})})
    check("同号重提返回原 ABORTED", code == 200 and r["state"] == "ABORTED", r)
    check("仍无锁", shard_locks(DB1_URL) == [] and shard_locks(DB2_URL) == [])


def f05_worked_example_half_commit_takeover():
    print("[F5] 题面示例：代1决定提交仅 db1 应用；代2 接管补齐；迟到代1 abort 拒绝")
    # 现场：分库 db1 COMMITTED、db2 PREPARED 持锁；协调库决定 COMMITTED
    txid = "f5T"
    body = {"txid": txid, "writes": W({"k": "a"}, {"k": "a"})}
    post(DB1_URL, "/prepare?epoch=3", body)
    post(DB2_URL, "/prepare?epoch=3", body)
    post(DB1_URL, "/commit?epoch=3", {"txid": txid})
    check("现场 db1 COMMITTED", shard_state(DB1_URL, txid) == "COMMITTED")
    check("现场 db2 PREPARED 持锁",
          shard_state(DB2_URL, txid) == "PREPARED"
          and any(l["txid"] == txid for l in shard_locks(DB2_URL)))
    applied_before = shard_stats(DB1_URL).get("commit_applied", 0)
    applied_before2 = shard_stats(DB2_URL).get("commit_applied", 0)
    insert_committed(txid, W({"k": "a"}, {"k": "a"}))

    # 新代 g1 端口重新拉起一个协调进程（代 4）执行接管
    kill(("coord", "g3"))
    forget_local_epoch()
    start_coord("g1")
    check("新协调领到代 4", coord_debug("g1")["epoch"] == 4, coord_debug("g1"))
    end = time.time() + 10
    while time.time() < end and shard_state(DB2_URL, txid) != "COMMITTED":
        time.sleep(0.1)
    check("db2 补齐 COMMITTED", shard_state(DB2_URL, txid) == "COMMITTED")
    check("db1 保持 COMMITTED", shard_state(DB1_URL, txid) == "COMMITTED")
    data = get(COORD_URLS["g1"], "/data?epoch=4")[1]["data"]
    check("两库 k 均为 a（整批补齐）",
          data["db1"].get("k") == "a" and data["db2"].get("k") == "a", data)
    check("锁全部释放",
          shard_locks(DB1_URL) == [] and shard_locks(DB2_URL) == [])
    applied_after = shard_stats(DB1_URL).get("commit_applied", 0)
    check("db1 未重复应用（计数不变）",
          applied_after == applied_before, (applied_before, applied_after))

    # 迟到的第 3 代 abort：必须 409 stale_epoch，终态/数据不变
    code, r = post(DB1_URL, "/abort?epoch=3", {"txid": txid})
    check("迟到旧代 abort(db1) 拒绝", code == 409
          and r.get("reason") == "stale_epoch", r)
    code, r = post(DB2_URL, "/abort?epoch=3", {"txid": txid})
    check("迟到旧代 abort(db2) 拒绝", code == 409
          and r.get("reason") == "stale_epoch", r)
    check("拒绝后终态仍双 COMMITTED",
          shard_state(DB1_URL, txid) == "COMMITTED"
          and shard_state(DB2_URL, txid) == "COMMITTED")
    data = get(COORD_URLS["g1"], "/data?epoch=4")[1]["data"]
    check("拒绝后两库 k 仍为 a",
          data["db1"].get("k") == "a" and data["db2"].get("k") == "a", data)
    check("应用次数仍不变",
          shard_stats(DB1_URL).get("commit_applied", 0) == applied_after
          and shard_stats(DB2_URL).get("commit_applied", 0)
          == applied_before2 + 1)


def f06_late_prepare_and_lost_commit_ack():
    print("[F6] 迟到 prepare 拒绝不改状态；提交应答丢失，接管重试只应用一次")
    # 边界：当前协调进程重启过（inflight 为空），重提一个已 COMMITTED 的
    # 历史事务必须返回原状态，而不是因缺少 worker 报错
    code, r = post(COORD_URLS["g1"], "/submit?epoch=4",
                   {"txid": "f5T", "writes": W({"k": "a"}, {"k": "a"})})
    check("重启后重提已决定事务返回原 COMMITTED",
          code == 200 and r["state"] == "COMMITTED", r)

    # 迟到 prepare：旧代号打一个新 txid 的 prepare
    code, r = post(DB1_URL, "/prepare?epoch=1",
                   {"txid": "f6late", "writes": W({"k6": "1"}, {})})
    check("迟到旧代 prepare 409", code == 409
          and r.get("reason") == "stale_epoch", r)
    check("stale prepare 不留痕", shard_state(DB1_URL, "f6late") is None)

    # 提交应答丢失：分库落盘后丢应答，协调者携带当代重试幂等
    post(DB1_URL, "/control/drop-next", {"phase": "commit"})
    post(DB2_URL, "/control/drop-next", {"phase": "commit"})
    before1 = shard_stats(DB1_URL).get("commit_applied", 0)
    before2 = shard_stats(DB2_URL).get("commit_applied", 0)
    code, r = post(COORD_URLS["g1"], "/submit?epoch=4",
                   {"txid": "f6a", "writes": W({"k6": "six"}, {"k6": "six"})})
    check("丢应答后仍 committed", code == 200 and r["state"] == "COMMITTED", r)
    time.sleep(0.3)
    check("db1 只应用一次",
          shard_stats(DB1_URL).get("commit_applied", 0) == before1 + 1)
    check("db2 只应用一次",
          shard_stats(DB2_URL).get("commit_applied", 0) == before2 + 1)

    # prepare 应答丢失同样收敛
    post(DB1_URL, "/control/drop-next", {"phase": "prepare"})
    code, r = post(COORD_URLS["g1"], "/submit?epoch=4",
                   {"txid": "f6b", "writes": W({"k6b": "b"}, {})})
    check("prepare 丢应答后 committed",
          code == 200 and r["state"] == "COMMITTED", r)
    check("f6b 终态 COMMITTED", shard_state(DB1_URL, "f6b") == "COMMITTED")


def f07_old_coordinator_still_alive():
    print("[F7] 旧协调进程存活：新代接管后旧代请求 409；stale 不促成决定")
    # 进行中的旧代事务：用分库延迟制造旧代 worker 重试窗口
    post(DB1_URL, "/control/delay-next", {"phase": "prepare", "seconds": 2.5})
    box = {}

    def old_submit():
        try:
            box["resp"] = post(COORD_URLS["g1"], "/submit?epoch=4",
                               {"txid": "f7b",
                                "writes": W({"k7b": "v"}, {"k7b": "v"})},
                               timeout=20)
        except OSError as e:
            box["resp"] = ("OSError", str(e))

    th = threading.Thread(target=old_submit)
    th.start()
    # 等旧代登记 PENDING 且 prepare 已在 db1 的延迟窗口内（持锁 sleep）
    end = time.time() + 10
    while time.time() < end and not (
            txn_row("f7b") is not None and txn_row("f7b")[0] == "PENDING"):
        time.sleep(0.05)
    check("旧代已登记 f7b 为 PENDING（prepare 在途）",
          txn_row("f7b") is not None and txn_row("f7b")[0] == "PENDING",
          txn_row("f7b"))
    time.sleep(0.4)  # 确保 prepare 已进入 db1 延迟，安装将排在其后

    # 此刻再起新代 g2：其 /install 等 db1 延迟结束后生效，
    # db1 随后给旧代 prepare 回的就是 stale_epoch（响应时代号已变）
    forget_local_epoch()
    start_coord("g2")
    check("g2 领到代 5", coord_debug("g2")["epoch"] == 5, coord_debug("g2"))
    check("两库升到代 5", shard_epoch(DB1_URL) == 5
          and shard_epoch(DB2_URL) == 5)
    check("代账 [1,2,3,4,5]", ledger_rows() == [1, 2, 3, 4, 5], ledger_rows())

    # 旧代 g1 新收到的客户端请求一律 409（由代号守卫拦截）
    code, r = post(COORD_URLS["g1"], "/submit?epoch=4",
                   {"txid": "f7a", "writes": W({"k7": "7"}, {})})
    check("旧代新请求 409 stale", code == 409
          and r.get("reason") == "stale_epoch", r)
    code, r = get(COORD_URLS["g1"], "/data?epoch=4")
    check("旧代一致查询 409（绝不返回半批）",
          code == 409 and r.get("reason") == "stale_epoch", r)
    code, r = get(COORD_URLS["g1"], "/txn/f6a/status?epoch=4")
    check("旧代状态查询也 409", code == 409
          and r.get("reason") == "stale_epoch", r)
    check("被拒请求不建事务", txn_row("f7a") is None)

    # 等待旧代 worker 撞上 db1 的 stale_epoch 响应并自我 fence-out
    end = time.time() + 15
    while time.time() < end and not coord_debug("g1")["fenced_out"]:
        time.sleep(0.1)
    th.join(timeout=10)
    check("旧代自我 fence-out", coord_debug("g1")["fenced_out"] is True,
          coord_debug("g1"))
    check("旧代客户端得到 409",
          isinstance(box.get("resp"), tuple) and box["resp"][0] == 409, box)

    # stale_epoch 不是反对票：日志证明旧代 worker 因代失效中止，
    # 且从未写过决定（无 persist_decision 落盘）。f7b 的 ABORTED 只能来自
    # 新代 g2 启动恢复，下面核对决定者身份。
    check("旧代 worker 因代失效中止（非投反对票）",
          log_contains("g1", "worker(txid=f7b) 因代失效中止"))
    decided_before = txn_row("f7b")
    # 此刻若新代恢复已撤销，decided_at 由 g2 写入；关键是 g1 日志无任何决定
    check("f7b 从未被旧代提交",
          not (decided_before and decided_before[0] == "COMMITTED"),
          decided_before)

    # fence-out 后旧代一切新请求都 409（即使带对的旧代号）
    code, r = get(COORD_URLS["g1"], "/data?epoch=4")
    check("fence-out 后查询仍 409", code == 409, r)

    # 新代 g2 恢复该 PENDING：撤销，两库清理
    code, r = post(COORD_URLS["g2"], "/submit?epoch=5",
                   {"txid": "f7b", "writes": W({"k7b": "v"}, {"k7b": "v"})})
    check("新代接管同号事务返回 ABORTED",
          code == 200 and r["state"] == "ABORTED", r)
    check("两库 f7b 均 ABORTED",
          shard_state(DB1_URL, "f7b") == "ABORTED"
          and shard_state(DB2_URL, "f7b") == "ABORTED")
    check("锁释放",
          shard_locks(DB1_URL) == [] and shard_locks(DB2_URL) == [])
    data = get(COORD_URLS["g2"], "/data?epoch=5")[1]["data"]
    check("k7b 不可见", "k7b" not in data["db1"] and "k7b" not in data["db2"])

    # 新代正常承接新事务
    code, r = post(COORD_URLS["g2"], "/submit?epoch=5",
                   {"txid": "f7c", "writes": W({"k7c": "go"}, {"k7c": "go"})})
    check("新代新事务 committed", code == 200 and r["state"] == "COMMITTED", r)


def f08_linearizability_and_ledger():
    print("[F8] 接管线性一致：错代查询无半批；代账只增不复用；终态/锁终检")
    # 再来一代 g3：领号 6，安装后服务
    forget_local_epoch()
    start_coord("g3")
    check("g3 领到代 6", coord_debug("g3")["epoch"] == 6)
    check("代账严格 1..6", ledger_rows() == [1, 2, 3, 4, 5, 6], ledger_rows())

    # 用一个 PENDING + 两库 PREPARED 制造“半批现场”，接管期间错代读取必须 409
    body = {"txid": "f8a", "writes": W({"k8": "half"}, {"k8": "half"})}
    post(DB1_URL, "/prepare?epoch=6", body)
    post(DB2_URL, "/prepare?epoch=6", body)
    insert_pending("f8a", W({"k8": "half"}, {"k8": "half"}))

    query_box = {}

    def stale_query():
        # 旧代号查询：新代安装后必 409，且任何时刻都读不到半批
        query_box["r"] = get(COORD_URLS["g2"], "/data?epoch=5", timeout=20)

    qt = threading.Thread(target=stale_query)
    qt.start()
    time.sleep(0.3)
    kill(("coord", "g3"))
    # g1 端口上仍是 f07 后已 fence-out 的旧进程，先释放端口再起代 7
    kill(("coord", "g1"))
    forget_local_epoch()
    start_coord("g1")  # 代 7：安装并恢复（f8a 撤销）
    qt.join(timeout=20)
    code, r = query_box["r"]
    check("接管窗口旧代查询 409", code == 409
          and r.get("reason") == "stale_epoch", query_box["r"])
    check("g1 新领到代 7", coord_debug("g1")["epoch"] == 7)
    check("代账 1..7 连续不复用",
          ledger_rows() == [1, 2, 3, 4, 5, 6, 7], ledger_rows())

    end = time.time() + 10
    while time.time() < end and txn_row("f8a")[0] != "ABORTED":
        time.sleep(0.1)
    check("f8a 被撤销", txn_row("f8a")[0] == "ABORTED", txn_row("f8a"))
    data = get(COORD_URLS["g1"], "/data?epoch=7")[1]["data"]
    check("无半批：k8 两库均不可见",
          "k8" not in data["db1"] and "k8" not in data["db2"], data)

    # 全量终检：所有历史已提交数据完好、无残留锁、终态不可反转
    check("最终无任何残留锁",
          shard_locks(DB1_URL) == [] and shard_locks(DB2_URL) == [])
    check("历史 k=a 双库保留",
          data["db1"].get("k") == "a" and data["db2"].get("k") == "a")
    check("f7c 双库可见",
          data["db1"].get("k7c") == "go" and data["db2"].get("k7c") == "go")
    # 终态不可反转：对已提交事务发旧代 abort 仍 409 stale
    code, r = post(DB1_URL, "/abort?epoch=1", {"txid": "f7c"})
    check("旧代 abort 已提交事务仍 409 stale",
          code == 409 and r.get("reason") == "stale_epoch", r)
    check("终态保持 COMMITTED", shard_state(DB1_URL, "f7c") == "COMMITTED")


def main():
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)

    # 预清理：杀掉可能残留的本测试进程（失败重跑场景）
    with contextlib.suppress(OSError):
        subprocess.run(
            ["pkill", "-f", "twopc/(shard|coordinator)\\.py"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(0.5)

    try:
        f01_fresh_cluster_and_basic()
        f02_epoch_protocol_validation()
        f03_crash_after_only_one_shard_install()
        f04_no_decision_abort_on_takeover()
        f05_worked_example_half_commit_takeover()
        f06_late_prepare_and_lost_commit_ack()
        f07_old_coordinator_still_alive()
        f08_linearizability_and_ledger()
    finally:
        stop_all()

    print("\n================ fenced 验收结果 ================")
    print("PASS=%d  FAIL=%d" % (PASS, FAIL))
    if FAIL:
        print("进程日志位于 %s/*.log" % WORK)
        sys.exit(1)
    print("全部通过 ✔")


if __name__ == "__main__":
    main()
