#!/usr/bin/env python3
"""fenced（代）模式 × guards 联合验收：真实进程、滚动接管与范围锁跨代保留。

覆盖：
  H1 带 guards 的代内整批提交/幻影拦截；guards 与 epoch 校验顺序（类型先于代号）
  H2 范围锁跨 /install 保留：代提升不清锁，新代仍受旧范围锁约束
  H3 无决定接管：旧代 PREPARED 持范围锁的 PENDING 被新代撤销，范围锁释放
  H4 半批接管：guards 事务已决定提交、仅一库应用，新代补齐，范围锁终结释放
  H5 旧代重发 prepare：stale_epoch 不重验、不促成决定；同代重试不重验
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
WORK = os.path.join(HERE, "run_guard_fence")

DB1_PORT, DB2_PORT = 15011, 15012
COORD_PORTS = {"h1": 15010, "h2": 15013}
DB1_URL = "http://127.0.0.1:%d" % DB1_PORT
DB2_URL = "http://127.0.0.1:%d" % DB2_PORT
COORD_URLS = {g: "http://127.0.0.1:%d" % p for g, p in COORD_PORTS.items()}
SHARED_COORD_DB = os.path.join(WORK, "coord_shared.sqlite")

PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  \033[32mPASS\033[0m %s" % name)
    else:
        FAIL += 1
        print("  \033[31mFAIL\033[0m %s %s" % (name, detail))


def _req(base, method, path, body=None, timeout=15):
    u = urllib.parse.urlparse(base)
    conn = http.client.HTTPConnection(u.hostname, u.port, timeout=timeout)
    try:
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"} if data else {}
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
            if get(url, "/health", timeout=2)[0] == 200:
                return True
        except OSError:
            pass
        time.sleep(0.1)
    return False


def W(db1=None, db2=None):
    return {"db1": db1 or {}, "db2": db2 or {}}


def G(db1=None, db2=None):
    return {"db1": db1 or [], "db2": db2 or []}


def shard_state(shard, txid):
    code, data = get(shard, "/txn/%s/status" % txid)
    return data.get("state") if code == 200 else None


def shard_locks(shard):
    return get(shard, "/debug/locks")[1]


def coord_debug(gen):
    return get(COORD_URLS[gen], "/debug/epoch")[1]


def shard_epoch(shard):
    return get(shard, "/debug/epoch")[1]["installed_epoch"]


def epoch_file_for(gen):
    return os.path.join(WORK, "epoch_%s.id" % gen)


def forget_local_epoch(gen=None):
    """模拟全新进程接管：删除指定代（默认当前活跃进程）的本地身份文件，
    保留共享代账（代只增不复用），使其重启时领取下一个新代。"""
    path = epoch_file_for(gen) if gen else epoch_file_for(ACTIVE["gen"])
    with contextlib.suppress(OSError):
        os.remove(path)


ACTIVE = {"gen": "h1"}

procs = {}
logfiles = {}


def start_shard(name, port):
    db = os.path.join(WORK, "%s.sqlite" % name)
    log = open(os.path.join(WORK, "%s.log" % name), "a")
    logfiles[("shard", name)] = log
    p = subprocess.Popen(
        [PY, "-I", os.path.join(HERE, "shard.py"),
         "--name", name, "--port", str(port), "--db", db, "--fenced"],
        stdout=log, stderr=subprocess.STDOUT)
    procs[("shard", name)] = p
    assert wait_health(DB1_URL if name == "db1" else DB2_URL), name


def start_coord(gen):
    port = COORD_PORTS[gen]
    ACTIVE["gen"] = gen
    log = open(os.path.join(WORK, "coord_%s.log" % gen), "a")
    logfiles[("coord", gen)] = log
    p = subprocess.Popen(
        [PY, "-I", os.path.join(HERE, "coordinator.py"),
         "--port", str(port), "--db", SHARED_COORD_DB,
         "--epoch-file", epoch_file_for(gen),
         "--db1-url", DB1_URL, "--db2-url", DB2_URL, "--fenced"],
        stdout=log, stderr=subprocess.STDOUT)
    procs[("coord", gen)] = p
    assert wait_health(COORD_URLS[gen]), gen


def kill(key):
    p = procs.get(key)
    if p and p.poll() is None:
        p.send_signal(signal.SIGKILL)
        p.wait(timeout=10)


def stop_all():
    for key in list(procs.keys()):
        kill(key)
    for f in logfiles.values():
        f.close()
    logfiles.clear()


def insert_pending(txid, writes, guards=None):
    guards = guards or G([], [])
    conn = sqlite3.connect(SHARED_COORD_DB, timeout=10)
    try:
        with conn:
            conn.execute(
                "INSERT INTO txn(txid,writes,state) VALUES(?,?,?)",
                (txid, canonical_request(writes, guards), "PENDING"))
    finally:
        conn.close()


def insert_committed(txid, writes, guards=None):
    guards = guards or G([], [])
    conn = sqlite3.connect(SHARED_COORD_DB, timeout=10)
    try:
        with conn:
            conn.execute(
                "INSERT INTO txn(txid,writes,state,decided_at) "
                "VALUES(?,?,?,datetime('now'))",
                (txid, canonical_request(writes, guards), "COMMITTED"))
    finally:
        conn.close()


def h1_basic_and_validation():
    print("[H1] 代内 guards 整批提交/幻影拦截；校验顺序：类型先于代号")
    # 先在两库种已提交基线
    code, r = post(COORD_URLS["h1"], "/submit?epoch=1",
                   {"txid": "h1base", "writes": W({"h10a": "v"}, {"h10a": "v"})})
    check("基线整批提交", code == 200 and r["state"] == "COMMITTED", r)

    # 带 guards 的整批提交（两库同范围快照匹配）
    code, r = post(COORD_URLS["h1"], "/submit?epoch=1",
                   {"txid": "h1ok",
                    "writes": W({"h10z": "1"}, {"h10z": "1"}),
                    "guards": G([["h10a", "h10b", {"h10a": "v"}]],
                                [["h10a", "h10b", {"h10a": "v"}]])})
    check("带 guards 两库提交", code == 200 and r["state"] == "COMMITTED", r)
    check("提交后无残留范围锁",
          shard_locks(DB1_URL)["range_locks"] == []
          and shard_locks(DB2_URL)["range_locks"] == [])

    # 幻影：先在 db1 持范围锁（当代），再经协调者写范围内键
    code, r = post(DB1_URL, "/prepare?epoch=1",
                   {"txid": "h1hold", "writes": W({}, {}),
                    "guards": G([["h20a", "h20z", {}]])})
    check("当代 db1 持范围锁", code == 200 and r["vote"] == "PREPARED", r)
    code, r = post(COORD_URLS["h1"], "/submit?epoch=1",
                   {"txid": "h1phantom",
                    "writes": W({"h20b": "x"}, {"h99": "y"})})
    check("幻影整批 abort", code == 200 and r["state"] == "ABORTED", r)
    check("db1 幻影事务 ABORTED 且无锁",
          shard_state(DB1_URL, "h1phantom") == "ABORTED"
          and all(l["txid"] != "h1phantom"
                  for l in shard_locks(DB1_URL)["range_locks"]))
    check("db2 同步 ABORTED", shard_state(DB2_URL, "h1phantom") == "ABORTED")
    post(DB1_URL, "/abort?epoch=1", {"txid": "h1hold"})

    # 校验顺序：非法 epoch 类型先于 guards 内容被判 400（即使 guards 也非法）
    code, r = post(COORD_URLS["h1"], "/submit?epoch=true",
                   {"txid": "h1bad", "writes": W({}, {}),
                    "guards": {"db1": [["c", "a", {}]], "db2": []}})
    check("类型先于代号/内容：非法 epoch 400",
          code == 400 and "epoch" in r.get("error", ""), r)
    # 代号合法但 guards 非法 -> 400 且不建事务
    code, r = post(COORD_URLS["h1"], "/submit?epoch=1",
                   {"txid": "h1bad2", "writes": W({}, {}),
                    "guards": {"db1": [["c", "a", {}]], "db2": []}})
    check("当代非法 guards 400 定位",
          code == 400 and "guards.db1[0].hi" in r.get("error", ""), r)
    check("非法不建事务", shard_state(DB1_URL, "h1bad2") is None)
    # 缺代：stale 409，guards 根本不被评估
    code, r = post(COORD_URLS["h1"], "/submit",
                   {"txid": "h1bad3", "writes": W({}, {}),
                    "guards": {"db1": [["c", "a", {}]], "db2": []}})
    check("缺代先于 guards：409 stale",
          code == 409 and r.get("reason") == "stale_epoch", r)


def h2_range_lock_survives_install():
    print("[H2] 范围锁跨 /install 保留：代提升不清锁，新代受旧范围锁约束")
    # 代 1 在两库持范围锁
    body = {"txid": "h2hold", "writes": W({}, {}),
            "guards": G([["h30a", "h30z", {}]], [["h30a", "h30z", {}]])}
    c1, r1 = post(DB1_URL, "/prepare?epoch=1", body)
    c2, r2 = post(DB2_URL, "/prepare?epoch=1", body)
    check("代1两库持范围锁", c1 == 200 and c2 == 200, (r1, r2))

    # 新协调进程领号 2 并安装（滚动接管，旧进程不杀也可）
    forget_local_epoch()
    start_coord("h2")
    check("h2 领到代 2", coord_debug("h2")["epoch"] == 2)
    check("两库升到代 2",
          shard_epoch(DB1_URL) == 2 and shard_epoch(DB2_URL) == 2)

    # 安装不清锁：范围锁仍在
    check("db1 范围锁跨代保留",
          any(l["txid"] == "h2hold"
              for l in shard_locks(DB1_URL)["range_locks"]),
          shard_locks(DB1_URL))
    check("db2 范围锁跨代保留",
          any(l["txid"] == "h2hold"
              for l in shard_locks(DB2_URL)["range_locks"]))

    # 新代仍受旧范围锁约束：范围内写被挡
    code, r = post(COORD_URLS["h2"], "/submit?epoch=2",
                   {"txid": "h2block",
                    "writes": W({"h30b": "1"}, {"h99": "1"})})
    check("新代范围内写被旧范围锁挡",
          code == 200 and r["state"] == "ABORTED", r)
    # 范围外写放行
    code, r = post(COORD_URLS["h2"], "/submit?epoch=2",
                   {"txid": "h2ok",
                    "writes": W({"h31": "1"}, {"h31": "1"})})
    check("新代范围外写提交", code == 200 and r["state"] == "COMMITTED", r)

    # 终结旧守卫（新代号 abort），范围锁释放
    code, r = post(DB1_URL, "/abort?epoch=2", {"txid": "h2hold"})
    check("新代 abort 旧事务", code == 200 and r["state"] == "ABORTED", r)
    post(DB2_URL, "/abort?epoch=2", {"txid": "h2hold"})
    check("终结后两库范围锁释放",
          shard_locks(DB1_URL)["range_locks"] == []
          and shard_locks(DB2_URL)["range_locks"] == [])


def h3_takeover_abort_pending():
    print("[H3] 无决定接管：持范围锁的 PENDING 被新代撤销，范围锁释放")
    body = {"txid": "h3t", "writes": W({"h40w": "1"}, {"h40w": "1"}),
            "guards": G([["h40a", "h40z", {}]], [["h40a", "h40z", {}]])}
    post(DB1_URL, "/prepare?epoch=2", body)
    post(DB2_URL, "/prepare?epoch=2", body)
    insert_pending("h3t", W({"h40w": "1"}, {"h40w": "1"}),
                   G([["h40a", "h40z", {}]], [["h40a", "h40z", {}]]))
    check("两库 PREPARED 持范围锁",
          any(l["txid"] == "h3t"
              for l in shard_locks(DB1_URL)["range_locks"])
          and any(l["txid"] == "h3t"
                  for l in shard_locks(DB2_URL)["range_locks"]))

    # h1 旧进程仍存活（代 1）：先杀掉，再以全新进程在同端口接管领号 3
    kill(("coord", "h1"))
    forget_local_epoch("h1")
    start_coord("h1")  # 领号 3，安装并恢复：PENDING -> abort
    check("h1 领到代 3", coord_debug("h1")["epoch"] == 3)
    end = time.time() + 10
    conn = sqlite3.connect(SHARED_COORD_DB, timeout=10)
    try:
        while time.time() < end:
            row = conn.execute(
                "SELECT state FROM txn WHERE txid=?", ("h3t",)).fetchone()
            if row and row[0] == "ABORTED":
                break
            time.sleep(0.1)
        check("协调库决定 ABORTED", row is not None and row[0] == "ABORTED", row)
    finally:
        conn.close()
    check("两库 h3t 终态 ABORTED",
          shard_state(DB1_URL, "h3t") == "ABORTED"
          and shard_state(DB2_URL, "h3t") == "ABORTED")
    check("范围锁与写键锁全部释放",
          shard_locks(DB1_URL)["range_locks"] == []
          and shard_locks(DB2_URL)["range_locks"] == []
          and shard_locks(DB1_URL)["locks"] == []
          and shard_locks(DB2_URL)["locks"] == [])
    data = get(COORD_URLS["h1"], "/data?epoch=3")[1]["data"]
    check("准备值不可见",
          "h40w" not in data["db1"] and "h40w" not in data["db2"])


def h4_half_commit_takeover():
    print("[H4] 半批接管：guards 事务已决定提交仅 db1 应用，新代补齐并释放范围锁")
    txid = "h4t"
    writes = W({"h50k": "a"}, {"h50k": "a"})
    guards = G([["h50a", "h50z", {}]], [["h50a", "h50z", {}]])
    body = {"txid": txid, "writes": writes, "guards": guards}
    post(DB1_URL, "/prepare?epoch=3", body)
    post(DB2_URL, "/prepare?epoch=3", body)
    post(DB1_URL, "/commit?epoch=3", {"txid": txid})
    check("现场 db1 COMMITTED", shard_state(DB1_URL, txid) == "COMMITTED")
    check("现场 db2 PREPARED 持范围锁",
          shard_state(DB2_URL, txid) == "PREPARED"
          and any(l["txid"] == txid
                  for l in shard_locks(DB2_URL)["range_locks"]))
    insert_committed(txid, writes, guards)

    # 旧 h1（代3）与旧 h2（代2）均可能存活占端口：先杀，再全新进程接管
    kill(("coord", "h1"))
    kill(("coord", "h2"))
    forget_local_epoch("h2")  # h2 作为全新进程重启，领取代 4
    start_coord("h2")  # 领号 4，恢复：补发 commit 给 db2
    check("h2 领到代 4", coord_debug("h2")["epoch"] == 4)
    end = time.time() + 10
    while time.time() < end and shard_state(DB2_URL, txid) != "COMMITTED":
        time.sleep(0.1)
    check("db2 补齐 COMMITTED", shard_state(DB2_URL, txid) == "COMMITTED")
    check("db1 保持 COMMITTED", shard_state(DB1_URL, txid) == "COMMITTED")
    data = get(COORD_URLS["h2"], "/data?epoch=4")[1]["data"]
    check("两库 h50k 均为 a（整批补齐）",
          data["db1"].get("h50k") == "a" and data["db2"].get("h50k") == "a",
          data)
    check("范围锁终结释放（两库）",
          shard_locks(DB1_URL)["range_locks"] == []
          and shard_locks(DB2_URL)["range_locks"] == [])
    check("写键锁终结释放（两库）",
          shard_locks(DB1_URL)["locks"] == []
          and shard_locks(DB2_URL)["locks"] == [])


def h5_stale_and_no_revalidate():
    print("[H5] 旧代 prepare 409 不重验不促成决定；当代重试不重验")
    # 旧代（3）对新 txid 发 prepare：stale，不留痕
    code, r = post(DB1_URL, "/prepare?epoch=3",
                   {"txid": "h5late", "writes": W({"h60k": "1"}, {}),
                    "guards": G([["h60a", "h60z", {}]])})
    check("旧代 prepare 409 stale",
          code == 409 and r.get("reason") == "stale_epoch", r)
    check("stale 不留事务痕迹", shard_state(DB1_URL, "h5late") is None)
    check("stale 不留范围锁",
          all(l["txid"] != "h5late"
              for l in shard_locks(DB1_URL)["range_locks"]))

    # 当代丢 prepare 应答后重发：幂等且 guard 只评估一次
    post(DB1_URL, "/control/drop-next", {"phase": "prepare"})
    before = get(DB1_URL, "/debug/stats")[1]["stats"].get("guard_eval", 0)
    body = {"txid": "h5r", "writes": W({}, {}),
            "guards": G([["h60a", "h60z", {}]])}
    try:
        post(DB1_URL, "/prepare?epoch=4", body, timeout=5)
    except OSError:
        pass
    check("丢应答后已落盘 PREPARED", shard_state(DB1_URL, "h5r") == "PREPARED")
    code, r = post(DB1_URL, "/prepare?epoch=4", body)
    check("当代重发幂等 PREPARED 不重验",
          code == 200 and r["vote"] == "PREPARED", r)
    after = get(DB1_URL, "/debug/stats")[1]["stats"].get("guard_eval", 0)
    check("guard_eval 只增 1", after == before + 1, (before, after))
    post(DB1_URL, "/abort?epoch=4", {"txid": "h5r"})

    # 同号不同 guards 属异内容：即使代号正确也 409（内容先于锁/条件）
    code, r = post(DB1_URL, "/prepare?epoch=4",
                   {"txid": "h5r", "writes": W({}, {}),
                    "guards": G([["h60a", "h60z", {"h60x": None}]])})
    check("当代异内容 409（非 stale）",
          code == 409 and r.get("reason") is None and "不一致" in r["error"], r)


def main():
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    with contextlib.suppress(OSError):
        subprocess.run(["pkill", "-f", "twopc/(shard|coordinator)\\.py"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.3)

    start_shard("db1", DB1_PORT)
    start_shard("db2", DB2_PORT)
    start_coord("h1")  # 代 1
    try:
        h1_basic_and_validation()
        h2_range_lock_survives_install()
        h3_takeover_abort_pending()
        h4_half_commit_takeover()
        h5_stale_and_no_revalidate()
    finally:
        stop_all()

    print("\n============ fenced × guards 验收结果 ============")
    print("PASS=%d  FAIL=%d" % (PASS, FAIL))
    if FAIL:
        print("进程日志位于 %s/*.log" % WORK)
        sys.exit(1)
    print("全部通过 ✔")


if __name__ == "__main__":
    main()
