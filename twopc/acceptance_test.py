#!/usr/bin/env python3
"""三进程端到端验收脚本（协调者 + db1 + db2，各自独立进程与 SQLite 库）。

覆盖验收点：
  1. 正常整批提交：两库各自写入，null 与字符串同批保留、新增/替换、空写集
  2. 持键冲突：交错事务抢同一键，立即反对 -> 整批 abort，锁最终释放
  3. 撤销先到：abort 先于 prepare 持久终态，迟到 prepare 不复活
  4. 提交响应丢失：分库落盘后丢应答，协调者重试幂等，不重复应用
  5. 准备应答丢失：同样幂等重试
  6. 强退点 A：单库 prepare 后强退协调者 -> 重启撤销，准备值不可见，锁释放
  7. 强退点 B：commit 落盘且仅一库应用后强退 -> 重启补齐，半批不暴露，
     同号重试不重复应用（stats 核对）
  8. 同 txid 同 JSON 重提返回原状态；异内容拒绝（且先于锁冲突）
  9. 非法输入逐字段定位且不建事务
 10. 多事务交错、字节序查询、分库重启持久化
"""

import http.client
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
WORK = os.path.join(HERE, "run_acceptance")

COORD_PORT, DB1_PORT, DB2_PORT = 18010, 18011, 18012
COORD_URL = "http://127.0.0.1:%d" % COORD_PORT
DB1_URL = "http://127.0.0.1:%d" % DB1_PORT
DB2_URL = "http://127.0.0.1:%d" % DB2_PORT

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

def _req(base, method, path, body=None, raw=None, timeout=10):
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


def post(base, path, body, **kw):
    return _req(base, "POST", path, body=body, **kw)


def get(base, path, **kw):
    return _req(base, "GET", path, **kw)


def submit(txid, writes):
    return post(COORD_URL, "/submit", {"txid": txid, "writes": writes})


def wait_health(url, timeout=15):
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


def wait_status(txid, expect, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        code, data = get(COORD_URL, "/txn/%s/status" % txid, timeout=2)
        if code == 200 and data.get("state") == expect:
            return True
        time.sleep(0.1)
    return False


def shard_state(shard, txid):
    code, data = get(shard, "/txn/%s/status" % txid)
    return data.get("state") if code == 200 else None


def shard_locks(shard):
    return get(shard, "/debug/locks")[1]["locks"]


def shard_stats(shard):
    return get(shard, "/debug/stats")[1]["stats"]


def both_data():
    return get(COORD_URL, "/data")[1]["data"]


# ---------------------------------------------------------------- 进程管理

procs = {}
logfiles = {}


def start_shard(name, port):
    db = os.path.join(WORK, "%s.sqlite" % name)
    log = open(os.path.join(WORK, "%s.log" % name), "a")
    logfiles[name] = log
    p = subprocess.Popen(
        [PY, "-I", os.path.join(HERE, "shard.py"),
         "--name", name, "--port", str(port), "--db", db],
        stdout=log, stderr=subprocess.STDOUT,
    )
    procs[name] = p
    url = DB1_URL if name == "db1" else DB2_URL
    assert wait_health(url), "%s 启动失败" % name


def start_coord():
    db = os.path.join(WORK, "coord.sqlite")
    log = open(os.path.join(WORK, "coord.log"), "a")
    logfiles["coord"] = log
    p = subprocess.Popen(
        [PY, "-I", os.path.join(HERE, "coordinator.py"),
         "--port", str(COORD_PORT), "--db", db,
         "--db1-url", DB1_URL, "--db2-url", DB2_URL],
        stdout=log, stderr=subprocess.STDOUT,
    )
    procs["coord"] = p
    assert wait_health(COORD_URL), "协调者启动失败"


def kill(name):
    p = procs.get(name)
    if p and p.poll() is None:
        p.send_signal(signal.SIGKILL)
        p.wait(timeout=10)


def start_all():
    start_shard("db1", DB1_PORT)
    start_shard("db2", DB2_PORT)
    start_coord()


def stop_all():
    for name in ("coord", "db1", "db2"):
        kill(name)
    for f in logfiles.values():
        f.close()
    logfiles.clear()


def W(db1=None, db2=None):
    return {"db1": db1 or {}, "db2": db2 or {}}


# ================================================================= 测试用例

def t01_basic_commit():
    print("[01] 正常整批提交：字符串与 null 同批、新增/替换、空写集、字节序")
    code, r = submit("t1a", W({"k1": "a"}, {"k1": None}))
    check("t1a committed", code == 200 and r["state"] == "COMMITTED", r)

    data = both_data()
    check("db1 保留 k1=a", data["db1"] == {"k1": "a"}, data)
    check("db2 保留 k1=null", data["db2"] == {"k1": None}, data)

    # 替换：字符串 -> 新字符串；null 覆盖旧字符串；新增键
    code, r = submit("t1b", W({"k1": "v2", "k2": "x"}, {"k1": None, "k9": "z"}))
    check("t1b committed", code == 200 and r["state"] == "COMMITTED", r)
    data = both_data()
    check("db1 替换+新增", data["db1"] == {"k1": "v2", "k2": "x"}, data)
    check("db2 null 覆盖 + 新增", data["db2"] == {"k1": None, "k9": "z"}, data)

    # 空写集合法
    code, r = submit("t1c", W({}, {}))
    check("空写集 committed", code == 200 and r["state"] == "COMMITTED", r)

    # 字节序：k10 必须排在 k2 之前（字典序/字节序一致）
    submit("t1d", W({"k10": "ten", "k2": "two", "k1": "one"}, {}))
    data = get(DB1_URL, "/data")[1]["data"]
    check("按 key 字节序返回", list(data.keys()) == sorted(data.keys(),
          key=lambda k: k.encode()), data)

    # 准备值不可见：构造一个持锁中的 PREPARED 事务并直接查分库
    # （借助冲突让协调者停在 abort 决定前不现实，改为检查已提交后无残留锁）
    check("提交后 db1 无残留锁", shard_locks(DB1_URL) == [])
    check("提交后 db2 无残留锁", shard_locks(DB2_URL) == [])


def t02_lock_conflict():
    print("[02] 持键冲突：交错 prepare，遇锁立即反对，整批 abort 并释放锁")
    barrier = threading.Event()
    results = {}

    def first():
        # 先在 db1 上把 kc 锁进 PREPARED：让 db2 侧 prepare 被卡住，
        # 通过对 db2 预置“撤销先到”做不到持锁；改为直接调用分库接口持锁。
        code, r = post(DB1_URL, "/prepare",
                       {"txid": "hold1", "writes": W({"kc": "held"}, {})})
        results["hold1"] = (code, r)
        barrier.set()

    th = threading.Thread(target=first)
    th.start()
    barrier.wait(5)
    th.join()
    check("db1 手动 prepare 成功持锁",
          results["hold1"][0] == 200
          and results["hold1"][1]["vote"] == "PREPARED", results)

    # 协调者事务在 db1 抢同一键 -> 立即反对（不等待），整批 abort
    t0 = time.time()
    code, r = submit("t2a", W({"kc": "try"}, {"x": "y"}))
    dt = time.time() - t0
    check("t2a aborted", code == 200 and r["state"] == "ABORTED", r)
    check("冲突立即反对不等待 (<3s)", dt < 3.0, "耗时 %.2fs" % dt)
    check("db1 中 t2a 为 ABORTED", shard_state(DB1_URL, "t2a") == "ABORTED")
    check("db2 中 t2a 同样 ABORTED", shard_state(DB2_URL, "t2a") == "ABORTED")
    check("kc 对可见数据无影响", both_data()["db1"].get("kc") is None
          and "kc" not in both_data()["db1"])

    # 释放 hold1 后同键新事务可成功
    post(DB1_URL, "/abort", {"txid": "hold1"})
    code, r = submit("t2b", W({"kc": "now-ok"}, {}))
    check("锁释放后同键事务可提交", code == 200 and r["state"] == "COMMITTED", r)
    check("无残留锁", shard_locks(DB1_URL) == [])


def t03_abort_before_prepare():
    print("[03] 撤销先到：abort 先持久终态，迟到 prepare 不复活，均幂等")
    code, _ = post(DB1_URL, "/abort", {"txid": "early1"})
    check("先到 abort 被接受", code == 200)
    check("db1 持久 ABORTED", shard_state(DB1_URL, "early1") == "ABORTED")

    code, r = post(DB1_URL, "/prepare",
                   {"txid": "early1", "writes": W({"e": "1"}, {})})
    check("迟到 prepare 不复活", code == 409 and r["vote"] == "ABORTED", r)
    check("终态仍 ABORTED", shard_state(DB1_URL, "early1") == "ABORTED")

    code, _ = post(DB1_URL, "/abort", {"txid": "early1"})
    check("重复 abort 幂等", code == 200)
    code, r = post(DB1_URL, "/commit", {"txid": "early1"})
    check("终态不得反转为 commit", code == 409, r)
    check("未持任何锁", all(l["txid"] != "early1" for l in shard_locks(DB1_URL)))


def t04_lost_responses():
    print("[04] 响应丢失：prepare/commit 丢应答后重试幂等，不重复应用")
    # commit 应答丢失（分库已落盘应用）
    post(DB1_URL, "/control/drop-next", {"phase": "commit"})
    post(DB2_URL, "/control/drop-next", {"phase": "commit"})
    before = shard_stats(DB1_URL).get("commit_applied", 0)
    code, r = submit("t4a", W({"k4": "four"}, {"k4": "four"}))
    check("t4a 最终 committed", code == 200 and r["state"] == "COMMITTED", r)
    time.sleep(0.3)
    after = shard_stats(DB1_URL).get("commit_applied", 0)
    check("db1 只应用一次", after == before + 1, (before, after))
    check("数据正确", both_data()["db1"].get("k4") == "four")

    # prepare 应答丢失：协调者重发 prepare，分库幂等返回
    post(DB1_URL, "/control/drop-next", {"phase": "prepare"})
    code, r = submit("t4b", W({"k4b": "b"}, {}))
    check("prepare 丢应答后仍 committed",
          code == 200 and r["state"] == "COMMITTED", r)
    check("db1 t4b 已提交", shard_state(DB1_URL, "t4b") == "COMMITTED")


def t05_crash_after_first_prepare():
    print("[05] 强退点 A：单库 prepare 后强退协调者，重启撤销、值不可见、锁释放")
    post(COORD_URL, "/control/crash-after-first-prepare", {})
    try:
        submit("t5a", W({"k5": "five"}, {"k5": "five"}))
        check("强退发生时请求中断", False, "未崩溃")
    except OSError:
        pass
    time.sleep(0.5)
    check("协调者确已退出", procs["coord"].poll() is not None)

    # 重启前：db1 处于 PREPARED 且持锁，值不可见
    check("db1 为 PREPARED", shard_state(DB1_URL, "t5a") == "PREPARED")
    check("db1 持有 k5 锁",
          any(l["txid"] == "t5a" and l["key"] == "k5"
              for l in shard_locks(DB1_URL)))
    check("准备值不可见", "k5" not in get(DB1_URL, "/data")[1]["data"])

    start_coord()  # 恢复：无决定事务 -> abort 两库
    check("重启后 t5a 终态 ABORTED", wait_status("t5a", "ABORTED"))
    check("db1 终态 ABORTED", shard_state(DB1_URL, "t5a") == "ABORTED")
    check("db2 终态 ABORTED", shard_state(DB2_URL, "t5a") == "ABORTED")
    check("锁已释放", shard_locks(DB1_URL) == [])
    data = both_data()
    check("两库均无 k5（半批不暴露）",
          "k5" not in data["db1"] and "k5" not in data["db2"], data)

    # 同 txid 同 JSON 重提：终态已定，返回原状态，不复活、不重复准备
    code, r = submit("t5a", W({"k5": "five"}, {"k5": "five"}))
    check("同号重提返回原 ABORTED", code == 200 and r["state"] == "ABORTED", r)
    check("仍无锁", shard_locks(DB1_URL) == [])


def t06_crash_after_commit_decision():
    print("[06] 强退点 B：commit 落盘、仅一库应用后强退，重启补齐且不重复应用")
    post(COORD_URL, "/control/crash-after-first-commit", {})
    try:
        submit("t6a", W({"k6": "six"}, {"k6": "six"}))
        check("强退发生时请求中断", False, "未崩溃")
    except OSError:
        pass
    time.sleep(0.5)
    check("协调者确已退出", procs["coord"].poll() is not None)

    # 重启前现场：协调者已 COMMITTED；db1 已应用，db2 仍 PREPARED
    check("db1 已 COMMITTED", shard_state(DB1_URL, "t6a") == "COMMITTED")
    check("db2 仍 PREPARED", shard_state(DB2_URL, "t6a") == "PREPARED")
    check("db2 持锁",
          any(l["txid"] == "t6a" for l in shard_locks(DB2_URL)))
    applied_before = shard_stats(DB1_URL).get("commit_applied", 0)

    start_coord()  # 恢复：已有 commit 决定，继续向双方补发
    check("重启后 t6a COMMITTED", wait_status("t6a", "COMMITTED"))
    check("db2 补齐 COMMITTED", shard_state(DB2_URL, "t6a") == "COMMITTED")
    check("db2 锁释放", shard_locks(DB2_URL) == [])

    data = both_data()
    check("两库同时可见 k6（整批等价）",
          data["db1"].get("k6") == "six" and data["db2"].get("k6") == "six", data)

    # 同号重试不重复应用
    applied_after = shard_stats(DB1_URL).get("commit_applied", 0)
    check("db1 应用计数未增加", applied_after == applied_before,
          (applied_before, applied_after))
    code, r = post(DB1_URL, "/commit", {"txid": "t6a"})
    check("分库重复 commit 幂等", code == 200 and "幂等" in r["note"], r)
    check("应用计数仍不变",
          shard_stats(DB1_URL).get("commit_applied", 0) == applied_after)

    # 异内容重提被拒绝
    code, r = submit("t6a", W({"k6": "different"}, {"k6": "six"}))
    check("同 txid 异内容拒绝", code == 409, r)


def t07_idempotency_and_content():
    print("[07] 重复提交：同 JSON 返回原状态；异内容先于锁冲突拒绝")
    code, r = submit("t7a", W({"k7": "7"}, {"k7": None}))
    check("t7a committed", code == 200 and r["state"] == "COMMITTED", r)
    code, r2 = submit("t7a", W({"k7": "7"}, {"k7": None}))
    check("同 JSON 重提返回原状态", code == 200 and r2["state"] == "COMMITTED", r2)

    # 异内容：协调者层直接 409，不触碰分库锁
    code, r = submit("t7a", W({"k7": "8"}, {"k7": None}))
    check("协调者拒绝异内容", code == 409, r)

    # 先制造分库持锁，再用异内容 prepare：内容冲突必须先于锁冲突
    post(DB1_URL, "/prepare",
         {"txid": "hold7", "writes": W({"shared": "x"}, {})})
    code, r = post(DB1_URL, "/prepare",
                   {"txid": "hold7", "writes": W({"shared": "DIFFERENT"}, {})})
    check("异内容先于锁冲突判断", code == 409 and "不一致" in r["error"], r)
    post(DB1_URL, "/abort", {"txid": "hold7"})


def t08_invalid_inputs():
    print("[08] 非法输入逐字段定位，且不建事务")
    cases = [
        (None, "body"),
        ("not-json-raw", "body"),
        ({}, "txid"),
        ({"txid": ""}, "txid"),
        ({"txid": "bad-id!"}, "txid"),
        ({"txid": 123}, "txid"),
        ({"txid": "x1"}, "writes"),
        ({"txid": "x1", "writes": []}, "writes"),
        ({"txid": "x1", "writes": {"db1": {}}}, "writes"),
        ({"txid": "x1", "writes": {"db1": {}, "db3": {}}}, "writes"),
        ({"txid": "x1", "writes": {"db1": {}, "db2": []}}, "writes.db2"),
        ({"txid": "x1", "writes": {"db1": {"": "v"}, "db2": {}}}, "writes.db1"),
        ({"txid": "x1", "writes": {"db1": {"k ": "v"}, "db2": {}}}, "writes.db1"),
        ({"txid": "x1", "writes": {"db1": {"k": 1}, "db2": {}}}, "writes.db1"),
        ({"txid": "x1", "writes": {"db1": {"k": True}, "db2": {}}}, "writes.db1"),
    ]
    for body, field in cases:
        if isinstance(body, str):
            code, r = _req(COORD_URL, "POST", "/submit", raw=body.encode())
        else:
            code, r = _req(COORD_URL, "POST", "/submit", body=body)
        ok = code == 400 and isinstance(r, dict) and field in r.get("error", "")
        check("非法输入定位 %s" % field, ok, (code, r))

    check("非法 txid 不建事务", get(COORD_URL, "/txn/x1/status")[0] == 404)
    check("分库无 x1 痕迹", shard_state(DB1_URL, "x1") is None
          and shard_state(DB2_URL, "x1") is None)


def t09_interleave_and_shard_restart():
    print("[09] 多事务交错提交 + 分库重启保留身份与结局")
    threads = []
    outs = [None] * 4
    def worker(i):
        txid = "t9%d" % i
        code, r = submit(txid, W({"k%d" % i: "v%d" % i},
                                 {"q%d" % i: None}))
        outs[i] = (code, r)
    for i in range(4):
        th = threading.Thread(target=worker, args=(i,))
        threads.append(th)
        th.start()
    for th in threads:
        th.join()
    check("4 个交错事务全部 committed",
          all(o[0] == 200 and o[1]["state"] == "COMMITTED" for o in outs), outs)

    data = both_data()
    check("db1 四键齐全",
          all(("k%d" % i) in data["db1"] for i in range(4)), data)
    check("db2 四 null 齐全",
          all(data["db2"].get("q%d" % i) is None and ("q%d" % i) in data["db2"]
              for i in range(4)), data)
    check("全部锁已释放",
          shard_locks(DB1_URL) == [] and shard_locks(DB2_URL) == [])

    # 重启 db1：身份与结局保留，数据不丢
    kill("db1")
    start_shard("db1", DB1_PORT)
    check("db1 重启后历史事务可查",
          shard_state(DB1_URL, "t90") == "COMMITTED")
    check("db1 重启后数据保留",
          both_data()["db1"].get("k0") == "v0")


def t10_network_partition():
    print("[10] 分库断连：db2 宕机期间提交，协调者等待不推翻，恢复后整批完成")
    kill("db2")
    time.sleep(0.3)
    box = {}

    def blocked_submit():
        box["resp"] = submit("t10a", W({"kp10": "ten"}, {"kp10": "ten"}))

    th = threading.Thread(target=blocked_submit)
    th.start()

    # 协调者应停在等待状态：db1 已 PREPARED 持锁，决定未落盘
    end = time.time() + 10
    while time.time() < end and shard_state(DB1_URL, "t10a") != "PREPARED":
        time.sleep(0.1)
    check("db1 已 PREPARED，协调者等待 db2",
          shard_state(DB1_URL, "t10a") == "PREPARED")
    check("协调者状态仍 PENDING（未以超时作决定）",
          get(COORD_URL, "/txn/t10a/status")[1]["state"] == "PENDING")
    check("准备值不可见", "kp10" not in get(DB1_URL, "/data")[1]["data"])
    time.sleep(1.5)
    check("断连期间仍在等待", th.is_alive())

    start_shard("db2", DB2_PORT)   # 恢复连通
    th.join(timeout=20)
    check("恢复后提交返回成功",
          box.get("resp", (None,))[0] == 200
          and box["resp"][1]["state"] == "COMMITTED", box)
    check("终态 COMMITTED", wait_status("t10a", "COMMITTED"))
    data = both_data()
    check("两库整批可见",
          data["db1"].get("kp10") == "ten" and data["db2"].get("kp10") == "ten", data)
    check("锁全部释放",
          shard_locks(DB1_URL) == [] and shard_locks(DB2_URL) == [])


def main():
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)

    start_all()
    try:
        t01_basic_commit()
        t02_lock_conflict()
        t03_abort_before_prepare()
        t04_lost_responses()
        t05_crash_after_first_prepare()   # 内部会重启协调者
        t06_crash_after_commit_decision()
        t07_idempotency_and_content()
        t08_invalid_inputs()
        t09_interleave_and_shard_restart()
        t10_network_partition()
    finally:
        stop_all()

    print("\n================ 验收结果 ================")
    print("PASS=%d  FAIL=%d" % (PASS, FAIL))
    if FAIL:
        print("进程日志位于 %s/*.log" % WORK)
        sys.exit(1)
    print("全部通过 ✔")


if __name__ == "__main__":
    main()
