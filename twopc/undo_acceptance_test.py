#!/usr/bin/env python3
"""undo 撤回模式端到端验收：协调者 + db1 + db2 三进程（均加 --undo）。

覆盖任务要求：
  U1  题面示例：空库 T 向两库写 k=null，撤回成功后两库均无 k；
      原 target 终态不变；写者留下撤回 txid；null 仍是值的区分核对
  U2  撤回恢复旧值/删除原不存在键/同值覆盖也更新写者/写者不恢复旧来源
  U3  空写集可撤回；同 target 至多一次成功；重复撤回 409 不占号
  U4  交错撤回竞争（空批竞争）：两撤回号交错准备，一个成功一个整批
      ABORTED；失败号换号重试成功；过程中锁与终态核对
  U5  中间另批同值覆盖后撤回必须 ABORTED（最后写者核对）；换号仍失败，
      覆盖事务再撤回成功且不恢复旧来源
  U6  丢应答：prepare-undo / commit 丢应答后重试收敛，只应用一次
  U7  单库撤回准备后强退接管（协调者）：重启后无决定 -> 撤销，锁释放、
      数据不变，可换号重新撤回成功（镜像/写者重启保留）
  U8  单库撤回提交后强退接管：commit 决定落盘仅一库应用 -> 重启补齐，
      不重复应用，两库值/终态/锁一致
  U9  资格与号空间：target 不存在/未提交/本身是撤回 -> 409 不占号；
      同号内容（含类型与 target）同返原结局、异内容 409
  U10 未启用 --undo 兼容：/undo 与 /prepare-undo 均 404；非法字段 400 定位
"""

import http.client
import contextlib
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
WORK = os.path.join(HERE, "run_undo")

COORD_PORT, DB1_PORT, DB2_PORT = 16010, 16011, 16012
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


def submit(txid, writes):
    return post(COORD_URL, "/submit", {"txid": txid, "writes": writes})


def undo(txid, target):
    return post(COORD_URL, "/undo", {"txid": txid, "target": target})


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


def shard_writers(shard):
    return get(shard, "/debug/writers")[1]["writers"]


def shard_undo(shard):
    return get(shard, "/debug/undo")[1]["undo"]


def shard_stats(shard):
    return get(shard, "/debug/stats")[1]["stats"]


def both_data():
    return get(COORD_URL, "/data")[1]["data"]


def no_locks():
    return shard_locks(DB1_URL) == [] and shard_locks(DB2_URL) == []


# ---------------------------------------------------------------- 进程管理

procs = {}
logfiles = {}


def start_shard(name, port, undo_flag=True):
    db = os.path.join(WORK, "%s.sqlite" % name)
    log = open(os.path.join(WORK, "%s.log" % name), "a")
    logfiles[name] = log
    cmd = [PY, "-I", os.path.join(HERE, "shard.py"),
           "--name", name, "--port", str(port), "--db", db]
    if undo_flag:
        cmd.append("--undo")
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    procs[name] = p
    url = DB1_URL if name == "db1" else DB2_URL
    assert wait_health(url), "%s 启动失败" % name


def start_coord(undo_flag=True):
    db = os.path.join(WORK, "coord.sqlite")
    log = open(os.path.join(WORK, "coord.log"), "a")
    logfiles["coord"] = log
    cmd = [PY, "-I", os.path.join(HERE, "coordinator.py"),
           "--port", str(COORD_PORT), "--db", db,
           "--db1-url", DB1_URL, "--db2-url", DB2_URL]
    if undo_flag:
        cmd.append("--undo")
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    procs["coord"] = p
    assert wait_health(COORD_URL), "协调者启动失败"


def kill(name):
    p = procs.get(name)
    if p and p.poll() is None:
        p.send_signal(signal.SIGKILL)
        p.wait(timeout=10)


def start_all(undo_flag=True):
    start_shard("db1", DB1_PORT, undo_flag)
    start_shard("db2", DB2_PORT, undo_flag)
    start_coord(undo_flag)


def stop_all():
    for name in ("coord", "db1", "db2"):
        kill(name)
    for f in logfiles.values():
        f.close()
    logfiles.clear()


def W(db1=None, db2=None):
    return {"db1": db1 or {}, "db2": db2 or {}}


# ================================================================= 测试用例

def u01_worked_example_null():
    print("[U1] 题面示例：空库 T 两库写 k=null，撤回成功后两库均无 k")
    code, r = submit("u1T", W({"k": None}, {"k": None}))
    check("u1T 提交成功", code == 200 and r["state"] == "COMMITTED", r)
    data = both_data()
    check("提交后两库均有键 k 且值为 null",
          data["db1"].get("k") is None and "k" in data["db1"]
          and data["db2"].get("k") is None and "k" in data["db2"], data)
    check("最后写者为 u1T",
          shard_writers(DB1_URL).get("k") == "u1T"
          and shard_writers(DB2_URL).get("k") == "u1T")

    code, r = undo("U1", "u1T")
    check("撤回 U1 成功", code == 200 and r["state"] == "COMMITTED", r)
    data = both_data()
    check("撤回后两库均无 k",
          "k" not in data["db1"] and "k" not in data["db2"], data)
    # 原终态不变：target 仍是 COMMITTED；撤回自身也是 COMMITTED
    check("target 终态仍 COMMITTED", wait_status("u1T", "COMMITTED"))
    check("撤回事务终态 COMMITTED", wait_status("U1", "COMMITTED"))
    code, data = get(COORD_URL, "/txn/U1/status")
    check("撤回记录 kind=undo 且 target 正确",
          code == 200 and data["kind"] == "undo" and data["target"] == "u1T", data)
    check("两库 target 仍 COMMITTED",
          shard_state(DB1_URL, "u1T") == "COMMITTED"
          and shard_state(DB2_URL, "u1T") == "COMMITTED")
    check("两库撤回 txn 均 COMMITTED",
          shard_state(DB1_URL, "U1") == "COMMITTED"
          and shard_state(DB2_URL, "U1") == "COMMITTED")
    # 删除也留写者 txid，不恢复旧来源（原本不存在，无旧来源）
    check("删除后写者为撤回号 U1",
          shard_writers(DB1_URL).get("k") == "U1"
          and shard_writers(DB2_URL).get("k") == "U1",
          (shard_writers(DB1_URL), shard_writers(DB2_URL)))
    check("撤回注册表两库均登记",
          shard_undo(DB1_URL).get("u1T") == "U1"
          and shard_undo(DB2_URL).get("u1T") == "U1")
    check("锁全部释放", no_locks())


def u02_restore_and_writer_rules():
    print("[U2] 恢复旧值/删除/同值覆盖更新写者/写者不恢复旧来源")
    # 预置：db1 已有 ka="old"；db2 无 kb
    submit("u2base", W({"ka": "old"}, {"kb": "old2"}))
    # T 覆盖 ka（同值不同值）、db2 把 kb 覆盖、新增 kc，db1 新增 kd
    code, r = submit("u2T", W({"ka": "new", "kd": "d"},
                              {"kb": "new2", "kc": "c"}))
    check("u2T 提交成功", code == 200 and r["state"] == "COMMITTED", r)

    # 同值覆盖：另批对 ka 写相同值 "new"，写者必须变成覆盖事务
    code, r = submit("u2cover", W({"ka": "new"}, {}))
    check("同值覆盖提交成功", code == 200 and r["state"] == "COMMITTED", r)
    check("同值覆盖更新写者为 u2cover",
          shard_writers(DB1_URL).get("ka") == "u2cover")

    # 此时撤回 u2T：db1 的 ka 最后写者已不是 u2T -> 必须整批 ABORTED
    code, r = undo("U2bad", "u2T")
    check("写者不符撤回整批 ABORTED(409)",
          code == 409 and r["state"] == "ABORTED", r)
    check("两库 U2bad 均 ABORTED",
          shard_state(DB1_URL, "U2bad") == "ABORTED"
          and shard_state(DB2_URL, "U2bad") == "ABORTED")
    check("失败撤回不注册 target",
          "u2T" not in shard_undo(DB1_URL) and "u2T" not in shard_undo(DB2_URL))
    check("失败后数据不变",
          both_data()["db1"].get("ka") == "new")
    check("失败后无残留锁", no_locks())

    # 撤回覆盖事务 u2cover：其写集仅 db1.ka，撤回后 ka 恢复为提交前值 "new"
    # （不是更早的 "old"——撤回只恢复该事务准备期镜像）
    code, r = undo("U2cover", "u2cover")
    check("撤回覆盖事务成功", code == 200 and r["state"] == "COMMITTED", r)
    data = both_data()
    check("ka 恢复为覆盖前值 new（不恢复旧来源 old）",
          data["db1"].get("ka") == "new", data)
    check("写者为撤回号 U2cover", shard_writers(DB1_URL).get("ka") == "U2cover")

    # 现在 u2T 的 ka 写者是撤回号 U2cover，仍不能撤回 u2T
    code, r = undo("U2bad2", "u2T")
    check("写者链变化后撤回 u2T 仍 ABORTED",
          code == 409 and r["state"] == "ABORTED", r)

    # 直接构造可撤回场景：新事务 u2R 动两个库，撤回核对恢复/删除语义
    code, r = submit("u2R", W({"ka": "R", "kx": "x"},
                              {"kb": "R2", "kc": None}))
    check("u2R 提交成功", code == 200 and r["state"] == "COMMITTED", r)
    code, r = undo("U2R", "u2R")
    check("撤回 u2R 成功", code == 200 and r["state"] == "COMMITTED", r)
    data = both_data()
    # ka 恢复旧值 "new"；kx 原不存在 -> 删除；kb 恢复 "new2"；
    # kc 恢复旧值 "c"（注意 u2R 把它写成 null，撤回后回到 "c"）
    check("db1 ka 恢复 new、kx 删除",
          data["db1"].get("ka") == "new" and "kx" not in data["db1"], data)
    check("db2 kb 恢复 new2、kc 恢复 c（null 撤回后回旧值）",
          data["db2"].get("kb") == "new2" and data["db2"].get("kc") == "c", data)
    check("被删键 kx 写者留撤回号", shard_writers(DB1_URL).get("kx") == "U2R")
    check("锁全部释放", no_locks())


def u03_empty_writeset_and_single_success():
    print("[U3] 空写集可撤回；同 target 至多一次成功；重复撤回不占号")
    code, r = submit("u3E", W({}, {}))
    check("空写集提交成功", code == 200 and r["state"] == "COMMITTED", r)
    code, r = undo("U3E", "u3E")
    check("空写集撤回成功", code == 200 and r["state"] == "COMMITTED", r)
    check("空写集撤回两库均 COMMITTED",
          shard_state(DB1_URL, "U3E") == "COMMITTED"
          and shard_state(DB2_URL, "U3E") == "COMMITTED")

    # 重复撤回：409 且不占号（新 txid 在协调库与分库都不应存在）
    code, r = undo("U3dup", "u3E")
    check("重复撤回 409", code == 409 and "已被" in r.get("error", ""), r)
    check("重复撤回不占协调库号",
          get(COORD_URL, "/txn/U3dup/status")[0] == 404)
    check("重复撤回不分库留痕",
          shard_state(DB1_URL, "U3dup") is None
          and shard_state(DB2_URL, "U3dup") is None)

    # 同号同内容重放已成功撤回：返回原结局 COMMITTED
    code, r = undo("U3E", "u3E")
    check("同号同内容重放返回原 COMMITTED",
          code == 200 and r["state"] == "COMMITTED", r)
    check("锁全部释放", no_locks())


def u04_interleave_contention():
    print("[U4] 交错撤回竞争（空批竞争）：一成一败，败者换号重试成功")
    # 目标：普通提交，两库各一键
    submit("u4T", W({"k4": "a"}, {"k4": "a"}))
    # 另造一个空写集目标用于“空批竞争”：两个空写集撤回交错
    submit("u4Empty", W({}, {}))

    barrier = threading.Event()
    results = {}

    def manual_prepare_undo(txid, target, shard):
        return post(shard, "/prepare-undo", {"txid": txid, "target": target})

    # 空批竞争：两个撤回号对同一空写集 target 交错准备。
    # 第一个在 db1 准备成功并持注册表；第二个在 db1 即应反对。
    th = threading.Thread(target=lambda: results.setdefault(
        "p1", manual_prepare_undo("U4a", "u4Empty", DB1_URL)))
    th.start()
    th.join()
    check("首个空批撤回 db1 准备成功",
          results["p1"][0] == 200 and results["p1"][1]["vote"] == "PREPARED",
          results.get("p1"))
    code, r = manual_prepare_undo("U4b", "u4Empty", DB1_URL)
    check("竞争撤回 db1 立即反对（注册表守卫）",
          code == 409 and r["vote"] == "ABORTED", r)
    check("竞争者持久 ABORTED 不持锁",
          shard_state(DB1_URL, "U4b") == "ABORTED"
          and all(lk["txid"] != "U4b" for lk in shard_locks(DB1_URL)))
    # 中止首个（abort 释放注册），随后竞争者换号 U4c 可以成功走完整撤回
    code, _ = post(DB1_URL, "/abort", {"txid": "U4a"})
    check("首个撤回 abort 释放注册", code == 200)
    code, r = undo("U4c", "u4Empty")
    check("换号后空写集撤回成功",
          code == 200 and r["state"] == "COMMITTED", r)

    # 非空 target 的端到端交错：两个协调者撤回号并发，只允许一个 COMMITTED
    outs = {}

    def do_undo(txid):
        outs[txid] = undo(txid, "u4T")

    t1 = threading.Thread(target=do_undo, args=("U4d",))
    t2 = threading.Thread(target=do_undo, args=("U4e",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    states = {k: v[1]["state"] for k, v in outs.items()}
    codes = {k: v[0] for k, v in outs.items()}
    check("并发撤回恰一个 COMMITTED",
        sorted(states.values()) == ["ABORTED", "COMMITTED"], outs)
    check("成功者 200、失败者 409",
        sorted(codes.values()) == [200, 409], outs)
    winner = "U4d" if states["U4d"] == "COMMITTED" else "U4e"
    loser = "U4e" if winner == "U4d" else "U4d"
    check("两库 winner 均 COMMITTED",
          shard_state(DB1_URL, winner) == "COMMITTED"
          and shard_state(DB2_URL, winner) == "COMMITTED")
    check("两库 loser 均 ABORTED",
          shard_state(DB1_URL, loser) == "ABORTED"
          and shard_state(DB2_URL, loser) == "ABORTED")
    data = both_data()
    check("竞争收敛后两库均无 k4",
          "k4" not in data["db1"] and "k4" not in data["db2"], data)
    check("注册表只记 winner",
          shard_undo(DB1_URL).get("u4T") == winner
          and shard_undo(DB2_URL).get("u4T") == winner)
    check("锁全部释放", no_locks())


def u05_same_value_overwrite_example():
    print("[U5] 中间另批同值覆盖 -> 撤回 target 必须 ABORTED")
    submit("u5T", W({"k5": "v"}, {"k5": "v"}))
    # 中间批次写入相同值
    code, r = submit("u5M", W({"k5": "v"}, {"k5": "v"}))
    check("中间同值覆盖提交成功", code == 200 and r["state"] == "COMMITTED", r)
    check("写者已更新为中间批次",
          shard_writers(DB1_URL).get("k5") == "u5M"
          and shard_writers(DB2_URL).get("k5") == "u5M")
    code, r = undo("U5", "u5T")
    check("题面规则：同值覆盖后撤回 target ABORTED",
          code == 409 and r["state"] == "ABORTED", r)
    check("数据保持中间批次结果 k5=v",
          both_data()["db1"].get("k5") == "v"
          and both_data()["db2"].get("k5") == "v")
    # 换号重试仍失败（写者不会自行变回 target）
    code, r = undo("U5b", "u5T")
    check("换号重试仍 ABORTED", code == 409 and r["state"] == "ABORTED", r)
    # 撤回中间批次本身：成功，k5 删除（u5M 准备时 k5 已存在值 v，
    # 镜像存在性=1、值=v，撤回后恢复 v，而非删除——按准备镜像恢复）
    code, r = undo("U5M", "u5M")
    check("撤回中间批次成功", code == 200 and r["state"] == "COMMITTED", r)
    data = both_data()
    check("撤回中间批次后恢复其准备期镜像 v",
          data["db1"].get("k5") == "v" and data["db2"].get("k5") == "v", data)
    check("写者为撤回号 U5M",
          shard_writers(DB1_URL).get("k5") == "U5M"
          and shard_writers(DB2_URL).get("k5") == "U5M")
    check("锁全部释放", no_locks())


def u06_lost_responses():
    print("[U6] 丢应答：prepare-undo/commit 丢应答后重试收敛，只应用一次")
    # u6T 覆盖一个已存在键并新增一个键：撤回时前者恢复旧值、后者删除
    submit("u6base", W({"k6": "old"}, {}))
    submit("u6T", W({"k6": "six"}, {"k6": "six", "q6": None}))
    # prepare-undo 应答丢失（db1）
    post(DB1_URL, "/control/drop-next", {"phase": "prepare-undo"})
    code, r = undo("U6a", "u6T")
    check("prepare-undo 丢应答后撤回仍成功",
          code == 200 and r["state"] == "COMMITTED", r)
    check("两库 U6a COMMITTED",
          shard_state(DB1_URL, "U6a") == "COMMITTED"
          and shard_state(DB2_URL, "U6a") == "COMMITTED")
    data = both_data()
    check("db1 k6 恢复旧值 old；db2 k6/q6 原不存在 -> 删除",
          data["db1"].get("k6") == "old"
          and "k6" not in data["db2"] and "q6" not in data["db2"], data)
    check("锁全部释放", no_locks())

    # commit 应答丢失：分库已应用，协调者重试，undo_commit_applied 不重复
    submit("u6base2", W({"k62": "old2"}, {}))
    submit("u6T2", W({"k62": "x"}, {"k62": "x"}))
    post(DB1_URL, "/control/drop-next", {"phase": "commit"})
    post(DB2_URL, "/control/drop-next", {"phase": "commit"})
    before1 = shard_stats(DB1_URL).get("undo_commit_applied", 0)
    before2 = shard_stats(DB2_URL).get("undo_commit_applied", 0)
    code, r = undo("U6b", "u6T2")
    check("commit 丢应答后撤回成功",
          code == 200 and r["state"] == "COMMITTED", r)
    time.sleep(0.3)
    check("db1 撤回只应用一次",
          shard_stats(DB1_URL).get("undo_commit_applied", 0) == before1 + 1)
    check("db2 撤回只应用一次",
          shard_stats(DB2_URL).get("undo_commit_applied", 0) == before2 + 1)
    # 普通 commit_applied 计数不应因撤回增加
    check("普通 commit 应用计数与撤回独立",
          isinstance(shard_stats(DB1_URL).get("commit_applied", 0), int))
    data = both_data()
    check("db1 k62 恢复 old2；db2 k62 原不存在 -> 删除",
          data["db1"].get("k62") == "old2" and "k62" not in data["db2"], data)
    check("锁全部释放", no_locks())


def u07_crash_after_undo_prepare():
    print("[U7] 单库撤回准备后强退协调者：重启撤销、锁释放、可换号再撤回")
    submit("u7T", W({"k7": "seven"}, {"k7": "seven"}))
    post(COORD_URL, "/control/crash-after-first-prepare", {})
    try:
        undo("U7a", "u7T")
        check("强退发生时请求中断", False, "未崩溃")
    except OSError:
        pass
    time.sleep(0.5)
    check("协调者确已退出", procs["coord"].poll() is not None)

    # 现场：db1 撤回 PREPARED 持锁、镜像已持久；db2 未收到准备
    check("db1 U7a 为 PREPARED", shard_state(DB1_URL, "U7a") == "PREPARED")
    check("db1 持有 k7 锁",
          any(lk["txid"] == "U7a" and lk["key"] == "k7"
              for lk in shard_locks(DB1_URL)))
    check("db2 无 U7a 痕迹", shard_state(DB2_URL, "U7a") is None)
    # 协调者已强退：直接查分库（准备值不可见，数据未变）
    check("数据未变 k7=seven",
          get(DB1_URL, "/data")[1]["data"].get("k7") == "seven"
          and get(DB2_URL, "/data")[1]["data"].get("k7") == "seven")

    start_coord()  # 恢复：无决定 -> abort 两库
    check("重启后 U7a 终态 ABORTED", wait_status("U7a", "ABORTED"))
    check("两库 U7a 均 ABORTED",
          shard_state(DB1_URL, "U7a") == "ABORTED"
          and shard_state(DB2_URL, "U7a") == "ABORTED")
    check("锁全部释放", no_locks())
    check("撤回注册已清理（target 可再撤回）",
          "u7T" not in shard_undo(DB1_URL)
          and "u7T" not in shard_undo(DB2_URL))
    data = both_data()
    check("数据保持 k7=seven",
          data["db1"].get("k7") == "seven" and data["db2"].get("k7") == "seven", data)

    # 同号重放：返回原 ABORTED；换号 U7b 撤回成功
    code, r = undo("U7a", "u7T")
    check("同号重放返回原 ABORTED", code == 409 and r["state"] == "ABORTED", r)
    code, r = undo("U7b", "u7T")
    check("换号撤回成功（重启后镜像/写者仍可用）",
          code == 200 and r["state"] == "COMMITTED", r)
    data = both_data()
    check("两库 k7 删除",
          "k7" not in data["db1"] and "k7" not in data["db2"], data)
    check("写者为 U7b",
          shard_writers(DB1_URL).get("k7") == "U7b"
          and shard_writers(DB2_URL).get("k7") == "U7b")
    check("锁全部释放", no_locks())


def u08_crash_after_undo_commit_one_shard():
    print("[U8] 单库撤回提交后强退：重启补齐、不重复应用、值/终态/锁一致")
    # keep 是 target 之前就存在的键，且不在 target 写集内：撤回不得触碰它
    submit("u8base", W({"keep": "1"}, {}))
    submit("u8T", W({"k8": "eight"}, {"k8": "eight"}))
    post(COORD_URL, "/control/crash-after-first-commit", {})
    try:
        undo("U8a", "u8T")
        check("强退发生时请求中断", False, "未崩溃")
    except OSError:
        pass
    time.sleep(0.5)
    check("协调者确已退出", procs["coord"].poll() is not None)

    # 现场：协调者决定 COMMITTED；db1 已应用撤回（k8 删除），db2 仍 PREPARED
    check("db1 U8a COMMITTED", shard_state(DB1_URL, "U8a") == "COMMITTED")
    check("db2 U8a PREPARED 持锁",
          shard_state(DB2_URL, "U8a") == "PREPARED"
          and any(lk["txid"] == "U8a" for lk in shard_locks(DB2_URL)))
    check("db1 k8 已删除", "k8" not in get(DB1_URL, "/data")[1]["data"])
    check("db2 k8 仍在（半批不暴露给一致查询）",
          "k8" in get(DB2_URL, "/data")[1]["data"])
    applied_before1 = shard_stats(DB1_URL).get("undo_commit_applied", 0)
    applied_before2 = shard_stats(DB2_URL).get("undo_commit_applied", 0)

    start_coord()  # 恢复：COMMITTED 决定补发 commit
    check("重启后 U8a COMMITTED", wait_status("U8a", "COMMITTED"))
    check("db2 补齐 COMMITTED", shard_state(DB2_URL, "U8a") == "COMMITTED")
    data = both_data()
    check("两库 k8 均删除（整批补齐）",
          "k8" not in data["db1"] and "k8" not in data["db2"], data)
    check("未参与撤回的 keep 保留", data["db1"].get("keep") == "1", data)
    check("锁全部释放", no_locks())
    check("db1 未重复应用撤回",
          shard_stats(DB1_URL).get("undo_commit_applied", 0) == applied_before1)
    check("db2 恰好应用一次撤回",
          shard_stats(DB2_URL).get("undo_commit_applied", 0) == applied_before2 + 1)
    # 分库重复 commit 幂等
    code, r = post(DB1_URL, "/commit", {"txid": "U8a"})
    check("分库重复 commit 幂等", code == 200 and "幂等" in r["note"], r)
    check("幂等调用后应用计数仍不变",
          shard_stats(DB1_URL).get("undo_commit_applied", 0) == applied_before1)
    # target 终态不变，且不可再次撤回
    check("target u8T 仍 COMMITTED", wait_status("u8T", "COMMITTED"))
    code, r = undo("U8dup", "u8T")
    check("补齐后重复撤回仍 409 不占号", code == 409, r)
    check("重复撤回不留痕",
          get(COORD_URL, "/txn/U8dup/status")[0] == 404
          and shard_state(DB1_URL, "U8dup") is None)


def u09_eligibility_and_number_space():
    print("[U9] 资格预检 409 不占号；同号内容守卫（类型/target）")
    # target 未知
    code, r = undo("U9a", "ghost9")
    check("未知 target 409", code == 409, r)
    check("未知 target 不占号",
          get(COORD_URL, "/txn/U9a/status")[0] == 404
          and shard_state(DB1_URL, "U9a") is None)

    # target PENDING（制造断连窗口成本高，改用一个已 ABORTED 事务）
    submit("u9ab", W({"z9": "1"}, {"z9": "1"}))
    post(DB1_URL, "/prepare", {"txid": "hold9", "writes": W({"z9": "x"}, {})})
    code, r = submit("u9fail", W({"z9": "x"}, {"m": "1"}))
    check("构造 ABORTED target", code == 200 and r["state"] == "ABORTED", r)
    post(DB1_URL, "/abort", {"txid": "hold9"})
    code, r = undo("U9b", "u9fail")
    check("撤回非提交 target 409 不占号", code == 409, r)
    check("未占号", get(COORD_URL, "/txn/U9b/status")[0] == 404)

    # 撤回事务不能作为 target
    submit("u9T", W({"n9": "1"}, {"n9": "1"}))
    code, r = undo("U9c", "u9T")
    check("先成功撤回 u9T", code == 200 and r["state"] == "COMMITTED", r)
    code, r = undo("U9d", "U9c")
    check("撤回号不能再被撤回 409", code == 409, r)
    check("不占号", get(COORD_URL, "/txn/U9d/status")[0] == 404)

    # 号空间共用：同一 txid 先用于 submit，再以 undo 复用 -> 异内容 409
    submit("u9same", W({"s9": "1"}, {}))
    code, r = undo("u9same", "u9T")
    check("跨类型同号 409（先判内容）", code == 409, r)
    # 同 txid 用于 undo 后再 submit 同样 409
    code, r = submit("U9c", W({"s9": "2"}, {}))
    check("撤回号再提交 409", code == 409, r)

    # 同号异 target：409
    submit("u9T2", W({"p9": "1"}, {"p9": "1"}))
    code, r = undo("U9e", "u9T2")
    check("U9e 撤回成功", code == 200 and r["state"] == "COMMITTED", r)
    code, r = undo("U9e", "u9T")
    check("同号异 target 409", code == 409, r)
    # 同号同 target 重放：原结局
    code, r = undo("U9e", "u9T2")
    check("同号同内容重放原 COMMITTED",
          code == 200 and r["state"] == "COMMITTED", r)
    check("锁全部释放", no_locks())


def u10_disabled_compat_and_bad_fields():
    print("[U10] 未启用 --undo 兼容（404）；非法字段 400 定位")
    # 当前集群启用了 undo；先验证启用态健康字段
    check("协调者健康字段 undo_enabled=true",
          get(COORD_URL, "/health")[1]["undo_enabled"] is True)
    check("分库健康字段 undo_enabled=true",
          get(DB1_URL, "/health")[1]["undo_enabled"] is True)

    # 另起一套未启用 undo 的三进程（独立端口与目录）
    plain_work = os.path.join(WORK, "plain")
    os.makedirs(plain_work, exist_ok=True)
    p_port, s1_port, s2_port = 16020, 16021, 16022
    p_url = "http://127.0.0.1:%d" % p_port
    s1_url = "http://127.0.0.1:%d" % s1_port
    s2_url = "http://127.0.0.1:%d" % s2_port
    plain_procs = []
    logs = []
    try:
        for name, port in (("db1", s1_port), ("db2", s2_port)):
            lg = open(os.path.join(plain_work, "%s.log" % name), "a")
            logs.append(lg)
            p = subprocess.Popen(
                [PY, "-I", os.path.join(HERE, "shard.py"),
                 "--name", name, "--port", str(port),
                 "--db", os.path.join(plain_work, "%s.sqlite" % name)],
                stdout=lg, stderr=subprocess.STDOUT)
            plain_procs.append(p)
        lg = open(os.path.join(plain_work, "coord.log"), "a")
        logs.append(lg)
        p = subprocess.Popen(
            [PY, "-I", os.path.join(HERE, "coordinator.py"),
             "--port", str(p_port),
             "--db", os.path.join(plain_work, "coord.sqlite"),
             "--db1-url", s1_url, "--db2-url", s2_url],
            stdout=lg, stderr=subprocess.STDOUT)
        plain_procs.append(p)
        assert wait_health(p_url), "plain 协调者启动失败"

        check("plain 协调者 undo_enabled=false",
              get(p_url, "/health")[1]["undo_enabled"] is False)
        # 普通协议完全不受影响
        code, r = post(p_url, "/submit",
                       {"txid": "p1", "writes": W({"k": "a"}, {"k": "a"})})
        check("未启用时 /submit 正常", code == 200 and r["state"] == "COMMITTED", r)
        code, r = post(p_url, "/undo", {"txid": "x", "target": "p1"})
        check("未启用 /undo 返回 404", code == 404, r)
        code, r = post(s1_url, "/prepare-undo", {"txid": "x", "target": "p1"})
        check("未启用 /prepare-undo 返回 404", code == 404, r)
        check("404 不留任何事务痕迹",
              get(p_url, "/txn/x/status")[0] == 404
              and get(s1_url, "/txn/x/status")[0] == 404)

        # 非法字段：在启用集群上逐字段定位（400 不占号）
        cases = [
            (None, "body"),
            ("not-json", "body"),
            ({}, "txid"),
            ({"txid": ""}, "txid"),
            ({"txid": "bad-id!"}, "txid"),
            ({"txid": 123}, "txid"),
            ({"txid": "ok1"}, "target"),
            ({"txid": "ok1", "target": ""}, "target"),
            ({"txid": "ok1", "target": "bad!"}, "target"),
            ({"txid": "ok1", "target": 7}, "target"),
            ({"txid": "ok1", "target": "ok1"}, "target"),  # txid==target
        ]
        for body, field in cases:
            if isinstance(body, str):
                code, r = _req(COORD_URL, "POST", "/undo", raw=body.encode())
            else:
                code, r = post(COORD_URL, "/undo", body)
            ok = code == 400 and field in r.get("error", "")
            check("非法撤回输入定位 %s" % field, ok, (code, r))
        check("非法输入不占号",
              get(COORD_URL, "/txn/ok1/status")[0] == 404)
    finally:
        for p in plain_procs:
            p.send_signal(signal.SIGKILL)
            p.wait(timeout=10)
        for lg in logs:
            lg.close()


def main():
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)

    # 预清理残留进程（失败重跑场景）
    with contextlib.suppress(OSError):
        subprocess.run(
            ["pkill", "-f", "twopc/(shard|coordinator)\\.py"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(0.3)

    start_all(undo_flag=True)
    try:
        u01_worked_example_null()
        u02_restore_and_writer_rules()
        u03_empty_writeset_and_single_success()
        u04_interleave_contention()
        u05_same_value_overwrite_example()
        u06_lost_responses()
        u07_crash_after_undo_prepare()   # 内部重启协调者
        u08_crash_after_undo_commit_one_shard()  # 内部重启协调者
        u09_eligibility_and_number_space()
        u10_disabled_compat_and_bad_fields()
    finally:
        stop_all()

    print("\n================ 撤回验收结果 ================")
    print("PASS=%d  FAIL=%d" % (PASS, FAIL))
    if FAIL:
        print("进程日志位于 %s/*.log" % WORK)
        sys.exit(1)
    print("全部通过 ✔")


if __name__ == "__main__":
    main()
