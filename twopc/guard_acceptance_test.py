#!/usr/bin/env python3
"""guards（范围守卫）三进程端到端验收（协调者 + db1 + db2，独立进程与库）。

覆盖题面全部语义：
  G1  题面示例：空库准备检查 ["a","c",{}] 后，另一事务写 b 被反对，写 c 可准备；
      两份同范围空检查可同时准备；终结后范围锁释放
  G2  幻影插入：守卫持锁期间，范围内新键提交被挡在 prepare；范围外可提交
  G3  真实 null：显式 null 值与键缺失不同；expected 精确快照匹配
  G4  范围锁互相兼容；范围锁与其他事务写键锁双向互斥；空范围不持锁
  G5  ASCII 左闭右开边界与 null 无穷端点
  G6  条件不匹配/冲突：持久 ABORTED 且不留任何锁（写键锁、范围锁均无）
  G7  非法检查 400：定位 guards.<库>[<零基项号>].<字段>，且不建事务
  --- 重建干净集群，隔离前序数据 ---
  G8  重试不重验：丢应答重发 prepare 幂等，guard_eval 只增一次
  G9  范围锁跨分库重启与协调者强退恢复保留，终结才释放
  G10 协调者并发：同范围守卫并发共存，守卫/写键冲突并发裁决
  G11 终态幂等：已提交事务重复 prepare 不重验；同号 guards 不同属异内容 409

每个组使用独立 key 前缀（g10/g20/...），互不污染范围快照。
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
WORK = os.path.join(HERE, "run_guard")

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


def W(db1=None, db2=None):
    return {"db1": db1 or {}, "db2": db2 or {}}


def G(db1=None, db2=None):
    return {"db1": db1 or [], "db2": db2 or []}


def submit(txid, writes, guards=None):
    body = {"txid": txid, "writes": writes}
    if guards is not None:
        body["guards"] = guards
    return post(COORD_URL, "/submit", body)


def shard_prepare(txid, writes, guards=None):
    body = {"txid": txid, "writes": writes}
    if guards is not None:
        body["guards"] = guards
    return post(DB1_URL, "/prepare", body)


def shard_state(shard, txid):
    code, data = get(shard, "/txn/%s/status" % txid)
    return data.get("state") if code == 200 else None


def shard_locks(shard):
    return get(shard, "/debug/locks")[1]


def shard_stats(shard):
    return get(shard, "/debug/stats")[1]["stats"]


def db1_data():
    return get(DB1_URL, "/data")[1]["data"]


def snapshot(lo, hi):
    """读取 db1 当前已提交数据在 [lo,hi) 内的精确快照（用于构造正确期望）。"""
    return {k: v for k, v in db1_data().items()
            if (lo is None or lo <= k) and (hi is None or k < hi)}


def both_data():
    return get(COORD_URL, "/data")[1]["data"]


def wait_status(txid, expect, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        code, data = get(COORD_URL, "/txn/%s/status" % txid, timeout=2)
        if code == 200 and data.get("state") == expect:
            return True
        time.sleep(0.1)
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


def restart_cluster():
    """强退三进程并清空全部库后重建：得到无数据、无锁、无历史的干净集群。"""
    stop_all()
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK)
    start_all()


# ================================================================= 测试用例

def g01_worked_example_and_shared_ranges():
    print("[G1] 题面示例：空库 [a,c) 空检查；写 b 反对、写 c 可准备；同范围可共存")
    # 本组独占 g10 前缀，但题面示例要求空库 [a,c)，故放在最前、库尚空时执行
    code, r = shard_prepare("g1T1", W({}, {}), G([["a", "c", {}]]))
    check("T1 db1 准备成功（空库空检查）",
          code == 200 and r["vote"] == "PREPARED", r)
    locks = shard_locks(DB1_URL)
    check("T1 持有范围锁 [a,c)",
          locks["range_locks"] == [{"txid": "g1T1", "lo": "a", "hi": "c"}], locks)
    check("T1 无写键锁", locks["locks"] == [])

    # 另一事务写 b（范围内）：协调者整批事务在 db1 被反对
    code, r = submit("g1T2", W({"b": "x"}, {"g10z": "1"}))
    check("写 b 的事务 ABORTED", code == 200 and r["state"] == "ABORTED", r)
    check("db1 持久 g1T2=ABORTED", shard_state(DB1_URL, "g1T2") == "ABORTED")
    check("反对不留任何锁",
          all(l["txid"] != "g1T2" for l in shard_locks(DB1_URL)["locks"])
          and all(l["txid"] != "g1T2"
                  for l in shard_locks(DB1_URL)["range_locks"]))

    # 写 c（范围右开，c 不在 [a,c)）：可准备
    code, r = shard_prepare("g1T3", W({"c": "y"}, {}))
    check("写 c 可准备（右开边界外）",
          code == 200 and r["vote"] == "PREPARED", r)

    # 两份同范围空检查可同时准备：范围锁互相兼容
    code, r = shard_prepare("g1T4", W({}, {}), G([["a", "c", {}]]))
    check("同范围空检查同时准备成功",
          code == 200 and r["vote"] == "PREPARED", r)
    rl = shard_locks(DB1_URL)["range_locks"]
    check("两条范围锁共存",
          sorted(l["txid"] for l in rl) == ["g1T1", "g1T4"], rl)

    post(DB1_URL, "/abort", {"txid": "g1T3"})
    post(DB1_URL, "/commit", {"txid": "g1T1"})
    post(DB1_URL, "/commit", {"txid": "g1T4"})
    check("终结后范围锁全部释放", shard_locks(DB1_URL)["range_locks"] == [])
    check("终结后写键锁全部释放", shard_locks(DB1_URL)["locks"] == [])


def g02_phantom_insert():
    print("[G2] 幻影插入：守卫持锁期间范围内新键被挡，范围外正常，终结后放行")
    # 本组键均以 g20 起头；上界取 g21（合法键字符），g20* 全落在 [g20,g21)
    lo, hi = "g20", "g21"
    submit("g2base", W({"g20a0": "1", "g20m": "1"}, {}))
    check("基线提交", get(COORD_URL, "/txn/g2base/status")[1]["state"]
          == "COMMITTED")

    expected = snapshot(lo, hi)
    check("基线快照含两键", expected == {"g20a0": "1", "g20m": "1"}, expected)
    code, r = shard_prepare("g2hold", W({}, {}), G([[lo, hi, expected]]))
    check("基线快照匹配，准备成功", code == 200 and r["vote"] == "PREPARED", r)

    # 幻影：范围内新键的整批提交必须在 db1 prepare 被反对
    code, r = submit("g2phantom", W({"g20b": "new"}, {}))
    check("幻影插入 g20b 被反对",
          code == 200 and r["state"] == "ABORTED", r)
    check("幻影数据不可见", "g20b" not in both_data()["db1"])

    # 范围外键正常提交
    code, r = submit("g2outside", W({"g2zout": "ok"}, {}))
    check("范围外写正常提交", code == 200 and r["state"] == "COMMITTED", r)
    check("范围外数据可见", both_data()["db1"].get("g2zout") == "ok")

    # 守卫仍在：再次幻影仍被挡
    code, r = submit("g2phantom2", W({"g20a9": "x"}, {}))
    check("持锁期间幻影持续被挡",
          code == 200 and r["state"] == "ABORTED", r)

    post(DB1_URL, "/commit", {"txid": "g2hold"})
    code, r = submit("g2after", W({"g20b": "now-ok"}, {}))
    check("终结后范围内写放行并提交",
          code == 200 and r["state"] == "COMMITTED", r)
    check("幻影位置最终可见", both_data()["db1"].get("g20b") == "now-ok")
    check("无残留范围锁", shard_locks(DB1_URL)["range_locks"] == [])


def g03_real_null_and_snapshot():
    print("[G3] 真实 null：显式 null 与缺键不同；expected 须恰等于已提交快照")
    lo, hi = "g30", "g31"  # g30* 前缀键全落在 [g30,g31)
    # 提交：g30n1=null（显式），g30n2="s"
    code, _ = submit("g3base", W({"g30n1": None, "g30n2": "s"}, {}))
    check("null 基线提交", code == 200)
    data = db1_data()
    check("显式 null 键存在",
          "g30n1" in data and data["g30n1"] is None, data)

    snap = snapshot(lo, hi)
    check("动态快照含真实 null",
          snap == {"g30n1": None, "g30n2": "s"}, snap)

    # 漏报 null 键 -> 不匹配
    code, r = shard_prepare("g3bad1", W({}, {}),
                            G([[lo, hi, {"g30n2": "s"}]]))
    check("漏报 null 键 -> 反对", code == 409 and r["vote"] == "ABORTED", r)
    check("不匹配持久 ABORTED", shard_state(DB1_URL, "g3bad1") == "ABORTED")

    # null 错报为字符串
    code, r = shard_prepare("g3bad2", W({}, {}),
                            G([[lo, hi, {"g30n1": "x", "g30n2": "s"}]]))
    check("null 值错报为字符串 -> 反对",
          code == 409 and r["vote"] == "ABORTED", r)

    # 多报一个不存在的键（显式 null 也不行：缺键不同于值 null）
    code, r = shard_prepare("g3bad3", W({}, {}),
                            G([[lo, hi,
                                {"g30n1": None, "g30n2": "s",
                                 "g30n3": None}]]))
    check("多报缺键（即使给 null）-> 反对",
          code == 409 and r["vote"] == "ABORTED", r)

    # 精确快照（含真实 null）匹配
    code, r = shard_prepare("g3ok", W({}, {}), G([[lo, hi, snap]]))
    check("含真实 null 的精确快照匹配 -> 准备",
          code == 200 and r["vote"] == "PREPARED", r)
    post(DB1_URL, "/abort", {"txid": "g3ok"})

    # 右无穷 [g30n2, null)：只含 g30n2
    code, r = shard_prepare("g3inf", W({}, {}),
                            G([["g30n2", None, {"g30n2": "s"}]]))
    check("null 右无穷端点范围匹配",
          code == 200 and r["vote"] == "PREPARED", r)
    # 左无穷 [null, g30n1)：严格小于 g30n1，不含 g30n1；本组更左处无已提交键
    code, r = shard_prepare("g3inf2", W({}, {}),
                            G([[None, "g30n1", snapshot(None, "g30n1")]]))
    check("null 左无穷端点快照匹配",
          code == 200 and r["vote"] == "PREPARED", r)
    post(DB1_URL, "/abort", {"txid": "g3inf"})
    post(DB1_URL, "/abort", {"txid": "g3inf2"})


def g04_compatible_ranges_and_mutex():
    print("[G4] 范围锁互相兼容；与写键锁双向互斥；空范围不持锁")
    # 全部使用 g40 前缀键与范围，与其他组隔离
    code, _ = shard_prepare("g4r1", W({}, {}),
                            G([["g40a", "g40m", {}]]))
    check("范围 1 准备", code == 200)
    code, r = shard_prepare("g4r2", W({}, {}),
                            G([["g40g", "g40z", {}]]))
    check("交叠范围 2 准备（范围锁兼容）",
          code == 200 and r["vote"] == "PREPARED", r)

    # 写键落入范围 -> 互斥
    code, r = shard_prepare("g4w1", W({"g40h": "1"}, {}))
    check("写键 g40h 被范围锁挡",
          code == 409 and r["vote"] == "ABORTED", r)
    code, r = shard_prepare("g4w2", W({"g40a": "1"}, {}))
    check("写键 g40a（左闭）被挡",
          code == 409 and r["vote"] == "ABORTED", r)
    # g40m 不在 [g40a,g40m)，但在 [g40g,g40z) 内
    code, r = shard_prepare("g4w3", W({"g40m": "1"}, {}))
    check("写键 g40m 被第二个范围挡",
          code == 409 and r["vote"] == "ABORTED", r)
    # 两范围之外
    code, r = shard_prepare("g4w4", W({"g40z": "1"}, {}))
    check("范围外写键 g40z 可准备",
          code == 200 and r["vote"] == "PREPARED", r)
    post(DB1_URL, "/abort", {"txid": "g4w4"})

    # 反向：先持写键锁（取两范围之外的 g40zz），再来覆盖它的范围锁 -> 互斥
    code, _ = shard_prepare("g4wk", W({"g40zz": "1"}, {}))
    check("写键 g40zz 持锁", code == 200)
    code, r = shard_prepare("g4r3", W({}, {}),
                            G([["g40zy", "g41", {}]]))
    check("覆盖写键 g40zz 的新范围被挡",
          code == 409 and r["vote"] == "ABORTED", r)
    code, r = shard_prepare("g4r4", W({}, {}),
                            G([["g40z", "g40za", {}]]))
    check("不覆盖 g40zz 的范围可准备",
          code == 200 and r["vote"] == "PREPARED", r)

    # 空范围（lo==hi）：不持锁，expected 只能为空
    code, r = shard_prepare("g4empty", W({}, {}),
                            G([["g40a", "g40a", {}]]))
    check("空范围准备成功", code == 200 and r["vote"] == "PREPARED", r)
    check("空范围不产生范围锁",
          all(l["txid"] != "g4empty"
              for l in shard_locks(DB1_URL)["range_locks"]))
    # 空范围不锁任何键：用一个不落入任何存活范围锁的新键验证
    code, r = shard_prepare("g4inempty", W({"g40zz2": "2"}, {}))
    check("空范围不挡范围外写键 g40zz2",
          code == 200 and r["vote"] == "PREPARED", r)

    post(DB1_URL, "/abort", {"txid": "g4r1"})
    post(DB1_URL, "/abort", {"txid": "g4r2"})
    post(DB1_URL, "/abort", {"txid": "g4wk"})
    post(DB1_URL, "/abort", {"txid": "g4r4"})
    post(DB1_URL, "/abort", {"txid": "g4empty"})
    post(DB1_URL, "/abort", {"txid": "g4inempty"})
    check("清理后无残留锁",
          shard_locks(DB1_URL)["range_locks"] == []
          and shard_locks(DB1_URL)["locks"] == [])


def g05_ascii_boundaries():
    print("[G5] ASCII 左闭右开边界、null 无穷与多字符字节序")
    # 字节序：g50k10 < g50k2（ASCII 字典序）
    submit("g5base", W({"g50k10": "x", "g50k2": "y"}, {}))
    # [g50k1, g50k2) 只含 g50k10，不含 g50k2
    code, r = shard_prepare(
        "g5b1", W({}, {}),
        G([["g50k1", "g50k2", {"g50k10": "x"}]]))
    check("字节序范围精确匹配 g50k10",
          code == 200 and r["vote"] == "PREPARED", r)
    # 全范围 [null,null) 期望全量快照（动态取，避免污染）
    code, r = shard_prepare("g5b2", W({}, {}),
                            G([[None, None, snapshot(None, None)]]))
    check("双侧无穷全范围匹配",
          code == 200 and r["vote"] == "PREPARED", r)
    # 全范围守卫挡住一切新写键
    code, r = shard_prepare("g5b3", W({"g50aaa": "1"}, {}))
    check("全范围守卫挡任意写键",
          code == 409 and r["vote"] == "ABORTED", r)
    post(DB1_URL, "/abort", {"txid": "g5b1"})
    post(DB1_URL, "/abort", {"txid": "g5b2"})
    # 守卫释放后同键可准备
    code, r = shard_prepare("g5b4", W({"g50aaa": "1"}, {}))
    check("释放后写键可准备", code == 200 and r["vote"] == "PREPARED", r)
    post(DB1_URL, "/abort", {"txid": "g5b4"})
    check("本组清理后无锁",
          shard_locks(DB1_URL)["range_locks"] == []
          and shard_locks(DB1_URL)["locks"] == [])


def g06_abort_persists_no_locks():
    print("[G6] 冲突/不匹配持久 ABORTED 且不留锁；终态不反转")
    check("起始无锁",
          shard_locks(DB1_URL)["locks"] == []
          and shard_locks(DB1_URL)["range_locks"] == [])

    # 先持一个范围锁（g60 前缀，空快照——该前缀尚无已提交键）
    code, _ = shard_prepare("g6hold", W({}, {}),
                            G([["g60a", "g60z", {}]]))
    check("g6hold 持范围锁", code == 200)
    # 条件不匹配（期望一个不存在的键）
    code, r = shard_prepare("g6mm", W({}, {}),
                            G([["g60a", "g60z", {"g60ghost": None}]]))
    check("不匹配投反对票", code == 409 and r["vote"] == "ABORTED", r)
    # 锁冲突（写键落入 g6hold 范围）
    code, r = shard_prepare("g6cf", W({"g60b": "1"}, {}))
    check("锁冲突投反对票", code == 409 and r["vote"] == "ABORTED", r)

    locks = shard_locks(DB1_URL)
    check("失败者无写键锁",
          all(l["txid"] not in ("g6mm", "g6cf") for l in locks["locks"]))
    check("失败者无范围锁",
          all(l["txid"] not in ("g6mm", "g6cf")
              for l in locks["range_locks"]))
    check("持有者 g6hold 范围锁仍在",
          [l["txid"] for l in locks["range_locks"]] == ["g6hold"], locks)

    # 终态不可逆：对 g6mm 重发同内容 prepare 仍 ABORTED（不重验、不复活）
    code, r = shard_prepare("g6mm", W({}, {}),
                            G([["g60a", "g60z", {"g60ghost": None}]]))
    check("ABORTED 终态不反转",
          code == 409 and r["vote"] == "ABORTED", r)
    post(DB1_URL, "/abort", {"txid": "g6hold"})
    check("清理后无锁",
          shard_locks(DB1_URL)["range_locks"] == []
          and shard_locks(DB1_URL)["locks"] == [])


def g07_invalid_guards_400():
    print("[G7] 非法 guards 逐字段/零基项号定位 400，且不建事务")
    cases = [
        ({"guards": []}, "guards"),
        ({"guards": {"db1": []}}, "guards"),
        ({"guards": {"db1": [], "db2": [], "db3": []}}, "guards"),
        ({"guards": {"db1": "x", "db2": []}}, "guards.db1"),
        ({"guards": {"db1": [["a", "c"]], "db2": []}}, "guards.db1[0]"),
        ({"guards": {"db1": [["a", "c", {}, "x"]], "db2": []}},
         "guards.db1[0]"),
        ({"guards": {"db1": [["a", "c", "x"]], "db2": []}},
         "guards.db1[0].expected"),
        ({"guards": {"db1": [[1, "c", {}]], "db2": []}}, "guards.db1[0].lo"),
        ({"guards": {"db1": [["a", 2, {}]], "db2": []}}, "guards.db1[0].hi"),
        ({"guards": {"db1": [[True, "c", {}]], "db2": []}},
         "guards.db1[0].lo"),
        ({"guards": {"db1": [["c", "a", {}]], "db2": []}},
         "guards.db1[0].hi"),
        ({"guards": {"db1": [["a-b", "c", {}]], "db2": []}},
         "guards.db1[0].lo"),
        ({"guards": {"db1": [["a", "c", {1: "x"}]], "db2": []}},
         "guards.db1[0].expected"),
        ({"guards": {"db1": [["a", "c", {"k": 1}]], "db2": []}},
         "guards.db1[0].expected"),
        ({"guards": {"db1": [["a", "c", {"k": True}]], "db2": []}},
         "guards.db1[0].expected"),
        # 键不在范围内（c 不在 [a,c)）
        ({"guards": {"db1": [["a", "c", {"c": None}]], "db2": []}},
         "guards.db1[0].expected.c"),
        # 键在 lo 左侧
        ({"guards": {"db1": [["m", "z", {"a": None}]], "db2": []}},
         "guards.db1[0].expected.a"),
        # 第二项（零基项号 1）非法：定位 [1]
        ({"guards": {"db1": [["a", "c", {}], ["x", "a", {}]], "db2": []}},
         "guards.db1[1].hi"),
        # 空范围 expected 非空（键不可能落入空范围）
        ({"guards": {"db1": [["a", "a", {"a": None}]], "db2": []}},
         "guards.db1[0].expected.a"),
        # db2 侧错误定位到 db2
        ({"guards": {"db1": [], "db2": [[None, 1, {}]]}},
         "guards.db2[0].hi"),
    ]
    for i, (extra, field) in enumerate(cases):
        txid = "g7bad%02d" % i
        body = {"txid": txid, "writes": W({}, {})}
        body.update(extra)
        code, r = post(COORD_URL, "/submit", body)
        ok = code == 400 and isinstance(r, dict) and field in r.get("error", "")
        check("非法定位 %s" % field, ok, (code, r))
        check("%s 协调库无事务" % txid,
              get(COORD_URL, "/txn/%s/status" % txid)[0] == 404)
        check("%s 分库无痕迹" % txid,
              shard_state(DB1_URL, txid) is None
              and shard_state(DB2_URL, txid) is None)

    # 分库入口同样拒绝（协议自洽），且不留痕
    code, r = post(DB1_URL, "/prepare",
                   {"txid": "g7shard", "writes": W({}, {}),
                    "guards": {"db1": [["c", "a", {}]], "db2": []}})
    check("分库非法 guards 400 且定位",
          code == 400 and "guards.db1[0].hi" in r.get("error", ""), r)
    check("分库不留痕", shard_state(DB1_URL, "g7shard") is None)

    # 省略 guards 与显式两空数组等价：均正常
    code, r = submit("g7omit", W({"g70ok": "1"}, {}))
    check("省略 guards 兼容",
          code == 200 and r["state"] == "COMMITTED", r)
    code, r = submit("g7empty", W({"g70ok2": "2"}, {}), G([], []))
    check("显式两空数组等价",
          code == 200 and r["state"] == "COMMITTED", r)


def g08_retry_no_revalidate():
    print("[G8] 干净集群：丢应答重发不重验，guard_eval 只增一次，幂等收敛")
    # 干净集群里先提交一个 g80 前缀基线键
    submit("g8base", W({"g80a1": "v"}, {}))
    post(DB1_URL, "/control/drop-next", {"phase": "prepare"})
    before = shard_stats(DB1_URL).get("guard_eval", 0)

    body = {"txid": "g8r", "writes": W({}, {}),
            "guards": G([["g80a0", "g80a9", {"g80a1": "v"}]])}
    try:
        code, r = post(DB1_URL, "/prepare", body, timeout=5)
    except OSError:
        code, r = None, None
    check("首次 prepare 应答丢失（连接断开）", code is None, (code, r))
    check("落盘已发生：g8r 为 PREPARED",
          shard_state(DB1_URL, "g8r") == "PREPARED")
    code, r = post(DB1_URL, "/prepare", body)
    check("重发同内容幂等 PREPARED（不重验）",
          code == 200 and r["vote"] == "PREPARED", r)
    after = shard_stats(DB1_URL).get("guard_eval", 0)
    check("guard_eval 只增 1 次（重试不重验）", after == before + 1,
          (before, after))
    check("范围锁仍只有一份",
          [l["txid"] for l in shard_locks(DB1_URL)["range_locks"]] == ["g8r"])
    post(DB1_URL, "/abort", {"txid": "g8r"})

    # 协调者路径丢 prepare 应答：整笔事务最终提交，分库评估只发生一次
    post(DB1_URL, "/control/drop-next", {"phase": "prepare"})
    before = shard_stats(DB1_URL).get("guard_eval", 0)
    code, r = submit("g8c", W({"g80a8": "z"}, {}),
                     G([["g80a0", "g80a9", {"g80a1": "v"}]]))
    check("协调者丢应答后事务 COMMITTED",
          code == 200 and r["state"] == "COMMITTED", r)
    check("协调者路径 guard_eval 只增 1 次",
          shard_stats(DB1_URL).get("guard_eval", 0) == before + 1)
    check("数据正确且无残留锁",
          both_data()["db1"].get("g80a8") == "z"
          and shard_locks(DB1_URL)["range_locks"] == [])


def g09_restart_and_crash_persistence():
    print("[G9] 范围锁跨分库重启、协调者强退恢复保留，终结才释放")
    # 分库重启：范围锁持久保留（g90 前缀，空快照——该前缀尚无已提交键）
    shard_prepare("g9r", W({"g90w1": "1"}, {}),
                  G([["g90a", "g90z", {}]]))
    kill("db1")
    start_shard("db1", DB1_PORT)
    locks = shard_locks(DB1_URL)
    check("重启后范围锁保留",
          any(l["txid"] == "g9r" and l["lo"] == "g90a" and l["hi"] == "g90z"
              for l in locks["range_locks"]), locks)
    check("重启后写键锁保留",
          any(l["txid"] == "g9r" and l["key"] == "g90w1"
              for l in locks["locks"]), locks)
    # 锁仍生效：范围内写被挡
    code, r = shard_prepare("g9block", W({"g90b": "1"}, {}))
    check("重启后范围锁仍生效",
          code == 409 and r["vote"] == "ABORTED", r)
    post(DB1_URL, "/abort", {"txid": "g9r"})
    check("终结后重启保留的锁释放",
          shard_locks(DB1_URL)["range_locks"] == []
          and shard_locks(DB1_URL)["locks"] == [])

    # 协调者强退点 A：带 guards 的事务单库 prepare 后强退，
    # 重启恢复撤销，分库范围锁随之释放
    post(COORD_URL, "/control/crash-after-first-prepare", {})
    try:
        submit("g9crash", W({"g90kc": "1"}, {"g90kc": "1"}),
               G([["g90a", "g90z", {}]], [["g90a", "g90z", {}]]))
        check("强退发生", False, "未崩溃")
    except OSError:
        pass
    time.sleep(0.5)
    check("协调者已退出", procs["coord"].poll() is not None)
    check("强退现场 db1 持有范围锁",
          any(l["txid"] == "g9crash"
              for l in shard_locks(DB1_URL)["range_locks"]),
          shard_locks(DB1_URL))
    check("强退现场 db1 持有写键锁",
          any(l["txid"] == "g9crash"
              for l in shard_locks(DB1_URL)["locks"]))
    start_coord()
    check("恢复后 g9crash ABORTED", wait_status("g9crash", "ABORTED"))
    check("两库范围锁均释放",
          shard_locks(DB1_URL)["range_locks"] == []
          and shard_locks(DB2_URL)["range_locks"] == [])
    check("两库写键锁均释放",
          shard_locks(DB1_URL)["locks"] == []
          and shard_locks(DB2_URL)["locks"] == [])
    check("准备值不可见", "g90kc" not in both_data()["db1"])


def g100coordinator_concurrency():
    print("[G10] 协调者并发：同范围守卫共存，守卫/写键冲突正确裁决")
    # 两个同范围守卫事务：写键放在守卫范围之外，二者皆可提交，范围锁共存过窗口
    outs = {}

    def guard_submit(tag):
        outs[tag] = submit(
            "g10g%s" % tag,
            W({"g100zz%s" % tag: "1"}, {}),
            G([["g100a", "g100c", {}]]))

    ths = [threading.Thread(target=guard_submit, args=(i,)) for i in range(2)]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    check("两个同范围守卫事务均提交（范围锁兼容）",
          all(o[0] == 200 and o[1]["state"] == "COMMITTED"
              for o in outs.values()), outs)

    # 用分库 delay 拉长 prepare 持锁窗口，制造真实交错
    post(DB1_URL, "/control/delay-next", {"phase": "prepare", "seconds": 2.0})
    box = {}

    def hold_guard():
        box["hold"] = post(
            DB1_URL, "/prepare",
            {"txid": "g10hold", "writes": W({}, {}),
             "guards": G([["g100a", "g100c", {}]])})

    th = threading.Thread(target=hold_guard)
    th.start()
    time.sleep(0.6)  # 确保进入 db1 延迟窗口并已在事务内排队
    results = {}

    def writer(tag, key):
        results[tag] = submit("g10w%s" % tag, W({key: "1"}, {}),
                              G([], []))  # 显式空 guards，省略等价

    w_in = threading.Thread(target=writer, args=("in", "g100b"))
    w_out = threading.Thread(target=writer, args=("out", "g100d"))
    w_in.start()
    w_out.start()
    w_in.join(timeout=20)
    w_out.join(timeout=20)
    th.join(timeout=10)
    check("持锁窗口内写 g100b 被裁决 ABORTED",
          results["in"][0] == 200
          and results["in"][1]["state"] == "ABORTED", results)
    check("范围外写 g100d 裁决 COMMITTED",
          results["out"][0] == 200
          and results["out"][1]["state"] == "COMMITTED", results)
    check("守卫自身 PREPARED",
          box["hold"][0] == 200 and box["hold"][1]["vote"] == "PREPARED", box)
    post(DB1_URL, "/abort", {"txid": "g10hold"})
    check("终态无残留锁",
          shard_locks(DB1_URL)["range_locks"] == []
          and shard_locks(DB1_URL)["locks"] == [])


def g110terminal_idempotency_and_content():
    print("[G11] 终态幂等不重验；guards 纳入内容判定，同号异 guards 为异内容")
    submit("g11base", W({"g110a1": "v"}, {}))
    code, r = submit("g11t", W({"g110q": "1"}, {}),
                     G([["g110a0", "g110a9", {"g110a1": "v"}]]))
    check("带 guards 事务提交", code == 200 and r["state"] == "COMMITTED", r)

    # 同号同内容重提：返回原 COMMITTED
    code, r = submit("g11t", W({"g110q": "1"}, {}),
                     G([["g110a0", "g110a9", {"g110a1": "v"}]]))
    check("同号同内容重提返回原状态",
          code == 200 and r["state"] == "COMMITTED", r)

    # 同号不同 guards：异内容 409（即使原 guards 已随终结释放）
    code, r = submit("g11t", W({"g110q": "1"}, {}),
                     G([["g110a0", "g110a9", {"g110a1": "different"}]]))
    check("同号异 guards 409", code == 409, r)
    # 同号不同写集同样 409
    code, r = submit("g11t", W({"g110q": "2"}, {}),
                     G([["g110a0", "g110a9", {"g110a1": "v"}]]))
    check("同号异写集 409", code == 409, r)

    # 已提交事务对分库重复 prepare：幂等返回 COMMITTED，不重验 guards
    before = shard_stats(DB1_URL).get("guard_eval", 0)
    code, r = post(DB1_URL, "/prepare",
                   {"txid": "g11t", "writes": W({"g110q": "1"}, {}),
                    "guards": G([["g110a0", "g110a9", {"g110a1": "v"}]])})
    check("分库重复 prepare 返回 COMMITTED 票（非 PREPARED 票按协议走 409）",
          r.get("vote") == "COMMITTED", r)
    check("终态重复 prepare 不重验（guard_eval 不变）",
          shard_stats(DB1_URL).get("guard_eval", 0) == before)

    # 分库侧同号异内容（guards 不同）409，且先于锁/条件判断
    code, r = post(DB1_URL, "/prepare",
                   {"txid": "g11t", "writes": W({"g110q": "1"}, {}),
                    "guards": G([["g110a0", "g110a9", {}]])})
    check("分库异内容 409", code == 409 and "不一致" in r.get("error", ""), r)


def main():
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)

    start_all()
    try:
        g01_worked_example_and_shared_ranges()
        g02_phantom_insert()
        g03_real_null_and_snapshot()
        g04_compatible_ranges_and_mutex()
        g05_ascii_boundaries()
        g06_abort_persists_no_locks()
        g07_invalid_guards_400()
        # G8 起重建干净集群：无数据、无锁、无历史，隔离前序用例
        restart_cluster()
        g08_retry_no_revalidate()
        g09_restart_and_crash_persistence()
        g100coordinator_concurrency()
        g110terminal_idempotency_and_content()
    finally:
        stop_all()

    print("\n================ guards 验收结果 ================")
    print("PASS=%d  FAIL=%d" % (PASS, FAIL))
    if FAIL:
        print("进程日志位于 %s/*.log" % WORK)
        sys.exit(1)
    print("全部通过 ✔")


if __name__ == "__main__":
    main()
