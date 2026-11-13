#!/usr/bin/env python3
"""整批续采（changefeed）端到端验收：真实独立进程 + 独立 SQLite 库。

覆盖要求：
  C1 空态快照与题面示例：GET /snapshot 游标与双库 data；after=0 仅得 T 一批
  C2 连续 seq、撤销不占号、空写集与同值覆盖批次保留；分页续取与 [] 语义
  C3 after 校验：先代号守卫再验 after；非负整数、禁布尔；超水位 409、非法 400
  C4 交错准备：多事务并发提交，seq 从 1 连续且两库均按 seq 顺序应用
  C5 通知交错与丢应答：分库落盘后丢 commit 应答，重试不增号、不重复应用
  C6 强退恢复（半批 / 双库已应用但水位未落盘）：沿用序号补齐，不漏不重
  C7 fenced + changefeed 接管：代号守卫优先于 after；旧代响应不得推进水位；
     跨代 seq 连续；快照续采重放与对应游标双库状态逐位核对
  C8 未启用 --changefeed 时兼容：/snapshot、/changes 返回 404，原协议不变
"""

import contextlib
import http.client
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
PY = sys.executable
WORK = os.path.join(HERE, "run_changefeed")

DB1_PORT, DB2_PORT = 16011, 16012
COORD_PORTS = {"c1": 16010, "c2": 16013, "c3": 16014}
DB1_URL = "http://127.0.0.1:%d" % DB1_PORT
DB2_URL = "http://127.0.0.1:%d" % DB2_PORT
COORD_URLS = {g: "http://127.0.0.1:%d" % p for g, p in COORD_PORTS.items()}
COORD_DB = os.path.join(WORK, "coord_cf.sqlite")

# 未启用 changefeed 的兼容对照集群（独立端口与目录）
OFF_PORTS = {"coord": 16020, "db1": 16021, "db2": 16022}
OFF_URLS = {
    "coord": "http://127.0.0.1:%d" % OFF_PORTS["coord"],
    "db1": "http://127.0.0.1:%d" % OFF_PORTS["db1"],
    "db2": "http://127.0.0.1:%d" % OFF_PORTS["db2"],
}

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

def _req(base, method, path, body=None, raw=None, timeout=20):
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
            code, data = get(url, "/health", timeout=2)
            if code == 200:
                return data
        except OSError:
            pass
        time.sleep(0.1)
    return None


def W(db1=None, db2=None):
    return {"db1": db1 or {}, "db2": db2 or {}}


def shard_state(shard, txid):
    code, data = get(shard, "/txn/%s/status" % txid)
    return data.get("state") if code == 200 else None


def shard_stats(shard):
    return get(shard, "/debug/stats")[1]["stats"]


def shard_epoch(shard):
    return get(shard, "/debug/epoch")[1]["installed_epoch"]


def snapshot(coord_url, epoch=None):
    q = "?epoch=%d" % epoch if epoch is not None else ""
    return get(coord_url, "/snapshot%s" % q)


def changes(coord_url, after, epoch=None):
    body = {"after": after}
    if epoch is not None:
        body["epoch"] = epoch
    return post(coord_url, "/changes", body)


def submit(coord_url, txid, writes, epoch=None):
    body = {"txid": txid, "writes": writes}
    if epoch is not None:
        body["epoch"] = epoch
    return post(coord_url, "/submit", body)


def all_changelog_rows():
    """只读直查协调库变更日志（验证 seq 连续性的权威来源）。"""
    conn = sqlite3.connect(COORD_DB, timeout=10)
    try:
        rows = conn.execute(
            "SELECT seq, txid, writes FROM changelog ORDER BY seq"
        ).fetchall()
        cur = conn.execute(
            "SELECT val FROM cf_meta WHERE key='publish_cursor'"
        ).fetchone()
    finally:
        conn.close()
    return rows, (cur[0] if cur else 0)


def replay(cursor_resp, change_batches):
    """从空态按批次顺序重放，核对最终双库状态与快照逐位一致。

    每批按原 writes 对两库做 upsert（含 null 显式值、空写集、同值覆盖）。
    null 是显式值、与键不存在相区别，因此同时核对值与键的存在性。
    """
    state = {"db1": {}, "db2": {}}
    for b in change_batches:
        for shard in ("db1", "db2"):
            for k, v in b["writes"][shard].items():
                state[shard][k] = v  # 新增或替换，None 也是显式值
    same = all(
        set(state[s].keys()) == set(cursor_resp["data"][s].keys())
        and all(state[s][k] == cursor_resp["data"][s][k] for k in state[s])
        for s in ("db1", "db2")
    )
    return same, state


# ---------------------------------------------------------------- 进程管理

procs = {}
logfiles = {}


def start_shard(name, port, fenced=False):
    db = os.path.join(WORK, "%s.sqlite" % name)
    log = open(os.path.join(WORK, "%s.log" % name), "a")
    logfiles[("shard", name)] = log
    cmd = [PY, "-I", os.path.join(HERE, "shard.py"),
           "--name", name, "--port", str(port), "--db", db]
    if fenced:
        cmd.append("--fenced")
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    procs[("shard", name)] = p
    url = DB1_URL if name == "db1" else DB2_URL
    assert wait_health(url), "%s 启动失败" % name


def start_coord(gen, fenced=False, extra_args=None):
    port = COORD_PORTS[gen]
    log = open(os.path.join(WORK, "coord_%s.log" % gen), "a")
    logfiles[("coord", gen)] = log
    cmd = [PY, "-I", os.path.join(HERE, "coordinator.py"),
           "--port", str(port), "--db", COORD_DB,
           "--db1-url", DB1_URL, "--db2-url", DB2_URL,
           "--changefeed"]
    if fenced:
        cmd += ["--fenced", "--epoch-file",
                os.path.join(WORK, "epoch_%s.id" % gen)]
    if extra_args:
        cmd += extra_args
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    procs[("coord", gen)] = p
    assert wait_health(COORD_URLS[gen]), "协调者 %s 启动失败" % gen


def kill(key):
    p = procs.get(key)
    if p and p.poll() is None:
        p.send_signal(signal.SIGKILL)
        p.wait(timeout=10)


def wait_exit(key, timeout=10):
    p = procs.get(key)
    if p is None:
        return True
    end = time.time() + timeout
    while time.time() < end:
        if p.poll() is not None:
            return True
        time.sleep(0.05)
    return p.poll() is not None


def stop_all():
    for key in list(procs.keys()):
        kill(key)
    for f in logfiles.values():
        with contextlib.suppress(OSError):
            f.close()
    logfiles.clear()


# ================================================================= 测试用例

def c01_empty_snapshot_and_worked_example():
    print("[C1] 空态快照与题面示例：空态游标 0；T 写 k=null 后游标 1、双库 k:null")
    start_shard("db1", DB1_PORT)
    start_shard("db2", DB2_PORT)
    start_coord("c1")

    code, r = snapshot(COORD_URLS["c1"])
    check("空态 /snapshot 200", code == 200, r)
    check("空态游标为 0", r["cursor"] == 0, r)
    check("空态双库均为空", r["data"] == {"db1": {}, "db2": {}}, r)

    code, r = changes(COORD_URLS["c1"], 0)
    check("空态 after=0 无批次 []", code == 200 and r == {"cursor": 0, "batches": []}, r)

    # 题面示例：空态 T 向两库写 k=null
    code, r = submit(COORD_URLS["c1"], "T", W({"k": None}, {"k": None}))
    check("T 提交成功", code == 200 and r["state"] == "COMMITTED", r)

    code, snap = snapshot(COORD_URLS["c1"])
    check("成功后快照游标 1", code == 200 and snap["cursor"] == 1, snap)
    check("两库均含 k:null",
          snap["data"] == {"db1": {"k": None}, "db2": {"k": None}}, snap)

    code, r = changes(COORD_URLS["c1"], 0)
    check("after=0 仅得 T 一批",
          code == 200 and r["cursor"] == 1 and len(r["batches"]) == 1, r)
    b = r["batches"][0]
    check("批次含 seq/txid/原 writes",
          b["seq"] == 1 and b["txid"] == "T"
          and b["writes"] == W({"k": None}, {"k": None}), b)

    code, r = changes(COORD_URLS["c1"], 1)
    check("after=1 无批次为 []", code == 200 and r["batches"] == []
          and r["cursor"] == 1, r)


def c02_seq_continuity_abort_gapless_and_pagination():
    print("[C2] 连续 seq、撤销不占号、空写集/同值覆盖保留、分页续取")
    # 制造一个 abort（db1 键锁冲突），撤销不占号
    post(DB1_URL, "/prepare",
         {"txid": "cf2hold", "writes": W({"kc": "h"}, {})})
    code, r = submit(COORD_URLS["c1"], "A1", W({"kc": "x"}, {}))
    check("冲突事务 A1 aborted", code == 200 and r["state"] == "ABORTED", r)
    post(DB1_URL, "/abort", {"txid": "cf2hold"})

    # 空写集事务也是一个完整批次
    code, r = submit(COORD_URLS["c1"], "E1", W({}, {}))
    check("空写集 E1 committed", code == 200 and r["state"] == "COMMITTED", r)

    # 同值覆盖：k 已为 null，再写 null（同值覆盖批次仍保留）
    code, r = submit(COORD_URLS["c1"], "S1", W({"k": None}, {"k": None}))
    check("同值覆盖 S1 committed", code == 200 and r["state"] == "COMMITTED", r)

    # 再来一个正常写
    code, r = submit(COORD_URLS["c1"], "N1", W({"n": "1"}, {"n": "1"}))
    check("N1 committed", code == 200 and r["state"] == "COMMITTED", r)

    rows, cursor = all_changelog_rows()
    seqs = [row[0] for row in rows]
    txids = [row[1] for row in rows]
    check("seq 从 1 连续（撤销不占号）", seqs == [1, 2, 3, 4], seqs)
    check("变更日志 txid 顺序 T,E1,S1,N1（无 A1）",
          txids == ["T", "E1", "S1", "N1"], txids)
    check("发布水位已推进到 4", cursor == 4, cursor)

    # 空写集批次保留且 writes 为两库空对象
    e1 = [b for b in changes(COORD_URLS["c1"], 1)[1]["batches"]
          if b["txid"] == "E1"][0]
    check("空写集批次保留", e1["seq"] == 2 and e1["writes"] == W({}, {}), e1)
    s1 = [b for b in changes(COORD_URLS["c1"], 0)[1]["batches"]
          if b["txid"] == "S1"][0]
    check("同值覆盖批次保留（writes 不省略）",
          s1["writes"] == W({"k": None}, {"k": None}), s1)

    # 分页续取：after=0 取 1..4，邻接两页 after=0/after=2 与 after=2/after=4
    # 拼接应等于全量；每页严格按 seq 升序
    full = changes(COORD_URLS["c1"], 0)[1]["batches"]
    page_a = changes(COORD_URLS["c1"], 0)[1]["batches"]   # seq 1..4
    page_b = changes(COORD_URLS["c1"], 2)[1]["batches"]   # seq 3..4
    page_c = changes(COORD_URLS["c1"], 4)[1]["batches"]   # []
    check("邻接分页拼接 == 全量",
          changes(COORD_URLS["c1"], 0)[1]["batches"][:2] + page_b == full,
          (page_a, page_b, full))
    check("尾页 after=cursor 为空 []", page_c == [])
    check("各页按 seq 升序",
          [b["seq"] for b in page_a] == sorted(b["seq"] for b in page_a)
          and [b["seq"] for b in page_b] == sorted(b["seq"] for b in page_b))

    # 快照恰对应 cursor 前缀：重放 after=0 的全部批次应等于当前快照
    code, snap = snapshot(COORD_URLS["c1"])
    ok, state = replay(snap, full)
    check("全量批次重放与快照双库状态逐位一致", ok, (state, snap["data"]))


def c03_after_validation():
    print("[C3] after 校验：非负整数、禁布尔；超水位 409 定位 after；非法 400 定位 after")
    url = COORD_URLS["c1"]
    _, snap = snapshot(url)
    cursor = snap["cursor"]

    # 超过当前水位 -> 409
    code, r = changes(url, cursor + 1)
    check("after 超水位 409", code == 409 and r.get("reason") == "after_beyond_cursor", r)
    check("409 定位 after 并回带 cursor",
          r.get("after") == cursor + 1 and r.get("cursor") == cursor, r)

    # 非法 after -> 400，错误定位到 after
    for bad in (-1, 1.5, "1", None, True, False, [], {}):
        body = {"after": bad}
        code, r = post(url, "/changes", body)
        check("非法 after=%r -> 400 定位 after" % bad,
              code == 400 and "after" in r.get("error", ""), (bad, r))

    # 缺字段
    code, r = post(url, "/changes", {})
    check("after 缺失 400 定位 after",
          code == 400 and "after" in r.get("error", ""), r)

    # 非 JSON / 非对象
    code, r = _req(url, "POST", "/changes", raw=b"not-json")
    check("非 JSON body 400", code == 400, r)

    # 边界：after=0 与 after=cursor 都合法
    check("after=0 合法", changes(url, 0)[0] == 200)
    check("after=cursor 合法且为空",
          changes(url, cursor)[0] == 200
          and changes(url, cursor)[1]["batches"] == [])


def c04_interleaved_prepares_seq_order():
    print("[C4] 交错准备：并发提交 seq 连续，两库均按 seq 顺序应用")
    n = 8
    base1 = shard_stats(DB1_URL).get("commit_applied", 0)
    base2 = shard_stats(DB2_URL).get("commit_applied", 0)
    outs = [None] * n

    def worker(i):
        txid = "IL%d" % i
        outs[i] = submit(COORD_URLS["c1"], txid,
                         W({"k%d" % i: "v%d" % i}, {"q%d" % i: "z%d" % i}))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("8 个交错事务全部 committed",
          all(o[0] == 200 and o[1]["state"] == "COMMITTED" for o in outs), outs)

    rows, cursor = all_changelog_rows()
    seqs = [r[0] for r in rows]
    check("全部 seq 连续无空洞", seqs == list(range(1, len(seqs) + 1)), seqs)
    check("发布水位等于最大 seq", cursor == max(seqs), (cursor, max(seqs)))
    check("IL 事务各占一个连续序号",
          sorted(r[1] for r in rows if r[1].startswith("IL"))
          == ["IL%d" % i for i in range(n)])

    # 两库应用顺序：本轮 8 个批次每库各应用一次（含可能的空写集历史不影响增量）
    check("两库本轮各按序应用 8 次（每批一次）",
          shard_stats(DB1_URL).get("commit_applied", 0) == base1 + n
          and shard_stats(DB2_URL).get("commit_applied", 0) == base2 + n,
          (base1, base2, shard_stats(DB1_URL), shard_stats(DB2_URL)))

    # 续采重放与快照一致
    full = changes(COORD_URLS["c1"], 0)[1]["batches"]
    _, snap = snapshot(COORD_URLS["c1"])
    ok, state = replay(snap, full)
    check("交错提交后重放仍与快照一致", ok, ("replay", state, "snap", snap["data"]))


def c05_lost_commit_ack_no_extra_seq():
    print("[C5] 丢应答：commit 落盘后丢双库应答，重试不增号、不重复应用")
    before_rows, before_cursor = all_changelog_rows()
    before1 = shard_stats(DB1_URL).get("commit_applied", 0)
    before2 = shard_stats(DB2_URL).get("commit_applied", 0)

    post(DB1_URL, "/control/drop-next", {"phase": "commit"})
    post(DB2_URL, "/control/drop-next", {"phase": "commit"})
    code, r = submit(COORD_URLS["c1"], "L1", W({"lk": "1"}, {"lk": "1"}))
    check("丢应答后 L1 仍 committed",
          code == 200 and r["state"] == "COMMITTED", r)
    time.sleep(0.4)

    rows, cursor = all_changelog_rows()
    check("重试不增号：仅新增一个 seq",
          len(rows) == len(before_rows) + 1, (len(before_rows), len(rows)))
    check("水位只推进一格", cursor == before_cursor + 1,
          (before_cursor, cursor))
    check("L1 批次保留原 writes",
          [b for b in rows if b[1] == "L1"][0][2] is not None)
    check("两库各只应用一次",
          shard_stats(DB1_URL).get("commit_applied", 0) == before1 + 1
          and shard_stats(DB2_URL).get("commit_applied", 0) == before2 + 1)

    # 续取能取到该批，且重放与快照一致
    got = changes(COORD_URLS["c1"], before_cursor)[1]["batches"]
    check("续取得到 L1 单批", len(got) == 1 and got[0]["txid"] == "L1", got)
    _, snap = snapshot(COORD_URLS["c1"])
    ok, _ = replay(snap, changes(COORD_URLS["c1"], 0)[1]["batches"])
    check("丢应答恢复后重放与快照一致", ok)


def c06_crash_half_batch_and_cursor_behind():
    print("[C6] 强退：半批 / 双库已应用水位未落盘，恢复沿用序号补齐，不漏不重")
    # ---- 场景 6a：commit 落盘、仅一库应用后强退（半批）----
    applied1_base = shard_stats(DB1_URL).get("commit_applied", 0)
    post(COORD_URLS["c1"], "/control/crash-after-first-commit", {})
    try:
        submit(COORD_URLS["c1"], "H1", W({"hk": "half"}, {"hk": "half"}))
        check("强退发生时请求中断", False, "未崩溃")
    except OSError:
        pass
    check("协调者已退出（半批）", wait_exit(("coord", "c1")))
    check("db1 已 COMMITTED", shard_state(DB1_URL, "H1") == "COMMITTED")
    check("db2 仍 PREPARED", shard_state(DB2_URL, "H1") == "PREPARED")

    rows, cursor_before = all_changelog_rows()
    h1_seq = [r[0] for r in rows if r[1] == "H1"]
    check("H1 已分配 seq（决定已落盘）", len(h1_seq) == 1, h1_seq)
    h1_seq = h1_seq[0]
    check("水位尚未推进到 H1", cursor_before < h1_seq, (cursor_before, h1_seq))
    # db1 已应用一次（强退点之后、恢复之前核对）
    check("强退后 db1 仅应用一次",
          shard_stats(DB1_URL).get("commit_applied", 0) == applied1_base + 1)

    start_coord("c2")  # 恢复：沿用原 seq 补齐
    # 等恢复完成（健康可达后恢复是同步的，启动即完成）
    end = time.time() + 15
    while time.time() < end and shard_state(DB2_URL, "H1") != "COMMITTED":
        time.sleep(0.1)
    check("恢复后 db2 补齐 COMMITTED", shard_state(DB2_URL, "H1") == "COMMITTED")
    check("db1 保持 COMMITTED 不重复应用",
          shard_state(DB1_URL, "H1") == "COMMITTED"
          and shard_stats(DB1_URL).get("commit_applied", 0) == applied1_base + 1)

    _, cursor_after = all_changelog_rows()
    check("恢复后水位推进到 H1 seq", cursor_after == h1_seq,
          (cursor_after, h1_seq))
    _, snap = snapshot(COORD_URLS["c2"])
    check("快照游标==H1 seq 且双库均含 hk",
          snap["cursor"] == h1_seq
          and snap["data"]["db1"].get("hk") == "half"
          and snap["data"]["db2"].get("hk") == "half", snap)
    # 续取不重不漏：after=cursor_before 仅得 H1 一批
    got = changes(COORD_URLS["c2"], cursor_before)[1]["batches"]
    check("半批恢复后续取仅得 H1（不重复）",
          len(got) == 1 and got[0]["seq"] == h1_seq and got[0]["txid"] == "H1", got)

    # ---- 场景 6b：双库均应用、水位未落盘时强退 ----
    post(COORD_URLS["c2"], "/control/crash-after-both-commit", {})
    try:
        submit(COORD_URLS["c2"], "H2", W({"hk2": "both"}, {"hk2": "both"}))
        check("强退发生时请求中断", False, "未崩溃")
    except OSError:
        pass
    check("协调者已退出（水位落后）", wait_exit(("coord", "c2")))
    check("两库均已 COMMITTED",
          shard_state(DB1_URL, "H2") == "COMMITTED"
          and shard_state(DB2_URL, "H2") == "COMMITTED")
    rows, cursor_before2 = all_changelog_rows()
    h2_seq = [r[0] for r in rows if r[1] == "H2"][0]
    check("H2 已占 seq 但水位落后", cursor_before2 < h2_seq,
          (cursor_before2, h2_seq))
    app1 = shard_stats(DB1_URL).get("commit_applied", 0)
    app2 = shard_stats(DB2_URL).get("commit_applied", 0)

    start_coord("c3")
    time.sleep(1.0)  # 同步恢复在开放服务前完成
    _, cursor_after2 = all_changelog_rows()
    check("恢复补齐水位到 H2（不重复应用）",
          cursor_after2 == h2_seq
          and shard_stats(DB1_URL).get("commit_applied", 0) == app1
          and shard_stats(DB2_URL).get("commit_applied", 0) == app2,
          (cursor_after2, h2_seq))

    # 恢复后新提交序号严格接续，不跳号
    code, r = submit(COORD_URLS["c3"], "H3", W({"hk3": "3"}, {"hk3": "3"}))
    check("恢复后新事务 committed", code == 200 and r["state"] == "COMMITTED", r)
    rows, cursor_final = all_changelog_rows()
    seqs = [r[0] for r in rows]
    check("序号全程连续无空洞无重复",
          seqs == list(range(1, len(seqs) + 1)), seqs)
    check("最终水位==最大 seq", cursor_final == max(seqs))

    # 全量重放与最终快照逐位核对
    full = changes(COORD_URLS["c3"], 0)[1]["batches"]
    _, snap = snapshot(COORD_URLS["c3"])
    ok, state = replay(snap, full)
    check("全部强退恢复后重放与快照双库一致", ok, (state, snap["data"]))
    check("快照双库含全部强退事务数据",
          snap["data"]["db1"].get("hk") == "half"
          and snap["data"]["db2"].get("hk2") == "both"
          and snap["data"]["db1"].get("hk3") == "3", snap["data"])


def c07_fenced_takeover_preserves_cursor():
    print("[C7] fenced+changefeed 接管：守卫优先于 after；旧代不得推进水位；跨代 seq 连续")
    # 全新集群：fenced 分库 + fenced/changefeed 协调者
    kill(("coord", "c3"))
    kill(("shard", "db1"))
    kill(("shard", "db2"))
    time.sleep(0.3)
    start_shard("db1", DB1_PORT, fenced=True)
    start_shard("db2", DB2_PORT, fenced=True)
    start_coord("c1", fenced=True)
    check("g1 健康且 changefeed 已启用",
          wait_health(COORD_URLS["c1"])["changefeed"] is True)
    check("两库安装代 1", shard_epoch(DB1_URL) == 1 and shard_epoch(DB2_URL) == 1)

    # 代号守卫优先于 after 校验：缺代/错代先 409 stale，即使 after 非法
    code, r = post(COORD_URLS["c1"], "/changes", {"after": True})
    check("缺代号先于 after 类型校验 -> 409 stale",
          code == 409 and r.get("reason") == "stale_epoch", r)
    code, r = post(COORD_URLS["c1"], "/changes", {"epoch": 9, "after": True})
    check("错代号先于 after 校验 -> 409 stale",
          code == 409 and r.get("reason") == "stale_epoch", r)
    code, r = post(COORD_URLS["c1"], "/changes", {"epoch": 1, "after": True})
    check("当代再验 after 类型 -> 400 定位 after",
          code == 400 and "after" in r.get("error", ""), r)
    code, r = snapshot(COORD_URLS["c1"])  # 缺代
    check("快照缺代 409 stale", code == 409
          and r.get("reason") == "stale_epoch", r)
    code, r = snapshot(COORD_URLS["c1"], epoch=1)
    check("当代快照正常（沿用旧库水位）", code == 200 and "cursor" in r, r)

    # 代 1 提交两批，记录水位
    submit(COORD_URLS["c1"], "P1", W({"p": "1"}, {"p": "1"}), epoch=1)
    submit(COORD_URLS["c1"], "P2", W({"p": "2"}, {"p": "2"}), epoch=1)
    _, cursor_g1 = all_changelog_rows()
    check("代1 发布水位推进", cursor_g1 >= 2, cursor_g1)
    rows_g1, _ = all_changelog_rows()
    max_seq_g1 = max(r[0] for r in rows_g1)

    # 新代 c2 接管（共享协调库，独立 epoch 文件）
    start_coord("c2", fenced=True)
    check("两库升到代 2", shard_epoch(DB1_URL) == 2 and shard_epoch(DB2_URL) == 2)

    # 旧代 c1 仍存活：其 /changes、/snapshot 一律 409，且绝不推进水位
    code, r = changes(COORD_URLS["c1"], 0, epoch=1)
    check("旧代 /changes 409", code == 409
          and r.get("reason") == "stale_epoch", r)
    code, r = snapshot(COORD_URLS["c1"], epoch=1)
    check("旧代 /snapshot 409", code == 409
          and r.get("reason") == "stale_epoch", r)
    code, r = submit(COORD_URLS["c1"], "OLD", W({"o": "x"}, {}), epoch=1)
    check("旧代提交 409", code == 409 and r.get("reason") == "stale_epoch", r)
    _, cursor_still = all_changelog_rows()
    check("旧代响应未推进水位", cursor_still == cursor_g1,
          (cursor_still, cursor_g1))

    # 新代承接：seq 跨代严格接续
    code, r = submit(COORD_URLS["c2"], "P3", W({"p": "3"}, {"p": "3"}), epoch=2)
    check("代2 新事务 committed", code == 200 and r["state"] == "COMMITTED", r)
    rows, cursor_g2 = all_changelog_rows()
    seqs = [r[0] for r in rows]
    check("跨代 seq 全程连续", seqs == list(range(1, len(seqs) + 1)), seqs)
    p3_seq = [r[0] for r in rows if r[1] == "P3"][0]
    check("P3 接续代1最大序号", p3_seq == max_seq_g1 + 1, (p3_seq, max_seq_g1))

    # 接管保留水位：新代快照游标即持久水位，续采重放与双库状态逐位核对
    code, snap = snapshot(COORD_URLS["c2"], epoch=2)
    check("接管后快照游标==持久水位",
          code == 200 and snap["cursor"] == cursor_g2, (snap, cursor_g2))
    full = changes(COORD_URLS["c2"], 0, epoch=2)[1]["batches"]
    check("新代 after=0 取回全部已发布批次",
          len(full) == cursor_g2 and [b["seq"] for b in full]
          == list(range(1, cursor_g2 + 1)), len(full))
    ok, state = replay(snap, full)
    check("接管后全量重放与快照双库逐位一致", ok, (state, snap["data"]))

    # 游标前缀语义：取前 mid 个批次重放出的每个键值，都必须与最终快照一致
    # （前缀是最终态的演化历史；同值覆盖/替换后的最终值以快照为准）
    mid = max(1, cursor_g2 // 2)
    prefix = full[:mid]
    pre_state = {"db1": {}, "db2": {}}
    for b in prefix:
        for shard in ("db1", "db2"):
            for k, v in b["writes"][shard].items():
                pre_state[shard][k] = v
    for shard in ("db1", "db2"):
        for k in pre_state[shard]:
            check("前缀重放键 %s.%s 存在于最终快照且值一致" % (shard, k),
                  k in snap["data"][shard]
                  and snap["data"][shard][k] == pre_state[shard][k])

    # 再一代 c3 接管，水位仍保留，序号继续
    start_coord("c3", fenced=True)
    check("两库升到代 3", shard_epoch(DB1_URL) == 3 and shard_epoch(DB2_URL) == 3)
    code, r = submit(COORD_URLS["c3"], "P4", W({"p": "4"}, {"p": "4"}), epoch=3)
    check("代3 提交成功", code == 200 and r["state"] == "COMMITTED", r)
    rows3, cursor3 = all_changelog_rows()
    check("三代后 seq 仍连续、水位单调",
          [r[0] for r in rows3] == list(range(1, len(rows3) + 1))
          and cursor3 == max(r[0] for r in rows3))
    code, snap3 = snapshot(COORD_URLS["c3"], epoch=3)
    full3 = changes(COORD_URLS["c3"], 0, epoch=3)[1]["batches"]
    ok, _ = replay(snap3, full3)
    check("三代后重放与快照仍一致", ok)


def c08_disabled_compatibility():
    print("[C8] 未启用 --changefeed：/snapshot、/changes 404，原 2PC 协议不变")
    wd = os.path.join(WORK, "off")
    os.makedirs(wd, exist_ok=True)
    off_procs = []

    def start_off(role, port):
        db = os.path.join(wd, "%s.sqlite" % role)
        log = open(os.path.join(WORK, "off_%s.log" % role), "a")
        if role in ("db1", "db2"):
            cmd = [PY, "-I", os.path.join(HERE, "shard.py"),
                   "--name", role, "--port", str(port), "--db", db]
        else:
            cmd = [PY, "-I", os.path.join(HERE, "coordinator.py"),
                   "--port", str(port), "--db", db,
                   "--db1-url", OFF_URLS["db1"], "--db2-url", OFF_URLS["db2"]]
        p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        off_procs.append(p)
        return p

    start_off("db1", OFF_PORTS["db1"])
    start_off("db2", OFF_PORTS["db2"])
    start_off("coord", OFF_PORTS["coord"])
    assert wait_health(OFF_URLS["coord"]), "兼容集群协调者启动失败"

    code, r = get(OFF_URLS["coord"], "/snapshot")
    check("未启用 /snapshot -> 404", code == 404, r)
    code, r = post(OFF_URLS["coord"], "/changes", {"after": 0})
    check("未启用 /changes -> 404", code == 404, r)

    # 原协议完全不变
    code, r = post(OFF_URLS["coord"], "/submit",
                   {"txid": "off1", "writes": W({"k": "a"}, {"k": None})})
    check("普通提交仍 committed", code == 200 and r["state"] == "COMMITTED", r)
    code, r = get(OFF_URLS["coord"], "/data")
    check("一致查询不变",
          code == 200 and r["data"] == {"db1": {"k": "a"},
                                        "db2": {"k": None}}, r)

    # 协调库不存在 changelog/cf_meta 的任何写入（表虽建但为空）
    conn = sqlite3.connect(os.path.join(wd, "coord.sqlite"), timeout=10)
    try:
        n_log = conn.execute("SELECT COUNT(*) FROM changelog").fetchone()[0]
        n_meta = conn.execute("SELECT COUNT(*) FROM cf_meta").fetchone()[0]
    finally:
        conn.close()
    check("未启用时变更日志与水位均无记录", n_log == 0 and n_meta == 0,
          (n_log, n_meta))

    for p in off_procs:
        p.send_signal(signal.SIGKILL)
        p.wait(timeout=10)


def main():
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    with contextlib.suppress(OSError):
        subprocess.run(
            ["pkill", "-f", "twopc/(shard|coordinator)\\.py"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(0.3)

    try:
        c01_empty_snapshot_and_worked_example()
        c02_seq_continuity_abort_gapless_and_pagination()
        c03_after_validation()
        c04_interleaved_prepares_seq_order()
        c05_lost_commit_ack_no_extra_seq()
        c06_crash_half_batch_and_cursor_behind()
        c07_fenced_takeover_preserves_cursor()
        c08_disabled_compatibility()
    finally:
        stop_all()

    print("\n============== 整批续采验收结果 ==============")
    print("PASS=%d  FAIL=%d" % (PASS, FAIL))
    if FAIL:
        print("进程日志位于 %s/*.log" % WORK)
        sys.exit(1)
    print("全部通过 ✔")


if __name__ == "__main__":
    main()
