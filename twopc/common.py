"""两阶段提交系统 —— 协调者与分库共用的输入校验与 HTTP 工具。

仅依赖 Python 标准库（http.client / json / re / urllib）。
"""

import http.client
import json
import re
import urllib.parse

# 标识符：非空 ASCII 字母数字串（txid 与 key 均受此约束）
_IDENT_RE = re.compile(r"^[A-Za-z0-9]+$")
SHARDS = ("db1", "db2")


class BadRequest(Exception):
    """非法输入（400），msg 必须定位到具体字段。"""

    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class Conflict(Exception):
    """状态/内容冲突（409），例如终态被要求反转、同 txid 异内容。"""

    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class StaleEpoch(Exception):
    """代（epoch）不符（409 stale_epoch）：请求不改变任何状态。

    installed: 分库/协调者当前已安装代（未安装过为 None）
    request_epoch: 请求携带的代（未携带为 None）
    """

    def __init__(self, msg, installed=None, request_epoch=None):
        super().__init__(msg)
        self.msg = msg
        self.installed = installed
        self.request_epoch = request_epoch


def is_ident(s):
    return isinstance(s, str) and bool(_IDENT_RE.match(s))


def validate_epoch(value, field="epoch"):
    """校验代（epoch）：必须是正整数。

    明确拒绝 bool（True/False 是 int 子类）、浮点、字符串、零与负数；
    非法时抛 BadRequest 并把错误定位到 epoch 字段。
    """
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BadRequest(
            "%s: 必须是正整数（禁止布尔/浮点/字符串/零/负数，实际: %r）"
            % (field, value)
        )
    return value


def validate_transaction(obj):
    """校验提交 JSON。

    结构要求：
      {"txid": 非空ASCII字母数字串,
       "writes": {"db1": {key: 字符串或null}, "db2": {同结构}}}
    writes 必须恰含两个库；每库的写集可以为空 {}（空写集合法）。
    返回规范化后的 (txid, {shard: {key: value}})，非法输入抛 BadRequest。
    """
    if not isinstance(obj, dict):
        raise BadRequest("body 必须是 JSON 对象")

    # txid
    if "txid" not in obj:
        raise BadRequest("txid: 字段缺失")
    txid = obj["txid"]
    if not isinstance(txid, str):
        raise BadRequest("txid: 必须是字符串")
    if not txid:
        raise BadRequest("txid: 不得为空字符串")
    if not _IDENT_RE.match(txid):
        raise BadRequest("txid: 必须是非空 ASCII 字母数字串")

    # writes
    if "writes" not in obj:
        raise BadRequest("writes: 字段缺失")
    writes = obj["writes"]
    if not isinstance(writes, dict):
        raise BadRequest("writes: 必须是对象")
    if set(writes.keys()) != set(SHARDS):
        raise BadRequest(
            "writes: 必须恰含两个库 db1 与 db2（实际键: %r）" % sorted(writes.keys())
        )

    norm = {}
    for shard in SHARDS:
        w = writes[shard]
        if not isinstance(w, dict):
            raise BadRequest("writes.%s: 必须是对象（key 到字符串或 null 的映射）" % shard)
        sw = {}
        for key, val in w.items():
            if not isinstance(key, str) or not key:
                raise BadRequest("writes.%s: key 必须是非空字符串" % shard)
            if not _IDENT_RE.match(key):
                raise BadRequest(
                    "writes.%s.%s: key 必须是非空 ASCII 字母数字串" % (shard, key)
                )
            if val is not None and not isinstance(val, str):
                raise BadRequest(
                    "writes.%s.%s: 值必须是字符串或 null" % (shard, key)
                )
            sw[key] = val
        norm[shard] = sw
    return txid, norm


def canonical_writes(norm):
    """规范化写集为稳定字节串，用于同 txid 内容比对（按 key 排序）。"""
    return json.dumps(
        {s: dict(sorted(norm[s].items())) for s in SHARDS},
        separators=(",", ":"),
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")


def _validate_guard_endpoint(value, field):
    """校验范围端点：必须是原键（非空 ASCII 字母数字串）或 null（无穷）。"""
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise BadRequest(
            "%s: 必须是原键（非空 ASCII 字母数字串）或 null（实际: %r）"
            % (field, value)
        )
    if not _IDENT_RE.match(value):
        raise BadRequest(
            "%s: 必须是非空 ASCII 字母数字串或 null（实际: %r）" % (field, value)
        )
    return value


def validate_guards(obj, norm):
    """校验可选 guards 字段，返回规范化检查项。

    结构：{"guards": {"db1": [[lo, hi, expected], ...], "db2": [...]}}
      * 省略 guards 等同两库均为空数组；若出现则必须恰含 db1、db2；
      * 每项恰为 3 元素数组；lo/hi 为原键或 null（null 端对应无穷），
        按 ASCII 满足 lo<=hi；范围为左闭右开 [lo, hi)；
      * expected 是 key->字符串或 null 的映射（沿用写集取值规则），
        每个键必须落在本项范围内；显式 null 值与键缺失是两回事，
        因此 expected 里写 null 表示要求该键存在且值为 null；
      * 空范围（lo==hi，两端均非 null）合法：不产生范围锁，
        其 expected 只能为空映射（任何键都不可能落在空范围内）。
    非法输入抛 BadRequest，错误定位到 guards.<库>[<零基项号>].<字段>。
    """
    if "guards" not in obj:
        return {s: [] for s in SHARDS}
    guards = obj["guards"]
    if not isinstance(guards, dict):
        raise BadRequest("guards: 必须是对象")
    if set(guards.keys()) != set(SHARDS):
        raise BadRequest(
            "guards: 必须恰含两个库 db1 与 db2（实际键: %r）"
            % sorted(guards.keys())
        )

    out = {}
    for shard in SHARDS:
        items = guards[shard]
        if not isinstance(items, list):
            raise BadRequest("guards.%s: 必须是检查项数组" % shard)
        norm_items = []
        for idx, item in enumerate(items):
            base = "guards.%s[%d]" % (shard, idx)  # 零基项号
            if not isinstance(item, list) or len(item) != 3:
                raise BadRequest(
                    "%s: 检查项必须是恰含 3 个元素的数组 [lo, hi, expected]"
                    % base
                )
            lo, hi, expected = item
            lo = _validate_guard_endpoint(lo, base + ".lo")
            hi = _validate_guard_endpoint(hi, base + ".hi")
            if lo is not None and hi is not None and lo > hi:
                raise BadRequest(
                    "%s.hi: 端点须满足 lo<=hi（实际 lo=%r hi=%r）"
                    % (base, lo, hi)
                )
            if not isinstance(expected, dict):
                raise BadRequest(
                    "%s.expected: 必须是键到字符串或 null 的映射" % base
                )
            exp = {}
            for key, val in expected.items():
                if not isinstance(key, str) or not key \
                        or not _IDENT_RE.match(key):
                    raise BadRequest(
                        "%s.expected: 键必须是非空 ASCII 字母数字串（实际: %r）"
                        % (base, key)
                    )
                if val is not None and not isinstance(val, str):
                    raise BadRequest(
                        "%s.expected.%s: 值必须是字符串或 null（实际: %r）"
                        % (base, key, val)
                    )
                # 键须在 [lo, hi) 内；null 端为无穷，不施加该侧约束
                if lo is not None and key < lo:
                    raise BadRequest(
                        "%s.expected.%s: 键 %r 不在范围 [%r, %r) 内"
                        % (base, key, key, lo, hi)
                    )
                if hi is not None and not key < hi:
                    raise BadRequest(
                        "%s.expected.%s: 键 %r 不在范围 [%r, %r) 内"
                        % (base, key, key, lo, hi)
                    )
                exp[key] = val
            norm_items.append([lo, hi, exp])
        out[shard] = norm_items
    return out


def canonical_request(norm, guards):
    """规范化整份事务内容（写集 + guards）为稳定字节串。

    同 txid 的内容判定包含 guards：任一部分不同即异内容。
    """
    payload = {
        "writes": {s: dict(sorted(norm[s].items())) for s in SHARDS},
        "guards": {
            s: [[lo, hi, dict(sorted(exp.items()))]
                for lo, hi, exp in guards[s]]
            for s in SHARDS
        },
    }
    return json.dumps(
        payload,
        separators=(",", ":"),
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")


def parse_request_blob(blob):
    """解析持久化的事务内容 BLOB，返回 (norm_writes, norm_guards)。

    兼容旧格式（仅写集的 {"db1":..,"db2":..}）：旧事务按 guards 全空处理。
    """
    if isinstance(blob, str):  # 兼容被外部以 TEXT 写入的记录
        blob = blob.encode("utf-8")
    obj = json.loads(blob.decode("utf-8"))
    if isinstance(obj, dict) and "writes" in obj and "guards" in obj:
        return obj["writes"], obj["guards"]
    # 旧格式 BLOB：顶层恰为两库写集
    return obj, {s: [] for s in SHARDS}


# ---------------------------------------------------------------- HTTP 客户端

def _with_epoch(path, epoch):
    """把已校验的正整数代附加为 query 参数；epoch 为 None 时原样返回。"""
    if epoch is None:
        return path
    sep = "&" if "?" in path else "?"
    return "%s%sepoch=%d" % (path, sep, epoch)


def http_request(base_url, method, path, body=None, timeout=5.0, epoch=None):
    """同步发一次 HTTP 请求，返回 (status_code, 解析后的 JSON 或 None)。

    连接失败、超时、响应非 JSON 等均以异常上抛，由调用方决定重试。
    epoch 非 None 时以 ?epoch=N 形式携带（fenced 模式协议请求必带）。
    """
    path = _with_epoch(path, epoch)
    url = urllib.parse.urlparse(base_url)
    conn = http.client.HTTPConnection(url.hostname, url.port, timeout=timeout)
    try:
        payload = None
        headers = {}
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        conn.request(method, path, body=payload, headers=headers)
        resp = conn.getresponse()
        raw = resp.read()
        if raw:
            try:
                data = json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                data = None
        else:
            data = None
        return resp.status, data
    finally:
        conn.close()


def post_json(base_url, path, body, timeout=5.0, epoch=None):
    return http_request(base_url, "POST", path, body=body,
                        timeout=timeout, epoch=epoch)


def get_json(base_url, path, timeout=5.0, epoch=None):
    return http_request(base_url, "GET", path, timeout=timeout, epoch=epoch)
