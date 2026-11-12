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


def is_ident(s):
    return isinstance(s, str) and bool(_IDENT_RE.match(s))


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


# ---------------------------------------------------------------- HTTP 客户端

def http_request(base_url, method, path, body=None, timeout=5.0):
    """同步发一次 HTTP 请求，返回 (status_code, 解析后的 JSON 或 None)。

    连接失败、超时、响应非 JSON 等均以异常上抛，由调用方决定重试。
    """
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


def post_json(base_url, path, body, timeout=5.0):
    return http_request(base_url, "POST", path, body=body, timeout=timeout)


def get_json(base_url, path, timeout=5.0):
    return http_request(base_url, "GET", path, timeout=timeout)
