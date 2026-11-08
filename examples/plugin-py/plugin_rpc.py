#!/usr/bin/env python3
"""plugin-proto 的 Python 参考实现。

这是 wire.md（docs/protocol/wire.md）的第二个实现。它存在的意义不是
「又一个 SDK」，而是**对规范本身的检验**：规范里任何含糊的地方，都会
在第二种语言实现它的那一刻暴露出来。

只用标准库。契约的 ground truth 是 crates/flow-plugin-sdk（protocol 模块）/tests/vectors/，
test_vectors.py 对它逐字节校验。

用法（作为插件被 proc-ipc 宿主拉起）：

    宿主 spawn:  python3 plugin_rpc.py
    通道交接:    环境变量 PROC_IPC_FD（固定 3，见 proc-ipc 文档）

协议要点（与 wire.md 一一对应）：
- 帧：[u8 kind][u32 BE len][payload]；kind 1=rpc 2=binary；上限 1 MiB。
- JSON-RPC 2.0 子集：无 batch；id 只许 string/number（error 响应可 null）。
- 规范形：信封字段固定序 + error 对象固定序 + 嵌套对象键排序。
- initialize 握手：主版本不等 → -32001 并退出，不降级。
"""

import json
import os
import socket
import sys

MAX_FRAME_BYTES = 1024 * 1024
KIND_RPC = 0x01
KIND_BINARY = 0x02

PROTOCOL_VERSION = 1
PROTOCOL_VERSION_MAJOR = PROTOCOL_VERSION >> 16

APP_ERROR_RANGE = range(-32099, -32000)  # -32099..=-32001
STANDARD_CODES = {-32700, -32600, -32601, -32602, -32603}


class ProtocolError(Exception):
    """帧或消息违反 wire.md。连接必须关闭，不尝试恢复。"""


class MethodNotFound(Exception):
    """dispatch 找不到方法：回 -32601，不静默丢弃。"""


# ---------------------------------------------------------------------------
# 帧编解码（wire.md §1）
# ---------------------------------------------------------------------------

def encode_frame(kind: int, payload: bytes) -> bytes:
    if len(payload) > MAX_FRAME_BYTES:
        raise ProtocolError(f"frame of {len(payload)} bytes exceeds limit")
    return bytes([kind]) + len(payload).to_bytes(4, "big") + payload


def decode_frame(raw: bytes) -> "tuple[int, bytes]":
    """从「正好一帧」的字节解码（向量测试用；流式见 read_frame）。"""
    if len(raw) < 5:
        raise ProtocolError("short frame")
    kind = raw[0]
    if kind not in (KIND_RPC, KIND_BINARY):
        raise ProtocolError(f"unknown frame kind 0x{kind:02x}")
    declared = int.from_bytes(raw[1:5], "big")
    if declared > MAX_FRAME_BYTES:
        raise ProtocolError(f"declared {declared} exceeds limit")
    payload = raw[5:]
    if len(payload) != declared:
        raise ProtocolError(f"truncated: declared {declared}, got {len(payload)}")
    return kind, payload


def read_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ProtocolError("stream ended mid-frame")
        buf += chunk
    return bytes(buf)


def read_frame(sock: socket.socket) -> "tuple[int, bytes]":
    head = read_exact(sock, 5)
    kind = head[0]
    if kind not in (KIND_RPC, KIND_BINARY):
        raise ProtocolError(f"unknown frame kind 0x{kind:02x}")
    declared = int.from_bytes(head[1:5], "big")
    if declared > MAX_FRAME_BYTES:
        raise ProtocolError(f"declared {declared} exceeds limit")
    return kind, read_exact(sock, declared)


# ---------------------------------------------------------------------------
# JSON-RPC 2.0 子集（wire.md §2）
# ---------------------------------------------------------------------------

def _canon(value) -> str:
    """嵌套值的规范形：键排序 + 紧凑分隔符 + 原生 UTF-8。"""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


def _jstr(text: str) -> str:
    return json.dumps(text, ensure_ascii=False)


def _encode_error(error: dict) -> str:
    parts = [f'"code":{error["code"]}', f'"message":{_jstr(error["message"])}']
    if "data" in error:
        parts.append(f'"data":{_canon(error["data"])}')
    return "{" + ",".join(parts) + "}"


def encode_message(message: dict) -> bytes:
    """按规范形编码（wire.md §2.4）：信封固定序、error 固定序、嵌套键排序。"""
    if "method" in message:
        order = ["jsonrpc", "id", "method", "params"]
    elif "result" in message:
        order = ["jsonrpc", "id", "result"]
    elif "error" in message:
        order = ["jsonrpc", "id", "error"]
    else:
        raise ProtocolError("message must carry method, result, or error")
    parts = []
    for key in order:
        if key not in message:
            continue
        if key == "error":
            parts.append(f'"error":{_encode_error(message["error"])}')
        else:
            parts.append(f"{_jstr(key)}:{_canon(message[key])}")
    return ("{" + ",".join(parts) + "}").encode("utf-8")


def _valid_id(value, allow_null: bool) -> bool:
    if value is None:
        return allow_null
    # 注意 bool 是 int 的子类：True 不是合法 id。
    return type(value) is int or isinstance(value, str)


def decode_message(payload: bytes) -> dict:
    """解析并做结构校验（wire.md §2.2）；违反即 ProtocolError。"""
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError(f"invalid json: {error}") from error
    if not isinstance(value, dict):
        raise ProtocolError("top level must be an object (batch is disabled)")
    if value.get("jsonrpc") != "2.0":
        raise ProtocolError('jsonrpc must be "2.0"')
    if "method" in value:
        method = value["method"]
        if not isinstance(method, str) or not method:
            raise ProtocolError("method must be a non-empty string")
        if "id" in value:
            if not _valid_id(value["id"], allow_null=False):
                raise ProtocolError("id must be string or number")
        return value
    has_result = "result" in value
    has_error = "error" in value
    if has_result == has_error:  # 都有或都没有
        raise ProtocolError("response must carry exactly one of result / error")
    if "id" not in value:
        raise ProtocolError("response must carry id")
    if not _valid_id(value["id"], allow_null=has_error):
        raise ProtocolError("id must be string, number, or null (error only)")
    if has_error:
        error = value["error"]
        if not isinstance(error, dict):
            raise ProtocolError("error must be an object")
        code = error.get("code")
        if type(code) is not int or (code not in STANDARD_CODES and code not in APP_ERROR_RANGE):
            raise ProtocolError(f"reserved or unknown error code {code!r}")
        if not isinstance(error.get("message"), str):
            raise ProtocolError("error.message must be a string")
    return value


# ---------------------------------------------------------------------------
# 握手与服务循环（wire.md §3 / §5）
# ---------------------------------------------------------------------------

def default_dispatch(method: str, params):
    if method == "ping":
        return "pong"
    raise MethodNotFound(f"method not found: {method}")


def serve(dispatch=None, capabilities=(), server_info=None) -> int:
    """插件主循环：接管通道 → initialize 握手 → 分发请求。

    通道交接按环境变量自动选择（与 proc-ipc 的约定一致）：
    - `PROC_IPC_FD`（unix）：接管的 socketpair fd，归一为阻塞模式。
    - `PROC_IPC_ADDR`（Windows / 回连）：named pipe 路径，按文件打开。

    返回进程退出码。任何 ProtocolError 都直接退出（wire.md：未知即明确，
    没有静默恢复路径）。
    """
    token = os.environ.get("PROC_IPC_TOKEN")
    try:
        sock = _open_channel()
    except OSError as error:
        print(f"channel open failed: {error}", file=sys.stderr)
        return 2
    dispatch = dispatch or default_dispatch
    try:
        _handshake(sock, capabilities, server_info, token)
        _serve_loop(sock, dispatch)
    except (ProtocolError, OSError) as error:
        print(f"plugin session end: {error}", file=sys.stderr)
        return 0
    finally:
        sock.close()
    return 0


def _open_channel():
    """按环境变量接管通道，返回具备 recv/sendall 接口的对象。"""
    raw_fd = os.environ.get("PROC_IPC_FD")
    if raw_fd is not None:
        sock = socket.socket(fileno=int(raw_fd))
        # 宿主侧为异步运行时把通道设成了非阻塞（tokio 要求）；本实现是
        # 同步串行循环，归一回阻塞模式，recv 才能正常等数据。
        sock.setblocking(True)
        return sock
    addr = os.environ.get("PROC_IPC_ADDR")
    if addr:
        # Windows named pipe：按文件打开即客户端连接（byte 模式，短读
        # 由 read_exact 循环归一）。
        return _PipeChannel(open(addr, "r+b", buffering=0))
    raise ProtocolError("PROC_IPC_FD / PROC_IPC_ADDR missing: not a plugin child")


class _PipeChannel:
    """named pipe 文件对象的 recv/sendall 适配（对齐 socket 接口）。"""

    def __init__(self, file):
        self._file = file
        self._fd = file.fileno()

    def recv(self, n: int) -> bytes:
        return os.read(self._fd, n)

    def sendall(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            written = os.write(self._fd, view)
            view = view[written:]

    def close(self) -> None:
        self._file.close()


def _handshake(sock, capabilities, server_info, token):
    kind, payload = read_frame(sock)
    if kind != KIND_RPC:
        raise ProtocolError("initialize must be an rpc frame")
    request = decode_message(payload)
    if "method" not in request or request["method"] != "initialize":
        raise ProtocolError("first message must be initialize")
    params = request.get("params") or {}
    remote = params.get("protocol_version")
    if type(remote) is not int or remote >> 16 != PROTOCOL_VERSION_MAJOR:
        # 版本门：不等即 -32001（message 带双方主版本），不降级。
        error = {
            "code": -32001,
            "message": (
                f"protocol major version mismatch: "
                f"host={remote >> 16 if type(remote) is int else '?'}, "
                f"peer={PROTOCOL_VERSION_MAJOR}"
            ),
            "data": {"peer_major": PROTOCOL_VERSION_MAJOR},
        }
        reply = {"jsonrpc": "2.0", "id": request.get("id"), "error": error}
        sock.sendall(encode_frame(KIND_RPC, encode_message(reply)))
        raise ProtocolError("major version mismatch")
    result = {
        "protocol_version": PROTOCOL_VERSION,
        "capabilities": list(capabilities),
        "server_info": server_info
        or {"name": "python-reference", "version": "0.1.0"},
    }
    # 会话令牌回验（wire.md §3.5）：回连承载必回显；fd 承载无令牌，
    # 字段缺省（保持与向量的规范形逐字节一致）。
    if token is not None:
        result["session_token"] = token
    reply = {"jsonrpc": "2.0", "id": request["id"], "result": result}
    sock.sendall(encode_frame(KIND_RPC, encode_message(reply)))


def _serve_loop(sock, dispatch):
    while True:
        kind, payload = read_frame(sock)
        if kind != KIND_RPC:
            raise ProtocolError("unexpected binary frame without negotiation")
        message = decode_message(payload)
        if "id" not in message:
            continue  # 通知：不回响应（wire.md §2.1）
        if "method" not in message:
            continue  # 迟到的响应：插件侧没有在飞的请求，忽略
        try:
            result = dispatch(message["method"], message.get("params"))
            reply = {"jsonrpc": "2.0", "id": message["id"], "result": result}
        except MethodNotFound as error:
            reply = {
                "jsonrpc": "2.0",
                "id": message["id"],
                "error": {"code": -32601, "message": str(error)},
            }
        sock.sendall(encode_frame(KIND_RPC, encode_message(reply)))


if __name__ == "__main__":
    sys.exit(serve())
