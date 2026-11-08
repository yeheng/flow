#!/usr/bin/env python3
"""echo 插件：动态节点类型的参考实现（e2e 专用）。

实现通用 node 契约（manifest methods.invoke = "node/invoke"）：
- initialize 握手（plugin-proto）；
- `node/invoke` 请求 `{params: {text, times, mode}}` → 响应
  `{output: {echo: <text 重复 times 次>, mode: <mode>, via: "python-echo"}}`。

用它验证「manifest 声明全新 node type → nodetypes.list → publish 接受 →
运行时派发进插件进程 → 输出落 journal」的完整闭环。
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from plugin_rpc import (  # noqa: E402
    KIND_RPC,
    MAX_FRAME_BYTES,
    ProtocolError,
    decode_message,
    encode_frame,
    encode_message,
    read_frame,
)

INVOKE = "node/invoke"


def main() -> int:
    raw_fd = os.environ.get("PROC_IPC_FD")
    if raw_fd is None:
        print("PROC_IPC_FD missing", file=sys.stderr)
        return 2
    import socket

    sock = socket.socket(fileno=int(raw_fd))
    sock.setblocking(True)
    token = os.environ.get("PROC_IPC_TOKEN")
    try:
        kind, payload = read_frame(sock)
        request = decode_message(payload)
        if request.get("method") != "initialize":
            raise ProtocolError("first message must be initialize")
        result = {
            "protocol_version": 1,
            "capabilities": [],
            "server_info": {"name": "echo-plugin", "version": "0.1.0"},
        }
        if token is not None:
            result["session_token"] = token
        reply = {"jsonrpc": "2.0", "id": request["id"], "result": result}
        sock.sendall(encode_frame(KIND_RPC, encode_message(reply)))

        while True:
            try:
                kind, payload = read_frame(sock)
            except (ProtocolError, OSError):
                return 0
            message = decode_message(payload)
            if "id" not in message:
                continue
            method = message.get("method")
            if method == INVOKE:
                params = (message.get("params") or {}).get("params") or {}
                text = str(params.get("text", ""))
                times = int(params.get("times", 1))
                mode = str(params.get("mode", "upper"))
                body = mode if mode in ("upper", "lower") else mode
                echoed = (text + " ") * max(1, times)
                echoed = echoed.upper() if mode == "upper" else echoed.lower()
                output = {"echo": echoed.strip(), "mode": body, "via": "python-echo"}
                reply = {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "result": {"output": output},
                }
            else:
                reply = {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "error": {"code": -32601, "message": f"method not found: {method}"},
                }
            sock.sendall(encode_frame(KIND_RPC, encode_message(reply)))
    except ProtocolError as error:
        print(f"echo plugin session end: {error}", file=sys.stderr)
        return 0
    finally:
        sock.close()


if __name__ == "__main__":
    raise SystemExit(main())
