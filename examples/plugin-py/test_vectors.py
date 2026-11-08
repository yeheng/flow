#!/usr/bin/env python3
"""对 golden vectors 的逐字节校验（跨语言用法示范，wire.md §7）。

用法：
    python3 test_vectors.py            # 默认指向 crates/flow-plugin-sdk（protocol 模块）/tests/vectors
    python3 test_vectors.py <目录>

对每条向量断言：
1. decode_frame(bytes_hex) 成功且 kind 正确；
2. encode_frame(kind, payload) 与 bytes_hex **逐字节一致**（帧级规范形）；
3. rpc：encode_message(decode_message(payload)) 与 payload **逐字节一致**
   （消息级规范形），且语义与 message 字段相等；
4. binary：payload 与 payload_hex 一致。

退出码非 0 即失败。拷到任何语言里照抄这套断言就是接入了。
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from plugin_rpc import (  # noqa: E402
    KIND_BINARY,
    KIND_RPC,
    ProtocolError,
    decode_frame,
    decode_message,
    encode_frame,
    encode_message,
)

DEFAULT_DIR = (
    pathlib.Path(__file__).resolve().parents[2]
    / "crates"
    / "flow-plugin-sdk"
    / "tests"
    / "vectors"
)


def check_vector(path: pathlib.Path) -> "list[str]":
    vector = json.loads(path.read_text(encoding="utf-8"))
    name = vector["name"]
    raw = bytes.fromhex(vector["bytes_hex"])
    problems = []
    try:
        kind, payload = decode_frame(raw)
    except ProtocolError as error:
        return [f"{name}: decode failed: {error}"]

    expected_kind = {"rpc": KIND_RPC, "binary": KIND_BINARY}[vector["kind"]]
    if kind != expected_kind:
        problems.append(f"{name}: kind {kind} != {expected_kind}")

    if encode_frame(kind, payload) != raw:
        problems.append(f"{name}: frame re-encode is not byte-identical")

    if vector["kind"] == "rpc":
        try:
            message = decode_message(payload)
        except ProtocolError as error:
            problems.append(f"{name}: message decode failed: {error}")
            return problems
        if encode_message(message) != payload:
            problems.append(f"{name}: canonical re-encode is not byte-identical")
        if message != vector["message"]:
            problems.append(f"{name}: semantic mismatch with vector message")
    else:
        if payload != bytes.fromhex(vector["payload_hex"]):
            problems.append(f"{name}: binary payload mismatch")
    return problems


def main() -> int:
    target = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DIR
    paths = sorted(target.glob("*.json"))
    if not paths:
        print(f"no vectors found in {target}", file=sys.stderr)
        return 1
    failures = []
    for path in paths:
        failures.extend(check_vector(path))
    for line in failures:
        print(f"FAIL {line}", file=sys.stderr)
    print(f"{len(paths)} vectors, {len(failures)} failures")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
