#!/bin/sh
# 桌面开发前置：构建 flow-executor 与 flow-harness-agent 并放到 Tauri
# externalBin 约定位置（web/src-tauri/bin/<bin>-<target-triple>）。runner
# 与 executor 同目录分发（S4：同批发布，协议由锁步构建挡住漂移）。
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${1:-$(rustc -vV | sed -n 's/^host: //p')}"
cd "$ROOT"
cargo build --release -p flow-executor -p flow-harness-agent --target "$TARGET"
mkdir -p "$ROOT/web/src-tauri/bin"
cp "target/$TARGET/release/flow-executor" "$ROOT/web/src-tauri/bin/flow-executor-$TARGET"
cp "target/$TARGET/release/flow-harness-agent" "$ROOT/web/src-tauri/bin/flow-harness-agent-$TARGET"
echo "externalBin ready: $ROOT/web/src-tauri/bin (executor + harness-agent, $TARGET)"
