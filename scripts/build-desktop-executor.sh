#!/bin/sh
# 桌面开发前置：构建 flow-executor 并放到 Tauri externalBin 约定位置
# （web/src-tauri/bin/flow-executor-<target-triple>）。tauri dev/build 都需要。
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${1:-$(rustc -vV | sed -n 's/^host: //p')}"
OUT="$ROOT/web/src-tauri/bin/flow-executor-$TARGET"
cd "$ROOT"
cargo build --release -p flow-executor --target "$TARGET"
mkdir -p "$ROOT/web/src-tauri/bin"
cp "target/$TARGET/release/flow-executor" "$OUT"
echo "externalBin ready: $OUT"
