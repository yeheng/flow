#!/bin/sh
# 桌面开发前置：构建 flow-executor 并放到 Tauri externalBin 约定位置
# （web/src-tauri/bin/flow-executor-<target-triple>）。tauri dev/build 都需要。
# harness agent 模式已内联进 executor（conga-head 无头装配），无独立 runner。
# Windows 桌面是 in_process 过渡态（B 方案），executor 是 unix-only 产品：
# 写占位文件让 tauri externalBin 检查通过即可（不会被执行），
# 省掉一轮真实构建；随命名管道 transport（A 方案）再换真实产物。
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${1:-$(rustc -vV | sed -n 's/^host: //p')}"
OUT="$ROOT/web/src-tauri/bin/flow-executor-$TARGET"
mkdir -p "$ROOT/web/src-tauri/bin"
case "$TARGET" in
  *-pc-windows-*)
    printf 'placeholder: flow-executor is unix-only; windows desktop runs in_process (plan B)' \
      > "$OUT.exe"
    echo "externalBin placeholder: $OUT.exe"
    exit 0
    ;;
esac
cd "$ROOT"
cargo build --release -p flow-executor --target "$TARGET"
cp "target/$TARGET/release/flow-executor" "$OUT"
echo "externalBin ready: $OUT"
