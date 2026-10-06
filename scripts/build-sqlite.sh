#!/bin/sh
# 构建单体 SQLite 形态的 flow 应用 → dist/sqlite/
#
# 产物：
#   dist/sqlite/flow           统一二进制（server/cli/executor/agent/journal-*）
#   dist/sqlite/run-server.sh  预设 FLOW_BACKEND=sqlite 的启动脚本
#
# 说明：
# - 后端不在构建期区分：同一份二进制，FLOW_BACKEND 在进程入口决定
#   （sqlite 缺省 | postgres）。本脚本的存在意义是「打包 + 预设环境 +
#   明确部署形态」，而不是裁剪二进制。
# - 执行器不需要单独分发：FLOW_EXECUTION_MODE=ipc 时主进程以
#   `flow executor` 自召唤同一文件（FLOW_EXECUTOR_BIN 可显式覆盖）。
# - 经 scripts/release.sh 包装：受 Xcode 21 ld LINKEDIT bug 影响的机器
#   自动切换 rust-lld。
#
# 用法：scripts/build-sqlite.sh [额外 cargo build 参数...]
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/dist/sqlite"

mkdir -p "$DIST"
"$ROOT/scripts/release.sh" cargo build --release -p flow-app "$@"
cp "$ROOT/target/release/flow" "$DIST/flow"

cat > "$DIST/run-server.sh" <<'EOF'
#!/bin/sh
# 单体 SQLite 部署启动脚本：单进程·单线程·单写者（canonical 后端）。
# 数据目录即用即建；端口/路径均可用环境变量覆盖。
DIR="$(cd "$(dirname "$0")" && pwd)"
export FLOW_BACKEND="${FLOW_BACKEND:-sqlite}"
export FLOW_DATA_DIR="${FLOW_DATA_DIR:-$DIR/data}"
export FLOW_DB="${FLOW_DB:-$FLOW_DATA_DIR/flow.db}"
export FLOW_ADDR="${FLOW_ADDR:-127.0.0.1:9800}"
export FLOW_HTTP_ADDR="${FLOW_HTTP_ADDR:-127.0.0.1:9801}"
# 可选：FLOW_EXECUTION_MODE=ipc 启用子进程执行（无需额外二进制，
# 主进程自召唤 ./flow executor）；FLOW_SCHEDULER=off 关闭 cron。
exec "$DIR/flow" server
EOF
chmod +x "$DIST/run-server.sh"

echo "built: $DIST/flow"
echo "entry: $DIST/run-server.sh  (FLOW_BACKEND=sqlite)"
echo "cli:   $DIST/flow cli --url ws://127.0.0.1:9800 --help"
