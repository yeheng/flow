#!/bin/sh
# 构建 Postgres 形态的 flow 应用 → dist/pg/
#
# 产物：
#   dist/pg/flow               统一二进制（server/cli/executor/agent/journal-*）
#   dist/pg/run-server.sh      预设 FLOW_BACKEND=postgres 的启动脚本
#
# 说明：
# - 后端不在构建期区分：同一份二进制，FLOW_BACKEND 在进程入口决定。
#   本脚本打包 PG 部署形态并预设 FLOW_BACKEND=postgres；多节点在多台
#   机器上各自解包同一份产物即可（对等模式，FLOW_ROLE=all/gateway/executor）。
# - 执行器不需要单独分发：FLOW_EXECUTION_MODE=ipc 时主进程以
#   `flow executor` 自召唤同一文件（FLOW_EXECUTOR_BIN 可显式覆盖）。
# - 经 scripts/release.sh 包装：受 Xcode 21 ld LINKEDIT bug 影响的机器
#   自动切换 rust-lld。
#
# 用法：scripts/build-pg.sh [额外 cargo build 参数...]
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/dist/pg"

mkdir -p "$DIST"
"$ROOT/scripts/release.sh" cargo build --release -p flow-app "$@"
cp "$ROOT/target/release/flow" "$DIST/flow"

cat > "$DIST/run-server.sh" <<'EOF'
#!/bin/sh
# Postgres 部署启动脚本：共享日志 + epoch 租约 + 持久 inbox（对等模式）。
DIR="$(cd "$(dirname "$0")" && pwd)"
export FLOW_BACKEND=postgres
# 必填：数据库连接串（缺省库名会按需建表）。
: "${FLOW_DATABASE_URL:?FLOW_DATABASE_URL is required, e.g. postgres://flow:flow@127.0.0.1:5432/flow}"
export FLOW_ROLE="${FLOW_ROLE:-all}"
export FLOW_ADDR="${FLOW_ADDR:-127.0.0.1:9800}"
export FLOW_HTTP_ADDR="${FLOW_HTTP_ADDR:-127.0.0.1:9801}"
exec "$DIR/flow" server
EOF
chmod +x "$DIST/run-server.sh"

echo "built: $DIST/flow"
echo "entry: $DIST/run-server.sh  (FLOW_BACKEND=postgres, FLOW_ROLE=all)"
echo "cli:   $DIST/flow cli --url ws://127.0.0.1:9800 --help"
