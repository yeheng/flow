#!/bin/sh
# 构建 Postgres 形态的 flow 应用 → dist/pg/
#
# 产物：
#   dist/pg/bin/flow-server        JSON-RPC 2.0 over WebSocket 服务
#   dist/pg/bin/flow-executor      受管理执行子进程（ipc 模式；与 server 同目录）
#   dist/pg/bin/flow-agent         远程执行中继（独立部署形态）
#   dist/pg/bin/flow-cli           命令行客户端
#   dist/pg/bin/flow-journal-tool  journal 离线维护
#   dist/pg/bin/flow-journal-*     journal 开发工具（server/bench/dev）
#   dist/pg/run-server.sh          预设 FLOW_BACKEND=postgres 的启动脚本
#
# 说明：
# - 后端不在构建期区分：同一份二进制集合，FLOW_BACKEND 在进程入口决定。
#   本脚本打包 PG 部署形态并预设 FLOW_BACKEND=postgres；多节点在多台
#   机器上各自解包同一份产物即可（对等模式，FLOW_ROLE=all/gateway/executor）。
# - 执行器定位契约（I09）：flow-server / flow-agent 在自身同目录召唤
#   flow-executor（部署形态即本脚本的 bin/ 布局；FLOW_EXECUTOR_BIN 可显式覆盖）。
# - 经 scripts/release.sh 包装：受 Xcode 21 ld LINKEDIT bug 影响的机器
#   自动切换 rust-lld。
#
# 用法：scripts/build-pg.sh [额外 cargo build 参数...]
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/dist/pg"
BINS="flow-server flow-executor flow-agent flow-cli flow-journal-tool flow-journal-bench flow-journal-server flow-journal-dev"

mkdir -p "$DIST/bin"
"$ROOT/scripts/release.sh" cargo build --release --workspace "$@"
for bin in $BINS; do
  cp "$ROOT/target/release/$bin" "$DIST/bin/$bin"
done

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
exec "$DIR/bin/flow-server"
EOF
chmod +x "$DIST/run-server.sh"

echo "built: $DIST/bin/  ($BINS)"
echo "entry: $DIST/run-server.sh  (FLOW_BACKEND=postgres, FLOW_ROLE=all)"
echo "cli:   $DIST/bin/flow-cli --url ws://127.0.0.1:9800 --help"
