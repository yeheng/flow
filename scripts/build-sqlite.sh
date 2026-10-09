#!/bin/sh
# 构建单机（默认 journal 后端）形态的 flow 应用 → dist/sqlite/
#
# 产物：
#   dist/sqlite/bin/flow-server        JSON-RPC 2.0 over WebSocket 服务
#   dist/sqlite/bin/flow-executor      受管理执行子进程（ipc 模式；与 server 同目录）
#   dist/sqlite/bin/flow-agent         远程执行中继（独立部署形态）
#   dist/sqlite/bin/flow-cli           命令行客户端
#   dist/sqlite/bin/flow-journal-tool  journal 离线维护
#   dist/sqlite/bin/flow-journal-*     journal 开发工具（server/bench/dev）
#   dist/sqlite/run-server.sh          单机 journal 启动脚本（缺省后端，无 FLOW_BACKEND）
#
# 说明：
# - 后端不在构建期区分：同一份二进制集合，storage.backend 在进程入口决定
#   （journal 缺省 | postgres；v1 sqlite 已删除）。本脚本的存在意义是
#   「打包 + 预设环境 + 明确部署形态」，而不是裁剪二进制。
# - 执行器定位契约（I09）：flow-server / flow-agent 在自身同目录召唤
#   flow-executor（部署形态即本脚本的 bin/ 布局；FLOW_EXECUTOR_BIN 可显式覆盖）。
# - 经 scripts/release.sh 包装：受 Xcode 21 ld LINKEDIT bug 影响的机器
#   自动切换 rust-lld。
#
# 用法：scripts/build-sqlite.sh [额外 cargo build 参数...]
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/dist/sqlite"
BINS="flow-server flow-executor flow-agent flow-cli flow-journal-tool flow-journal-bench flow-journal-server flow-journal-dev"

mkdir -p "$DIST/bin"
"$ROOT/scripts/release.sh" cargo build --release --workspace "$@"
for bin in $BINS; do
  cp "$ROOT/target/release/$bin" "$DIST/bin/$bin"
done

cat > "$DIST/run-server.sh" <<'EOF'
#!/bin/sh
# 单机 journal 部署启动脚本：v2 唯一权威 + 可重建投影。
# 数据目录即用即建；端口/路径均可用环境变量覆盖。
DIR="$(cd "$(dirname "$0")" && pwd)"
# 缺省后端 = journal（v1 sqlite 已删除；如需 Postgres 用 build-pg.sh 形态）
export FLOW_DATA_DIR="${FLOW_DATA_DIR:-$DIR/data}"
export FLOW_ADDR="${FLOW_ADDR:-127.0.0.1:9800}"
export FLOW_HTTP_ADDR="${FLOW_HTTP_ADDR:-127.0.0.1:9801}"
# 可选：FLOW_EXECUTION_MODE=ipc 启用子进程执行（主进程在 bin/ 内召唤
# 同目录的 flow-executor）；FLOW_SCHEDULER=off 关闭 cron。
exec "$DIR/bin/flow-server"
EOF
chmod +x "$DIST/run-server.sh"

echo "built: $DIST/bin/  ($BINS)"
echo "entry: $DIST/run-server.sh  (journal, default)"
echo "cli:   $DIST/bin/flow-cli --url ws://127.0.0.1:9800 --help"
