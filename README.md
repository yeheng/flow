# flow

工作流引擎。统一二进制 `flow` 提供全部产品入口，后端（SQLite / Postgres）在进程启动时由 `FLOW_BACKEND` 选择，不在构建期区分。

## 快速开始

```sh
# 构建 SQLite 单体形态 → dist/sqlite/（含 run-server.sh）
scripts/build-sqlite.sh

# 构建 Postgres 部署形态 → dist/pg/（启动前须设置 FLOW_DATABASE_URL）
scripts/build-pg.sh

# 直接运行（SQLite 缺省）
./dist/sqlite/run-server.sh
# 客户端
./dist/sqlite/flow cli --url ws://127.0.0.1:9800 --help
```

## 子命令

| 子命令 | 职责 |
|---|---|
| `flow server` | JSON-RPC 2.0 over WebSocket 服务 + cron + webhook |
| `flow cli` | 命令行客户端（纯 RPC，后端无关） |
| `flow executor` | 受管理执行子进程（主进程自召唤或独立部署） |
| `flow agent` | 受信任远程执行中继（双 TLS 上联） |
| `flow journal-tool` | journal 离线维护（verify / index / backup / repair） |
| `flow journal-bench` | journal 写入基准 |
| `flow journal-server` | JSONL v2 开发服务（WS RPC + 下载） |
| `flow journal-dev` | JSONL v2 开发运行器（run / resume / import） |

执行器与主进程是同一个二进制：缺省以 `flow executor` 自召唤，
可用 `FLOW_EXECUTOR_BIN` 显式指定独立二进制。

## 仓库结构

- `crates/` — Rust workspace（引擎、journal、后端、RPC、执行器、agent 等）
- `web/` — Vue 前端（浏览器 + Tauri 桌面双入口，见 `web/README.md`）
- `docs/` — 设计与运维文档：`DESIGN.md`（总设计）、`JSONL_DEVELOPMENT.md`（JSONL v2 开发入口）、`AGENT_OPS.md`（远程 agent 运维）
- `scripts/` — 构建打包脚本

## 开发校验

与 CI 门禁一致（详见 `docs/DESIGN.md` §13）：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```
