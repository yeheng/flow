# VENDOR.md — agent 运行时来源与升级流程

本 workspace 的 `agent-core` / `agent-host` 裁剪自第三方 agent core/host
（原 vendored 位置：flow 仓库 `crates/flow-harness-agent-core` 与
`crates/flow-harness-agent-host`，2026-11 迁出为本独立项目并去 flow 前缀）。

## 为什么迁出

1. **运行时隔离**：agent 循环（provider SSE、工具进程、会话状态机）整体
   搬进 `flow-agent-plugin` 插件进程，flow-executor 只持有 wire 类型，
   不再链接任何 agent 运行时代码——升级 agent 运行时不再触碰执行器。
2. **复用**：这套运行时 + 插件协议可被 flow 之外的宿主使用（传输与协议
   层是通用件：`proc-ipc` / `plugin-proto`）。

## 升级流程（自上游第三方版本）

1. 对照上游 tag 做**裁剪式**合并：只取 agent 循环、provider、类型与
   headless 装配相关文件；上游的交互式 UI / CLI 面不要。
2. 保持 crate 对外 API 面最小：executor 只经 `agent-plugin` 的 wire 类型
   交互，core/host 的类型不得出现在 flow 侧。
3. 全量回归：`cargo test --manifest-path agent-plugin/Cargo.toml`（本
   workspace）+ flow 侧 `cargo test -p backend-e2e --test agent_isolation
   --test agent_lifecycle --test agent_provider --test agent_workdir`
   （真实插件进程端到端）。
4. 任何 wire 语义变化（方法、事件、错误码）先改 `README.md` 的契约表，
   再改实现——文档是契约本体。

## 命名

crate 名不带 flow 前缀（`agent-core` / `agent-host` / `agent-plugin`）；
产物二进制名 `flow-agent-plugin` 保留 flow 前缀：它是被 flow-executor 按
「同目录兄弟」定位的部署件，名字里带宿主标识避免与未来其他宿主的
插件二进制混淆。
