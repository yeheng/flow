# 内联 agent 与工作流运行边界修复

日期：2026-10-23。保留单主 SQLite 权威、统一 reducer、现有授权/结果/恢复协议；没有新增 agent 调度内核或改变持久化格式。

## 已实现

- agent 生命周期从取得许可开始统一计时，覆盖目录准备、conga 初始化、回合、session 清理和 outcome 发布。取消/超时后保留不确定结果。独立系统线程在五秒宽限后退出 executor，检查粒度 250ms；父端另有跨重连保留、心跳不能延长的截止时间与有界取消回收。
- 服务关闭现在等待执行端口的跟踪任务完成：本地空闲/预热/在途 executor 必须 kill/wait/reap，远程端口停止监听与会话。没有审计接收者时不再等空闲心跳。补充真实 PID 的关闭回归，避免 `close()` 返回后子进程仍存活。
- agent 成功后也回收整个 executor 会话/Job；普通任务继续复用池。并发至多 `ceil(limit/2)`，避免长 agent 用完普通节点的全部派发容量。只有一个槽时串行执行。
- Linux/macOS 每 250ms 采样执行器会话的 RSS 总和与进程数，预算 1 GiB / 64；Unix 工具继承单文件 256 MiB 硬限制。最终文本在拼接时就截断，避免先复制全部输出再截断。
- 每个工作流常驻最新版本与最高已发布版本。历史版本保留在 SQLite；历史列表复用已有、有响应大小上限的数据库事务读取，不持有协调器决策锁。旧版本发布、启动、固定子流程与 webhook 继续支持。
- 并发发布后的启动提交使用已冻结且不可变的已发布定义，只重新检查工作流存在性和删除状态，避免把缓存逐出误判成版本不存在。
- 新增 `resident_definition_versions` 与 `resident_waiting_nodes` 诊断。1000-run 验收改用等待计数轮询，避免每 100ms 导出全历史、持有决策锁干扰被测系统。
- OPS/EXECUTION 已移除旧 runner 的 HOME 隔离、stdin 传密钥和独立进程组取消描述。新增 `flow-capacity agent-soak`：普通任务混合内联 agent 两轮模型与真实 bash 工具，provider 为本地 mock。

## 边界与新发现

- RSS/PID 是周期采样预算，不能阻止瞬时超量，共享页会保守累加；单文件限制不能代替总磁盘配额。CPU、总磁盘、模型总费用、脱离会话的 daemon 需要部署级约束。Windows 的 RSS/PID 预算本轮没有实现和验收；Linux 原生验收也不能由 macOS 代替。
- HOME/XDG/CARGO_HOME 仍按白名单继承，临时工作目录不是安全沙箱。一次性 executor 消除了任务之间的 host 全局状态继承，没有改变 FullAuto 的工具权限。
- 仍保留全局 16 个派发、1000 个非终态运行上限；没有将扩远程 worker 等同于提高全局并发，也没有实现 master HA。
- 事实、正文、冻结定义与幂等回执不自动删除。先明确磁盘/备份预算与引用闭包，再讨论归档 GC；不会通过盲目 TTL 损坏恢复语义。
- 首轮 agent-soak 使用 `printf agent-soak`，暴露 conga 固定 rev `425d786` 的 shell 帧缺陷：非换行输出与完成 sentinel 拼在同一行，而解析只接受行首 sentinel。引擎会有界超时并进入 Uncertain，但这个依赖缺陷仍存在，不能宣称任意 shell 工具都能正常完成。保留专项测试 `unterminated_shell_output_cannot_hold_an_agent_slot_forever` 与失败压测报告；常规压测使用 `echo agent-soak`。未修改 Cargo 缓存或悄悄更换上游版本。

## 验证

环境：macOS / aarch64；依赖按 Cargo.lock 离线构建。所有压测均为本机可重复回归，不能作为生产 SLO 或饱和吞吐承诺。

- `cargo build --locked --offline --workspace` 通过；`cargo test --locked --offline --workspace --all-targets --no-fail-fast`：84 个测试目标，477 项通过、0 失败、11 项忽略。ignored 的 1000-run 另行显式执行，不能把其他 ignored 专项算作通过。
- `cargo fmt --all -- --check`、`git diff --check` 与全 workspace / all-targets 严格 clippy（`-D warnings`）均通过。
- `journal_capacity::thousand_waiting_runs_release_slots_and_recover_bounded_dispatch` 显式启用 ignored 后通过：1000 个非终态运行占满准入、人工等待释放槽位、重启恢复、取消；2 个执行槽位，整项耗时 187.50 秒（包含创建、等待、重启、取消）。首次使用全历史轮询的版本超时，未计为通过。
- 最终 release `flow-capacity agent-soak`：请求 180 秒，实测 180.345 秒；857 次提交，735 成功、122 预期取消，0 失败、0 超时；244 次 mock provider 请求，覆盖 122 次 agent 两轮与真实 bash 调用。
- 该混合负载端到端延迟 p95 1158.49ms / p99 1585.73ms；重开 28.42ms；关闭后进程树采集完整，子进程列表为空。并发上限 4、最多 4 个在飞 run、200ms 提交间隔，没有饱和吞吐承诺。
- 最终采样的观测日志 `queue_dropped=214492`、`history_incomplete=true`，保留/存储丢弃均为 0。高频日志按既有有界观测合同可丢弃；这次验证没有建立逐条日志完整留存保证。
- 首轮无换行输出负载：168 次提交中 24 次 agent 等待超时，结果保留为 Uncertain；命令返回失败状态并保留失败报告。常规负载通过不能抵消此依赖缺陷。

原始证据：

- `/private/tmp/flow-agent-soak-20261023-reaped.json`：最终压测及资源/恢复采样。
- `/private/tmp/flow-agent-soak-20261023/failed-soak.json`：无换行输出缺陷的失败压测。
- `/private/tmp/flow-agent-capacity-1000-final.log`：1000-run 专项。
- `/private/tmp/flow-agent-repair-complete-tests.log`、`/private/tmp/flow-agent-repair-complete-clippy.log`：最终 workspace 检查。

复测命令见 [OPS](../OPS.md)。Windows/Linux 目标平台的原生执行、真实模型费用与更长时间负载仍需在对应部署环境验收。
