# 07 · 崩溃恢复与测试策略（§12、§14）

> 本文是 [DESIGN.md](../DESIGN.md) 的分篇。章节编号与全文一致；总索引与核心抉择见 [DESIGN.md](../DESIGN.md)。

## 12. 崩溃恢复

### 12.1 启动序列

1. 抢占数据目录 flock，校验 SQLite 格式和数据集身份；旧格式目录拒绝打开，不自动迁移。
2. 打开只读查询连接，加载控制元数据、schedule fire 水位、活跃 run/node/current attempt。
3. 终态 run、旧 attempt 和值进度按需读取；缓存不承担永久去重职责。
4. 播种 dirty runs，安装协调器，提交 master epoch 后继续安全执行。

不需要应用 checkpoint，也不等待独立投影。WAL 的恢复由 SQLite 完成。

### 12.2 故障处置

| 现象 | 处置 |
| --- | --- |
| 当前实体摘要不符 | 报错停止使用；不自动覆盖权威数据 |
| 写入/COMMIT 出错 | writer 冻结，修复介质条件后重开；原请求结果可能未知，按身份查回执 |
| 历史事实或正文校验失败 | 显式报告；停写、保留证据、从校验通过的一致备份恢复 |

备份使用 SQLite Backup API 的一致快照，包含事实、当前状态、回执和 BLOB。
不支持把任意旧 LSN 的日志前缀直接恢复成完整当前库。

### 12.3 观测日志不是权威

observations 使用独立队列与磁盘限额。删除后日志永久丢失，但不改变执行结果。
`flow.sqlite` 及运行中的 WAL 不具备这种可删除属性。

### 12.4 故障注入

`flow-backend/src/execution/dispatch.rs::fault` 提供一个**全局**命名观测点注册表
（`register` / `clear` / `checkpoint`），测试用它确定性地在「结果提交前」
「取消后」等精确位置切断。因为注册表是全局的，注入测试必须串行执行。

---

## 14. 测试策略

| 层 | 位置 | 覆盖 |
| --- | --- | --- |
| **单元** | 各 crate `#[cfg(test)]` | 折叠规则、gate 判定、模板展开、codec 预算、协议编解码、幂等去重 |
| **e2e** | `crates/backend-e2e/tests/` | 与 `remote-e2e` 共同覆盖真实 server/executor/agent、故障注入和新版产品合同 |
| **性能** | `crates/backend-perf/` | 黑盒压测 harness |
| **故障注入** | `execution/dispatch.rs::fault` | 在「结果提交前」「取消后」等精确位置确定性切断 |

**e2e 是这份设计的真正守门人**。原因：本设计的多数不变量（I6–I24）只有在
「进程真的被杀、连接真的断」时才可能被违反。单测里 mock 掉的时序，恰是这些
bug 的栖息地。
