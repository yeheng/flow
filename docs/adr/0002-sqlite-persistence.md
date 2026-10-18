**ADR 0002：SQLite 数据结构与提交协议**

状态：当前实现。字段与 SQL 以 `crates/flow-journal/src/schema.sql` 为准。

`flow.sqlite` 是唯一业务权威。transactions/events 保存已接受事实，current_entities 保存同一次状态转移生成的当前行和永久命令回执，value_chunks 保存被事实引用的正文。唯一 writer 在一个 SQLite 事务中提交它们；COMMIT 成功后才更新内存与返回回执。SQLite WAL、FULL 同步与数据目录排他锁承担本地事务和单写者边界。

正常启动加载当前实体，不全量回放历史。`check-projection` 是离线校验：使用同一个 reducer 重放事实，再比较当前行，能发现摘要仍合法但业务状态不符的情况。它不自动修复权威数据。

外部系统不参与本地事务。意图与授权必须先提交，执行器再调用外部系统；结果持久捕获后才可推进。未知结果默认等待裁决，仅显式声明 repeatable 的节点可获得机器重试授权。数据库事务不能赋予任意 HTTP 调用恰好一次语义。

格式 v4 对旧程序设置执行语义门；表结构沿用 v3。`flow-journal-tool upgrade` 只支持 v3 到 v4 的离线复制升级，保留源目录，不覆盖目标目录。完整合同、证据包与命令见 [EXECUTION.md](../EXECUTION.md)。

备份使用 SQLite Backup API 的一致快照，包含事实、当前状态、回执和正文。不支持将任意历史 LSN 前缀直接作为当前数据库启动。业务日志与永久回执继续保留；observations 独立且可丢弃。当前部署为单机权威加可扩展执行器，不提供控制平面 HA。
