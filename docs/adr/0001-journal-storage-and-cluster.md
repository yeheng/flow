# ADR 0001：保留 journal 权威，限制集群职责，保留 SQLite 替代路线

状态：采纳。日期：2026-10-16。范围：桌面、离线单机、单 master 加远程计算；本次不替换生产持久层。

## 决策与证据

保留现有 JSONL journal 作为唯一权威；执行历史改存可重建的 SQLite 派生索引，内存只保留活跃执行状态和有界缓存。原来的完整历史常驻与整 run 重写问题不需要先迁移权威存储才能解决。

[容量报告](../backend-capacity-evidence.md)使用同一台 macOS/aarch64 机器、release profile，对照原审查提交 e58b9ab84a0a26d97a2c1f52ac20529385b2adaf。固定一个活跃 run、10 万终态时，RSS 从 8.31 GB 降到 48.04 MB，checkpoint 从失败变为约 1.96 MB。代价是更多索引写入及磁盘空间：该工作负载目录从 814 MB 增到 2,106 MB。没有声称写吞吐提高。

相同 1 KiB 逻辑事实的最小原型，对比 journal 与 SQLite WAL / synchronous=FULL：

| 10,000 事实 | journal | SQLite 事件事务 |
| --- | ---: | ---: |
| 单事实提交总时长 | 108.34 s | 1.14 s |
| 单事实提交 P99 | 12.22 ms | 0.157 ms |
| 64 条一批总时长 | 1.96 s | 0.120 s |
| 64 条一批提交 P99 | 14.26 ms | 6.47 ms |
| 64 条一批，回放全部事实 | 33.63 ms | 16.34 ms |
| 64 条一批，关闭后文件字节 | 11,318,292 | 13,697,024 |

[原始结果](../evidence/backend-review-2026-10-16/flow-cold-storage-10k.json)。journal 的默认聚合等待计入提交延迟；两条路径均等待各自持久提交。SQLite 原型没有实现 journal 的完整值协议、权威 reducer、索引重建、离线工具和兼容性，不能把这些数字称为完整产品吞吐比较。macOS FULL/WAL 与 journal sync 的真实掉电保证未实测。

两种存储均通过提交 128 条后 SIGKILL 的前缀探针。journal 恢复 129 条，SQLite 恢复 128 条；多出的 journal 条目是尚未确认但可能已持久提交的事务，调用方必须以稳定身份重试，不能把没有收到响应等同未提交。两边已确认的 128 条前缀完整。普通产品测试另覆盖损坏/缺失尾部、权威校验、旧 checkpoint、派生索引删除重建、重复及冲突审计。

## 为什么暂时保留

JSONL 当前提供明确的 LSN/身份/校验前缀、可离线核对与导出、历史值地址和现有恢复工具。它不是吞吐优势；最小实验反而支持把 SQLite 事件事务列为下一次服务端存储评估候选。当前瓶颈的直接原因是内存所有权和投影粒度，先修这些可以保持已有文件及 wire 契约。

维护成本必须显式承担：segment、值地址、checkpoint、尾部恢复、版本迁移和双派生库都需测试。新增 execution-index.sqlite 按提交前缀版本化并带摘要；产品投影只消费已提交变化，不再维护第二棵 reducer。删除派生库后可由原 journal 重建。历史保留导致磁盘增长，需独立保留策略；本次没有删除历史来换取容量数字。

## 未来迁移与回退契约

1. 停机后离线验证原 journal，保留 journal_id、逻辑 LSN、事务/event 身份、audit 序号和 ValueRef 的 codec、长度、摘要、内容。SQLite 原型只用于评估，不能直接接管现有目录。
2. 导入到新目录，保留每个原始事务的边界和幂等回执；依原顺序回放同一 reducer。比较每个 checkpoint 前缀、完整历史读、重复/冲突请求及原值字节，验证结束前旧目录只读保留。
3. 切换只能有一个权威 writer。禁止运行两套独立权威或把异步双写误称原子提交。新路径在故障后必须保住已确认前缀，未确认事务允许存在且可按稳定身份重试。
4. 首次切换失败且未接受新写入时，直接切回旧目录。接受新写入后，必须先将增量按逻辑 LSN 验证导出到兼容 journal，再停机切换；未实现这种导出前禁止承诺随时降级。
5. 本轮 binary 降级：停机并保留 journal，移走派生 projection.sqlite、execution-index.sqlite 和新 checkpoint，让旧 binary 回放原日志。新 binary 使用 reducer v5；不把旧 v3/v4 的完整历史 checkpoint 作为活跃状态载入。不要删除权威日志或值数据。

## 集群职责与产品边界

journal-cluster 的 membership/lease 仅用于明确部署的成员发现、身份与所有权约束；它不复制 journal，不提供自动故障切换、共识或多 master 写入。master 的磁盘丢失仍需要备份恢复，远程 agent 的计算扩容不改变此故障域。网络断连由 Resume 与已提交事实裁决，不允许推测 HTTP 等副作用可以安全重做。

桌面和离线产品继续不要求 PostgreSQL 或外部服务；未启用 feature 时配置 cluster 会在打开 journal 前明确拒绝。本轮编译 journal-cluster，但没有实际 PG 故障实验。

真实多租户服务端或 HA 需求出现时，另立评估：实现一条同业务的 Temporal 适配路径，衡量部署运维、离线不可用性、活动/重试及副作用语义、历史导出、升级回退、恢复时间和成本。没有该需求与实验前，不新增平台抽象，不把当前 membership 包装成 HA。
