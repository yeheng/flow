# 后端审查修复记录

本文件记录 backend-review-linus.md 的实施状态。未列为完成的工作仍需继续；默认测试通过不能替代容量验收。

| 发现 | 当前实施状态 |
| --- | --- |
| F01/F02 | harness 并发处理 stdin/stdout/stderr/退出；全程 timeout 与取消；stdout 限制为 MAX_VALUE_BYTES；stderr 512 字节尾部；失败 kill 并等待回收，返回 uncertain，不提交截断成功结果。新增 4 项管道故障测试。 |
| F03 | 上联消息携带 OwnedSemaphorePermit，发送失败、包装失败、队列关闭和取消均由所有权归还预算。新增反复队列关闭/发送失败测试。 |
| F04 | 每个 run 事件应用后统一聚合所有 uncertain wait，终态优先；旧 checkpoint 加载时规范化非终态聚合状态。新增多等待裁决与终态优先测试。 |
| F05 | Definition 数据私有，只提供只读 Deref；DefinitionData 是编辑 DTO，editable() 后 compile() 生成新定义及新索引。现有只读字段访问无需迁移；写字段的调用方须迁移到编辑 DTO。序列化保持 nodes/edges，无缓存字段。新增重命名重编译测试。 |
| F06/F07 | **未完成**：仍需拆分活跃状态、永久审计磁盘索引及增量投影，并缩小 reducer staging。不能删除终态历史或去重证据。 |
| F08 | **未完成**：Wait tagged enum 及旧快照序列化适配尚未实施。 |
| F09 | 保留现有 wire 格式；定义校验拒绝 run-input/prepared-params 和超过 253 UTF-8 字节的节点 ID。旧定义仍保留原始 ID；重新发布或执行校验时明确拒绝，须显式创建新定义版本，不静默改名。旧已运行执行的兼容矩阵和后续协议版本迁移仍需验证。 |
| F10 | input 节点 resolved 输出经过 store_value；100 KiB 真实 executor 集成测试成功，输出 Ref 且字节一致。 |
| F11 | write/flush/shutdown/入站投递监听取消；close 后阻塞任务退出；Drop 排空写队列有宽限，超时 abort 并回收。新增停读和满入站队列测试。真实 TCP/flush 永久 pending 专项仍待扩展。 |
| F12 | 日志入口 256 条、每行最多 8 KiB 加短截断标记，总排队载荷有固定上限；有限软硬预算、入口丢弃计数、100 ms 周期 flush；关闭超时 abort + join。新增洪泛与稀疏日志测试。 |
| F13 | 对账整轮 30 秒、每页 5 秒、shutdown 可中断；正式校验页号与完整 dispatch 集；executor 启动移到最多 4 个并发的 JoinSet，启动数计入 slots。句柄丢弃会取消执行器。静默 master、慢启动与取消竞态的完整故障矩阵仍待扩展。 |
| F14 | TLS/WSS 握手 permit + deadline；认证后注册表独立限制 256 agent，同身份可替换；TaskTracker 管理监听/握手/会话，shutdown 取消并等待；第二监听及执行服务启动失败回滚。新增静默 WSS 握手关闭和端口重新绑定测试。256 连接容量矩阵仍待执行。 |
| F15 | 共享入站循环改用有界 runner 队列 try_send；满/关闭明确使会话失效并触发 Resume 裁决，避免永久阻塞。未实现额外公平积压，尚需一慢多快的专门故障测试。 |
| F16 | legacy-source 不可变；每次导入创建独立工作副本做 WAL checkpoint，退出时清理；WAL 源双次导入、源/证据字节相同的集成测试通过。 |
| F17 | baseline/raw 恢复读取永久 state.legacy，不依赖 COMMAND_WINDOW；baseline 物化既有数据比较，raw 核对源 digest/路径/偏移。超过 8192 条后重入的容量测试尚未执行。 |
| F18 | config.update 持有写锁跨越读改写和保存；config/secret 使用独占唯一临时文件、sync_all、rename；Unix 同步父目录。非 Unix 不宣称父目录断电持久保证。新增临时文件策略沿用写失败内存回滚测试。父目录 sync 失败发生在 rename 后，此时返回失败但磁盘可能已变更。 |
| F19 | **未完成**：元数据专用读取和 timeline 请求总物化预算/分页仍待实施。 |

R08 的存储/集群 ADR、R16 的历史规模与 30 分钟 soak 均未完成，不能以本次单元和集成测试代替。

验证：

- 四个核心 crate 单元测试：137 passed，0 failed。日志 `/private/tmp/flow-review-final-unit.log`。
- legacy 导入集成：2 passed，0 failed。日志 `/private/tmp/flow-review-import.log`。
- 大值 IPC/remote 集成：3 passed，0 failed。日志 `/private/tmp/flow-review-transfer.log`。
- workspace 编译通过。
- workspace 全量测试未全部通过：`backend-e2e::journal_v2::configured_cluster_requires_feature_before_opening_the_journal` 失败；迁移后的 `flow-journal-server-e2e` 薄壳调用 Config::load(None)，没有读取测试传入的 --config。测试目录迁移属于同时进行的工作区改动，本次保留。
- 严格 clippy 被既有 `flow-config::AgentCredential` 可派生 Default 告警挡住；未将其描述为通过。

未验证 Windows、PG、真实断电、完整远程故障矩阵及长期容量。
