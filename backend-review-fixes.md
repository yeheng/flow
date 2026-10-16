# 后端审查修复记录

本文件记录 backend-review-linus.md 的实施状态。以下状态表已更新到第三轮；首轮、第二轮记录保留作历史，当前验证以文末第三轮为准。默认测试通过不能替代容量验收。

| 发现 | 当前实施状态 |
| --- | --- |
| F01/F02 | harness 并发处理 stdin/stdout/stderr/退出；全程 timeout 与取消；stdout 限制为 MAX_VALUE_BYTES；stderr 512 字节尾部；失败 kill 并等待回收，返回 uncertain，不提交截断成功结果。新增 4 项管道故障测试。 |
| F03 | 上联消息携带 OwnedSemaphorePermit，发送失败、包装失败、队列关闭和取消均由所有权归还预算。新增反复队列关闭/发送失败测试。 |
| F04 | 每个 run 事件应用后统一聚合所有 uncertain wait，终态优先；旧 checkpoint 加载时规范化非终态聚合状态。新增多等待裁决与终态优先测试。 |
| F05 | Definition 数据私有，只提供只读 Deref；DefinitionData 是编辑 DTO，editable() 后 compile() 生成新定义及新索引。现有只读字段访问无需迁移；写字段的调用方须迁移到编辑 DTO。序列化保持 nodes/edges，无缓存字段。新增重命名重编译测试。 |
| F06 | 已完成冷历史迁移：execution-index.sqlite 永久保存可重建、带前缀版本和摘要的 run/node/attempt、audit 回执及 publication；master 只常驻活跃 run 和当前 attempt，发布缓存上限8192。checkpoint v5 不含终态历史；projector 从 committed 索引消费增量，不持第二份 reducer。10万终态历史 RSS48.04 MB、checkpoint约1.96 MB，旧索引删除及旧 checkpoint 回放已验证。 |
| F07 | 已拆到 attempt：事务回滚保存 run头、节点执行摘要和受影响 attempt 槽；产品投影拆 run/run_node/run_attempt，同事务合并相同实体更新。纯审计只改 attempt 和永久回执。10万重试仅常驻1个 attempt，RSS19.94 MB；10万审计RSS16.71 MB，未取消权威校验。 |
| F08 | Wait 改为内部 Delay/Signal/Child/Retry/Uncertain 枚举，Delay/Retry 必须有 wake_at，Child 必须有非空 child_run_id；兼容适配器保持旧 JSON kind/wake_at/child_run_id/output 形状，拒绝不适用字段及缺失必要字段。增加五种旧形状往返、非法组合测试，真实 retry/delay/child 恢复测试通过。 |
| F09 | 保留现有 wire 格式；定义校验拒绝 run-input/prepared-params 和超过 253 UTF-8 字节的节点 ID。旧定义仍保留原始 ID；重新发布或执行校验时明确拒绝，须显式创建新定义版本，不静默改名。旧已运行执行的兼容矩阵和后续协议版本迁移仍需验证。 |
| F10 | input 节点 resolved 输出经过 store_value；100 KiB 真实 executor 集成测试成功，输出 Ref 且字节一致。 |
| F11 | write/flush/shutdown/入站投递监听取消；close 后阻塞任务退出；Drop 排空写队列有宽限，超时 abort 并回收。新增停读和满入站队列测试。第二轮修正 graceful close/join 后 Drop 重复 poll 已完成 JoinHandle 的 panic；完成的句柄立即移出所有者，超时保留并回收未完成任务，新增部分排空回归测试。真实 TCP/flush 永久 pending 专项仍待扩展。 |
| F12 | 日志入口 256 条、每行最多 8 KiB 加短截断标记，总排队载荷有固定上限；有限软硬预算、入口丢弃计数、100 ms 周期 flush；关闭超时 abort + join。新增洪泛与稀疏日志测试。 |
| F13 | 对账整轮 30 秒、每页 5 秒、shutdown 可中断；正式校验页号与完整 dispatch 集；executor 启动移到最多 4 个并发的 JoinSet，启动数计入 slots。句柄丢弃会取消执行器。静默 master、慢启动与取消竞态的完整故障矩阵仍待扩展。 |
| F14 | TLS/WSS 握手 permit + deadline；认证后注册表独立限制 256 agent，同身份可替换；TaskTracker 管理监听/握手/会话，shutdown 取消并等待；第二监听及执行服务启动失败回滚。新增静默 WSS 握手关闭和端口重新绑定测试。256 连接容量矩阵仍待执行。 |
| F15 | 共享入站循环改用有界 runner 队列 try_send；满/关闭明确使会话失效并触发 Resume 裁决，避免永久阻塞。未实现额外公平积压，尚需一慢多快的专门故障测试。 |
| F16 | legacy-source 不可变；每次导入创建独立工作副本做 WAL checkpoint，退出时清理；WAL 源双次导入、源/证据字节相同的集成测试通过。 |
| F17 | baseline/raw 恢复读取永久 state.legacy，不依赖 COMMAND_WINDOW；baseline 物化既有数据比较，raw 核对源 digest/路径/十进制字符串偏移，修正字符串与数字比较导致的同源重入误拒绝。新增 8,448 条无关命令淘汰全部 legacy 短期回执后，再进行同源重入的测试并通过；尚未执行 8,192 个 raw fragment 的大数据量测试。 |
| F18 | config.update 持有写锁跨越读改写和保存；config/secret 使用独占唯一临时文件、sync_all、rename；Unix 同步父目录。非 Unix 不宣称父目录断电持久保证。新增临时文件策略沿用写失败内存回滚测试。父目录 sync 失败发生在 rename 后，此时返回失败但磁盘可能已变更。 |
| F19 | workflow.versions 使用元数据专用接口，不读取 definition；版本/模板/时间线读取不克隆整个 State。timeline 在读取任何业务值前计算 definition、所有节点 params/output 及 run output 的总预算（8 MiB），超过则拒绝整次请求；物化移到 blocking worker。run 元数据、分页及订阅不再装配节点账本；完整节点装配也有 8 MiB 聚合限制。保留原 API 形状，以明确超限代替静默截断；未新增 timeline 分页 API。 |

R08 已交付 [存储/集群 ADR](docs/adr/0001-journal-storage-and-cluster.md) 及 SQLite 对照/SIGKILL 原型；R16 已交付 [1k/10k/100k 容量与30分钟真实运行证据](docs/backend-capacity-evidence.md)。部署扩展矩阵与真实断电不混入本次验收。

验证：

- 四个核心 crate 单元测试：137 passed，0 failed。日志 `/private/tmp/flow-review-final-unit.log`。
- legacy 导入集成：2 passed，0 failed。日志 `/private/tmp/flow-review-import.log`。
- 大值 IPC/remote 集成：3 passed，0 failed。日志 `/private/tmp/flow-review-transfer.log`。
- workspace 编译通过。
- workspace 全量测试未全部通过：`backend-e2e::journal_v2::configured_cluster_requires_feature_before_opening_the_journal` 失败；迁移后的 `flow-journal-server-e2e` 薄壳调用 Config::load(None)，没有读取测试传入的 --config。测试目录迁移属于同时进行的工作区改动，本次保留。
- 严格 clippy 被既有 `flow-config::AgentCredential` 可派生 Default 告警挡住；未将其描述为通过。

未验证 Windows、PG、真实断电、完整远程故障矩阵及长期容量。


第二轮兼容性与实施边界：

- journal 事件和 wire 协议不变。新增 audit-index.sqlite 是永久回执的派生磁盘索引，删除后启动从原 journal 重建；不依赖有界 command receipt 窗口。相同身份异内容仍冲突，重复提交返回原 LSN。
- 新 checkpoint reducer_version 为 4，读端兼容 v3。旧 binary 不会误用缺少内存审计表的 v4 checkpoint，而会回放原 journal。产品 projection.sqlite 使用 split-v1 标记；新 binary 检测旧整 run 布局并以验证过的 committed state 原子重建。降级旧 binary 前应删除派生 projection.sqlite（停机后进行），使旧 binary 重建其整 run 布局；不要删除 journal。
- projector 中终态 run 行留在 SQLite，折叠内存立即逐出；后续终态审计仍由 journal 分页/订阅提供。完整 Run 读取按相同 SQLite 事务装配节点，普通 metadata 读取跳过节点账本。
- F06 的 master 冷历史迁移、R08 对照 ADR、R16 的 1k/10k/100k 历史与 30 分钟 soak 仍未完成。本轮未删除终态 run 或审计证据来伪造容量达标。

第二轮验证结果见下方最终记录；旧的首轮测试计数不代表本轮全量结果。


第二轮最终验证：

- `cargo build --offline --workspace` 通过。
- `cargo test --offline --workspace`：390 passed、0 failed、11 ignored；日志 `/private/tmp/flow-remaining-workspace-clean.log`。检查该次完整日志无 `panicked`、`JoinHandle polled after completion`。未与其他 cargo 构建并行执行，避免 doctest 使用被替换的依赖产物。
- 全量之后补充的真实 IPC 时间线总预算测试：1 passed；四个 2 MiB 节点输出的流程成功，整条 timeline 明确报 RESPONSE_TOO_LARGE，单个 2 MiB run output 仍可完整读取。日志 `/private/tmp/flow-remaining-timeline.log`。读取产品结果前测试等待可见性屏障。
- 磁盘 audit 索引、旧投影迁移、metadata 不读取 definition、完整窗口淘汰后的导入重入、Delay/Retry/Child 重启恢复、晚到审计订阅、投影原子回滚均覆盖；新增部分排空测试及传输模块合计 15 passed。
- 严格 clippy：`cargo clippy --offline -p flow-engine -p flow-backend -p flow-store -p flow-rpc --lib -- -D warnings` 通过。当前基线已修复首轮提到的 Default 告警，无需豁免。
- `journal-cluster` feature 编译通过；未实际连接 PostgreSQL。
- `git diff --check` 通过。

仍不把上述结果当作冷历史迁移完成或长期容量/断电验收。第一轮遗留的默认测试迁移失败现已不再出现；当前测试使用产品 server 验证 --config 的 feature 边界。

## 第三轮：冷历史、attempt 粒度与实测证据

本轮完成前两轮遗留的 F06/F07 实施及 R08/R16 本轮证据。所有旧“仍未完成”描述仅指其所在轮次。

- journal 是唯一权威；派生 execution-index.sqlite 包含提交前缀版本，冷 run/旧 attempt/旧 publication 按需加载。取消发生在 reducer 已应用、索引尚未完成之间时，保留 applying 事务并重试索引，不重复应用 reducer。
- checkpoint reducer_version=5，仅活跃状态和有界值目录。v3/v4 完整历史 checkpoint 不直接载入，回放原 journal；删除派生库可重建。全量 state() 保留为显式诊断导出，生产运行读取改为单 run/current attempt 路径。
- 历史 workflow/run 关联保留集合，冷 run 逐出后仍不能误删已被运行引用的 workflow。晚到审计按原 dispatch owner 与原前缀加载，不污染当前 attempt。
- 修复无执行器模式下 dirty_runs 留存终态的增长；诊断只返回前256个 ID及总数。
- 真实 soak 发现取消后的会话复用竞态，现要求业务成功、session健康、未发送Cancel才复用；lease 丢弃触发取消，所有 driver/startup 纳入 TaskTracker，关闭回收子进程。补充满命令/事件队列和真实子进程关闭测试。
- 同机 release 原审查基线及修复版均完成10万终态、10万重试、10万审计。原始JSON及边界解释见容量报告，未把基线 checkpoint 失败隐藏为成功。
- 真实混合运行1800.0066秒：8966提交、7472成功、1494预期取消、0失败、0超时；关闭后0子进程、0Tokio任务。

第三轮验证：

- workspace build通过；workspace tests：398 passed、0 failed、11 ignored，完整日志 `/private/tmp/flow-cold-workspace-final.log`，无panic或重复poll句柄。ignored专项未计为通过。
- 严格 clippy（核心crate、RPC、perf，all-targets）通过；journal-cluster编译通过，未实际连接PG。
- 容量报告明确记录磁盘写入/空间代价、100k历史重启40秒、RSS正斜率及未执行的平台扩展矩阵，不宣称无限容量或生产HA。
