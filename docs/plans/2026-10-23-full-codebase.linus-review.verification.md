# 全代码库 Linus Review：复核与修复记录

复核日期：2026-10-23。原报告：[2026-10-23-full-codebase.linus-review.md](2026-10-23-full-codebase.linus-review.md)。

复核对象是当前工作树，包含已暂存的 agent runtime 修复；原报告以 `0af6e46` 为基线。下面区分代码事实、后果推断与改造建议。原报告保留，避免把复核后的结论倒写成原始结论。

**【核心判断】值得修补实际缺陷；不值得按“可直接删除”清单拆掉现有功能。**

**【品味评分】骨架合理，边界实现有漏洞。** 单写者、事务提交、未知结果人工裁决继续保留。把正确性规则集中在写入边界和共享校验函数，比重写状态机有效。

## 逐条核对

| 原编号 | 是否属实 | 本轮处理与证据 |
| --- | --- | --- |
| F1 | FD 泄漏属实，部分后果推断过度 | socketpair 两端默认 CLOEXEC，仅在目标 fork 子进程中放行交接端；executor 接管立即恢复。harness 也配置继承 FD。真实 IPC→harness 测试断言没有 socket。CLOEXEC 不解决同 UID 内存读取，系统没有因此成为多租户安全沙箱；command harness 能读到 agent permit 的时序推断也不能直接成立。 |
| S1 | 属实 | NodeLogger 发射前统一脱敏，再截断。处理 `console.log('prefix', input)` 产生的嵌入 JSON，以及文字中的多个 URL。单测与真实 script 日志持久化测试覆盖；当前日志落独立 observations，并非原文所说的权威 event.jsonl。 |
| S2 | 部分属实，run output 的缺陷判断不成立 | timeline 的节点输入、输出、error 和 fatal_error 共用脱敏函数。`observability-design.md` §8 与现有 run_execution 测试明确要求 run 级 output 保留原值、与 run.get 一致，因此继续保留这一数据契约。测试确认节点展示脱敏，run output 和原始错误证据仍保留。 |
| S3 | 已修复命令竞争窗口 | 开始、signal、adjudicate、定义导入／保存、legacy 导入及子工作流映射输入使用暂存值。值块、发布、业务事实、当前态与回执由单写者在一次 SQLite COMMIT 内提交。提交时仍在状态锁内校验和去重；被拒、并发重复请求、调用方取消及崩溃均不再留下半份命令输入。保留已有历史事实，详见后续闭环记录。 |
| S4 | 属实 | 方法名、读写类别和注册来自 `flow_dto::rpc::Method` 同一目录；主分发穷尽匹配枚举，新增方法漏分支会编译失败。wire 名称不变。 |
| S5 | 异形行 panic 路径属实，正常 RPC 可达性被夸大 | 控制行恢复时先检查 object，命令合并前也检查，去掉相应 unwrap。真实异形 schedule 持久行恢复测试返回 Invalid。正常写入已有配置验证，并非任意 RPC 都能制造异形行。 |
| S6 | unwrap 属实，统一“中毒后继续写”方案不安全 | 配置锁恢复已有快照；权威租约锁中毒后永久撤销，不能恢复有效租约。测试确认不 panic、也不能续租复活。实际 writer panic 后冻结是正确性边界，继续保留。 |
| 3.1 | 缺少类型属实，“拼错会删掉有效 run”不成立 | 写入边界改用 EntityKind；未知种类反序列化失败；投影未知种类返回错误。SQL 删除按 `(kind,key)` 限定，不会跨命名空间删有效 run。SQL 名称和格式 4 不变，无需迁移。原建议 CHECK 清单遗漏多种现有实体，不采用不完整约束。 |
| 3.2 | 函数长属实，重构收益未被证明 | 保留已有回滚算法。审计只回滚水位／回执，业务事件回滚节点／attempt 摘要，两类快照具有真实的成本差异。仅把这些表搬进 Undo 不会减少状态语义；本轮修复实际的校验漂移，不凭行数重写事务折叠。现有失败原子性测试继续验证。 |
| 3.3 | 部分属实 | 命令入口原本已验证 decision 和 reason 非空，原文“两个都不校验”错误；折叠侧缺 4096 上限属实。共享 `validate_adjudication`／`adjudicate_guard` 用于命令、预检和折叠。 |
| 3.4 | 重复包装与部分死代码属实 | 删除三个 `_bounded` 重复包装、未使用 timeout 常量、输入快照截断函数、空 unused 函数和 agent 侧多余能力常量；保留规范的默认有界 JS API。`redact_snapshot` 已成为真实出口，不能删除。移除无用 dead_code 抑制与未读的注册表 boot 字段。 |
| 3.5 | 属实 | 命名排序查询显式带部分索引谓词并使用现有索引。测试直接检查生产 QueryBuilder 生成的 SQL，断言使用 entity_name_order 且无临时排序。 |
| 3.6 | 所描述的取消／丢失回执导致复用 LSN 不成立 | writer 以磁盘 last_lsn 分配并校验 prepared LSN；调用者取消后后台 pending 提交仍完成，后续读取／命令刷新折叠。已有取消调用者测试覆盖。存储歧义时停写是有意设计，不能仅回读游标就跳过事实应用。 |
| 3.7 | 双入口属实，“必须删除一条”不成立 | raw patch 与 typed 命令已共用 trigger_event／template_event 最终校验；raw 还承担 invocation 字段配置。两者都有真实调用，保留现有公共 API，补共同的形状校验。 |
| 3.8 | “同名两义”不成立 | 两处均表示提交对查询是否可见：普通成功回执已等待 durable，状态查询再核对 SQLite 水位。visible 是公共 wire 字段，删除会破坏调用方。 |
| 3.9 | 散落字面量属实，两个预算不同并非错误 | 产品 JSON 物化上限集中为 `flow_journal::VALUE_BUDGET`（8 MiB），Store 列表保持独立 4 MiB 预算。原有事务行、实体、值块上限含义各异，不能全部合并。 |
| 3.10 | 属实 | Store.list 改成一个 CTE／窗口累计预算查询，在 SQLite 内选出连续前缀后才返回正文；避免原提案直接取 256 个大正文造成内存放大。测试覆盖截断与下一页衔接。 |
| 3.11 | regress 入参不一致属实，十进制契约解释过度 | run.regress.version 改用已有 optional_version，兼容 number／string。持久游标和身份的 u64 字符串契约不等于所有配置版本数字都必须改成字符串。 |
| 3.12 | 属实 | hooks 与 invoke 共用 invocation_error，Limit 统一为 HTTP 429。 |
| 3.13 | 属实 | 设置 WAL 后读取实际模式，非 wal 返回 Unavailable，拒绝静默降级。 |
| 3.14 竞态 | 属实 | 分页检查请求代际、run、客户端和 audit 模式；切 run 作废旧请求。前端回归测试覆盖 audit 选择改变后丢弃迟到响应。 |
| 3.14 订阅 | 属实 | 等连接／等订阅 ACK 共用 30 秒超时，超时清理连接和 pending；保留重连订阅。测试覆盖两种静默超时、首次只订一次和断线重订。 |
| 3.14 表单 | 属实 | parseInputFields 保留 type，assembleInput 和弹窗共享 schema-aware parser；inline 表单显示类型错误。测试确认字符串 `5` 保持字符串，integer／boolean／object 正确解析。 |
| 3.14 编辑器 | O(E×N) 属实，“每帧都序列化”夸大 | 边序列化改查节点端口 Map；dirty 用缓存的逐行序列化比较已保存快照。Vue computed 原本就缓存，不是每次模板 render 都计算。测试覆盖保存中编辑、切工作流与恢复原内容后清除 dirty。 |

## 删除建议的复核

| 建议 | 结论 |
| --- | --- |
| 删除集群登记 | 不采纳。登记／租约是现有可选功能，并不等于承诺 HA。ClusterView 在禁用时已有明确说明，并非“永远空白”；可选 feature 编译继续验证。 |
| 删除 legacy 兼容带 | 不采纳。legacy 导入是现有恢复／迁移工具功能；没有前端消费者不代表导入工具、历史数据和离线调用不存在。删除已持久化事件处理并非零行为变化。 |
| 删除 JSONL codec | 采纳。encode／decode／read_line 仅旧格式自身测试调用，删除其重复 envelope 和测试；保留实际 SQLite 使用的事务验证、摘要、指纹与 bounded_json。 |
| 删除 scan_fast／SyncLatency | 部分采纳。scan_fast 无调用，删除；SyncLatency 实际用于 writer stats 和同步延迟统计，保留。 |
| 删除 Windows Job Object | 不采纳。它负责真实的进程树清理。没有原生运行验收不等于没有功能；“嵌套 Job 必然失败”也不成立：[Microsoft 文档](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects) 明确 Windows 8／Server 2012 起支持嵌套 Job。具体限制仍须实机验证，当前机器未做 Windows 运行验收。 |
| 删除 legacy-baseline | 不采纳。显式历史性能基线比较开关有独立用途，不在正常执行路径上；默认关闭不能作为零行为价值的证据。 |
| clap 脱离 storage | 采纳。storage 只含存储依赖；新增 cli feature，仅 CLI bins 要求它。默认仍构建原有工具，下游 library 显式选择 storage。单独 engine 依赖图不含 clap。 |
| 前端死 helper | 删除无人使用的 webhookUrl。文本截断长度和展示语义不同的函数不强行合并。 |

## 修复边界

- **命令值已原子发布。** 大 JSON 先写入匿名临时文件，未提交时不进入权威日志。值块仍各占一个原有格式的小事务，但这些事务与最终命令在同一个 SQLite 事务中一起提交或回滚；不会跨多次 COMMIT 暴露中间状态。输入暂存预算为 16 MiB（每个值最多 8 MiB），writer 逐块读取并保留额外的单条记录工作预算；不把整个大输入压进写队列。配置的写预算不足时返回 Limit，未发布的暂存文件随句柄释放，进程被强杀时由 OS 回收。已经持久化的历史事实继续完整保留，不按“当前没有业务引用”删除历史块；执行器捕获外部响应的流式证据路径也继续保留原有语义。
- **脱敏是结构与字段规则。** 日志与节点展示中的 JSON 敏感键、URL query／fragment 受保护；任意没有字段名的裸密钥、用户主动改名、既有历史日志不在自动识别／回写范围内。run 级 output 按既有契约保留原值；原始事实和受权原值下载保留执行证据。
- **FD 清理不是 OS 沙箱。** CLOEXEC 在 fork 子进程内调整，避免并发 spawn 继承交接通道；多余 FD 设为 exec 时关闭，避免提前 close 误伤 Rust 内部错误管道。现有单用户进程模型与 agent 预算边界保留；command 模式及 Windows 的硬资源限额、逃离 session 的进程隔离并未因本轮修补获得额外承诺。
- **没有数据库格式升级和 RPC wire 改名。** 当前 SQL 实体名称、格式 4、raw／typed 入口、visible 和离线维护工具继续兼容。

## 中文任务清单与验收

| 任务目标／原因 | 影响范围与方案 | 验收方式 | 约束 |
| --- | --- | --- | --- |
| 关闭 IPC 继承口 | engine unix／spawn／process，默认 CLOEXEC、定点交接 | 通道标志单测＋真实 harness 无 socket | 不改 IPC 帧与 permit wire |
| 补日志／timeline 脱敏 | NodeLogger 与 journal_views 共用函数 | 真实脚本日志与错误／输出检查 | 不破坏原始执行证据 |
| 统一权威类型和守卫 | Method、EntityKind、裁决校验、异形行、租约中毒 | 穷尽编译、恢复 Invalid、永久撤销测试 | 不改变提交与故障冻结边界 |
| 减少无效写与多余查询 | commands 共享预检；Store 单次预算查询；索引命中 | value_chunks 数量、连续分页、EXPLAIN | 保留预算和现有分页语义 |
| 修复前端交互 | 订阅、分页、输入类型、dirty | Vitest 全量、构建、lint | 保留重连、保存中编辑与撤销语义 |
| 删除已确认死代码 | codec／JS wrappers／scan_fast／无用 helper；CLI feature 分离 | workspace build／test／clippy 和最小 feature 检查 | 不删除现有产品／平台能力 |

## 验证命令

先 `cargo build --locked --offline --workspace` 构建 CLI／executor，供端到端套件定位真实二进制。

- `cargo test --locked --offline --workspace --no-fail-fast`
- `cargo clippy --locked --offline --workspace --all-targets -- -D warnings`
- `cargo check --locked --offline -p flow-rpc --features journal-cluster`
- `cargo check --locked --offline -p flow-engine --no-default-features`
- `cargo fmt --all -- --check`
- `NODE_OPTIONS=--no-experimental-webstorage npm --prefix web test`
- `npm --prefix web run build`
- `npm --prefix web run lint`

Node 26 的原生 Web Storage 与 jsdom 全局存储冲突，测试用上述 flag 让 jsdom 提供 localStorage。前端 50 个文件、339 项测试通过；构建通过（保留既有的大 bundle 提示），lint 通过。Rust 全量 496 项通过、0 失败，11 项按原套件设置忽略；clippy（all-targets、warnings-as-errors）、可选 journal-cluster 编译、无默认 feature 编译、fmt 与 diff 空白检查均通过。未执行被忽略的大容量专项或 Windows 原生运行验收。


## 剩余竞争窗口闭环：原子命令值

- `PreparedValue` 持有 inline 值或匿名暂存文件；暂存阶段不分配 LSN、不写事实和发布行。拒绝、取消或重复命令返回时释放文件及暂存配额，无需后台扫描临时目录。
- writer 为一个命令依次写入值块事务和最终发布／业务／回执事务，只做一次 SQLite COMMIT。数据集身份、每条事务的尺寸上限、LSN 顺序、内容摘要和格式 4 保持不变；每块仍按原读路径重建并验证。
- reducer 的批量 preview／apply 共用原有受影响实体回滚算法，只允许最终业务事务前出现数据集级 ValueChunk。校验、预览或实际折叠失败时，发布缓存、业务状态与游标一起恢复，避免复制整份 State。
- 开始 run、投递 signal、人工裁决、导入／保存定义、legacy 导入与子工作流创建共享该路径。流式捕获的外部调用证据属于已接受事实，不作为命令暂存垃圾删除。
- 验证覆盖：7 MiB 输入在原有 4 MiB writer 队列预算内单次 COMMIT；最终回执冲突／COMMIT 故障回滚全部块；COMMIT 前精确暂停后 SIGKILL 重启；暂存预算耗尽后的取消释放；预检后取消导致的拒绝；8 个大输入并发重试只生成一个 run／一份值；调用方取消后完整提交与回执恢复；大定义导入和相同定义复用。
- 历史保留按现有完整事实策略执行。未进行改写旧日志的历史 GC，也未增加新的日志格式或隐式过期规则。

本轮验证：workspace 全量 496 项通过、0 失败，11 项按原配置忽略；clippy（all-targets、-D warnings）、journal-cluster 可选功能编译、无默认功能编译、fmt 和 diff 空白检查通过。覆盖真实 COMMIT 前 SIGKILL、重启恢复、最终事务失败回滚、并发重试、调用方取消、暂存预算释放、定义复用与子工作流／legacy 导入兼容性。
