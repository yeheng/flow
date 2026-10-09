# 可观察性设计：节点运行状态、日志与数据流全部上前端

> 状态：v2（终稿）。v1 → v2 修订（评审结论）：删除 `run.logs` RPC——订阅回放已从 seq=1
> 推送全部日志，前端 drainBuffer 直接收集即可；resync 统一为 re-attach，消灭补洞与去重；
> 预算计数归属、广播容量公式、终态兜底 fsync 的契约点写死。前置阅读：DESIGN.md §3
> （事件日志是唯一权威）、§6（Driver）、§9（RPC）、§10（JS 沙箱）。

## 1. 要解决的问题

用户在运行详情页 / 编辑器运行面板能看到：节点状态着色、时间线、每个节点的 output 与 error。
看不到的：

1. **节点运行日志**——script 节点里 `console.log` 打的东西去哪了？http_call 实际发了什么请求、
   返回了什么状态码？condition 求值结果是真还是假？重试是第几次、为什么重试？
2. **节点的输入面**——模板 `${}` 展开后 params 长什么样？节点实际拿到的数据是什么？
   现在只有输出（output），没有输入（展开结果）。
3. **过程叙事**——一个 run 从头到尾按时间排序的完整流水：哪里执行了什么、哪里吐了什么数据、
   引擎做了什么决策（重试、接管、等待信号）。

一句话：把 event.jsonl 里已经发生的事，**多记一点、全部送到前端、按人和按节点两个视角展示**。

## 2. 核心决策：日志就是事件，不建第二条管道

本系统是事件溯源架构：`event.jsonl` 是唯一权威，崩溃恢复靠重放，前端实时监控靠
`run.subscribe`（run_tail 状态机：seq=1 回放 + 实时追流，严格连续）。事件同时是
恢复日志和观察数据——这个架构已经给了我们一套完整的_seq 空间、缺口协议、订阅机制_。

因此运行日志（console.log、http 元信息、重试决策…）**作为新的事件变体进入同一条流**：

- 一个 seq 空间：日志行和状态事件统一编号，连续性校验（`validate_sequence`）天然覆盖。
- 一个订阅通道：`run.subscribe` / `run_tail` 零改动，日志自动获得回放 + 追流 + 去重语义。
- 一套前端协议：`seqAction`（apply/skip/resync）原样复用。

**否决的替代方案**：独立 `log.jsonl` + 独立 `run.logtail` 订阅。那是两个 seq 空间、
两套缺口处理、两份订阅状态机，前端还得按时间戳把两条流归并排序才能显示
"重试发生在哪两条日志之间"。凡是需要在展示层合并的，就应该在存储层同流。

## 3. 数据结构

### 3.1 新事件变体

```rust
/// 日志级别词汇表（serde 小写）。
pub enum LogLevel { Debug, Info, Warn, Error }
/// 日志来源：engine=引擎决策，stdout/stderr=脚本输出。
pub enum LogStream { Engine, Stdout, Stderr }

NodeLog {
    node_id: String,
    attempt: u32,          // 归属到具体 attempt，重试的日志不串
    level: LogLevel,
    stream: LogStream,
    message: String,       // 发射端已截断 ≤ 8KB
},
```

- `node_id()` 辅助函数纳入 NodeLog。
- **fold 规则：NodeLog 不改变任何 NodeState，折叠时跳过。** 日志是观察数据，
  不是恢复状态——恢复重放遇到它就是跳过一行，成本 = 一次 serde 反序列化。

### 3.2 NodeStarted 增加 input（节点的输入面快照）

```rust
NodeStarted {
    node_id: String,
    attempt: u32,
    child_run_id: Option<String>,   // 现状
    input: Option<Value>,           // 新增：模板展开后的 params 快照
},
```

`#[serde(default, skip_serializing_if = "Option::is_none")]`，旧日志反序列化兼容
（与 child_run_id 同一模式）。内容 = `expand_params` 的结果——这是**新增信息**：
run input 在 run_started 里、前驱输出在前驱的 node_completed.output 里，前端都能推导，
唯独"模板把 params 插值成了什么"只有引擎知道。**只存这一个事实，其余靠推导。**

为此 `expand_params` 从 `exec::execute` 上移到 driver 的 `start_node`：
展开成功 → 带 input 写 node_started → 派发执行；展开失败 → 照旧先写 node_started
再写 node_failed，写序协议不变。DESIGN §10 的"统一参数展开"语义原样搬移，
只是执行时机提前到快照点。

### 3.3 耐久性分层：事件是设备级持久，日志是进程级持久

现状：`append` 返回即 durable（严格组提交，§3.2）。日志如果照抄，脚本节点刷
console.log 就是每行一次 fsync 语义——把观察数据的代价抬到恢复数据的级别，不值。

规则两条，无定时器、无后台任务：

1. 日志行走 `EventLog::append_log`（新方法）：**只 write_all，不进组提交队列**。
   单文件字节顺序由 LogHandle 的互斥保证，与现状一致。
2. **搭车 fsync**：同文件上任何严格事件 append 的 fsync 会把先于它写入的日志行一并刷盘；
   run 终态事件（completed/failed/cancelled）写之前，driver 先对日志做一次显式
   `sync_all` 兜底。语义一句话：**进程崩溃日志不丢（page cache 在），OS 崩溃最多丢
   最后一次 fsync 之后的尾巴，run 终态点之前的日志保证在盘上。**

边界（诚实写明）：发射即忘意味着「发射」到「driver 排空落盘」之间存在微小窗口，
SIGKILL 可能丢失最后几条尚未落盘的日志行——与 stdout 缓冲同类，不影响崩溃
叙事的主体（已落盘部分跨重启完整可读）。

崩溃窗口推演（为什么不会出 seq 空洞）：日志行 seq 5 写后未刷、OS 崩溃丢行 →
其后的事件行也未经 fsync，一起丢 → 文件尾部连续，`open` 修复逻辑照常工作。
任何丢失都只发生在尾部，不产生中间空洞。

**Postgres 没有更弱的持久层**（事务即落库），所以「廉价层」只能靠**摊薄次数**
而不是降级单次成本。所有 `node_log`（console 输出 + 引擎叙事日志）只走
`RunEventSink::append_log_batch` 一个出口，两条语句一个事务：`runs.last_seq += N`
一次（runs 行在事务里只被 UPDATE 一次）+ `jsonb_array_elements ... WITH ORDINALITY`
整批插入 + 一次 NOTIFY。逐条追加 = N 个受保护事务 + 3N 往返 + N 次刷盘。

批能不能真的批起来，取决于**攒批窗口**：产者是另一个线程，纯 `try_recv` 排空在
刷屏时每批只有 1 行（批接口形同虚设）。日志层没有时延契约，允许 1ms 窗口等一小
会儿；等待按批摊销不是按行。实测脚本刷 300 行：逐条排空 = 300 批，攒批 = 3 批。

**终态兜底的契约点**：run 终态事件（completed/failed/cancelled）的**唯一写点**处，
append 之前先对该 run 的日志做一次显式 `sync_all`（一行 + 注释说明为什么）。
不新建 API、不做后台刷盘任务；专项测试钉死：终态返回后日志必须已可读。
不承认"OS 崩溃丢终态后日志尾巴"的弱化语义——终态 run 的日志是事后排查的
全部依据，一次 sync 的代价买这个保证，值。

### 3.4 预算：一个 run 的日志量上限

日志是用户代码的输出，必须假设它会刷屏。发射端一处管住（logger 内部计数，
EventLog 保持愚蠢）：

- 每 run 默认预算 10000 条（环境变量 `FLOW_RUN_LOG_BUDGET` 可调）。
- 超预算后：warn/error 照记，debug/info 丢弃，并写一条摘要日志
  （"已达日志预算，已丢弃 N 条 info/debug"），终态前再摘要一次。
- 单行截断 8KB，截断时 message 尾部带 `…[truncated N bytes]` 标记，N 是
  **真正丢掉的字节数**（按字符边界回退后的截断点算，不是 `len - 8KB`——
  多字节内容上前者偏小）。
- **总量硬顶 10 万行**（`HARD_LOG_LINE_LIMIT`，所有级别合计，且不小于软
  预算）。软预算只管 debug/info，warn/error 按「错误可观测性不打折」不参与
  它——但那个前提是错误量偶发。一个 `while (true) console.error()` 的脚本
  能把 event.jsonl / PG run_events 写到无界：磁盘比「多几条 error」更值得
  保护。硬顶取 `max(软预算, 常量)`，所以调大 `FLOW_RUN_LOG_BUDGET` 时它
  跟着抬高，不会变成 info 的第二道隐性预算（两道取小者会让旋钮失真）。
  摘要行本身也过预算（`admit(Warn)`）——否则被硬顶按住的 run 每排空一批
  就补一条摘要，摘要自己成了新的无界增长源。

预算同时保护三样东西：event.jsonl 体积（重放成本）、PG 后端日志表、前端订阅带宽。

归属契约：预算计数是 **per-run** 共享状态（`LogBudget`，emitted / total /
dropped 三个原子计数），driver 创建并持有，NodeLogger 克隆 Arc——logger 是
per-node-attempt 的，预算判断必须落在 per-run 一处，EventLog 对预算无感知。

`emitted` 只数 debug/info：warn/error 不碰它。上限的语义是「debug/info 合计
10000 条」，不是「日志总量 10000 条」——共享一个计数器的话，刷满 warn 的节点
会把整条 run 的 info 预算吃光（`warn_burst_does_not_consume_the_info_budget`
钉住）。

`total` 数所有放行的行（含 warn/error），只被硬顶消费（见上）。
（`hard_cap_stops_even_warn_and_error` / `hard_cap_never_below_the_info_budget`
钉住硬顶语义。）

## 4. 发射点清单（谁在哪儿写日志）

| 位置 | 事件 | level/stream | 内容 |
|---|---|---|---|
| exec: script | 用户 console.log/info/warn/error | 透传 / stdout | QuickJS 注入 console 对象，主机函数桥接到 logger；多参数在宿主侧拼串，对象 JSON.stringify |
| exec: script | 执行异常 | error / engine | JS 异常消息 + 行号（现状 NodeFailed 的 error 太粗，日志里给细节） |
| exec: http_call | 请求发出 | info / engine | `{method} {url}`（headers 脱敏后择要） |
| exec: http_call | 响应到达 | info / engine | `{status} {latency_ms}ms body {size}B`；非 2xx 升级 warn |
| exec: http_call | 超时/连接错误 | error / engine | 错误细节（比 NodeFailed 的单行 error 富） |
| exec: condition | 求值结果 | debug / engine | `{expr} → {true|false}` |
| exec: delay | 入睡 | debug / engine | `{ms}ms` |
| driver | 重试调度 | warn / engine | `第 {n} 次重试（attempt {n}）` |
| driver | 接管/重启残留节点 | info / engine | 现有 `tracing::warn!("接管/重启残留节点")` 同步成事件（运行叙事进流，运维日志留给 tracing） |
| driver | human_task 开始等待 | info / engine | 等待信号交付 |
| child_run | 子 run 启动/结束 | info / engine | 带 child_run_id，前端可点 |

**原则：已有事件能表达的不重复发**。signal_received 本身就是事件，前端把它渲染成
日志行（"收到信号 payload=…"）是展示层的推导，不是后端再发一条 NodeLog。
NodeLog 只补"没有事件形态的过程信息"。

发射通道：`NodeExecContext` 增加 `logger: NodeLogger`（内部 = per-run 预算计数
（`Arc<AtomicUsize>`）+ `tokio::sync::mpsc::UnboundedSender<LogLine>`）。exec 在阻塞
线程池里同步发射即忘，不阻塞节点执行；driver 的 select 循环增加一条 arm 排空通道，
逐条 `append_log` 写盘（driver 单点写，单文件顺序天然保证）。

**广播容量公式**（随日志量必须重算的唯二参数之一，另一个是预算）：

```text
capacity = max(现有广播容量, FLOW_RUN_LOG_BUDGET)     // 条数
内存上界  = capacity × 平均行宽                       // 行宽截断上限 8KB 是极端值，
                                                      // 实际以 console/http 行为主，均值远小
```

Lagged 时所有订阅者走 §7.2 的 re-attach 重放——代价有界（预算封顶），不另设机制。
文件与 PG 两个后端按同一条公式核算。

## 5. fold / 恢复重放的兼容

- fold 增加 NodeLog → 跳过 arm（一行 match）。
- 前端 `applyEvent` 的 switch 无 default、未知类型自然 no-op——从"侥幸"升级为"契约"：
  加注释 + 单测钉死"未知事件类型必须被忽略"（向前兼容的显式保证）。
- **不支持降级读**：新二进制写的事件日志（含 NodeLog）旧二进制读会反序列化失败。
  单二进制部署、升级时排空运行中 run，即无此场景；文档明示，不做兼容层。

## 6. RPC 面

- **`run.timeline`**：TimelineNode 增加 `input: unknown | null`（来自最近一次
  node_started.input，node_started 重试会覆盖）。时间线仍是节点状态快照，不掺日志。
- **`run.subscribe` / `run_tail`：零改动，这是本设计的承重墙**。回放协议从 seq=1
  推送全部事件（含日志行）到实时增量——历史日志是订阅回放的免费产物，前端
  drainBuffer 收集即可（§7.2）。**不新增任何日志查询 RPC**：同两份数据两个来源，
  就要去重、要对齐、要补洞，全是自造的税。

终态 run 的历史查看同样走订阅（回放至终态事件后流自然结束），与运行中 run 同一条
路径，无特例。

## 7. 前端设计

### 7.1 信息架构（三个视角回答三个问题）

1. **画布节点（FlowNode/RunCanvas）**：状态色（已有）+ attempt 徽标（attempts>1 显示 ×N，
   重试一目了然）。P3 再考虑"最新一条 error 日志"小红点。
2. **节点检查器（NodeInspector，新组件）**：点画布节点或日志行的节点标签打开
   （运行详情页），回答"这个节点发生了什么"：
   - 头部：名称、类型、状态、attempt、开始/结束/耗时、skipped 原因；
   - **输入**：`input`（展开后 params，JSON 树可折叠查看器）+ 前驱输出快照
     （前端从时间线投影推导各前驱的 output，不新增请求）；
   - **输出**：现有 output 换 JSON 树查看器；error 单独红块；
   - **日志**：该节点的 NodeLog 列表（按 attempt 分组）；
   - human_task 信号交付留在时间线上方的交付框（已有逻辑）；sub_workflow：钻取链接。
3. **日志控制台（LogConsole，新组件）**：运行详情页与编辑器运行面板的下部标签页
   （时间线 | 日志），回答"整个 run 按时间发生了什么"：
   - 行 = `[时刻] [级别色块] [节点名] 消息`，节点名可点击 → 打开该节点检查器；
   - 级别过滤（默认 info+）、文本搜索（前端过滤）；状态叙事（signal_received /
     run 级事件）留在时间线页签——日志控制台只呈现 node_log，一条流不硬造两份；
   - **跟随模式**：贴底自动滚动，向上滚动即暂停并出现"回到底部"；
   - 运行中增量 append，历史日志来自订阅回放（见 7.2）；DOM 行窗口化渲染（状态存全量 ≤ 预算，
     默认渲染最近 1500 行，「加载更早」每次向前扩 1500 行并保持视口位置
     （补偿 scrollHeight 增量），**扩窗有 4 倍天花板**——没有它，「加载更早」
     就是把起始窗口变成累加器，点六次 = 10500 行 DOM，正好把这段代码存在的
     理由（避免万行节点卡死）撤销掉。切 run / 改过滤条件时窗口归零）。
     content-visibility 兼护。

### 7.2 状态管理与对齐协议（monitor.ts / monitor-logic.ts）

`RunProjection` 增加 `logs: LogLine[]`（含 seq、ts、level、stream、node_id、message、attempt）。

- **attach 保持两步**：订阅（回放 + 实时进缓冲）→ timeline 对齐 → 原子换入 → drain。
  历史日志不用另拉：回放早已把它们推进 buffer，`drainBuffer` 改为——状态事件照旧
  `seq > lastSeq` 过滤，**node_log 全量收集**（回放段 seq ≤ lastSeq 的就是历史日志，
  实时段走 applyEvent 追加；run_tail 单订阅内 seq 不重复，客户端零去重逻辑）。
- **增量**：`applyEvent` 增加 `node_log` case → 追加 logs（环形上限 = 预算）。
  seqAction 语义不变。
- **resync = re-attach 同一个 run**：seqAction 的 resync 分支与 onReconnect 都改为
  重走 attach（退订旧的 → 新订阅回放进缓冲 → timeline 对齐 → 原子换入）。投影永远
  从新订阅整体重建而非旧状态上打补丁，日志补洞与跨来源去重这两个概念不需要存在。
  删除 `resyncFor` 的缝合逻辑；面包屑不动的语义 attach 已天然满足。
- **attach 与 re-attach 期间**：日志区显示连接态（跟随既有 phase 显示），无独立
  失败降级分支——回放失败由 run_tail 的补齐重试机制兜底，前端只需等缓冲到达。

### 7.3 类型与 API 层

`types.ts`：RunEvent 联合加 `node_log`（node_id/attempt/level/stream/message）；
TimelineNode 加 `input`；LogLine/LogLevel/LogStream 接口。`api/flow.ts` **零新增**
（不新增日志查询 RPC）。

## 8. 脱敏

- 固定敏感键列表（authorization / cookie / token / password / secret / api_key /
  apikey，**不分大小写子串匹配**）在**节点 output 的所有展示面**生效——值替换
  为 `"***"`：
  1. `run.timeline` 的 `nodes[].output`（展示投影）；
  2. `run.subscribe` 通知里的 `node_completed.output`（实时推送）。

  两处必须同规则：前端 `applyEvent(node_completed)` 会用事件里的值覆盖 timeline
  的展示值，只脱一处等于没脱（实时观看时显示原文、事后查看反而脱敏——同一个
  字段两种命运）。`subscription_redacts_node_output_like_timeline` 钉住。

  事件日志（`run.events` / event.jsonl）里存的**仍是原始值**：那是下游节点的
  数据面，fold 与模板展开要消费它。
- 写入面：`node_started.input`（模板展开后的 params）在写盘前脱敏
  （`build_node_prep`）——快照即展示，没有第二个出口。
- URL 的 query 串：`redact_url` 把命中敏感键的参数值换成 `***`（host/path 保留）。
  `?api_key=…` 这类把凭据放 query 的接口很常见，而 JSON 脱敏管不到字符串。
  http_call 的请求行、以及 `node_failed.error` 里回显的 URL 都走它。
- `node_failed.error` 里回显的上游响应体也过 `redact_value`（http_call /
  email 的失败消息）——错误消息是展示面，和 output 同规则。harness 失败消息
  只带 stderr 尾部（截尾 512 字节），不经 `redact_value`。
- run 级 output（`run_completed.output`）**不脱敏**：它就是 `run.get` 那个数据面
  值，同名字段必须同值。
- 子串匹配的代价：`password_policy`、`tokenizer` 这类合法字段会被误脱敏（安全侧
  可接受，展示侧可能困惑）。参数级 `x-secret` 标记 → P3。
- http 日志行**不含** headers（只记 `→ {method} {url}`），所以不存在"请求头
  脱敏"这个出口；响应头在 output 里，由 output 的两个展示面覆盖。

## 9. 风险与对策

- **订阅 Lagged 概率上升**（日志量 >> 状态事件量）：预算封顶 + 广播容量按 §4 公式
  与预算挂钩；Lagged 的恢复路径就是 re-attach 重放（§7.2），代价有界，无新增机制。
- **重放成本**：fold 多跳过日志行；预算 10000 行 × serde 反序列化 ≈ 毫秒级，可忽略。
  P3 优化项：Envelope 两段解析（先头后体，fold 路径跳过日志体）。
- **event.jsonl 体积**：双重封顶——debug/info 预算 + 总量硬顶（§3.4）。即使
  极端刷屏 × 全 8KB 行，总量也有 10 万行的确定上界；顺带在 §13（明确不做）记录
  "event.jsonl 生命周期/GC"仍是既有欠账，本设计不变更它。
- **PG 后端**：NodeLog 进既有事件表，订阅查询不变；量级受同一预算约束。
- **广播容量公式分叉**（已修）：单机臂曾按 §4 公式 `max(1024, 预算)` 核算，PG 臂
  写死 1024——同一份日志两个后端两种命运（PG 上日志量超 1024 就把订阅者打进
  Lagged）。现在两臂共用 `flow_engine::budget_from_env()` 这一个读数。

## 10. 测试策略

- **flow-engine 单测**：NodeLog 序列化/反序列化往返；fold 跳过 NodeLog；seq 连续性
  含日志行；append_log 搭车 fsync 与终态兜底（读回验证）；预算截断与摘要行、
  **硬顶**（`hard_cap_stops_even_warn_and_error`）；脱敏函数表驱动用例（含
  `redact_url` 的 query 脱敏）；script console 桥接（log/warn/error/对象参数）。
- **flow-rpc 单测**：订阅通知的展示脱敏与 timeline 同规则
  （`subscription_notice_redacts_node_output_like_timeline`），run 级 output 不脱敏。
- **backend-e2e**：subscriptions.rs 扩展——订阅流含 node_log 且 seq 严格连续、回放段
  含历史日志；**订阅通知脱敏而 run.events 不脱敏**
  （`subscription_redacts_node_output_like_timeline`）；crash_recovery.rs 扩展——
  恢复后日志仍在且不缺 seq、**终态返回后日志已持久可读**（终态兜底 fsync 的钉子）；
  sub_workflow 子 run 的日志归各自 run。
- **前端 vitest**：monitor-logic（node_log 追加、drainBuffer 收集历史日志、
  resync=re-attach 投影整体重建、**环形裁剪后索引与 logs 引用级一致**）；
  LogConsole **帧合并节流**（`逐 tick 推送多行只渲染一次`，旧实现 60 行进 60 次渲染）、
  首帧不闪空态、级别过滤。
- **Playwright e2e**：跑含 console.log 的 script 工作流 → 控制台页签可见日志、
  点节点开检查器、输入/输出/日志三块齐全；重试工作流 → attempt 徽标与 warn 日志。

## 11. 新增不变量（并进 DESIGN §12）

1. node_started 先于节点副作用（现状），input 随 node_started 落盘——输入面快照点唯一。
2. NodeLog 不改变投影状态；fold 与前端对未知事件类型必须 no-op。
3. 日志发射即忘、有界（debug/info 预算 + 总量硬顶）；节点执行永不因日志写盘阻塞。
4. 事件 = 设备级持久；日志 = 进程级持久，run 终态前必须兜底 fsync。
5. 丢失只允许发生在尾部；seq 连续性是全 consumers 的硬契约。
6. 不支持降级读事件日志。
7. 终态返回 ⇒ 终态前的日志已在盘上（唯一终态写点的 sync + 专项测试）。
8. 节点 output 的**所有展示面**同规则脱敏；事件日志（数据面）保持原始值。

## 12. 分期

- **P1 后端**：Event::NodeLog + append_log/终态兜底 fsync + per-run 预算 + 
  expand_params 上移 + NodeStarted.input + 全部发射点 + run.timeline 扩展。
  后端单测 + e2e。**RPC 面净变化：零新增方法。**
- **P2 前端**：类型 + monitor（drainBuffer 收日志、node_log 增量、resync=re-attach）
  + LogConsole + NodeInspector + 两个页面接入 + vitest + Playwright。
- **P3 打磨**：日志导出（届时若确有需求，再议是否加导出专用 RPC——不是现在）、
  x-secret 参数级脱敏、画布日志徽标、Envelope 两段解析。

## 13. 明确不做

- 不建独立日志存储/第二订阅通道（§2）。
- **不加日志查询 RPC**（`run.logs`）：订阅回放已是唯一事实来源，第二个来源只会
  造出去重与补洞两种税（评审结论）。
- 不做日志的结构化检索引擎（grep 级前端过滤足够，量被预算封死）。
- 不做运行时动态调级别（改级别=改环境变量重启；工作流引擎不是常驻服务调参场）。
- 不改 event.jsonl 的生命周期/GC 策略（既有欠账，另行立项）。
- **不做 OpenTelemetry 接入**（本文范围之外）。将来接入的设计单独见
  `docs/opentelemetry-design.md`：OTel 是本事件流的**又一个消费者**，不改事件
  模型、不动 `fold`；届时本文的预算与脱敏纪律直接复用（脱敏要扩到新的网络出口，
  见该文 §7）。
