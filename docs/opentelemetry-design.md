# OpenTelemetry 接入设计：把事件流投影成 OTel 数据模型

> 状态：**设计已定，尚未实现**。本文是 `docs/DESIGN.md` 的延伸专题——DESIGN 描述
> 当前代码的实际语义，本文描述将来接入 OpenTelemetry 时的取舍与契约。两者不冲突：
> 本文**不要求改动事件模型**（§2 论证为什么不需要），因此实现它不会动摇 DESIGN §12
> 的任何不变量。
>
> 前置阅读：`docs/observability-design.md`（可观察性现状：日志即事件、耐久性分层、
> 预算）。本文假定读者已接受那套设计，特别是 §2 的「日志就是事件，不建第二条管道」。
>
> 术语：本文的 `trace_id` / `span_id` / `LogRecord` / semconv 等外部概念一律以
> OpenTelemetry 规范为准。**规范会演进**（semconv 的属性名改过名、severity 分档有过
> 调整），落地时必须对当时的规范版本核对属性名，不要照抄本文的字面值。

## 1. 要解决的问题与范围

要解决的问题只有一个：**把 flow 的一次执行变成可与外部系统关联的遥测**。具体是
三件事——

- 一次 run 的日志、节点耗时、重试分布能与外部 HTTP 调用的链路、上下游服务、
  运维告警放在同一张图上看；
- 节点级失败的定位不再只靠翻 `event.jsonl`；
- 未来接 metrics（节点延迟直方图、attempts 分布、恢复耗时）不必再造管道。

**范围之外**（详见 §12）：不把 flow 做成 tracing 的**生产端**（即不生成
W3C `traceparent` 之外的自有协议）、不做 OTel Collector 本身的部署与调优、
不改事件模型、不动 `fold`。

## 2. 核心决策：OTel 是事件流的第 N+1 个消费者

`RunState::fold` 已经证明了这个架构的原生能力：**一条严格有序、无洞、可续传的
事件流，N 个互不知情的消费者**。今天有三个——恢复（`resume_run`）、只读时间线
（`run.timeline`）、订阅（`run_tail`）。

**决策：第四个消费者叫 OpenTelemetry。** 它读同一条流，把事件**投影**成 OTel 数据
模型后经 OTLP 发走。事件模型、seq 空间、耐久性分层、折叠语义**一概不动**。

### 2.1 否决的替代方案：给事件加 trace 字段

最自然的错误做法：既然要导出 span，就在 `node_started` 里加 `trace_id` /
`parent_span_id` 字段，让 exporter 直接读。

**否决理由**：`run_id` 本身就是 128 bit 的 UUIDv7（`flow-backend/src/sqlite.rs`
的 `Uuid::now_v7()`），**恰好就是 OTel `trace_id` 的宽度与语义**。再加一个
`trace_id` 字段就是新的影子副本——而影子副本正是这个仓库这半年在系统性消灭的东西
（DESIGN §12.5 的 `output`/`error`/`attempts` 三份影子，§4 的并行 `outputs` map）。
更糟的是它会诱使 `fold` 参与 trace 上下文，而 trace 上下文是**导出期概念**，
不属于恢复状态。

### 2.2 否决的替代方案：替换掉 `node_log`

既然日志要进 Collector，还留在 `event.jsonl` 里做什么？

**否决理由**：`run.events`（`from_seq` 增量拉取）是**排障的权威出口**——时间线页
的日志面板、CLI 的 `flow-cli run events`、事后复盘都读它。删掉等于让「日志只在
Collector 里有」，而 Collector 的保留期由运维决定，与 run 的生命周期脱钩。
**事件流继续持有日志**，OTel 是增量出口。

这条也带来一个附带好处：将来若真要停止向 `event.jsonl` 写日志（直发 Collector），
`fold` 本就显式跳过 `node_log`（DESIGN §4），**状态机一侧零改动**——当初那句
显式声明现在成了退路。

### 2.3 否决的替代方案：在 exec 层直接调 OTel SDK

让 `exec.rs` 在发 HTTP 请求时顺手开一个 span、写 `traceparent` 头。

**否决理由**：exec 层不知道 run 的身份、不知道重试与恢复、也拿不到判定该 span
该结束的全部信息。跨进程传播确实最终要落到 exec 层（§8.2），但那是**注入一个
已有的 span 上下文**，不是让 exec 层自己拥有 span 生命周期。生命周期归事件流
（§5.2），exec 只消费。

## 3. 前置：现有数据能映射出什么

盘点当前事件流里已经有的、**不需要新事件**就能映射的东西：

| 现有字段 | 来源 | 映射目标 |
|---|---|---|
| `run_id` | `Uuid::now_v7()`，`sqlite.rs` | `trace_id`（16 字节，天然够宽） |
| `(node_id, attempt)` | 事件自带 | 派生 `span_id` + `span.name` |
| `node_started` / `node_completed` / `node_failed` | 事件 | span 的开始 / 结束 / 结果 |
| `node_completed.duration_ms` | 事件 | span duration（**白送**） |
| `NodeState::Failed{error, retryable}` | fold 结果 | span `status` + `error.type` |
| `NodeState::Skipped{reason}` | fold 结果 | span 事件（`branch_not_taken` 等） |
| `node_log.{message,level,stream}` | 事件 | `LogRecord.{body,severity_number,severity_text}` + attributes |
| `child_run_id`（`{run}:{node}:{attempt}`） | `driver.rs` 确定性派生 | 子 run 的 trace 亲缘线索 |
| 定义的 `edges` | `Definition` | span 拓扑的 parent / link |
| 预算摘要行的「已丢弃 N 条」 | `nodelog.rs` | counter（§5.5） |

**盘点结论：数据基本齐了，缺的是「身份 + 语义 + 拓扑」三样加工，而不是采集。**

## 4. 身份模型

### 4.1 trace 根 = run

`run_id` 是 `Uuid::now_v7().to_string()`，去掉连字符得到 32 位小写 hex——
**正是 OTel `trace_id` 的格式**。规范要求 trace_id 不得全零；UUIDv7 有固定的
version/variant 位，天然非零。

> **实现要点：`run_id` 是调用方传入的 `String`（`StartRun.run_id`），引擎不保证它
> 是 UUID。** 生产路径是 `Uuid::now_v7().to_string()`（`flow-backend/src/sqlite.rs`），
> 但测试与压测 harness 会用 `run-<uuid7>`、`run-0` 这类形态。导出器不能假设 hex
> 格式：解析失败时应**回退到 `SHA-256(run_id)[0..16]`** 派生 trace_id 并记一条
> 警告，而不是丢弃整个 run 的遥测。定不下来的话，一个 run 的遥测会因为 id 长得
> 不标准而全丢——这类失败在接入初期最难发现。

### 4.2 span_id = H(trace_id ‖ node_id ‖ attempt)

OTel `span_id` 是 8 字节 / 16 hex。引擎不持久化 span_id，**导出期确定性派生**：

```
span_id = SHA-256(trace_id_bytes ‖ 0x00 ‖ node_id_utf8 ‖ 0x00 ‖ attempt_be32)[0..8]
（若全零则置为固定非零常量——规范硬性要求）
```

**为什么必须确定性派生**（这是本节最要紧的一条）：崩溃恢复会**重放**残留节点，
`RecoveryPlan::classify` 派发的是 `attempt + 1` 之后的 attempt；接管路径
（PG executor）会重建同一个 attempt。派生确定性带来两个性质——

- **幂等**：同一个 (node_id, attempt) 无论被导出几次都是同一个 span_id，
  Collector 侧天然去重，不会因为重放而出现重复 span；
- **与既有设计同构**：`child_run_id` 已经是「确定性派生 + 随 `node_started`
  落盘 + 重放沿用」这套（DESIGN §12.11），span_id 沿用同一个思路，不是新发明。

同一 run 内 `attempt` 单调递增、`node_id` 唯一，因此 (node_id, attempt) 在 trace
内唯一——**span_id 无需额外去重**。

### 4.3 不引入 parent_span_id 字段

父子关系在**导出期**由定义的 DAG 计算（§5.3），与 §2.1 同理：拓扑不是恢复状态，
不进事件流。

## 5. 信号映射

### 5.1 `node_log` → LogRecord

| OTel 字段 | 来源 | 备注 |
|---|---|---|
| `time_unix_nano` | 事件 `ts` | `DateTime<Utc>` → epoch nanos |
| `severity_number` | `level` | 4 档映射到规范的分档区间（规范用 1–24 的整数分档，不是字符串枚举） |
| `severity_text` | `level` | 原样 `debug`/`info`/`warn`/`error` |
| `body` | `message` | 已按 8KB 截断（`nodelog.rs`） |
| `trace_id` / `span_id` | §4 派生 | **这是 OTel 的核心价值**：日志与 span 关联 |
| attributes | `stream`、`node_id`、`attempt` | `stream`（engine/stdout/stderr）在 OTel 没有对应字段，作 attribute |

`stream` 的语义（脚本 console vs 引擎叙事）在 OTel 侧只能退化成 attribute。
这是可接受的损失——恢复时重建的那段叙事本来也带着「接管/重启」的信息。

### 5.2 节点事件 → Span

**span 生命周期完全从现有事件投影，不新增事件**：一个 span = 一对
`node_started` 与其后的终态事件（`node_completed` / `node_failed`）。

这个映射之所以成立，靠的是 DESIGN §3.2 的**写序协议**——`node_started` 必在副作用
之前落盘，因此「span 有 start」这条 OTel 硬性要求由既有不变量免费保证。多数日志
系统做不到这点（先打日志后做事，崩了对不上），这里是白送的。

| span 字段 | 来源 |
|---|---|
| `name` | `node_id`（或 `node_id` + attempt 后缀，视重试可读性定） |
| `kind` | 按节点类型定（§6） |
| `start_time` / `end_time` | 两条事件的 `ts` |
| `duration` | `node_completed.duration_ms`（毫秒精度，注意 OTel 原生是纳秒） |
| `status` | `Completed` → `Ok`；`Failed{retryable:false}` → `Error`；`Failed{retryable:true}` → `Error` + 标记「将重试」 |
| attributes | `node.type`、`node.attempt`、语义属性（§6） |

### 5.3 拓扑：parent 是「活动路径」，其余边是 Link

定义是 DAG，不是树，所以「parent_span_id」表达不了全部关系。映射规则：

- 每个节点（`start` 除外）取其**入边中实际满足的那条**（`edge_state` 判为
  `Satisfied` 的那条）作为 `parent_span_id`；无入边（`start`）或无可满足入边
  （`Skipped`）则无 parent，挂在 trace 根下；
- **condition 的未选分支**判为 `Unsatisfied("branch_not_taken")`——这条边天然是
  「未走的 link」，投影为 `Span.links`；
- 因此一个 condition 节点的两个下游可能同时是「parent」和「link」。

**这是 workflow 场景用 OTel Link 而非纯 parent 的标准形态**（多分支、并行
join 天然不成树）。选 parent+link 混合而不是全 link：串行主干（最常见的形态）
保持可读的主链路。

`edge_state` 是 `Driver` 的私有方法（`driver.rs`），**导出器拿不到**。这是实现
时要解决的一处：拓扑投影需要的「哪条边被满足」信息，今天只存在于调度期内存里，
事件流里没有。两条路——

1. 导出器独立重算 `edge_state`（纯函数，输入 = folded `RunState` + `Definition`，
   可从 `run.timeline` 的同一份数据复现）；
2. 或在 `node_completed` 上记录被选中的端口。

**倾向前者**：不新增事件字段，与 §2.3 同一条原则。

### 5.4 被弃 attempt 与悬空 span

平台故障路径**故意**不写 `node_failed`——节点留 `Running` 等恢复
（`driver.rs` 的 `handle_result`：平台故障不写 `NodeFailed`，冒泡到 `run()` 投影
`awaiting_resume`）。于是存在两类没有终态的 `node_started`：

1. **被弃 attempt**：崩溃/接管后未重放，节点以新 attempt 重试；
2. **悬空 span**：run 已终结，但该 attempt 的 `node_started` 没有终态事件。

**现状的歧义**：单看事件流，这两者与「日志刚好丢在发射窗口里」不可区分
（`node_log` 本来就有发射即忘的窗口，observability-design §3.3）。

**投影规则**（数据已够，不需要新事件）：

- 折叠后 `state` 是终态、而某 attempt 只有 `node_started` → 补一个
  `status=Error` 的 span，attribute 标 `flow.abandoned=true`；
- 该节点在 `state` 里的**最终** attempt 才是「活」的 span，前面的被弃 attempt
  各自成 span 并互为 link（重试链）。

这条规则让 §5.4 的歧义在导出侧被消解，且不需要引擎知道 OTel 存在。

### 5.5 Metrics：另开一条，不混进日志管道

OTel 最常用的信号是 metrics，而本系统今天一个 counter/histogram 都没有。
但**数据都在同一条流里**，导出侧 fold 一次即可派生：

| 指标 | 类型 | 来源 |
|---|---|---|
| 节点执行耗时 | histogram | `node_completed.duration_ms`，按 `node.type` 分组 |
| 节点 attempts 分布 | histogram | 每个 `node_started` 计一次 |
| 日志丢弃条数 | counter | 预算摘要行「已丢弃 N 条」 |
| 恢复耗时 | histogram | `run_started` 的 `ts` 差（停驻到 `driver` 接手） |
| run 终态分布 | counter | `run_completed`/`run_failed`/`run_cancelled` |

**明确不做的**：不为了指标而给事件加计数器字段。指标是**导出期聚合**，
与 §2.1 同理。

## 6. 语义约定（semconv）

现状的日志是**给人看的字符串**：

```
→ POST http://api.example.com/v1/chat          （info）
← HTTP 429（83ms，body 118B）                  （warn，非 2xx 升级）
```

语义信息（方法、URL、状态码）只存在于句子里，**不可机读**。这是接入 OTel
唯一需要动 `exec` 层的地方。

| 节点类型 | `span.kind` | 关键属性 |
|---|---|---|
| `start` / `end` | `INTERNAL` | — |
| `script` / `condition` / `delay` | `INTERNAL` | — |
| `http_call` | `CLIENT` | 请求方法、完整 URL、响应状态码、耗时 |
| `harness` | `INTERNAL` | 命令、退出码、耗时（stdout/stderr 体不出 span，见 §7） |
| `email` | `CLIENT` | 收件方域名（**不含地址**，见 §7） |
| `sub_workflow` | `INTERNAL` | 子 run 的 trace 亲缘（§8.1） |
| `human_task` | `INTERNAL` | `flow.awaiting=human_task` |

原 `llm` 节点已删除（由 `harness` 取代）；GenAI 语义约定（模型名、usage 属性）
随之不再适用——harness 是本地子进程，不是 LLM API 客户端。

**动作**：把 http_call 的请求/响应行从字符串改成结构化属性。字符串行可以保留
（人读友好，且已落盘），但**机读属性必须另出**。这是 exec 层唯一的改动，
且不改变事件模型——结构化属性在导出期从 `node_completed.output`
（`{status, headers, body}`）投影即可，**一行 exec 代码都不用改**。

> 落地时必须核对当时的 semconv 属性名。HTTP 相关属性在 2024 年经历过一轮改名
> （`http.method` → `http.request.method` 一类），照抄任何版本的字面值都可能过时。
> 建议把用到的属性名收进一个常量表并加快照测试，升级 semconv 时只改那一处。

## 7. 出口与脱敏：OTLP 是新的数据出口

**这是本文最重要的一节，也是最容易被漏掉的一节。**

`event.jsonl` 是**本机磁盘**，信任边界是这台机器。OTLP 是**网络出口**，终点是
外部 Collector——一旦发出去，数据就离开了本机的控制范围。

而现状的脱敏纪律是**按出口**分的，不是按数据分的：

| 出口 | 脱敏 | 位置 |
|---|---|---|
| `node_started.input` | 写入时脱敏 + 限幅 | `driver.rs`：`cap_input_snapshot(&redact_value(&node.params))` |
| `run.timeline` 的 `nodes[].output` | 展示时脱敏 | `flow-rpc`：`redact_value(&v)` |
| **`node_completed.output`（磁盘上的原文）** | **不脱敏** | 事件里就是节点返回的原值 |

也就是说：**`http_call` 节点的输出 `{status, headers, body}` 原样躺在
`event.jsonl` 里**，其中 `headers` 与 `body` 完全可能回显凭据
（DESIGN §9 已经承认「节点输出可能是上游 HTTP 响应、会回显凭据」，所以时间线
展示层才要脱敏）。本机磁盘上放这个是可接受的；**把它发给外部 Collector 不可接受**。

**因此：OTel 导出器必须把 `run.timeline` 的脱敏纪律原样复制一遍**——

- `node_completed.output` → span attributes 前经 `redact_value`；
- `node_log.message` 不经 `redact_value`（它是自由文本，键名脱敏对它无效），
  但**不得把 output 原文塞进 log body**——这是唯一可靠的规则：**凭据只经结构化
  属性出口，不经自由文本出口**；
- `SENSITIVE_KEYS` 表（`nodelog.rs`，7 个键的子串匹配）成为**跨出口共享的
  单一清单**——新增敏感键时三个出口一起跟随。

**边界（诚实写明）**：子串匹配会误伤（`tokenizer`、`secretary` 会被脱敏成 `***`），
也会漏（`x-auth`、`session` 不在表里）。这是**既有**的脱敏能力上限，不是 OTel 引入
的。OTel 让它从「本机展示瑕疵」变成「跨网络数据泄漏风险」，所以**误伤方向可以接受、
漏掉方向必须收敛**——落地时应当扩充这张表，并把本节写成出口清单的守卫测试。

## 8. 资源属性与跨进程传播

### 8.1 Resource

OTel 的每个信号都挂在一个 `Resource` 上（描述产生遥测的实体）。本系统可提供：

| 属性 | 来源 |
|---|---|
| `service.name` | `flow-journal-server`（bin 名） |
| `service.version` | 需新增（当前无版本概念） |
| `service.instance.id` | 需新增（单机后端没有 instance 概念，进程级 UUID 即可） |
| `deployment.environment` | 需新增（当前无环境概念） |
| `flow.backend` | 固定 `journal`（唯一后端；v1 的 sqlite/postgres 已删除） |

单机后端没有 `instance_id` 概念，需在 `Engine` 构造时生成一个进程级 UUID。

### 8.2 传播

**出站（http_call）**：注入 `W3C traceparent` 头，值由 §4 派生。这是 exec 层
**唯一**需要的真实改动（`exec.rs` 的请求构造处），且它消费的是已存在的 span
上下文，不自己开 span（§2.3）。

**父子 run（sub_workflow）**：子 run 是**独立 trace**还是同 trace 的子 span？
两个都站得住——

- 同 trace：子 run 的节点成为父 run 节点的子 span，链路完整；
- 独立 trace + link：子 run 生命周期与父 run 解耦（父可能取消、接管、重试子 run），
  link 表达「从这里去那条 trace」。

**决策：独立 trace + link。** 理由是子 run 有自己的恢复、接管、取消级联与独立
终态（DESIGN §6.8），把它塞进同一条 trace 会让「父 run 的 span 树」被子 run 的
重试历史污染。`child_run_id` 的确定性派生（`{父run}:{节点}:{attempt}`）正好是
link 的目标 id。

## 9. 耐久性：与 OTel 方向相反

OTel 天生 **best-effort、可丢、tail sampling**——Collector 自带缓冲与重试，
丢一批遥测是正常现象。因此：

**当前设计已经与 OTel 语义天然契合**——`node_log` 走「进程级持久」（只
`write_all`，搭严格事件 fsync 便车）而不是设备级持久，这正是 OTel 想要的形状。
日志不需要为可观察性付 fsync 的价，这条决策**不用改**。

**但有一处理由要改**：终态事件写入前的那次强制 `sync_all`（`EventLog::append`
的 `is_run_terminal()` 分支），现有注释的理由是「终态返回 ⇒ 终态前的**日志**已在
盘上」。接入 OTel 后日志本就不需要这个保证，但**状态事件仍然需要**——所以 sync
必须留着，**只是它的理由从「保护日志」变成「保护终态之前的状态事件」**。
注释不改会误导后来人以为观察数据需要设备级持久，进而在别处照抄这个误解。

## 10. 容量与基数

OTel 对单个 span 的属性数量与属性值长度有默认上限（属性数上限是 128），且
**强烈建议控制基数**。现状两个数据点不守规矩：

1. **`node_completed.output` 完全无上限。** 实测本仓库现有 run 的
   `event.jsonl` 里 `node_completed` 占 **96.7% 的字节**（总 4.25 MB / 55 行，
   平均每行约 75 KB），而 `node_log` 一条都没有。导出时必须裁剪，并按 OTel 惯例
   **置 flag 标明被截断**（而不是静默截断）。
2. **`node_started.input` 已有 8KB 限幅**（`cap_input_snapshot`），体面得多。

**这条与可观察性现状是同一个洞**：日志预算（10000 条）保护的是 `event_log`，
而真正占 `event.jsonl` 体积的 `node_completed.output` 不受任何约束
（详见 DESIGN §3.2 与 observability-design §3.4 的偏差说明）。接入 OTel 会把
这个洞从「本机磁盘」放大到「每次执行都往外部发」。**给 node 输出加上限应当先于
或至少同步于 OTel 落地**，否则第一版 exporter 就会把 75KB 的值当 span 属性发出去。

## 11. 分期

1. **第一期（只读投影，不动任何生产路径）**：实现 `OtelProjector`——读事件流，
   产出 OTel 数据模型结构体，用单测对着**固定事件样本**断言映射结果。不接线、
   不发网。这一期验证全部映射规则（尤其 §5.3 拓扑与 §5.4 悬空 span），风险最低。
2. **第二期（接线 + 脱敏）**：接 `RunEventSink`/订阅面，落地 §7 的脱敏纪律与
   出口清单守卫测试。此时 `run.timeline` 与 OTel 出口共用同一个 `redact_value`
   与 `SENSITIVE_KEYS`。
3. **第三期（传播 + 资源）**：`traceparent` 注入、Resource 属性、父子 run 的 link。
4. **第四期（metrics）**：从同一事件流 fold 聚合，独立的 pipeline。
5. **先决项（不属分期）**：给 `node_completed.output` 加上限（§10）。

## 12. 明确不做

- **不改事件模型**。不加 `trace_id`/`span_id`/`parent_span_id` 字段，不加计数器
  字段（§2.1、§5.5）。这是本文最重要的一条约束。
- **不生成自有遥测协议**，不做 Collector 部署与调优，不选 backend（OTLP/HTTP
  与 OTLP/gRPC 由部署环境决定，属配置）。
- **不追求 100% 覆盖**。可丢是 OTel 的语义，不是缺陷；不为了「不丢日志」去动
  耐久性分层。
- **不改 `fold`**。它是恢复状态与只读视图的唯一来源，OTel 投影是纯函数、旁路。
- **暂不做跨 run 的 trace 关联**（如 workflow 版本的发布 trace、cron 触发 trace）。
  等单 run 的投影稳定后再议。

## 13. 并入 DESIGN §12 的不变量（实现时新增）

19. **OTel 是事件流的消费者，不是事件模型的参与者**：投影是纯函数、旁路，
    事件流与 `fold` 不知道 OTel 存在（§2）。这与 §12.3「`fold` 是唯一状态转移
    函数」同源——投影不进 fold。
20. **trace 身份由 `run_id` 派生，span_id 由 (node_id, attempt) 确定性派生**，
    两者都不落盘（§4）。重放与接管重建得到同一个 span_id，天然幂等。
21. **凭据只经结构化属性出口**：span/log 的自由文本不得承载 output 原文；
    `node_completed.output` 导出前必经 `redact_value`，与 `run.timeline` 同一份
    敏感键清单（§7）。
22. **节点输出有体积上限**：导出前必裁剪并置截断 flag（§10）。

## 14. 落地时要核对的外部规范版本

本文引用的 OTel 事实（trace_id/span_id 宽度与格式、`traceparent` 拼接格式、
severity 分档数值、属性数上限、semconv 属性名）**均随规范演进**。实现第一步应当：

- 锁定一个 semconv 版本，把用到的属性名收进常量表 + 快照测试（§6）；
- severity_number 的分档边界按当时的规范核对，不照抄本文的描述；
- W3C trace context 的版本位与 flags 按当时的规范核对。
