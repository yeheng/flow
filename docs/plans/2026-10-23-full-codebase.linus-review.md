# Linus Review：全代码库架构审查（KISS / DRY / YAGNI / 鲁棒性 / 过度设计）

> **后续复核**：本报告包含部分错误推断与不适用的删除建议，逐条结论、修复和验证见 [复核与修复记录](2026-10-23-full-codebase.linus-review.verification.md)。以下保留原始审查内容。

> 审阅对象：`/Users/yeheng/workspaces/flow` 全仓库（15 个 Rust crate + `web/` 前端 + `web/src-tauri`）
> 审阅者：linus（只 review，不写实现）
> 审阅方式：全量阅读核心文件 + 5 路并行子系统取证 + 编译期/运行时/DB 层实证复现
> 结论状态：**骨架是对的，不要重写。1 个致命隔离缺陷必须先修；之后是 6 个严重问题和一批可删的死代码。**
>
> 规模：生产 Rust 约 37.6k LOC（`crates/flow-*/src`），测试 20.9k LOC，`web/src` 24k LOC。
> 方法论：每条结论均带 `file:line`；关键结论经独立实证复现（见 §7 证据表）。
>
> **审查基线**：本报告的行号锚定在提交 `0af6e46`（已逐条用 `git show HEAD:<file>` 复核，见 §7）。
> 审查期间该工作区有**另一个并行会话正在实施 harness agent 模式**，`git status` 在
> 00:56 后出现 8 个文件变动（含新增 `crates/flow-engine/src/process/agent.rs`）。
> 本报告对这些文件的结论**按提交 `0af6e46` 给出**；涉及并行改动部分的复核结论见 §1 F1 末尾与 §5.3。
> 未做任何回滚或改写他人工作。

---

## 0. 结论先行

**【核心判断】值得做 —— 但只做减法与补洞，绝不重写。**

这个仓库的**正确性骨架是真的**：单写者 + flock、事实与当前态同事务提交、外部副作用前先落意图、
未知结果停下来等人工裁决、执行器进程隔离、协议版本门、崩溃安全备份。DESIGN.md §0 的七条抉择
推导链条成立，文档对"不做什么"（无 HA、无多租户、无恰好一次）也诚实。**这些不要动。**

问题全部集中在**表面**：一条没关严的隔离边界、三个脱敏出口、一份靠字符串维持的类型系统、
一批测试专用/一次性遗留代码、两条并行的写路径。这些是**减法能解决的**，不是架构病。

**【品味评分】凑合（偏上）。**
加分项：零编译警告、CI 双架构 `clippy -D warnings --all-targets`、提交协议与崩溃安全确实到位、
前端纪律（代际守卫/弹层栈/轮询各一份）、文档诚实。
扣分项：一个用户可控子进程能继承权威 socket；JS 日志与 timeline 两处脱敏出口实际是空的；
`apply_event` 543 行；实体类型靠 `&str` 维持。

**【是否过度设计】是，但局部而非系统性。**
投机面集中在 4 处（`journal-cluster` PG 集群面、v1 legacy 兼容带、JSONL 编解码残骸、
Windows Job Object），合计约 2.5k LOC。**删掉它们不改变任何已承诺的行为。**
反过来说：测试脚手架 20.9k LOC（占全仓 35%）**不是过度设计**，是这仓库最值钱的资产——
它换来的是"零编译警告 + 439 项测试通过 + 故障矩阵 + 备份恢复演练"。别为了"看起来简单"去砍它。

---

## 1. 致命问题

### F1 【致命】IPC 通道 FD 泄漏进用户可控的 harness 子进程

这是本次唯一"必须立刻修"的问题。它让 DESIGN.md 的核心承诺 **C2（节点执行语义只存在于
`flow-executor` 子进程；master 只做裁决与调度）** 在最常见的 harness 节点上失效——而 harness
恰恰是**唯一能自由执行任意二进制**的节点类型。

**证据链（已实证复现，非推断）：**

| 环节 | 位置 | 事实 |
| --- | --- | --- |
| 建 socketpair | `crates/flow-engine/src/execution_protocol/unix.rs:21` | `libc::socketpair(AF_UNIX, SOCK_STREAM, 0, ...)` — **无 `SOCK_CLOEXEC`**。同文件 `:14` 注释自陈："FD 无 CLOEXEC" |
| 执行器接管 FD | `crates/flow-engine/src/execution_protocol/unix.rs:39-43` | `StdStream::from_raw_fd(fd)` + `set_nonblocking(true)`。**未设 `FD_CLOEXEC`**（`from_raw_fd` 按 std 契约不代劳） |
| 执行器拉起 harness | `crates/flow-engine/src/process.rs:27-42` | `ManagedChild::spawn_harness` → `spawn(command, false)` → 只做 `command.process_group(0)`。**无 `pre_exec`、无 FD 清理** |
| 文档承诺的防线 | `crates/flow-engine/src/execution_protocol/unix.rs:3-4` | 模块文档写"spawn + pre_exec 关闭多余 FD 的 Unix 实现" —— **该防线只存在于 master→executor 一侧**（`spawn.rs:118` `pre_exec` 逐个 close 除通道 FD 外的全部继承句柄），executor→harness 一侧**根本没有** |

**实证复现**（`/tmp/fdtest`，macOS aarch64，rustc 1.99.0，完全复刻上述三段代码）：

```
channel fd = 4
harness sees fds: 0 1 2 3 4 5 6
RESULT: channel fd 4 leaked into harness = true
```

**后果（按严重度）：**

1. **密钥明文外泄。** 通道是**双向** `AF_UNIX` socketpair，主进程解析出的凭证明文装在
   `Message::OperationPermit { credential: Option<String> }`（`crates/flow-engine/src/execution_protocol/message.rs:400-407`，
   注释"主进程解析的凭证值（仅本次调用使用，不进入授权事实）"）里发给执行器
   （`crates/flow-backend/src/execution/dispatch.rs:1315-1320` 处 `get_secret()` 解析后随 permit 下发）。
   harness 继承了同一个 socket 读端 → **可以读走这条明文**。
2. **可伪造权威结果帧。** master 侧对帧**只校验 `dispatch_id` 与 audit_seq 连续性**
   （`crates/flow-backend/src/execution/dispatch.rs:638-659`），**无帧级 MAC**。继承 FD 的 harness
   可以写入伪造 `Message::Result` / `AuditBatch`。
3. 同 uid 下 harness 可读 `/proc/<executor>/mem` 取密钥真值。

**修法（最小改动）：**
`unix.rs:39-43` 的 `channel_from_raw` 在 `from_raw_fd` 之后立刻
`libc::fcntl(fd, F_SETFD, FD_CLOEXEC)`（argv 交接在 exec 时已完成，此刻设 CLOEXEC 不影响
执行器自己用这个 FD——它已在 exec 之前被 dup 进执行器地址空间？**否**：执行器是 exec 后才拿到这个
FD，所以 CLOEXEC 必须在执行器进程内、拿到 FD 之后设置，而不是 master 侧）。
即：`channel_from_raw` 在执行器进程内调用 → 在那里设 `FD_CLOEXEC` 即可；master 侧的父端 FD
本就该设 CLOEXEC（`socketpair_channel` 里给 `fds[0]` 设）。
再给 `ManagedChild::spawn` 的 harness 路径补 `pre_exec` 关闭继承句柄清单（可复用
`spawn.rs:149` 的 `open_fds()`，但它目前是私有函数，需上提）。

**必须补的回归测试**：harness 启动后 `ls /dev/fd`（Linux 用 `/proc/self/fd`）断言看不到通道 FD。
现有 `crates/backend-e2e/tests/journal_ipc_packaging.rs:381` 只覆盖"父进程持有的非通道 FD 不出现"
（master→executor 一侧），**executor→harness 一侧零覆盖**。

**并行改动复核（2026-10-23 00:56 之后的在途实现）**：审查期间工作区新增
`crates/flow-engine/src/process/agent.rs`（`AgentWatchdog`：RLIMIT_FSIZE 封顶、
会话级 RSS/进程数采样、硬 deadline、取消后 5s 宽限、超限 `exit(75)`）。
这部分**正面回应了 §5.A 的"执行器无 rlimit"缺口**，设计意图正确（agent 模式用一次性执行器、
不复用 host 全局状态）。但它**没有触及本条致命问题**：
`crates/flow-engine/src/process.rs` 的 `ManagedChild::spawn` 在 harness 路径（`session=false`）
仍然只有 `command.process_group(0)`，**仍无 FD 清理**；`unix.rs:39` 的 `channel_from_raw`
**仍未设 `FD_CLOEXEC`**。**F1 在当前工作树依然成立。**

---

## 2. 严重问题

### S1 日志面完全不过脱敏 —— JS `console.log(input)` 把含密原文写进 event.jsonl

- `crates/flow-engine/src/expr.rs:357-359`：JS console 桥把用户字符串**原样**交给
  `emit_logger.log(level, stream, message)`。
- `crates/flow-engine/src/nodelog.rs:198-214`：`NodeLogger::log()` 里
  `message: truncate_message(&message)` —— **全程没有任何 redact 调用**。
- 因此 `console.log(input)` 会把整个节点输入（含 secret 字段）原样写进日志流。

讽刺的是，**修复它的函数已经写好了**：`redact_snapshot`（`nodelog.rs:352`，JSON 键 + URL
query/fragment 双重脱敏）**全仓零调用者**。`nodelog.rs:295-307` 的文档还把"节点 output 展示面"、
"`node_failed.error`"列为脱敏出口——**这两条出口一个都没接上**。

**修法**：`nodelog.rs:198` 的出口统一过 `redact_snapshot`（现成），并删掉那句不成立的注释。

### S2 timeline 的 error 与 run 级 output 不脱敏

`crates/flow-backend/src/journal_views.rs`：
- `:510` 节点 output → `redact_value` ✅
- `:515` 节点 input(params) → `redact_value` ✅
- `:520` **节点 error → 原样** ❌（`nodelog.rs:300` 明确点名这条出口要脱敏）
- `:529` **run 级 output → 原样** ❌（与节点级 output 行为不一致）

且 `redact_value` **只认 JSON 键**，管不到字符串里的 URL。`nodelog.rs:368-372` 写明了这一点：
"`?api_key=…` 这类把凭据放 query 的接口很常见" —— 于是 `{"url":"https://x/?api_key=SECRET"}`
在 timeline 里原样输出。**`redact_url` 写好了，但没接到 timeline 上。**

**修法**：timeline 全部改走 `redact_snapshot`（键 + URL 双管），error 一并纳入。

### S3 值先落盘、命令后提交 → 孤儿值永久堆积

- `crates/flow-backend/src/journal_commands.rs:774`：`run_start` 先 `store_json(input)`，
  命令在 `:809` 才提交。命令被拒（workflow 不存在 / 版本未发布 / trigger 已禁用，
  见 `:810-823`）→ **值块已进 journal 且无人引用**。
- `crates/flow-backend/src/journal_commands.rs:956-964`：`run_signal` **无条件 store 两次**
  （`output` 与 `adjudicated_output`），之后 `:980` 才校验是否接受 → 拒绝时留下 **2 个**孤儿块。
- `run_adjudicate:459` 同理。
- **无回收**：`docs/PRODUCTION_READINESS.md` 自述"当前没有 TTL/GC"。

仅 >64 KiB 的载荷触发（`INLINE_BYTES`），但触发次数无上限。

**修法**：用 `inspect_committed` 预判命令是否会被接受再 store；`run_signal` 的两次 store 合并成一次；
或加"未被引用的 value_chunks"回收器。

### S4 RPC 方法目录与分发跨 crate，`unreachable!()` 是运行期炸弹

- 目录：`crates/flow-dto/src/rpc.rs:2-60`，一个 `pub const READ_METHODS: &[&str]`。
- 分发：`crates/flow-rpc/src/journal_v2.rs:321-503` 一个大 `match`，兜底 `_ => unreachable!()`，
  另有第二处 `:558`。
- 两个 crate 之间**没有编译期绑定**。往目录加一个方法 → 编译通过 → **首次调用 panic**。
- `jsonrpsee` 0.26 在连接任务内 `.await` handler 且全 crate 无 `catch_unwind` →
  **panic 直接杀掉连接**，客户端只看到"连接断开"，拿不到 `-32603`。

**修法**：`flow-dto` 里把方法名改成一个 `enum Method`，用 `match` 生成 `dispatch_call`，
删掉两个 `unreachable!()`。这是**全仓性价比最高的一处改动**。

### S5 异形配置行直接 panic 权威（且穿透到 RPC）

`crates/flow-backend/src/journal_commands.rs`：
- `:1178` `value.as_object_mut().unwrap().remove("input")`
- `:1254` / `:1255` `value.as_object_mut().unwrap().remove("nodes" / "edges")`

这些 `value` 来自 `state.schedules.get(key)` / `state.templates.get(key)`，而这些行是
**原样**从 `current_entities` 载入的（`crates/flow-backend/src/journal_history.rs:91`、`:97`），
**无形状校验**。serde_json 对非对象非 null 值调用 `as_object_mut().unwrap()` → **直接 panic**。

这些 unwrap 位于 `command()` 的 decide 闭包内（RPC 路径），**该路径没有 `catch_unwind`**
（`journal_driver.rs:289-300` 的 `catch_unwind` 只覆盖协调器循环）→ 一次 panic 掀掉 RPC 处理任务。

**修法**：载入即校验 `value.is_object()`，否则 `invalid(...)`；unwrap 换 `ok_or(invalid(...))`。

### S6 Mutex 中毒策略不一致，同一文件里两种做法

- `crates/flow-journal/src/writer.rs:106,116,119`：`WriteAdmission` 的 `Mutex` 用 `.unwrap()`。
- `crates/flow-journal/src/writer.rs:501-506`：同文件的 `lock_queue` **刻意**做了毒容忍。

 poisoning 后果被放大：`writer.rs:452-467` 的 `catch_unwind` 捕获 `pump_step` 的 panic 后调用
`freeze()` → **整个唯一写者被永久冻结**，直到进程重启。

同类：`crates/flow-rpc/src/journal_v2.rs:873,879,898` 与 `crates/flow-rpc/src/lib.rs:103-104`
对 `RwLock` 用 `.unwrap()`。

**修法**：全部换 `.unwrap_or_else(|p| p.into_inner())`。

---

## 3. 一般问题（按修算力排序）

### 3.1 实体类型靠 `&str` 维持，失败是静默的

`current_entities.kind TEXT NOT NULL`（`crates/flow-journal/src/schema.sql:42`）—— **没有 CHECK 约束**。
全仓**没有 `EntityKind` 枚举**：写侧 `keys.insert(("run", ...))`（`journal.rs:842-891`），
读侧 `match kind { ... _ => None }`（`journal.rs:897-937`）。

而 `crates/flow-journal/src/sqlite.rs:21` 写明：**"None is a deletion"**（`EntityChange.value`
为 `None` 即删除）。所以 **一个拼错的 kind 会让实体被静默删除**，不是报错。

`entity_digest(kind, key, body)`（`sqlite.rs:45-52`）把 kind 混进摘要，但读侧用**同一个 kind**
重算 → 永远自洽，**摘要检查挡不住 kind 写错**。

**修法**：`kind` 改 enum + 加 `CHECK(kind IN (...))`；`_ => None` 改成 `unreachable!()` 或返回错误。

### 3.2 `apply_event` 543 行 / `apply_mode` 337 行

`crates/flow-engine/src/journal_state.rs:924`（543 行）与 `:507`（337 行）。
`apply_mode` 内含 **14 张旁路 undo 表**（`:572-598`）+ 140 行"先收集 undo"前置遍历，
`audit_only` 分支嵌套 6 层。

**修法**：拆成 `collect_undo(&tx) -> Undo` 与 `apply_events(&tx, &mut Undo)`；14 张表合并成
一个 `Undo` 结构体；`audit_only` 用早返回压平到 3 层内。

### 3.3 人工裁决谓词两处独立实现，且已经漂移

- `crates/flow-engine/src/journal_state.rs:1394-1410`（折叠侧，注释自陈"与命令层同规则"）
- `crates/flow-backend/src/journal_commands.rs:464-471`（命令侧）

**已经漂移**：折叠侧校验 `reason` 非空 + `decision ∈ {accept_output, retry, failed}`；
命令侧**两个都不校验**。且 `reason` 长度上限 4096 只在命令侧（`journal_commands.rs:440`）。

**修法**：抽成 `flow_engine::journal_state::adjudicate_guard(state, run_id, node_id, op_id, decision, reason) -> Result<()>`，
两层都调它，长度校验一并下沉。

### 3.4 六个逐字节相同的死 API

`crates/flow-engine/src/expr.rs`：
- `:412` `eval_body` 与 `:422` `eval_body_bounded` —— **完全相同**，都转发 `eval_body_recorded(.., None)`
- `:456` `eval_expr` 与 `:475` `eval_expr_bounded` —— 相同
- `:506` `expand_templates` 与 `:514` `expand_templates_bounded` —— 相同

除测试外**零调用者**；生产只走 `_recorded` 两个（`crates/flow-executor/src/task.rs:821,519`）。
`eval_body` 的文档甚至把重复当成卖点写下来（`:409-411` "与 `_bounded` 同一份默认预算"）。

同批死代码：`crates/flow-engine/src/exec.rs:2` `DEFAULT_JS_TIMEOUT_MS`、
`crates/flow-engine/src/nodelog.rs:281` `cap_input_snapshot`、`:352` `redact_snapshot`
（均只有自身测试调用）；`crates/flow-executor/src/task.rs:1946` `#[allow(dead_code)] fn unused(){}`；
`crates/flow-agent/src/pool.rs:21` 能力常量（全仓无引用）。

**修法**：删。**净减约 120 行、零行为变化。**

### 3.5 `entity_name_order` 是死索引（已实证）

`crates/flow-journal/src/schema.sql:64`：
```sql
CREATE INDEX entity_name_order ON current_entities(name,key) WHERE kind IN ('workflow','template');
```
生产查询 `crates/flow-store/src/store.rs:364-376` 绑定**单个 kind**（`push_bind(kind)`，
调用点 `crates/flow-backend/src/journal_views.rs:420` 传 `"template"`）。

**实证（sqlite3）**：
```
WHERE kind='template'            → SCAN current_entities + USE TEMP B-TREE FOR ORDER BY   ← 索引未用
WHERE kind IN ('workflow','template') → SCAN current_entities USING INDEX entity_name_order ← 命中
```
SQLite 无法证明 `kind='template'` 蕴含 `kind IN (...)`。
`INDEXED BY entity_name_order` 会直接报 `no query solution`。

**后果**：每次 template 列表全表扫描 + 临时 B-tree 排序。
**修法**：谓词拆成两个单 kind 索引，或查询改用 IN 形式；CI 加一条 `EXPLAIN QUERY PLAN` 断言。

### 3.6 LSN 分配只靠内存计数器

`crates/flow-backend/src/journal.rs:529`：`lsn: committed.state.applied_lsn + 1`。
`applied_lsn` 只由 `apply` 推进（`:559`），**从不用 SQLite 的值来修正分配计数器**
（`store.applied_lsn()` 只在 `:628-633` 用于算 `visible` 标志）。

**歧义提交**（写已落盘但 ack 丢失）→ `applied_lsn` 不前进 → 下次提交复用同一 LSN →
`transactions.lsn` 是 PRIMARY KEY（`schema.sql:9`）→ UNIQUE 冲突 → **提交永久停滞**。
`journal.rs:534-536` 的 `preview` 会先跑一遍 reducer，把 reducer 自身错误拦在提交前，
所以残留触发点只剩歧义提交 —— 概率低，但后果是不可自愈的永久停摆。

**修法**：`pending = None`（`:556`）移到 `apply` 成功之后；提交出错时从 store 回读 `applied_lsn`。

### 3.7 同一实体两条并行写路径，语义不同

`crates/flow-rpc/src/journal_v2.rs`：
- `:435` `schedule.change` / `webhook.change` → `b.config_change(...)`（**raw patch** 路径）
- `:446` `schedule.create/update/delete`、`webhook.*`、`template.*` → `product_command_route`（**typed** 路径）

两条都汇到 `trigger_event`/`template_event`，但**合并语义与校验面不同**：
`journal_commands.rs:1032-1070`（raw，带可写字段白名单）与 `:1117-1259`（typed，142 行、
三层 match 嵌套、三处 `unreachable!()`）。

**修法**：删掉 raw patch 路径，RPC 全部走 typed；或反过来只留 raw。**规则只剩一份。**

### 3.8 `visible` 字段同名两义

`crates/flow-backend/src/journal.rs:607-611` 的 `visible()` **永远置 true**（空壳）；
`:628-633` 又用 `store.applied_lsn() >= receipt.commit_cursor.lsn` 给同一字段另一套语义。
`CommandReceipt.visible` 生产代码零消费。

**修法**：删字段，别留两义。

### 3.9 硬编码的尺寸上限有 5 个且互相不一致

`crates/flow-store/src/store.rs`：
- `:110` SQL 内联 `8388608`（8 MiB）
- `:126` `8 * 1024 * 1024`（8 MiB）
- `:158` `8 * 1024 * 1024 - body.len()`（8 MiB）
- `:285` `4 * 1024 * 1024`（**4 MiB** — list 的上限比 get 小一半）
- `:376`/`:378` `LIMIT 10001` / `> 10000`

全 crate `8 * 1024 * 1024` 字面量共 **15 处**（`journal_commands.rs:386/459/774/957/961/1413`、
`journal_driver.rs:915`、`journal_import.rs:147/187`、`regression.rs:132`、
`execution/dispatch.rs:215/1045/1053/1085`），只有 `journal_views.rs:13` 给同值起了名 `VALUE_BUDGET`。

**修法**：一个 `const VALUE_BUDGET` + 一个 `const LIST_BUDGET`。

### 3.10 `store.list()` 是 N+1 查询

`crates/flow-store/src/store.rs:273-304`：第一个查询取 `key + length(body)`，
然后**对每一行再查一次** `SELECT body,digest`。既然第一个查询已经 touch 了 body
（`length(CAST(body AS BLOB))`），一次 `SELECT body,digest,length(body)` 就能一趟拿完。

### 3.11 `run.regress` 的 u64 有三套 wire 规则

- `crates/flow-rpc/src/journal_v2.rs:388`：只收 JSON number
- 同文件 `:68-77` `optional_version`：number/string 都收
- 同文件 `:50-61` `decimal_param`：只收规范十进制字符串

且与 `crates/flow-journal/src/lib.rs:52` 的"永不是 JSON number"自相矛盾。

### 3.12 hook 处理器内联重写了错误映射，漏掉 429

`crates/flow-rpc/src/journal_triggers.rs:482-488` 内联抄了一份 `invocation_error`（`:169-182`），
**漏掉 `Limit → 429` 分支**（落到 `_ => 503`）。同一 journal 错误在 `/hooks/` 与 `/invoke/`
下返回不同状态码。

### 3.13 `PRAGMA journal_mode=WAL` 结果未校验

`crates/flow-journal/src/sqlite.rs:310`：`execute_batch("PRAGMA journal_mode=WAL; ...")`
不检查返回模式。WAL 启用失败 → 静默降级为 DELETE journal → **durable 契约无声改变**。
**修法**：随后 `PRAGMA journal_mode` 断言等于 `wal`。

### 3.14 前端：一处竞态、一处永久卡死、一处类型丢失

- **竞态**（已核对）：`web/src/views/JournalView.vue:148-156` 的 `nextPage()` **缺代际守卫**。
  同文件 `pullObservations`（`:55-60`）与 `refreshRun`（`:139-140`）都有 `if (runId.value !== id) return;`，
  唯独 `nextPage` 没有。切 run 时 A 的响应会覆盖 B 的 `page` → **审计面显示错数据**。
  `web/src/state/stale.ts:5-6` 的注释写着"漏一份就出竞态"——此处正是漏了一份。
- **永久卡死**：`web/src/rpc/websocket.ts:142-152` 的 `sendSubscribe` **没有超时**，
  与同文件 `call()` 的超时契约（`:23-24, 89-92`）矛盾。服务端接受 WS 但不回 `run.subscribe`
  → `monitor.ts:202` 的 `await api.subscribeRun(...)` 永不 settle → `monitor.starting` 永远为 `true`
  → "运行"按钮永久禁用。`monitor.ts:149` 的 AbortController 只挡 `requestRunInput`。
- **类型丢失**：`web/src/state/run-input.ts:126-146` 的 `assembleInput` 对每个字段无脑 `JSON.parse`，
  字符串字段填 `5` 变成数字 `5`。`parseInputFields`（`:149`）**丢掉了字段 `type`**，
  与 schema 感知的另一条路径 `components/WorkflowInputForm.vue:80-89` 行为不一致。
- **性能**：`web/src/state/editor.ts:161` 的 `dirty` = `JSON.stringify(flowToDefinition())`，
  而后者每条边都 `editor.nodes.find(...)`（`:448`）→ **O(E×N)**，且模板 `EditorView.vue:273`
  每帧触发全图序列化 + find 风暴。

---

## 4. 过度设计清单（可直接删，零行为变化）

| # | 投机面 | 规模 | 判据 |
| --- | --- | --- | --- |
| 1 | `journal-cluster` PG 集群面 | `flow-backend/src/journal_cluster.rs` 533 行 + `flow-rpc` 侧 + `migrations/journal_cluster_v1.sql` + CI 专用 PG job + 前端 Cluster UI | DESIGN.md C5 明说"不提供 master 高可用"、§1 明说"集群只有登记，没有业务调度"。**功能本身有真实消费者**（`flow-rpc/src/lib.rs:198`），但它默认 `default = []` 关闭 → **默认构建下前端 Cluster UI 永远空白**。要么开成默认并给承诺，要么整条摘掉，别留半吊子 |
| 2 | v1 legacy 兼容带 | `state.legacy: BTreeMap<String, Value>`（`journal_state.rs:445`）+ `EventKind::LegacyImport` + undo 机制（`:598-611`、`:829`）+ 折叠分支（`:1074-1080`）+ `journal_import.rs` 321 行 + `legacy.list`/`legacy.get` 两个 RPC | DESIGN.md 已宣告 v1 删除，但兼容带**仍接在核心状态机里**（每次折叠都要维护 undo）。`legacy.list`/`legacy.get` **零客户端**（web + flow-cli + src-tauri 三处全查） |
| 3 | JSONL 编解码残骸 | `flow-journal/src/codec.rs:172-183, 272-302, 304-329, 331-378`（约 120 行） | SQLite 之前的日志格式残留，**只有自己的单测在用** |
| 4 | `scan_fast` / `SyncLatency` 双直方图 | `flow-journal/src/storage.rs:71-79`、`:155-172` | 全仓零生产调用 |
| 5 | Windows Job Object | `flow-engine/src/process.rs:190-252`（约 60 行 unsafe，CREATE_SUSPENDED + Toolhelp） | CI **只 `cargo check`**，从不运行；且嵌套 Job Object 环境（容器/CI）下 `AssignProcessToJobObject` 直接失败 → 执行器根本起不来 |
| 6 | 六个逐字节相同的公开 API | `flow-engine/src/expr.rs`（见 §3.4） | 纯死代码 |
| 7 | `backend-perf` 的 `legacy-baseline` feature | `crates/backend-perf/Cargo.toml:42` | 历史基线对比用，无人启用 |
| 8 | `dep:clap` 挂在 library feature 上 | `crates/flow-journal/Cargo.toml:10` | `storage` feature 把 CLI 参数解析器塞进 library，所有下游被动依赖 clap |

---

## 5. 做得好的地方（不要动）

1. **提交协议**：`crates/flow-journal/src/sqlite.rs:347` 一条 `BEGIN IMMEDIATE` 同时落
   事实 + 事件 + 值块 + `current_entities` + 不可变回执 + watermark，`:554` 一条 `COMMIT`。
   写侧与读侧共用同一套校验（`writer.rs:562` → `sqlite.rs:251`），不存在"写得进读不回"。
2. **崩溃安全**：`PRAGMA synchronous=FULL + fullfsync=ON + checkpoint_fulljsync=ON`（`sqlite.rs:310`）；
   `sqlite.rs:294-304` 能识别"user_version=0 且无表"的半初始化态并重新 bootstrap；
   `:329-331` 校验 `MAX(lsn)` 与 `committed_lsn` 一致性，不一致直接报错。
3. **备份恢复**：`flow-journal/src/maintenance.rs:153-202` —— SQLite online backup 取一致快照 →
   `journal_mode=DELETE` 封存 → `sync_all` → **在 staging 上跑完整 `verify`** → `hard_link` 无覆盖发布 →
   `StagingGuard`（`:130-147`）整体回滚不留残骸；`prepare_destination`（`:93-126`）拒绝任何外来内容。
4. **单写者模型**：目录 `flock`（`sqlite.rs:271-273`）+ symlink 三处拒绝（`sqlite.rs:56,281`），
   工具与在线服务互斥。
5. **协议纪律**：`PROTOCOL_VERSION` 单点定义（`contract.rs:25`），握手双端硬校验，
   **明确拒绝旧版本、不做兼容垫片**；帧长在分配前校验（`frame.rs:44-52`）；
   postcard 必须恰好吃完帧体防走私（`:123-129`）；`Routed` 递归取内层预算防绕过（`:33`）；
   未知变体索引直接杀会话（`transport.rs:885-907`）。
6. **JS 沙箱**：每次求值新建 Runtime（不跨求值复用，规避 quickjs 原子表无界增长）；
   32 MiB 堆上限、512 KiB 原生栈、interrupt handler 按时钟中断、输入深度 ≤64、输出 `bounded_json`；
   **无任何 IO 绑定**。这是全仓最强的一块。
7. **工程纪律**：`cargo build` **零警告**；CI 跑 `clippy --workspace --all-targets --locked -- -D warnings`，
   x86_64 + aarch64 **双架构原生**；生产代码仅 3 处 `#[allow(dead_code)]`。
8. **前端纪律**：代际守卫（`state/stale.ts`）、弹层栈（`state/overlays.ts` + `state/modal.ts`）、
   轮询脚手架（`state/polling.ts`）、时间格式化各只有一份；写方法清单与后端
   `crates/flow-dto/src/rpc.rs` **逐项对齐**；11 条路由全可达、零孤儿组件；22 处非测试 `JSON.parse` 全有 try/catch。
9. **文档诚实**：DESIGN.md / PRODUCT_REVIEW.md / PRODUCTION_READINESS.md 明确写出"不做什么"，
   并给出 439 项测试、故障矩阵、备份演练证据，且把"未验证"的部分列成待办而不是含混过去。

### 5.A 执行器资源上限：在途实现已补上大半

提交 `0af6e46` 时执行器**无任何进程级 rlimit**——`spawn.rs` 只**读** rlimit 用于 FD 扫描，
从不施加；harness 子进程更没有。契约里"常驻内存 ≈ X_max × 单进程 RSS"
（`crates/flow-engine/src/execution_protocol/contract.rs:59-61`）只是估算，不是上限。

**在途实现（`crates/flow-engine/src/process/agent.rs`，新增）** 已补上：RLIMIT_FSIZE 封顶、
会话级 RSS/进程数采样（Linux `/proc/<pid>/statm`、macOS `proc_pidinfo`）、
硬 deadline、取消后宽限期、超限 `exit(75)` 并由 `ManagedChild` 回收整个会话。
这是正确方向——**agent 模式用一次性执行器、不复用 host 全局状态**。

**仍未闭合**：(a) Windows 走 `#[cfg(not(any(target_os = "linux", target_os = "macos")))]`
的 `session_usage() -> Ok((0, 0))` **恒不触发**，Windows 侧仍无预算；
(b) 该预算只作用于 **agent 模式**，`command` 模式的 harness 仍无 rlimit；
(c) 注释自陈"detached sessions need an OS sandbox"——这是诚实的，但意味着
"用户命令不能打爆整机"这条承诺**目前只对 agent 模式成立**。

---

## 6. 关键洞察

- **数据结构**：`current_entities` 用"JSON body + 生成列 + 显式键部件（parent/slot/extra）"混合建模，
  很聪明；但代价是**实体种类靠字符串维持**，且**键编码格式成了跨 crate 的隐式契约**
  （`store.rs:36,172,249` 直接依赖 `extra` 的第三位语义，改 `key_parts` 会静默改变查询结果）。
- **复杂度**：复杂度不长在算法上，长在**同一语义写两遍**——两套握手（`flow-agent/src/runtime.rs:222-272`
  生产 vs `:891-944` 仅 e2e）、两套 relay 解包、两份能力表（4 处副本）、
  两套重连顺序、两条触发器写路径、两份裁决谓词、六个重复 API。
- **风险点排序**：F1 FD 泄漏 > S1/S2 脱敏出口 > S3 孤儿值 > S4 `unreachable!()` >
  S5 异形行 panic > S6 中毒 > 3.1 kind 静默删除 > 其余。

---

## 7. 证据表（本报告的独立复现）

| 结论 | 复现方式 | 结果 |
| --- | --- | --- |
| harness 继承通道 FD | `/tmp/fdtest` 完全复刻 `unix.rs:21`+`:41`+`process.rs:37` 三段代码，macOS aarch64 / rustc 1.99.0 | `RESULT: channel fd 4 leaked into harness = true` |
| `entity_name_order` 是死索引 | `sqlite3` 复刻 schema.sql:64 + 生产查询 `store.rs:365-376` | `kind='template'` → TEMP B-TREE；`kind IN (...)` → 命中索引 |
| `redact_snapshot` 零调用者 | 全仓 grep（`crates/` + `web/src`） | 仅 `nodelog.rs:352` 定义处命中 |
| `legacy.list`/`legacy.get` 零客户端 | web/src + web/src-tauri + flow-cli/src 三处全查 | 两处均无调用 |
| `redact_value` 管不到 URL query | `nodelog.rs:325-346` 只按 JSON 键匹配；`redact_url`（`:378`）与 `redact_snapshot`（`:352`）零调用 | URL 内凭据在 timeline 原样输出 |
| `config_change` 是生产路径 | `flow-rpc/src/journal_v2.rs:435` | **是**（subagent 曾误判为"仅测试调用"，已修正） |
| `visible` 同名两义 | `journal.rs:607-611` vs `:628-633` | 确认 |
| 零编译警告 | `cargo build --workspace` | 0 warnings |
| 本报告全部行号锚定提交 `0af6e46` | `git show HEAD:<file>` 逐条复核 `unix.rs:21/:39-43`、`process.rs:37-39`、`message.rs:400-407`、`dispatch.rs:638-641`、`nodelog.rs:198/:352`、`journal_state.rs:507/:924` | **全部逐字命中** |
| F1 在并行改动后依然成立 | 复核在途新增的 `crates/flow-engine/src/process/agent.rs` 与改后的 `process.rs:29-48` | `AgentWatchdog` 只加资源预算；`spawn` 的 harness 路径仍无 FD 清理，`channel_from_raw` 仍未设 `FD_CLOEXEC` |
| 异形配置行 panic 路径 | `journal_commands.rs:1178/:1254/:1255` + `journal_history.rs:91/:97` | 确认（`value` 原样载入，无形状校验） |
| `run_signal` 双重 store | `journal_commands.rs:956-964` | 确认（两次 `store_json` 均在命令校验之前） |
| 前端代际守卫缺失 | `JournalView.vue:139-140` vs `:148-156` | 确认（`nextPage` 无 `runId.value !== id` 守卫） |

---

## 8. 任务清单

### T1 【阻断】堵住 IPC 通道 FD 泄漏

- **目标**：harness 子进程不再继承权威 socket。
- **影响范围**：`crates/flow-engine/src/execution_protocol/unix.rs`、`crates/flow-engine/src/process.rs`。
- **技术方案**：
  1. `channel_from_raw`（`unix.rs:39`）在 `from_raw_fd` 后立即 `fcntl(fd, F_SETFD, FD_CLOEXEC)`；
  2. `socketpair_channel`（`unix.rs:17`）给**父端** `fds[0]` 设 `FD_CLOEXEC`；
  3. `ManagedChild::spawn`（`process.rs:27`）harness 路径补 `pre_exec` 关闭继承句柄
     （把 `spawn.rs:149` 的 `open_fds()` 上提为 `pub(crate)`，避开 `spawn.rs:110` 注释警告的
     枚举/close 竞态：枚举在**父进程**做，close 在 `pre_exec` 做）；
  4. 同步修正 `unix.rs:3-4` 的模块文档（它现在承诺了一个只在 master 侧存在的防线）。
- **测试**：新增 e2e —— harness 启动后 `ls /dev/fd`（Linux `/proc/self/fd`）断言看不到通道 FD；
  补 executor→harness 一侧的 FD 继承断言（现有 `journal_ipc_packaging.rs:381` 只覆盖 master 侧）。
- **验收**：harness 进程 FD 列表中无通道 FD；既有 IPC 全链路测试全绿。
- **禁止**：不要改动 `Message::OperationPermit` 的 wire 格式（会破坏协议版本兼容）。

### T2 【阻断】把脱敏接回三个出口

- **目标**：含密值不再出现在日志流、timeline、run 级 output。
- **影响范围**：`crates/flow-engine/src/nodelog.rs`、`crates/flow-backend/src/journal_views.rs`。
- **技术方案**：
  1. `nodelog.rs:198` 的 `NodeLogger::log()` 出口统一过 `redact_snapshot`（`nodelog.rs:352`，已存在）；
  2. `journal_views.rs:520` 的 node `error`、`:529` 的 run `output` 改走 `redact_snapshot`，
     与 `:510`/`:515` 保持一致；
  3. 删除 `nodelog.rs:295-307` 中"输出即已脱敏"的不成立表述，改成与实现一致；
  4. 顺带修 `crates/flow-rpc/src/lib.rs:13` 的陈旧模块文档
     （引用了不存在的 `flow-rpc::timeline_value` 与 `flow-rpc::redact_display_envelope`）。
- **测试**：单测覆盖"secret 在 JSON 键下""secret 在 URL query 下""secret 在 error 文本中"三种形态。
- **验收**：含 `api_key` / `token` 的输入经 `console.log(input)` 后，event.jsonl 与 timeline 中
  均不出现明文。

### T3 【严重】让 RPC 目录与分发同源

- **目标**：新增 RPC 方法不再可能编译通过但运行期 panic。
- **影响范围**：`crates/flow-dto/src/rpc.rs`、`crates/flow-rpc/src/journal_v2.rs`。
- **技术方案**：`flow-dto` 导出 `enum Method`（含全部方法名常量），`dispatch_call` 改为
  `match method { Method::X => ... }`；删除 `journal_v2.rs:502`、`:558` 两处 `unreachable!()`。
- **测试**：加一条编译期断言——`Method` 的所有变体都被 `dispatch_call` 覆盖（靠穷尽 match 保证）。
- **验收**：往目录加方法时，编译器在**未加 match 分支时**报错。
- **禁止**：不得改变任何现有方法的 wire 名称。

### T4 【严重】读入即校验形状，消除 panic

- **目标**：异形配置行不再 panic 权威、不再掀掉 RPC 处理任务。
- **影响范围**：`crates/flow-backend/src/journal_history.rs:91,97`、
  `crates/flow-backend/src/journal_commands.rs:1178,1254,1255`。
- **技术方案**：载入 schedule/webhook/template 行时校验 `is_object()`，否则记 `Invalid`；
  `as_object_mut().unwrap()` 换 `ok_or_else(|| invalid("malformed config row"))`。
- **测试**：写入一行 `kind='schedule'` 但 body 为数组 → 断言返回 `Invalid` 而不是 panic。

### T5 【严重】值先验证后落盘

- **目标**：不再产生孤儿值块。
- **影响范围**：`crates/flow-backend/src/journal_commands.rs:774`（`run_start`）、
  `:956-964`（`run_signal`）、`:459`（`run_adjudicate`）。
- **技术方案**：`store_json` 之前先用 `inspect_committed` 预判命令会被接受；
  `run_signal` 的两次 store 合并为一次（`adjudicated_output` 只在被接受时才需要）。
- **验收**：对不存在的 workflow 连续调 `run.start` 100 次 → `value_chunks` 表行数不增长。

### T6 【严重】统一 Mutex 中毒策略

- **目标**：单写者不会被一次 poisoning 永久冻结。
- **影响范围**：`crates/flow-journal/src/writer.rs:106,116,119`；
  `crates/flow-rpc/src/journal_v2.rs:873,879,898`；`crates/flow-rpc/src/lib.rs:103-104`。
- **技术方案**：全部改 `.unwrap_or_else(|p| p.into_inner())`，与 `writer.rs:501-506` 的既有策略一致。
- **测试**：注入一次 panic 使锁中毒，断言后续提交仍可成功。

### T7 【重要】把实体种类从字符串变成类型

- **目标**：kind 写错时**响亮失败**而非静默删除。
- **影响范围**：`crates/flow-journal/src/schema.sql:42`、`crates/flow-backend/src/journal.rs:842-941`、
  `crates/flow-store/src/store.rs` 全部 `kind: &str` 签名。
- **技术方案**：`flow-dto` 定义 `enum EntityKind`；`flow-journal` 的 `EntityChange.kind` 改用该类型；
  `schema.sql` 加 `CHECK(kind IN ('run','run_node','run_attempt','workflow','workflow_version',
  'schedule','webhook','template','dispatch','child','legacy'))`（需走一次 schema 升级）。
- **注意**：这是**破坏性 schema 变更**，须与 `docs/adr/0002-sqlite-persistence.md` 的
  v3→v4 升级路径对齐，走 `maintenance.rs` 的现有升级工具，不手改。
- **验收**：写入未知 kind → DB 拒绝；读侧 `_ => None` 分支消失。

### T8 【重要】合并两套裁决谓词与两套触发器写路径

- **目标**：同一规则只剩一份。
- **影响范围**：`crates/flow-engine/src/journal_state.rs:1394-1410`、
  `crates/flow-backend/src/journal_commands.rs:440-443,464-471,1032-1091,1117-1259`。
- **技术方案**：
  1. 裁决谓词抽成 `flow_engine::journal_state::adjudicate_guard(...)`，折叠侧与命令侧都调它，
     `reason` 长度上限一并下沉；
  2. 删 `config_change` / `template_change` raw patch 路径，RPC 全部走 typed 路径，
     `backend-e2e` 相应改写。
- **验收**：裁决规则只有一处实现；`grep -c` 裁决谓词只剩 1 处。

### T9 【重要】删死代码与投机面（零行为变化）

- **目标**：净减 2.5k~3k LOC，不动任何已承诺行为。
- **删除清单**：`flow-engine/src/expr.rs` 六个重复 API 中的三个（`:412/:456/:506`）+
  `exec.rs:2` `DEFAULT_JS_TIMEOUT_MS` + `nodelog.rs:281` `cap_input_snapshot` +
  `flow-executor/src/task.rs:1946` + `flow-agent/src/pool.rs:21` +
  `flow-journal/src/codec.rs:172-183,272-378` + `flow-journal/src/storage.rs:71-79` +
  `flow-backend/src/journal.rs:158-186`（`state()`/`full_state()`，生产零调用）+
  `flow-backend/src/execution/remote.rs:209,240` 两处 `#[allow(dead_code)]` +
  `flow-engine/src/execution_protocol/transport.rs:25` 的陈旧 `#[allow(dead_code)]`。
- **另需决策**：`journal-cluster`（见 §4 表 1）**开成默认并给承诺，或整条摘掉**——
  当前"默认关闭 + 前端 UI 在"是三者里最差的一种。
- **注意**：`redact_snapshot` 与 `cap_input_snapshot` 在 T2 完成前**不可删**（T2 会让 `redact_snapshot` 变成活代码）。

### T10 【一般】修索引、限值、LSN 与前端四洞

- **索引**：`schema.sql:64` 谓词拆成 `kind='workflow'` 与 `kind='template'` 两个索引，
  或 `store.rs:365` 改 IN 形式；CI 加 `EXPLAIN QUERY PLAN` 断言。
- **限值**：`flow-store/src/store.rs` 的 5 个魔数收敛成 2 个具名常量；
  全 crate 15 处 `8 * 1024 * 1024` 收敛到 `flow-journal` 的一个 `VALUE_BUDGET`。
- **LSN**：`journal.rs:556` 的 `pending = None` 移到 `apply` 成功之后；提交出错时从
  `store.applied_lsn()` 回读修正分配计数器。
- **前端**：
  1. `web/src/views/JournalView.vue:148` 加代际守卫（与 `:139-140` 对齐）；
  2. `web/src/rpc/websocket.ts:142` 的 `sendSubscribe` 加 `DEFAULT_CALL_TIMEOUT_MS` 上界；
  3. `web/src/state/run-input.ts:149` 的 `parseInputFields` 保留字段 `type`，
     `assembleInput` 按类型解析（字符串字段透传原文）；
  4. `web/src/state/editor.ts:161` 的 `dirty` 改增量脏标记，别在拖拽帧上跑 O(E×N) 全图序列化；
  5. 6 份截断助手收敛到 `state/time.ts` 的 `truncate(s, n)`；删 `api/flow.ts:452` 死导出
     `webhookUrl()`（`TriggersView.vue:243` 改调它）。

---

## 9. 结论

这个仓库**不是**过度设计的产物。它的问题是**在一份好架构上，边界没有关严、旧世界没有清干净**。

- **KISS**：凑合。骨架简单，但 `apply_event` 543 行、六个重复 API、五个魔数、三处 `unreachable!()`
  说明"该拆的没拆"。
- **DRY**：凑合。大部分重复已被纪律消灭（前端 25/25 方法对齐、`R_MAX` 单点定义、
  `NODE_TYPE_TABLE` 派生表是全仓最好的设计），但裁决谓词、触发器写路径、能力常量、
  握手、重连顺序各写两遍，且**裁决谓词已经漂移**。
- **YAGNI**：凑合偏下。4 处投机面约 2.5k LOC，删掉零行为变化。
- **鲁棒性**：**存储与协议层强**（单事务提交、崩溃安全、备份演练、版本门、帧预算都做对了）；
  **边界层弱**（FD 泄漏、脱敏出口、panic 穿透 RPC）。强项在数据面，弱项在"用户代码与权威之间的缝"。
- **是否够简洁**：不够，但差得不多。做完 T1/T2/T3/T4/T5/T6（**约 6 个点**），
  这个仓库就会同时具备"能上线"和"能改"。
- **是否过度设计**：是，局部的。删 §4 的 8 项 ≈ 2.5k~3k LOC，行为不变。

**T1 和 T2 上线前必须做。** 其余按 §8 顺序推进。
