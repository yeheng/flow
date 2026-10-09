# SQLite v1 → v2：切换方案（不做数据迁移）——已完成

> 终态（2026-10-09）：**v1 后端已删除**。缺省后端 = journal（JSONL v2
> 权威）；`FLOW_BACKEND=sqlite` 显式报错并指向本文。历史数据舍弃、不迁移；
> 想保全 v1 数据的用户走 `flow-journal-dev import-legacy` 只读保全。
> 本文保留为决策记录与行为差异清单。
>
> 语义参考：v1 引擎行为（Postgres 臂仍共用）见 [DESIGN.md](DESIGN.md)；
> v2 行为契约见 [JSONL_DEVELOPMENT.md](JSONL_DEVELOPMENT.md)。

## 0. 结论

- 单机后端只剩 **journal**（JSONL v2 唯一权威）；多节点用 Postgres。
  e2e 矩阵（postgres / journal 两臂，删除前为三臂 209 用例）全绿，journal
  臂的行为差异全部按 §3 显式分支断言。
- 历史数据不迁移：journal 只接受全新数据目录；v1 布局目录（有 flow.db）
  在 journal 模式下拒绝启动（防混用护栏）。

## 1. 已实现：v2 接入 v1 公共契约（M1 完成）

### 1.1 后端选择

- `flow-config`：`StorageBackend::Journal`（`"journal"` / `"jsonl"`）。
  `data_dir` 即 journal 根（`projection.sqlite` 在其下，可随时重建）。
- `flow_backend::open`：journal 臂构造 `JournalBackend`；
  `AnyBackend::Journal(Arc<JournalBackend>)` 成为闭集枚举第三臂——所有
  公共方法在编译器逼迫下逐个实现（适配层：`flow-backend/src/journal_arm.rs`）。
- runtime：journal 用多线程（投影回放/值读取大量 spawn_blocking；sqlite 保持
  current_thread 单写者纪律）。
- `flow_rpc::run` 的 journal 臂启动序列：`start_execution`（进程内/IPC/远程
  同一配置）+ `journal_triggers`（cron）+ v1 webhook HTTP（`POST /hook/:token`，
  走 AnyBackend 公共面）。

### 1.2 RPC 面对齐表

| v1 方法 | journal 臂实现 |
|---|---|
| workflow.create/update/publish/get/list/versions/delete | journal 命令 + State 折影；非法定义落库即拒（-32010）；已有 run 拒绝删除（-32012）；**相同 checksum 复用版本号**（v1 §8，编辑器重复保存不刷版本） |
| run.start/get/list/stats | 命令幂等（request_id）+ State 折影；「只有 published 可执行」复用 `resolve_runnable_definition` 单点规则 |
| run.timeline / snapshot | journal Run（v2 折影）→ 引擎 RunState 映射（NodeState 状态词汇、skip reason、**child_run_id 从 parent 关系反查**——等待解除后不丢） |
| run.events | journal 事件分页 → v1 Envelope（见 §3 映射）；from_seq 闭区间（seq >= from_seq） |
| run.subscribe | journal 尾读适配：指定 run 回放+追尾到终态；全局纯实时增量（LSN 基线在订阅建立时刻采集；追尾 cursor 每轮顶到新 durable 界） |
| run.cancel / run.signal | journal 命令（同步落账回执）；signal_id 兼作幂等键，回执只回显客户端提供的 id（v1 非 pg 契约） |
| run.signal（副作用裁决） | **桥接 v2 裁决面**：uncertain 节点上的 `payload.action`（retry/succeeded/failed）映射到 `run.adjudicate` 的三种决策 |
| run.signal_status | journal 臂明确拒绝（-32010，语义同 sqlite 臂） |
| schedule/webhook CRUD | `ScheduleChanged`/`WebhookChanged` 配置事实；cron 在命令内校验 |
| schedule 触发 | `journal_triggers`（触发身份 = 命令身份，天然去重，无 schedule_fires 表） |
| template CRUD | `TemplateChanged` 事件 + State.templates + 投影 kind（名字唯一在命令内拒绝） |
| config/secrets/nodetypes | 进程级面，后端无关，原样可用 |

### 1.3 v2 侧新增能力（为对齐 v1 裁决面）

- **裁决决策扩为三种**（`run.adjudicate` 增加 `decision` 参数，
  journal_v2 RPC 兼容缺省 `accept_output`）：
  - `accept_output`：人工接受产出（原语义，不伪造外部响应被捕获）；
  - `retry`：人工显式授权重发——节点回 pending、清除未决操作，驱动器以
    attempt+1 重新派发（「绝不自动重发」禁令针对机器；人工裁决就是授权
    凭据）。重放再次失败/超时仍按 v2 语义进 uncertain；
  - `failed`：人工判定失败——节点终态 failed，run 由收尾判定写 RunFailed。
- **折影保留 skip reason**（`Node.skip_reason`，serde default 兼容旧快照）：
  upstream_failed / upstream_skipped / branch_not_taken 与 v1 同词。

### 1.4 测试安全网

- `flow-backend/tests/journal_arm.rs`：公共契约直测（workflow 生命周期 /
  run 读面与事件 / 信号与取消 / 触发器与模板 / 订阅回放与实时 / 重启仅从
  journal 恢复）。
- `flow-rpc/tests/ws_journal.rs`：真起 `FLOW_BACKEND=journal` 的 flow-server，
  走 wire 验证全量 RPC + SIGKILL 重启恢复。
- **backend-e2e 三臂矩阵全绿**（`e2e_test!` 宏为每个用例生成 sqlite /
  postgres / journal 变体；v1 重试引擎专属用例走 `e2e_test_v1_only!`）。

## 2. 阶段记录（全部完成）

1. ~~**默认切换**~~（2026-10-09）：`StorageConfig::default().backend = Journal`；
   v1 布局目录（有 flow.db、无 journal/ 段目录）在 journal 模式下拒绝启动
   （防混用护栏，验收测试 `ws_journal::default_backend_is_journal_on_fresh_directory`）。
2. ~~**删除 v1**~~（2026-10-09，同日完成）：`sqlite.rs`、flow-store v1 Store、`AnyBackend::Sqlite` 臂、
   v1 调度器（scheduler.rs）、schedule_fires；`FLOW_BACKEND=sqlite` 报错并
   指向本文。DESIGN.md §3 的权威描述改为 journal。

## 3. 已知行为差异（journal 臂 vs v1，切换日变更面）

| 面 | v1 | journal 臂 | 说明 |
|---|---|---|---|
| 事件时间戳 | 真实墙钟 | `ts` = Unix epoch | journal 无墙钟（权威时间是 LSN/run_seq）；前端时间列显示 1970/空 |
| run ended_at / 节点耗时 | 真实值 | `null` / 0 | 同上；started_at（created_at）保留 |
| 事件 seq | 严格连续 1..N | 严格递增、允许空洞 | v2 权威序号含不映射到 v1 面的事实（WaitRegistered 等）；前端以 seq 为水位，空洞无害 |
| node_log 事件 | 走事件流 | 不发射 | v2 观测走 ObservationStore（run.observations.page）；前端日志控制台为空 |
| http 等外部操作失败 | retryable 自动重试 | 一律 uncertain 等裁决 | v2 核心安全立场，**人工授权 retry 后重放再超时仍回 uncertain**（e2e 已按此断言） |
| 子 run id | 确定性派生 | uuid v7 + parent 关系 | 时间线 child_run_id 经 parent 反查保持可用 |
| run.start 幂等 | 无（双击 = 双 run） | 服务端 request_id 幂等 | journal 臂内部即幂等；v1 契约无键时每次新键（行为同 v1） |
| workflow.update 相同定义 | 复用版本号 | **复用版本号（已对齐）** | checksum 相同即复用最新版本 |

### 已澄清（曾有「挂起」误报，回归钉死）

- **pre-auth SIGKILL 窗口**：SIGKILL 落在 DispatchStarted 与
  OperationAuthorized 之间时，重启**安全地重新派发**（未授权 = 请求从未
  发出，重发无副作用风险）；重放请求按节点 timeout_ms 计超时后进
  uncertain。此前的「任务静默挂起」是误报：http 默认 30s 超时大于测试的
  20s 等待窗口。回归：`flow-rpc/tests/sigkill_recovery.rs`（pre-auth 杀 +
  短超时重放 + 人工裁决 failed 落终态，全链路）。e2e 的 journal 臂在杀前
  等 500ms 授权落账，对齐用例本意「请求已发出后杀」。

## 4. 运维入口

部署、执行模式、离线维护与故障处置的完整手册见 [OPS.md](OPS.md)；本节只记
切换日特有的事实：

```sh
# journal 后端起服务（全新目录；FLOW_BACKEND 缺省即 journal）
FLOW_DATA_DIR=/var/lib/flow-v2 flow-server

# 打包形态（build-sqlite.sh / build-pg.sh 产物 bin/ 布局同样适用）
./bin/flow-server
```

- v1 布局目录（有 `flow.db`、无 `journal/`）在 journal 模式下**拒绝启动**。
- 想保全历史 v1 数据的用户：停写后用 `flow-journal-dev import-legacy` 做
  只读保全（[JSONL_DEVELOPMENT.md](JSONL_DEVELOPMENT.md)、[OPS.md](OPS.md) §4），
  但该基线与 journal 后端的日常服务面互相独立——切换不依赖它。
