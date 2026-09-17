# flow 分布式部署设计（多节点执行）

> 状态：**设计稿，未实现**。本文是 `DESIGN.md` 的续篇——§1 的「单机单进程」非目标
> 由本文打开。实施分四个阶段（§11），每阶段独立可验收。
> 姊妹篇：`SCHEDULER.md`（调度者 + executor 指派模式）——与本文的对等抢占模式
> 共享租约防双写与全部恢复不变量，可共存、可互换。
> 前置阅读：`DESIGN.md` 全部，尤其 §3（事件日志）、§7（崩溃恢复）、§12（不变量）。

## 1. 目标与非目标

**目标**（两个诉求都覆盖）：

- **高可用**：任一执行节点死亡，其 run 由其他节点接手，语义与单机崩溃恢复一致；
- **横向扩展**：run 分散到多个执行节点，吞吐随节点数近似线性。

**非目标**（明确不做，理由见 §2）：

- 不做节点间直连通信、gossip、自建 Raft——**数据库是唯一协调点**；
- 不做跨 run 的全局分布式调度器/工作队列微调度（sticky queue、task list 那套）；
- 不做多活 SQLite——多写者问题不解决，单机模式保持现状；
- 不改节点内语义：AND-join、跳过传播、fatal、裁决，一个字都不动。

## 2. 核心判断：数据结构不需要换，只需要搬家

v1 最值钱的三个资产，分布式版**原样保留**：

| v1 资产 | 单机形态 | 分布式形态 | 改动 |
|---|---|---|---|
| 事件日志权威 | 本地 `event.jsonl`，fsync/事件 | Postgres `run_events` 表，事务/事件 | 存储介质，非语义 |
| fold 唯一状态机 | `RunState::fold` | 同一个函数 | **零改动** |
| 恢复分类表 | `RecoveryPlan::classify`（谓词：有 started 无终态） | 接手节点跑同一个函数 | **零改动** |

关键洞察：**租约过期接管 = 进程崩溃重启**。节点 A 持有 run 的租约后死亡，
节点 B 在租约到期后接手，B 看到的磁盘状态与「A 重启后自己看到」完全相同——
所以 §7 的整个分类表（纯节点重放 / http_call 裁决 / human_task 续等）无需重写。
这是本设计最大的省力点，也是选「共享 DB + 租约」而不是「分片多主」的根本理由。

另一个关键洞察：**单写者不变量不升级为分布式共识，只升级为排他租约**。
一个 run 同一时刻至多一个有效租约持有者，事件写入在 DB 事务内校验租约——
脑裂双写被数据库条件写物理拦截，不需要任何共识协议。

## 3. 总体架构

```
                ┌────────────┐
   WS 客户端 ───│  节点 1..N  │───┐        每个节点都是全角色（gateway + executor），
   (前端/CLI)   │ gateway +  │   │        小规模最简部署；角色可按 §10 拆开。
                │ executor   │   │
                └────────────┘   │
                ┌────────────┐   │
                │  节点 2..N  │───┼────>  Postgres（唯一协调点）
                └────────────┘   │        - run_events   事件日志（权威）
                                 │        - runs+租约列   run 元数据 + lease
                                 │        - run_signals  待投递信号
                                 └        - workflows/versions（不变）
```

节点之间**不直接通信**。所有协调（谁驱动哪个 run、信号投递、事件广播扇出）
都经过 Postgres。这让「加节点」=「起进程指同一 DB」，运维上是无聊的——
这是特性不是缺陷。

## 4. 数据模型变更

### 4.1 `run_events`（新的权威日志）

```sql
CREATE TABLE run_events (
    run_id  TEXT NOT NULL,
    seq     BIGINT NOT NULL,
    ts      TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,           -- Envelope（去掉 run_id 冗余列后的事件体）
    PRIMARY KEY (run_id, seq)
);
CREATE INDEX idx_run_events_run ON run_events (run_id, seq);
```

- 追加即 `INSERT`，主键 `(run_id, seq)` 物理拒绝 seq 重复或空洞错位；
- 单条 INSERT 事务提交 ≈ v1 的 `write_all + fsync`（`synchronous_commit = on`）；
- 残缺尾行问题**消失**（事务要么全有要么全无）——`EventLog::open` 的截断逻辑
  仅保留给 sqlite 单机模式；
- `validate_sequence` 保留为读取时的防御性校验（成本 O(n)，序列化到 payload 的
  seq 与行号对不上就是数据损坏）。

### 4.2 `runs` 增加租约列

```sql
ALTER TABLE runs ADD COLUMN
    lease_owner     TEXT,        -- executor 节点 id（uuid）
ALTER TABLE runs ADD COLUMN
    lease_expires_at TIMESTAMPTZ; -- NULL = 未租出（含所有终态 run）
```

不变量：**终态 run 的 lease 必须为空**（finalize 时释放）。

### 4.3 `run_signals`（跨节点信号投递）

```sql
CREATE TABLE run_signals (
    run_id   TEXT NOT NULL,
    node_id  TEXT NOT NULL,
    payload  JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

现有写序「先 `signal_received` 落盘、再解除等待」保持不变，只是投递路径变长：
gateway 收到 `run.signal` → 写入 `run_signals` → 持有者轮询取走 →
`append(signal_received)` → 解除 human_task 等待。
崩溃窗口语义（signal_received 已落盘无终态）与 §7.1 完全一致，零新概念。

### 4.4 SQLite 单机模式保留

`FLOW_BACKEND=sqlite`（默认）走现状，`postgres` 走新路径。分叉点在
`EventLog` 与 `Store` 的 trait 化（§11 Phase 0），不是运行时开关两套逻辑。

## 5. 租约协议（本设计唯一的新核心机制）

### 5.1 生命周期

```
获取   run.start 时 / 接手时：
       UPDATE runs SET lease_owner = $me, lease_expires_at = now() + $ttl
       WHERE id = $run_id
         AND (lease_expires_at IS NULL OR lease_expires_at < now())
       -- 影响 0 行 = 没抢到（别人持有或已终态）

续期   持有期间每 TTL/3：
       UPDATE runs SET lease_expires_at = now() + $ttl
       WHERE id = $run_id AND lease_owner = $me
       -- 影响 0 行 = 租约已被判死，立即停止驱动（见 5.3）

释放   finalize 后：SET lease_owner = NULL, lease_expires_at = NULL
```

TTL 默认 15s（`FLOW_LEASE_TTL_MS`），心跳间隔 TTL/3。

### 5.2 接手（failover）

每个 executor 每 2s 扫描（`FLOW_SCAN_INTERVAL_MS`）：

```sql
SELECT id FROM runs
WHERE status IN ('running', 'awaiting_resume')
  AND run_id NOT IN (SELECT DISTINCT run_id FROM run_events WHERE ...)
  -- 伪代码：实际按 runs.heartbeat_at 与 lease_expires_at 过滤可接手集合
  AND (lease_expires_at IS NULL OR lease_expires_at < now())
LIMIT $capacity - $my_inflight_runs
```

对每个候选：抢租约（5.1）→ 成功 → `resume_run`（现有代码）→
`RecoveryPlan::classify` 决定重放/裁决/续等。**接手与重启跑的是同一条代码路径。**

### 5.3 防脑裂双写（唯一新增不变量）

**写事件必须持有效租约，且校验与写入同事务。**

```sql
INSERT INTO run_events (run_id, seq, ts, payload)
VALUES ($run_id, $seq, now(), $payload)
WHERE EXISTS (
    SELECT 1 FROM runs
    WHERE id = $run_id AND lease_owner = $me AND lease_expires_at > now()
);
-- 影响 0 行 → EngineError::LeaseLost，Driver 立即静默退出
-- （不写 run_failed——租约丢了，真相由接手者决定，自己闭嘴）
```

这把「单写者」从进程内纪律升级为数据库强制。老持有者因为 GC 停顿/网络分区
而「以为」自己还活着时，它的写会被物理拒绝——最坏情况是它丢弃自己的进度，
接手者从权威日志续跑，这正是我们要的。

### 5.4 容量与公平

- 每个 executor 有 in-flight run 上限（`FLOW_MAX_RUNS`，默认 CPU 核数）；
- 接手扫描 `LIMIT` 剩余额度，先扫到先服务；
- 不做优先级/队列/亲和性——run 数上千、需要任务队列时再谈（YAGNI）。

## 6. 各机制的去向（v1 → 分布式映射表）

| v1 机制 | 分布式形态 |
|---|---|
| `EventLog::append`（fsync） | `run_events` 单行事务 + 租约校验 |
| `EventLog::open` 截断残缺尾行 | postgres 模式下消失（事务原子性）；sqlite 模式保留 |
| Driver tokio 任务 + registry | 不变：租约持有者进程内照旧 spawn Driver；registry 是本进程视图 |
| `Engine::signal`（mpsc→oneshot） | gateway 写 `run_signals` → 持有者轮询 → 现有 handle_signal |
| `recover_unfinished`（启动扫本地） | executor 循环扫描过期租约（§5.2），同一 `resume_run` |
| broadcast 事件扇出 | 本节点订阅者照旧；跨节点订阅见 §8 |
| `DbRunStatus` 词汇表 + 校验 | 不变 |
| http_call 裁决 / human_task 续等 | **零改动**（§2） |
| cancel | `run.cancel` 改为：写 `run_signals`（action=cancel）或直接清租约 + 置 cancelled_pending；持有者（或接手者）看到后走现有 abort 路径。P1 细化，见 §12 |

## 7. 副作用语义的不变量升格

DESIGN.md §12 的八条不变量全部保留，三条升格措辞：

1. 「事件日志是权威」→ 权威在 `run_events` 表，任何节点任何时刻可折叠验证；
2. 「崩溃窗口 = 有 started 无终态」→ **租约过期与进程崩溃产生同一谓词**，
   恢复分类不区分死因；
3. 新增第九条：**「写事件必须在有效租约内，校验与写入同事务」**。

特别值得写下来的推论：**http_call 的裁决语义在故障转移后天然成立**。
节点 A 发出 POST 后死亡 → B 接手 → 看到 Running 无终态 → `has_side_effect` →
`awaiting_resume` 等人工裁决。不猜测、不自动重放——和单机行为逐字一致。

## 8. 订阅与时间线

- `run.subscribe`：持有者节点照旧进程内 broadcast（零改动）；
  **非持有者节点的订阅者**：该节点为每个被订阅的 run 维护 `last_seq`，
  轮询 `SELECT ... WHERE run_id = $r AND seq > $last` 增量转发；
  轮询间隔 250ms（`FLOW_SUBSCRIBE_POLL_MS`）——进度 UI 的实时性足够，
  Temporal 的 UI 也是这个量级。LISTEN/NOTIFY 是 Phase 3 的可选优化。
- `run.timeline` / `run.events`：直接查 DB，天然全局一致，**比 v1 更简单**
  （不再依赖某台机器的本地文件）。
- broadcast Lagged 语义不变：慢订阅者丢事件用 `run.events(from_seq)` 补。

## 9. 时钟与顺序

- `seq` 是唯一全序权威，**不信任任何墙钟**；
- `ts` 仅用于展示；跨节点时钟漂移只影响显示排序，折叠器不读 ts——
  （`RunState` 的 started_at/ended_at 是展示字段，状态转移不依赖）；
- 租约用 DB `now()` 而不是节点本地时钟判定过期——**过期判定只有一个时钟源**。

## 10. 部署形态

**最小可用**（开发/小规模）：

```
2 × flow-server（全角色，各自 FLOW_DATA_DIR 仅缓存）+ 1 × Postgres
```

**规模化拆分**（角色可以只改配置不改编码，进程还是同一个）：

- gateway 池：`FLOW_EXECUTOR=off`，只接 WS、读写元数据；
- executor 池：`FLOW_GATEWAY=off`（或内部端口不对外），专注租约与驱动；
- Postgres：主从 + 故障切换（Patroni/RDS——DB 的 HA 是 DBA 的成熟问题，
  不进本设计范围）。

配置新增：`FLOW_BACKEND`、`FLOW_DATABASE_URL`、`FLOW_NODE_ID`（默认 uuid 自生成）、
`FLOW_LEASE_TTL_MS`、`FLOW_MAX_RUNS`、`FLOW_SCAN_INTERVAL_MS`、
`FLOW_SUBSCRIBE_POLL_MS`。

## 11. 分阶段实施

每阶段独立合入、独立验收，不许跳。

### Phase 0：trait 化（纯重构，零行为变更）

- `EventLog` 从具体类型改为 trait（`append` / `last_seq` / `read_all`），
  sqlite 实现即现状；
- `Store` 已天然按方法分组，补齐 trait 抽取；
- **验收**：现有 32 测试全绿，clippy 0，无任何行为差异。

### Phase 1：Postgres 后端 + 租约（最小分布式 = HA）

- `PgEventLog`（append 含 §5.3 租约校验）、`PgStore`（含 `ensure_run_status` 同款校验）；
- `runs` 租约列 + 抢占/续期/释放；
- executor 循环接手（§5.2）；
- `run_signals` 表 + 轮询投递；
- **验收**：双进程 + 同一 Postgres 的新集成测试——
  1. 杀死持有者（SIGKILL），另一节点在 TTL+扫描间隔内接手，run 语义正确
     （复用 ws_rpc.rs 的 SIGKILL 手法，改为杀「对的那个」进程）；
  2. 人工裁决/续等/重放三类在接手后行为与单机一致（复用恢复分类断言）；
  3. 双写防护：暂停持有者（SIGSTOP）越过 TTL，接手者接管后恢复原持有者，
     原持有者的下一次 append 被拒绝且不产生重复 seq。

### Phase 2：横向扩展

- 多 executor 负载分摊 + `FLOW_MAX_RUNS` 容量上限；
- 非持有者节点的 `run.subscribe` 轮询转发（§8）；
- **验收**：N 节点并发起 100 个 run，分布近似均匀；杀任意单节点，
  全部 run 最终达到正确终态。

### Phase 3：优化（有实测数据才做）

- `run_events` 组提交（批量事务，吞吐 ×，延迟不变量保持「先日志后副作用」）；
- LISTEN/NOTIFY 替代信号/事件轮询；
- delay 剩余时间恢复（单机版已知缺口，分布式故障转移会放大它，优先级上调）。

## 12. 风险与开放问题

1. **每事件一次 DB 往返**：单 run 节点数多、事件密集时延迟叠加。
   Phase 3 组提交解决吞吐，单事件延迟由 `synchronous_commit` 决定——
   如果证明不可接受，降级 `synchronous_commit=off` 需重新论证崩溃窗口
   （DB 崩溃可能丢尾部事务，接手者看到的是截断日志——语义等价于 v1 的
   残缺尾行截断，但**没有**「只有最后一行可残缺」的保证，需明确接受或拒绝）。
2. **cancel 跨节点路径未细化**（§6 表）：设计意图是「持有者执行，接手者兜底」，
   具体走信号还是专用列，Phase 1 实现时定，倾向信号复用（少一个概念）。
3. **Postgres 单点**：DB 挂 = 全集群停摆，但状态零丢失，恢复即全量接手。
   这是本设计用「可用性换简单性」的自觉选择；要更强可用性先解决 DB HA。
4. **`FLOW_DATA_DIR` 在 postgres 模式下退化为纯缓存目录**，文档与清理策略
   需在 Phase 1 明确，避免运维误当权威备份。
