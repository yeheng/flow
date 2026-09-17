# flow 调度者 + Executor 模式设计

> 状态：**设计稿，未实现**。`DISTRIBUTED.md` 的姊妹篇——两者共享
> 租约防双写（其 §5.3）与全部恢复不变量（其 §7），区别只在「谁发起租约」。
> 对等模式（executor 抢占）与本文的指派模式可以在同一集群**共存**（§7）。

## 1. 先回答：调度者到底「调度」什么

中心化调度不是把架构画得更好看，它必须有**对等模式做不了的决策内容**。
逐条审视，只有这些是真需求：

| 决策 | 对等模式的困境 | 调度者的解法 |
|---|---|---|
| **优先级** | 抢占是 `LIMIT` 先扫到先服务，run 多了以后「先创建先跑」不合理 | 全局视图排序 `priority DESC, started_at ASC` |
| **工作流级并发上限** | 每个 executor 只知道自己局部的 run 数，无法限制「workflow X 最多同时跑 10 个」 | 指派前 `COUNT` 校验，天然全局 |
| **配额/公平** | 多租户下一个高频 workflow 打满集群，其他 workflow 饿死 | 按 workflow/租户维度分配容量 |
| **可运维的调度决策** | 「为什么这个 run 在那台机器上」没有答案，散在 N 个 executor 的日志里 | 决策集中在一处，可审计、可解释 |

**反面清单**（这些不是上调度者的理由）：

- 「抢占有惊群」——那是实现问题，`SELECT ... FOR UPDATE SKIP LOCKED` 就是
  数据库原生的无惊群抢占，**不需要任何调度进程**；
- 「负载均衡」——对等抢占本身就是负载均衡（闲的抢得多）；
- 「看起来更企业级」——空转的调度者是一个把 DB 写放大成「DB + 进程内存」
  的转发器，多一个故障点，少一分诚实。

**结论**：需要优先级/配额/工作流级限流 → 本文；只要 HA 和横向扩展 →
`DISTRIBUTED.md` 的对等模式就够，别给自己加进程。

## 2. 核心设计：指派 = 租约换个发起方

整个模式可以压缩成一句话：

> **lease_owner 改名 assigned_to；「获取租约」的动作从 executor 抢占（UPDATE
> ... WHERE expires < now()）改为 scheduler 指派（UPDATE ... SET assigned_to = $e）；
> 续期、释放、防双写、接手恢复全部不变。**

三个动作的归属变化：

| 动作 | 对等模式 | 指派模式 |
|---|---|---|
| 获取（谁写 assigned_to） | executor 自己抢 | **scheduler 指派** |
| 续期（谁延期） | executor 心跳（不变） | executor 心跳（不变） |
| 释放/改派 | executor finalize 后清空；死亡靠 TTL 过期 | 同左；**外加 scheduler 主动改派** |

防双写 SQL 与 `DISTRIBUTED.md` §5.3 **逐字相同**（只换列名）：

```sql
INSERT INTO run_events (run_id, seq, ts, payload)
VALUES ($run_id, $seq, now(), $payload)
WHERE EXISTS (
    SELECT 1 FROM runs
    WHERE id = $run_id
      AND assigned_to = $me
      AND assigned_expires_at > now()
);
-- 影响 0 行 → EngineError::LeaseLost → Driver 静默退出（真相由被指派者决定）
```

改派竞态无需新机制：scheduler 改派的瞬间老 executor 若还在写事件，
条件里 `assigned_to = $me` 已不成立——写被物理拒绝，老 executor 静默退出。
这与脑裂防写的语义是同一条不变量：**「写事件必须在有效指派内，校验与写入同事务」**。

## 3. 架构

```
                     ┌────────────────────┐
                     │  scheduler（×1 活跃）│←─ standby（advisory lock 选主，§6）
                     │  无状态：决策循环    │
                     └─────────┬──────────┘
                               │ 指派（写 runs.assigned_to）
                               ▼
   ┌────────────┐      ┌────────────┐      ┌────────────┐
   │ executor A │      │ executor B │      │ executor C │   ← 只认领 assigned_to = me
   │ 认领+驱动+  │      │            │      │            │     的 run；心跳续期
   │ 心跳续期    │      │            │      │            │
   └─────┬──────┘      └─────┬──────┘      └─────┬──────┘
         │    心跳 / 事件写入 / 信号轮询（全部经 DB）    │
         └────────────────────┼────────────────────┘
                              ▼
                        Postgres（唯一协调点）
                        run_events / runs(+指派列) /
                        run_signals / executors / workflows(+并发上限)
```

gateway 角色不变（`DISTRIBUTED.md` §10）：WS 客户端连任意 gateway，
gateway 照旧写 `runs` + `run_signals`，与 scheduler、executor 均无直接通信。
**scheduler 与 executor 之间也不直接通信**——指派、心跳、死亡判定全部
通过数据库行，延续了「DB 是唯一协调点」的边界。

## 4. 数据模型（相对 DISTRIBUTED.md 的 delta）

```sql
-- 4.1 新表：executor 注册与心跳
CREATE TABLE executors (
    id               TEXT PRIMARY KEY,     -- FLOW_NODE_ID
    capacity         INT  NOT NULL,        -- FLOW_MAX_RUNS（executor 自己声明）
    inflight         INT  NOT NULL DEFAULT 0, -- executor 心跳上报的真值镜像
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 4.2 runs：lease_owner / lease_expires_at 更名为
--     assigned_to / assigned_expires_at（语义：指派即租约，见 §2）

-- 4.3 决策输入（调度者存在的理由，§1）
ALTER TABLE runs ADD COLUMN priority INT NOT NULL DEFAULT 0;
-- run.start 增加 priority 参数（0-9）
ALTER TABLE workflows ADD COLUMN max_concurrent_runs INT;
-- NULL = 不限；scheduler 指派时校验
```

`executors.inflight` 是镜像不是权威——真值在 executor 进程内（registry），
心跳携带上报。scheduler 基于镜像做指派，最坏情况短暂过载一个 run，
finalize 时被纠正。**不做反向对账**（YAGNI，等出问题再说）。

## 5. 协议

### 5.1 executor 生命周期

```
注册   启动时 INSERT INTO executors (id, capacity, ...) ON CONFLICT UPDATE
心跳   每 TTL/3（TTL 默认 15s，同对等模式）：
       UPDATE executors SET last_heartbeat_at = now(), inflight = $n WHERE id = $me;
       同时为每个持有中的 run 续期：
       UPDATE runs SET assigned_expires_at = now() + $ttl
       WHERE id = $run_id AND assigned_to = $me;
       -- 0 行 = 已被改派 → 立即静默退出该 run 的 Driver
退出   正常关闭：finalize 在跑的 run 或显式移交；DELETE FROM executors
```

### 5.2 scheduler 决策循环（每 1s，`FLOW_SCHEDULER_INTERVAL_MS`）

三个步骤，每步幂等，循环崩溃重启无恢复成本：

```
① 死亡清理
   UPDATE runs SET assigned_to = NULL, assigned_expires_at = NULL
   WHERE assigned_to IN (
       SELECT id FROM executors
       WHERE last_heartbeat_at < now() - $ttl)
     AND status IN ('running', 'awaiting_resume');
   -- 心跳超时 = 死亡。改派后的接手走 resume_run + classify，
   -- 与对等模式的租约过期接手完全同一条代码路径——
   -- DISTRIBUTED.md §2 的核心洞察在本模式仍然成立：
   -- 「指派过期接管 ≡ 进程崩溃重启」。

② 容量计算
   候选 executor = 心跳新鲜且 inflight < capacity 的集合

③ 指派
   无主 run（assigned_to IS NULL AND status IN running, awaiting_resume）
   ORDER BY priority DESC, started_at ASC
   逐个：校验 workflow 并发上限 → 挑 (capacity - inflight) 最大的 executor →
   UPDATE runs SET assigned_to = $e, assigned_expires_at = now() + $ttl
   WHERE id = $run_id AND assigned_to IS NULL
   -- 0 行 = 被并发指派抢了（多 scheduler 实例时），跳过即可
```

**executor 视角的认领**（与对等模式的扫描几乎相同，只是过滤条件从
「租约过期可抢」变成「指派给我」）：

```
每 2s：SELECT id FROM runs
       WHERE assigned_to = $me AND status IN ('running','awaiting_resume')
         AND id NOT IN (本进程已驱动的 run)
       → resume_run → RecoveryPlan::classify → 重放/裁决/续等
```

### 5.3 故障矩阵

| 故障 | 影响 | 恢复 |
|---|---|---|
| executor 死亡 | 其 run 无人驱动 | 心跳超时（≤TTL）→ scheduler 清理 → 改派 → classify 接手 |
| scheduler 死亡（有 standby） | 新 run 停在无主 ≤ 选主超时（≤10s） | advisory lock 过期 → standby 转正 |
| scheduler 死亡（无 standby） | 新 run 停在无主；**在跑的 run 完全不受影响**（executor 心跳续期不依赖 scheduler） | 拉起 scheduler 即恢复，零状态迁移 |
| Postgres 死亡 | 全停，零丢失 | DB 恢复即全量续跑（同对等模式） |

第三行是本设计最重要的安全性质：**scheduler 只在「指派时刻」起作用**，
不参与任何数据面（事件写入、信号、续期）。它的可用性预算与 gateway 同级，
远低于 executor。

## 6. scheduler 高可用：advisory lock 选主

不引入 Raft/etcd。scheduler 可以部署多个实例，同一时刻只有一个活跃：

```sql
-- 会话级咨询锁：持锁者即主。进程死亡连接断开，锁自动释放。
SELECT pg_try_advisory_lock(hashtext('flow-scheduler'));
```

- active 实例：持锁，跑 §5.2 循环；
- standby 实例：每 5s `try_lock` 一次，拿到即转正；
- 决策幂等性保证切换安全：重复指派被 `WHERE assigned_to IS NULL` 挡住，
  改派判定基于心跳超时（确定性输入），双主窗口内最坏情况是同一 run
  被先后指派给两个 executor——第二个 UPDATE 因 assigned_to 非空而失败，
  且防双写（§2）在事件层物理兜底。

选主代码量约 50 行。**要更强的 scheduler HA 之前，先问为什么 gateway 不需要**——
它们是同级的无状态协调者。

## 7. 与对等模式的共存与选择

两种模式共享 §2 的租约列与防双写不变量，差异只在「谁写 assigned_to」：

| | 对等模式（DISTRIBUTED.md） | 指派模式（本文） |
|---|---|---|
| 谁写 assigned_to | executor 抢占 | scheduler 指派 |
| 需要新进程 | 否 | 是（scheduler ×1 活跃） |
| 优先级/配额/工作流级限流 | 无 | 有 |
| run 等待指派的额外延迟 | 无（直接抢） | ≤ 心跳周期 + 决策周期（~3s） |
| 复杂度 | 低 | 中（多一个角色 + 一张表 + 选主） |

**共存**：指派模式的 executor 认领条件是 `assigned_to = $me`；
对等模式的 executor 抢占条件是 `assigned_expires_at IS NULL OR < now()`。
把抢占条件收紧为「`assigned_to IS NULL` 才可抢」，两种 executor 即可混跑——
scheduler 存在时它的指派优先落地，scheduler 缺席时对等抢占兜底。
小集群起步用对等，规模上来加装 scheduler，**不需要迁移任何数据**。

## 8. 分阶段实施

依赖 `DISTRIBUTED.md` 的 Phase 0（trait 化）与 Phase 1（Postgres + 租约）先行完成。

### Phase S1：指派内核

- `runs` 列更名（lease → assigned）+ `executors` 表 + 注册/心跳/认领；
- scheduler 进程骨架：§5.2 三步循环 + advisory lock 选主；
- **验收**：双 executor + 单 scheduler，SIGKILL 持有者 → 改派接手语义正确
  （复用对等模式 Phase 1 的三个集成测试，仅把「抢占」换成「被指派」）；
  SIGSTOP 双写防护测试结果必须与对等模式逐字一致。

### Phase S2：调度决策（scheduler 存在的理由落地）

- `priority`、`max_concurrent_runs` + `run.start` 传参；
- 指派审计：决策写入 tracing 日志（「run X → executor Y，因为 Z」）；
- **验收**：高优先级 run 插队语义；workflow 并发上限在多 executor
  集群下全局成立；杀 scheduler，在跑 run 零影响（§5.3 第三行）。

### Phase S3：混跑与运维

- 对者/指派混跑（§7 共存条件）；
- `scheduler.status` RPC：当前无主 run 数、各 executor 负载、最近决策；
- **验收**：摘掉 scheduler（standby 也杀），集群退化为对等模式继续服务。

## 9. 风险与开放问题

1. **决策循环的 DB 扫描成本**：无主 run 多时 ③ 的逐个校验变慢。
   起步规模（<1k 活跃 run、1s 周期）单轮 <10ms，够用；
   真正的瓶颈出现时把「workflow 并发 COUNT」缓存进循环内存，
   而不是给 scheduler 加状态。
2. **inflight 镜像漂移**：心跳上报间隔内的容量计算偏差 ≤ TTL/3。
   指派超载一个 run 是可接受的瞬时误差；若观察到持续漂移，
   检查 executor 是否在 finalize 路径漏报（bug），而不是加对账协议。
3. **改派的副作用语义**：scheduler 主动改派（非死亡接管）何时合法？
   默认**不做**主动改派——只有心跳超时才清空指派。运行时迁移（drain、
   版本升级滚动重启）走「executor 优雅退出 = finalize 或移交」，不依赖 scheduler。
   这条边界防止「调度器想帮忙结果砍掉正在执行的 http_call」。
4. **与单机 sqlite 模式无关**：本模式只在 postgres backend 下存在，
   `FLOW_BACKEND=sqlite` 集群行为不定义（单进程本来就没有调度问题）。
