# flow 调度者 + Executor 模式设计

> 状态：**设计稿，未实现**。依赖 DISTRIBUTED.md 的行锁、lease epoch、持久 inbox 与副作用边界。
> 本文新增持久容量预留与指派决策，不允许和对等抢占模式在同一集群混跑。

## 1. 需要解决的决策

只需要 HA 和多个 executor 时使用对等模式。需要以下决策时再引入 scheduler：

| 决策 | 本模式的规则 |
|---|---|
| 优先级 | 对当前可指派 run 按 priority DESC、started_at ASC、id 排序 |
| 工作流并发上限 | 同一 workflow 的未释放租约预留数不得超过上限 |
| executor 容量 | 未释放租约预留数不得超过 capacity，包含已指派未认领的 run |
| 调度可审计 | 记录 run、目标 instance、epoch、决策依据 |

这些规则可以在数据库中实现，不能声称“存在中心进程就天然满足全局约束”。
租户配额、加权公平和饥饿避免需要独立业务规则与数据模型，当前不承诺实现。
持续高优先级流量下低优先级任务可能等待；不得把严格优先级排序描述为公平队列。

## 2. 数据与所有权

保留 DISTRIBUTED.md 的 `lease_owner / lease_epoch / lease_expires_at / last_seq`，
不改名为另一组同义字段。scheduler 将 lease_owner 设置为 executor 的进程实例 id；
executor 只续期、追加与释放，不能自行获取无主 run。

**指派即持久容量预留**。容量真值是 runs 表内非空、非终态的租约数量，包含尚未认领的指派。
已过期的租约在明确持锁回收前仍占名额，不能仅凭时间条件从计数中排除。
心跳中的 inflight 只能作监控，不参与容量或配额的正确性判断。

每个 run 的写入/接管仍严格执行 DISTRIBUTED.md §5：同一行锁、持锁后检查时间、epoch fencing。
普通 EXISTS 条件不是隔离协议。终态事件、元数据和释放必须原子提交。

## 3. 架构

```text
client -> gateway -> Postgres <- scheduler (one active session)
                         ^
                         |
                    executor 1..N
```

所有组件通过数据库协调。scheduler 只负责授予租约与预留容量，不参与节点结果和信号投递。
调度器停止时，持有有效租约的 executor 可以继续续期和运行；新 run 与需要重新指派的 run 等待调度恢复。
不自动退回对等模式，因为那会绕过优先级和工作流上限。

## 4. 数据模型

```sql
CREATE TABLE cluster_settings (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    scheduling_mode TEXT NOT NULL CHECK (scheduling_mode IN ('peer', 'scheduled'))
);

CREATE TABLE executors (
    instance_id TEXT PRIMARY KEY,
    node_name TEXT NOT NULL,
    capacity INT NOT NULL CHECK (capacity > 0),
    accepting BOOLEAN NOT NULL DEFAULT TRUE,
    observed_inflight INT NOT NULL DEFAULT 0 CHECK (observed_inflight >= 0),
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE runs ADD COLUMN priority INT NOT NULL DEFAULT 0
    CHECK (priority BETWEEN 0 AND 9);
ALTER TABLE workflows ADD COLUMN max_concurrent_runs INT
    CHECK (max_concurrent_runs IS NULL OR max_concurrent_runs > 0);
CREATE INDEX runs_active_leases ON runs (lease_owner, lease_expires_at)
    WHERE status IN ('running', 'awaiting_resume');
CREATE INDEX workflow_active_leases ON runs (workflow_id, lease_expires_at)
    WHERE status IN ('running', 'awaiting_resume');
```

共享表内保存集群模式，不能只靠各进程的环境变量各自决定。启动时配置不匹配必须拒绝服务，
所有租约获取入口也须校验数据库模式，避免已有旧进程继续使用错误路径。
同 node_name 重启生成新 instance_id，不能通过 upsert 复活上一进程身份。

## 5. 指派协议

### 5.1 executor 生命周期

启动时注册新 instance_id，心跳更新活性与 observed_inflight。
已有 run 逐个按 DISTRIBUTED.md §5.2 续期，需 owner/epoch 匹配且租约尚未过期；
不能让过期指派在容量名额已被重新分配后通过迟到心跳恢复有效。

认领扫描只读取 owner=本实例、租约未过期、status 非终态的 run。
每个 instance 内按 run_id/epoch 防止重复 spawn；恢复期间也计入本地容量。
已经因 LeaseLost 停止的 epoch 不得在下一次扫描中重新启动。
executor 应先通过受保护续期确认指派仍有效，再恢复日志；所有后续追加继续校验 epoch。

正常 drain 先 accepting=false，继续续期直到已持有 run 结束。
需要强制移交时依 DISTRIBUTED.md §7 处理在途副作用，再停止本地 Driver、按原 epoch 释放。
没有未释放租约时才删除 executor 注册信息。心跳超时是活性怀疑，不等于进程死亡。

### 5.2 每次指派的事务

周期扫描只是选择候选；每个指派在同一连接的 READ COMMITTED 事务执行，锁序为：
**workflow 行 → 目标 executor 行 → run 行**。

1. 确认本连接仍是 scheduler 活跃会话，数据库模式为 scheduled。
2. 锁 workflow 与 executor 行，之后重新查询该 workflow 和该 instance 的未释放租约数量。
3. 检查 capacity、max_concurrent_runs、accepting 和心跳活性；容量不足则不指派。
4. 锁候选 run 行，再检查它属于该 workflow、非终态，且租约为空。过期租约先按 §5.3 回收。
5. 更新 owner=目标 instance、epoch=epoch+1、expires_at=clock_timestamp()+TTL，提交。

所有指派者、capacity 和 workflow 上限的修改者遵守相同前置锁序。
不得将“COUNT 后 UPDATE”拆成无锁的两个事务，或者只在内存里减去本轮已经指派的名额。
COUNT 在拿到控制行锁之后用新的语句执行，以看到之前指派事务已提交的结果。

持久预留的计数谓词为：

```sql
SELECT count(*) FROM runs
WHERE workflow_id = $1
  AND status IN ('running', 'awaiting_resume')
  AND lease_owner IS NOT NULL;
```

executor 计数把 workflow_id 条件替换为 lease_owner。相应的 workflow/executor 行锁必须已经持有。
该锁使所有增加同一受限集合的指派串行化；完成/释放只减少占用，续期不改变预留数。
不能用 expires_at > now 过滤计数：续期可能在到期前获准、到期后才提交；并发 COUNT 看见
过期的旧版本会提前释放名额，然后迟到续期提交就会超配。明确回收必须与续期锁同一 run 行。
目标 run 抢占失败时事务不新增预留，继续尝试其他候选。

容量和工作流上限限定的是**未释放预留数**，不是数据库外尚未结束的 HTTP 请求数。
旧持有者副作用仍可能在途，要求限制远端并发时必须由下游或额外执行隔离保证。
human_task 和 awaiting_resume 同样占用预留；等待节点休眠并释放配额不是当前设计的一部分。
降低上限不能撤销已有合法执行：若低于当前预留数，配置修改返回 conflict，需先 drain。

### 5.3 过期指派与选择策略

扫描同时检查无租约和已过期租约的 run，不能漏掉过期但 owner 非空的行。
过期回收事务按 workflow → 原 executor → run 的顺序加锁，重新检查 owner/epoch 及
clock_timestamp 下的过期状态；仍过期则 epoch+1 并清空租约，提交后释放名额。
若等待 run 锁期间续期已经提交，必须读到新值并放弃回收。回收后再走 §5.2 新指派。
executor 心跳超时后停止向它指派新 run；现有租约仍以各自的 expires_at 为接管依据，
不靠批量清空 owner 提前撤销有效租约。

按 priority、started_at、id 排序，跳过当前受 workflow 上限阻塞的候选。
选择具有持久剩余容量的活跃 executor，在指派事务内重新验证容量。
一次指派即使 executor 尚未轮询到，也已经占用名额；调度器重启后直接从数据库恢复计数。

## 6. scheduler 高可用

使用专用数据库连接持有会话级 advisory lock：

```sql
SELECT pg_try_advisory_lock(74102, 1);
```

固定的两个 int 是本服务保留的命名空间和锁号。活跃者仅在这条**相同连接**执行决策事务，
不能持锁连接独立存活，却用连接池里的其他连接继续指派。连接中断、状态不明或重新连接时，
立即停止决策；新连接先重新获得锁，才能恢复。
standby 每隔一段时间尝试获取，成功后读取持久状态继续。

会话锁**没有 TTL**。正常连接关闭后锁释放，网络分区下需等待 PostgreSQL 检测会话失效。
必须配置并验证 TCP keepalive、连接检测以及 statement/idle transaction 超时；
没有这些测量结果时不承诺“10 秒内切换”。不能把 scheduler 与 gateway 的可用性要求混为一谈：
scheduler 暂停会阻止新指派，gateway 可以多副本独立接请求。

即使部署错误产生并行决策，§5.2 的控制行锁仍保护容量与 workflow 上限，run 行锁和 epoch
仍保护日志顺序。`owner IS NULL` 本身既不能保护全局配额，也不能替代选主连接规则。

| 故障 | 行为 |
|---|---|
| executor 退出或失联 | 不再给其新指派；租约到期后按容量和副作用约束重新指派 |
| scheduler 退出，有 standby | 等旧会话锁实际释放后接管；无硬编码切换时间保证 |
| scheduler 退出，无 standby | 有效持有者继续；新指派和过期接管等待 scheduler 恢复 |
| PostgreSQL 不可用 | 停止新派发；已有外部请求仍可能完成，恢复后核对持久日志 |

## 7. 模式选择与迁移

| 模式 | 谁获取新租约 | 优先级/工作流并发上限 | scheduler 故障 |
|---|---|---|---|
| peer | executor | 不提供 | 无此角色 |
| scheduled | scheduler | 按本文事务协议提供 | 暂停新指派，不自动降级 |

不支持同一集群混跑或热切换。数据结构共用，不等于两套授权规则可以同时使用。
从 peer 切到 scheduled 时停止接收新 run、等待所有 run 终结或按副作用规则完成处置、
停掉旧 executor，确认没有有效租约后更新 cluster_settings，再启动新角色。
反向迁移同样要求排空，并明确告知优先级/配额保证将不再提供。
只摘掉 scheduler 不构成迁移，不允许 executor 自行抢占绕过规则。

## 8. 实施与验收

前置：DISTRIBUTED.md 的共享日志、epoch fencing、信号原子消费及副作用约束先完成。

### Phase S1：持久指派

实现 executor 注册、单一集群模式、指派与认领、专用连接选主、固定锁序与持久预留计数。
所有新任务与故障接管共用这条授权路径；不建立与租约重复的 assigned_* 字段。

验收必须包含：

1. capacity=1，暂停 executor 认领并延迟多个心跳周期，连续调度不能产生两个有效指派。
2. 指派后立刻重启 scheduler，已指派未认领的 run 仍占容量。
3. 两个并发指派事务竞争同一 executor/workflow，结果仍满足上限。
4. 过期心跳不能复活旧 epoch；executor 重启使用新 instance，不能续旧身份的租约。
5. 续期在到期前获准、到期后延迟提交时，旧预留持续占用；回收等待该事务并重新校验。

### Phase S2：优先级与工作流约束

增加 priority 和 max_concurrent_runs 的管理入口、决策审计及状态查询。
验证高优先级排序、受限 workflow 不阻塞其他可执行候选、上限修改与指派的并发行为。
在多个 executor 上验证 workflow 上限，不用单进程计数替代全局断言。

### Phase S3：故障与运维

测试活跃 scheduler 的连接失效、网络隔离、长事务、SIGSTOP 与 SIGKILL；
同时停止 scheduler 和持有者时任务应保持可恢复，scheduler 恢复后能接管过期非空 owner。
杀 scheduler 不影响已有有效持有者的续期，peer 入口在 scheduled 模式下始终被拒绝。
按部署实际测量决策延迟、故障切换时间和数据库负载，再定义运行指标。

## 9. 暂不实施

多租户公平/配额、自动抢占低优先级 run、运行时迁移、混跑自动降级，以及根据上报 inflight
直接决定容量均不在本版协议内。未来新增策略不能绕开持久预留和所有权事务。
