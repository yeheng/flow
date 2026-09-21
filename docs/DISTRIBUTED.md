# flow 分布式部署设计（多节点执行）

> 状态：**对等抢占模式（peer）已实现**（Phase 0 后端边界 + Phase 1 共享日志与接管
> + Phase 2 容量与查询，见 `crates/flow-pg` 与 `flow-backend` 的 Postgres 适配器
> （`PgBackend`；上层 RPC 经 `Backend` trait 屏蔽后端差异））；
> SCHEDULER.md 的中心指派模式仍未实现。验收覆盖情况见 §11。
> 依赖 DESIGN.md 的单机恢复契约。依赖本文定义对等抢占模式；
> SCHEDULER.md 定义中心指派模式。一个集群只启用一种模式。
> 事件模型、fold 和 DAG 语义已复用；租约、持久信号与副作用边界已按本文协议实现。

## 1. 目标与边界

- 高可用：执行进程失效后，其他进程取得新租约并恢复未完成 run。
- 横向扩展：不同 run 分散到不同 executor，同一 run 的日志始终按顺序追加。
- Postgres 是唯一协调点，不引入节点间直连、gossip 或自建共识服务。
- 单机 SQLite 保持独立后端；不允许多个进程共享单机数据目录驱动 run。
- 保留 AND-join、跳过传播、独立分支完成后再报告 fatal 的现有语义。
- 租约保证日志写入顺序，不保证任意远端 HTTP 的副作用去重，边界见 §7。

吞吐、故障切换时间和数据库容量必须通过实测确定，不承诺随节点数线性增长。

## 2. 数据所有权

| 数据 | 权威来源 | 规则 |
|---|---|---|
| 工作流定义 | 不可变 workflow_versions | run 固定某个 published 版本 |
| 执行事件 | run_events | 持有正确租约代次才能追加 |
| 执行状态 | 按 seq 折叠事件 | runs.status/output/error 为查询投影 |
| 当前所有权 | runs 的 lease 列 | 协调状态，不能从旧执行日志替代恢复 |
| 待处理外部输入 | run_signals | 持久 inbox，消费与事件提交同事务 |

租约过期表示所有权可以转移，**不表示旧进程已停止**。
接管必须先排除旧持有者尚未提交的日志事务，再读完整的已提交事件。
只有建立这个边界，才能复用单机 fold 和恢复分类；也不能遗漏等待重试及已记录的裁决。

## 3. 架构与入口

```text
client -> gateway -> Postgres <- executor 1..N
                     runs
                     run_events
                     run_signals
                     workflows / workflow_versions
```

小部署可以在同一个进程里运行 gateway 和 executor。gateway 负责元数据和持久输入，
不绕过容量控制直接 spawn Driver。executor 获取租约后驱动节点，registry 只是本进程缓存。
中心指派模式的 executor 获取入口由 scheduler 替代，详见 SCHEDULER.md。

`run.start` 在**一个事务**内校验 published 版本、插入 run 和 seq=1 的 RunStarted，
提交后才确认创建成功。此时 run.status=running、lease 为空、last_seq=1，表示已入队。
这里的 running 不保证已获得执行容量。Postgres 后端不存在“已插 run 但没有首事件”的合法状态；
单机后端仍按 DESIGN.md 的 initializing 协议工作。

## 4. 数据模型

以下为相对于现有 runs 表的 Postgres 结构变更；不是 SQLite 迁移脚本。

```sql
ALTER TABLE runs ADD COLUMN lease_owner TEXT;
ALTER TABLE runs ADD COLUMN lease_epoch BIGINT NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN lease_expires_at TIMESTAMPTZ;
ALTER TABLE runs ADD COLUMN last_seq BIGINT NOT NULL DEFAULT 0;
ALTER TABLE runs ADD CONSTRAINT lease_pair CHECK (
    (lease_owner IS NULL) = (lease_expires_at IS NULL)
);

CREATE TABLE run_events (
    run_id TEXT NOT NULL REFERENCES runs(id),
    seq BIGINT NOT NULL CHECK (seq > 0),
    ts TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, seq)
);

CREATE TABLE run_signals (
    run_id TEXT NOT NULL REFERENCES runs(id),
    signal_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('signal', 'cancel')),
    node_id TEXT,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'applied', 'rejected')),
    event_seq BIGINT,
    error JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, signal_id),
    CHECK ((kind = 'signal' AND node_id IS NOT NULL)
        OR (kind = 'cancel' AND node_id IS NULL))
);
CREATE INDEX pending_run_signals ON run_signals (run_id, created_at, signal_id)
    WHERE status = 'pending';
```

`payload` 是 Event，不重复存储 Envelope 的 run_id/seq/ts；读取时由列构造 Envelope。
主键只保证 seq 唯一，连续性由持锁事务内的 last_seq 分配和事件 INSERT 共同保证，
读取仍校验序列。初次写入及迁移必须使 last_seq 与日志末尾一致。

`lease_owner` 使用每次进程启动新生成的 instance UUID；运维节点名可以重复，实例标识不可复用。
每次获取或重新指派都递增 lease_epoch，即使目标 executor 恰好相同。
清空租约不能重置 epoch。终态事件、元数据投影、租约释放在同一事务提交。

## 5. 所有权协议

### 5.1 共同规则

隔离级别为 READ COMMITTED。所有涉及同一 run 的获取、续期、追加、释放、信号消费，
都先锁住同一个 runs 行，锁持有至提交；获取与追加不能分别只写两张互不互斥的表。
锁顺序固定为 runs 行在前、该 run 的信号行在后；调度容量锁的前置顺序见 SCHEDULER.md。

过期检查在取得行锁后使用数据库 `clock_timestamp()`。`now()` 是事务开始时间，
不能用它判断等待行锁后租约是否仍有效。锁等待、语句执行和 idle transaction 必须有超时，
否则暂停的客户端可以长期阻止接管。TTL 是活性参数，不是强制抢走数据库锁的时限。

### 5.2 获取、续期和释放

获取事务：

1. `SELECT ... FROM runs WHERE id = $1 FOR UPDATE`。
2. 检查 status 为 running/awaiting_resume，且租约为空或已过期；检查当前集群模式允许此入口。
3. 写入新 instance UUID，epoch+1，expires_at=clock_timestamp()+TTL；提交并返回 epoch。
4. **提交后**读取事件并折叠。恢复过程中再失去租约，则下一次受保护操作失败，不得继续派发。

续期事务同样先锁 run 行，只允许 owner、epoch 都匹配且当前未过期的持有者延长 TTL。
已经过期的租约不能靠迟到心跳复活；必须重新获取、递增 epoch 并重新恢复。
任何不匹配都视为 LeaseLost，停止本地派发、取消本地任务并丢弃未提交结果。

正常释放先停止派发、处理本地未决工作，再在持锁事务内校验 owner/epoch 和有效期，
清空 owner/expires_at。崩溃或已经失效时不再尝试清空新持有者的租约。
尚有不明 HTTP 的主动移交必须满足 §7，不能仅靠取消本地 future 声称外部工作已停止。

### 5.3 受保护的事件追加

下面的参数为示意绑定：$1=run_id、$2=instance、$3=epoch、$4=Event JSON。
必须在同一连接和事务执行；锁获取后再执行第二条语句：

```sql
BEGIN;
SELECT id FROM runs WHERE id = $1 FOR UPDATE;

WITH owned AS (
    UPDATE runs
    SET last_seq = last_seq + 1
    WHERE id = $1
      AND status IN ('running', 'awaiting_resume')
      AND lease_owner = $2
      AND lease_epoch = $3
      AND lease_expires_at > clock_timestamp()
    RETURNING id, last_seq
)
INSERT INTO run_events (run_id, seq, ts, payload)
SELECT id, last_seq, clock_timestamp(), $4::jsonb FROM owned
RETURNING seq;
COMMIT;
```

没有返回行时调用方应 ROLLBACK 并返回 LeaseLost，不确认 append。
插入失败则 last_seq 的更新一起回滚。commit 结果不明时不得发送新副作用，
停止当前 Driver，通过重新获取租约和读日志确认提交结果。

有效期在持锁后的准入点检查。若事务获准后跨过 TTL，接管事务仍会等待其提交或回滚；
旧事务若成功提交，接管者读日志时必须已能看到它。不能承诺“墙钟过期后绝无旧事务提交”，
可以保证“新持有者完成接管后的恢复读，不会再出现旧代次迟到的事件”。
单条 `INSERT ... SELECT ... WHERE EXISTS` 的普通快照读不提供这个保证。

terminal append 在该事务中同步写 status/output/error/ended_at、清空租约，并将剩余 pending
信号标记 rejected，避免终态 run 留下永不处理的输入。非终态投影更新也须匹配 owner/epoch。
现有单机 `RunObserver` 的无条件 UPDATE 不能直接用于 Postgres；LeaseLost 不写 RunFailed，
也不能在失去所有权后回写 failed/awaiting_resume。

### 5.4 扫描与容量

对等模式 executor 只扫描 running/awaiting_resume 且无租约或租约过期的 run。
扫描结果是候选，不是授权；逐个执行 §5.2，失败则跳过。
本进程用容量许可覆盖“获取中 + 正在恢复 + 已驱动”的 run，释放许可必须跟随 Driver 退出，
防止多个扫描循环重复消费空闲额度。所有新 run 也走这个入口。
默认不提供优先级或工作流全局限流；需要它们时选择中心指派模式。

## 6. 持久信号和取消

### 6.1 入队与确认

Postgres 模式的信号请求带稳定的 signal_id，客户端重试必须复用同一个 id。
gateway 在锁 run 行的事务中检查 run 未终结，插入 inbox；相同 id、相同内容返回原结果，
相同 id、不同 kind/node/payload 返回 conflict。若原请求已处理，即使 run 已终结也可查询原结果。

入队提交只代表 accepted，不能返回 `delivered=true`。gateway 等待该行变为 applied/rejected，
applied 才返回 delivered，rejected 返回已有 invalid/conflict 错误体系。
等待超时返回明确的 pending 结果和 signal_id；客户端用新增 `run.signal_status` 查询，
不能把 pending 当交付成功。此响应扩展需要在 Postgres RPC 能力协商中声明，SQLite 接口不变。

### 6.2 原子消费

持有者按以下顺序消费，每一步在同一事务/连接内：

1. 锁 runs 行，核对 owner/epoch/有效期；确保内存 fold 的 last_seq 与已提交日志一致。
2. 锁一条 pending 输入，按 created_at、signal_id 排序。
3. 校验节点是否等待、裁决是否有效。非法请求只标 rejected，不写 RunFailed。
4. 有效 signal 分配 seq 并插入 SignalReceived，同时设置 inbox.status=applied、event_seq。
5. 提交后才更新内存并解除等待、执行裁决。提交失败则两项都回滚。

消费崩溃前事务回滚，信号仍 pending；提交后崩溃则从 SignalReceived 恢复。
恢复必须消费 human_task 与 HTTP 裁决两类已记录信号，且下一次 NodeStarted 清除 last_signal。
重复消费通过 inbox 状态和 signal_id 被拒绝，不能把重复信号当 run 的致命错误。

cancel 同样作为 inbox 命令，但消费事务直接写 RunCancelled、终态投影、租约释放，
并处理其余 pending 输入；提交后 abort 本地任务。取消不解决已发送 HTTP 的副作用歧义。
gateway 不得通过直接清租约或只改 status 实现取消。

## 7. 外部副作用与恢复

成功接管后复用 DESIGN.md §7 的状态分类：待执行、Running、待重试、已记录的信号、
不可重试失败都必须处理。delay 仍按单机约定重放全时长，重试退避计算剩余时间。

日志 fencing 不能阻止旧进程发网络请求。例如 A 写完 NodeStarted 后暂停，B 接管并获准
重试，A 恢复后仍能发出旧 POST，尽管它下一次追加会失败。发送前再查租约仍有检查与执行竞态。

必须按调用能力明确支持范围：

- 下游支持幂等键：同一逻辑操作的重试与接管复用稳定 key；key 不能随着 lease_epoch 或 attempt 改变。
  调用参数也必须稳定，不能在重试模板里生成新的随机业务请求。实现时须提供稳定执行上下文或持久请求快照。
- 下游支持 fencing：下游原子拒绝旧代次，并对同一逻辑操作去重；不能只把 token 放在无人校验的 header 中。
- 下游不支持上述能力：接管后保持人工介入。重新执行前必须确认旧执行进程不会再发请求，
  并核对在途请求的最终业务结果；无法证明时保持等待，或由操作者明确承担重复风险。

该前提同样适用于已经记录的 retry 裁决和自动 HTTP 重试。分布式实现必须在执行恢复分类之前
增加副作用准入检查，不能直接照搬“收到 retry 就发送”的单机路径。当前没有通用的自动恰好一次保证。

## 8. 查询与订阅

`run.timeline`/`run.events` 从共享日志读取。订阅者按 run_id 维护 last_seq 游标拉取增量，
LISTEN/NOTIFY（频道 `flow_events`，载荷仅 run_id）仅作低延迟唤醒提示；正确性不依赖
通知——通知可丢失（断连期间），由 subscribe_poll 兜底轮询兜住；进程接管不能改变
订阅源的正确性。等待子 run 终态（child.rs）复用同一通知频道加兜底轮询。
from_seq 的 API 语义保留现有闭区间，客户端传 last_seq+1。
增量读取在 SQL 层下推过滤（`WHERE seq >= from`），只校验相邻 seq 连续；
全量读取（接管恢复、`from_events`）仍要求首条为 1 + 相邻连续。
候选查询 `watch_runs` 是 LIMIT 256 的有界查询：活跃 run 超过 256 时
按 started_at 取最早的，最新 run 可能延迟若干轮询周期才进入订阅候选。
只在确认已转发后推进游标；重复消息按 seq 去重，发现缺口后通过 run.events 补齐。
全局订阅必须为各 run 分别跟踪游标，不能将不同 run 的 seq 当成一个全局序列。

## 9. 时钟与持久性

seq 决定事件顺序；ts 用于展示和退避剩余时间计算，不决定哪个节点已经完成。
租约过期使用取得行锁后的 DB clock_timestamp。DB 墙钟跳变影响接管时延，行锁与 epoch
仍负责日志隔离；时钟同步和监控属于部署要求。

事件事务要求 `synchronous_commit=on`。不能将已确认日志的丢失解释为可忽略的残缺尾行：
NodeStarted 丢失可能让已经发生的副作用被再次执行。Postgres 故障切换的持久性还依赖复制策略；
只有部署明确提供的 RPO 才能成为数据保留承诺，异步副本切换不能宣称零丢失。

## 10. 部署配置

规划配置：FLOW_BACKEND、FLOW_DATABASE_URL、FLOW_NODE_ID（展示名）、启动时生成的 instance UUID、
FLOW_LEASE_TTL_MS、FLOW_MAX_RUNS、FLOW_SCAN_INTERVAL_MS、FLOW_SUBSCRIBE_POLL_MS
（订阅兜底轮询间隔，默认 10s；NOTIFY 是正常路径）。
TTL/续期/轮询间隔在实施阶段依据数据库延迟实测设定。长事务必须另设锁和语句超时。

可拆分 gateway 与 executor 池。Postgres 模式下 FLOW_DATA_DIR 仅是缓存；备份必须覆盖
Postgres 定义、事件、inbox 和协调表。数据库不可用时停止新的派发，已有 HTTP 仍可能完成。

## 11. 实施与验收

### Phase 0：后端边界

只有开始实现第二后端时才抽取最小日志与元数据接口。接口需表达受保护提交、信号消费和
LeaseLost，不能只把文件 append 改名为 trait。单机日志格式与现有测试保持兼容。

### Phase 1：共享日志与接管（已实现，`crates/flow-pg`）

实现原子创建、epoch 租约、持锁 append/终态、inbox、单一对等模式入口。
`flow-engine` 抽取了 `RunEventSink` 后端边界（受保护提交、信号原子消费、LeaseLost），
Driver 在单机/Postgres 两种后端上共用同一份调度与恢复语义。
必须验证（现状：1/2/3/4/6 由 `flow-pg/tests/{protocol,recovery}.rs`
与 `flow-rpc/tests/ws_pg.rs` 钉住；5 以确定性前缀事件验证副作用准入
（接管后不自动重放、裁决后恰好一次请求），真实的跨进程 SIGSTOP 副作用计数
尚未在验收中落地）：

1. 在租约校验后、事件提交前暂停 A，B 接管必须等待；提交后 B 必须读到该事件。
2. B 接管完成后 A 的追加、续期、投影更新、释放都失败；同节点名重启也不能复用旧权利。
3. SIGKILL 后，纯节点、待重试、human_task、已记录裁决和 fatal 的结果与单机契约一致。
4. inbox 入队、写事件、标记消费、解除等待各断点中断，已确认信号不丢且只生效一次。
5. NodeStarted 后、发 HTTP 前 SIGSTOP；B 接管后依 §7 的能力/人工条件处理，再恢复 A，核对真实副作用次数。
6. run 创建事务与终态事务各断点中断，不产生孤立首事件、无首事件的可执行 run 或旧 epoch 回写。

### Phase 2：容量与查询（已实现基础版）

容量许可（FLOW_MAX_RUNS）、扫描间隔（FLOW_SCAN_INTERVAL_MS）、订阅轮询
（FLOW_SUBSCRIBE_POLL_MS，按 run_id 维护 last_seq 从头追平）均已实现并有测试。
多节点压测与吞吐/DB 瓶颈测量**未做**，不以“近似均匀”代替每个 run 的正确性验收。

### Phase 3：有数据支持的优化

再评估批量事件提交和 delay 剩余时间恢复（LISTEN/NOTIFY 唤醒已实现，见 §8）。
任何优化仍须保持先持久化 NodeStarted 再发副作用，不允许通过放弃已确认事件持久性换吞吐。
