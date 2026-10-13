# flow 运维手册

> 状态：**当前代码实际语义**（2026-10-13）。设计与语义见 [DESIGN.md](DESIGN.md)；
> 本文只讲**怎么部署、怎么跑、坏了怎么办**。
>
> 本文所有数字都能在代码里对上。凡代码里没有的能力，本文一律标注「未实现」，
> 不按路线图描述。

---

## 1. 部署形态

### 1.1 二进制清单

`scripts/build-journal.sh` 产出 `dist/journal/`：

```
dist/journal/
  run-server.sh          # 入口脚本
  bin/
    flow-journal-server  # 产品服务（WS RPC + cron + webhook + 下载）
    flow-executor        # 受管理执行子进程（被 server/agent 召唤）
    flow-agent           # 远程执行中继
    flow-cli             # 命令行客户端（纯 RPC）
    flow-journal-tool    # journal 离线维护
    flow-journal-bench   # 写入基准
    flow-journal-dev     # 开发运行器
```

**`flow-executor` 必须与主进程同目录**。定位规则
（`ExecutorInvocation::locate`，`flow-engine/src/execution_protocol/contract.rs`）：

```
FLOW_EXECUTOR_BIN 显式路径（最高优先）
  → current_exe() 同目录下的 flow-executor
  → 找不到即启动失败（"cannot locate flow-executor next to ..."）
```

**不静默回退**。缺执行器时 server 明确起不来，不会退化成进程内执行——
进程内执行模式已全平台删除。

### 1.2 三种部署形态

#### A. 单机（默认，`mode = "ipc"`）

```
一台机器：
  flow-journal-server ──spawn──> flow-executor ×x_max
```

- 权威（journal 写者）与计算（执行器）同机，但**不同进程**。
- 承载：unix 走 socketpair；Windows 及其余平台走 TCP loopback 回连。

#### B. 计算扩容（`mode = "remote"`）

```
master 机：
  flow-journal-server ──mTLS :9700 / WSS──> flow-agent ×N
flow-agent 机（N 台）：
  flow-agent ──spawn──> flow-executor ×slots
```

- **只有算力横向扩展**。权威仍在 master 单点，提交点只有一份。
- 每台 agent 需要：证书（CN 必须等于 `agent_id`）、CA、`slots` 配置。

#### C. 桌面内嵌（Tauri）

- `web/src-tauri` 在 app 进程内直接调用同一个 RPC 模块，不起独立 server。
- 默认开启 `execution.prewarm = true`（省首次派发的进程启动 + 握手延迟）；
  服务端默认关闭。
- `prewarm` 会占一个 `x_max` 槽位；失败自动降级为懒加载。

---

## 2. 配置

### 2.1 分层加载（高覆盖低）

```
CLI --config（显式路径）  >  环境变量  >  flow.toml  >  内置默认值
```

配置文件搜索顺序：`./flow.toml` → 平台配置目录 `flow/flow.toml`。
显式路径用 `FLOW_CONFIG` 指定。

**加载必须在构造 tokio runtime 之前同步完成**——配置错误要尽早失败。

### 2.2 完整配置面

```toml
[server]
scheduler_enabled = true          # cron 调度器开关
journal_trigger_tick_secs = 20    # 触发器扫描周期（必须 > 0）

[storage]
backend = "journal"               # 唯一值；sqlite/postgres 已删除
data_dir = "data"                 # 密钥存储 + 桌面历史数据

[execution]
mode = "ipc"                      # ipc | remote；in_process 已删除
executor_bin = null               # 缺省定位同目录 flow-executor
x_max = 4                         # 1..=16，既是池容量也是 A_MAX 上限
prewarm = false                   # 启动时预热一个空闲执行器

[execution.remote]                # mode=remote 时必填
control_addr = "master:9700"      # v5 起唯一上联地址（单流，无第二条 data 连接）
ca_cert = "..."                   # 三者都不能为空
cert = "..."
key = "..."
attach_timeout_ms = 120000
wss_addr = null                   # 代理终止 TLS 场景；与 agent_token 成对
agent_token = null
# data_addr 已废弃：值被忽略，仅为存量配置兼容保留
#   （上联自 v4 单流化起只有一条连接，v5 延续；不存在独立的 data 通道）

[agent]                           # flow-agent 侧
agent_id = null                   # 须与客户端证书 CN 一致
control_addr = null
ca_cert = null
cert = null
key = null
executor_bin = null
slots = 4                         # 必须 > 0
wss_url = null                    # 优先于 mTLS 直连
token = null

[journal]
addr = "127.0.0.1:9802"           # WS RPC
http_addr = "127.0.0.1:9803"      # 下载 + webhook（Bearer 认证）
data_dir = null                   # journal 权威目录

[journal.cluster]                 # 需要 --features journal-cluster
cluster_id = ""
node_id = ""
database_url_env = "FLOW_CLUSTER_DATABASE_URL"
rpc_url = null
http_url = null
heartbeat_ms = 5000               # >= 100
lease_ms = 30000                  # >= 3 × heartbeat_ms，且 <= 300000
```

**`deny_unknown_fields`**：配置里出现已删除的键（如 `[server].rpc_addr`、
`FLOW_ADDR`）会**显式报错**，不会静默忽略。存量配置升级时必须先清理。

### 2.3 环境变量

| 变量 | 必填 | 说明 |
| --- | --- | --- |
| `FLOW_JOURNAL_TOKEN` | **是** | 同一个 token 值，**两种承载方式**（见下），**≥ 32 字节** |
| `FLOW_JOURNAL_DATA_DIR` | 是 | journal 权威目录（覆盖 `[journal].data_dir`） |
| `FLOW_JOURNAL_ADDR` | 否 | 覆盖 `[journal].addr` |
| `FLOW_JOURNAL_HTTP_ADDR` | 否 | 覆盖 `[journal].http_addr` |
| `FLOW_CONFIG` | 否 | 显式配置文件路径 |
| `FLOW_DATA_DIR` | 否 | 密钥/桌面数据目录 |
| `FLOW_SCHEDULER` | 否 | `off` 关闭 cron 调度器 |
| `FLOW_EXECUTOR_BIN` | 否 | 显式执行器路径 |
| `FLOW_EXECUTION_MODE` | 否 | `ipc` 覆盖默认；`in_process` 显式设置即报错 |
| `FLOW_SECRET_<名称>` | 否 | 密钥（如 `FLOW_SECRET_RESEND_KEY`），只走 env 不进配置 |
| `RUST_LOG` | 否 | env-filter；缺省 `info,flow_engine=debug,flow_rpc=debug,flow_backend=debug` |

**token 是单用户凭据，不是多租户授权**（DESIGN §C7）。拿到同一个 token 的不同
调用方拥有完全相同的权限。多租户必须在外层自备。

### 2.4 启动

```sh
# 标准方式
export FLOW_JOURNAL_TOKEN="$(openssl rand -base64 48)"   # ≥32 字节
export FLOW_JOURNAL_DATA_DIR=/var/lib/flow/journal
dist/journal/run-server.sh
```

`run-server.sh` 的默认值：

| 变量 | 默认 |
| --- | --- |
| `FLOW_DATA_DIR` | `$DIR/secrets` |
| `FLOW_JOURNAL_DATA_DIR` | `$DIR/journal` |
| `FLOW_JOURNAL_ADDR` | `127.0.0.1:9802` |
| `FLOW_JOURNAL_HTTP_ADDR` | `127.0.0.1:9803` |

**就绪标记**：启动行带 `local_addr=` / `http_addr=` 字段。测试 harness 用它判断
端口已绑（`flow_test_support::io::spawn_reporting_ports`），**不要改这两个字段名**。

**启动失败是明确失败，不是降级**：

| 现象 | 报错 |
| --- | --- |
| token < 32 字节 | `FLOW_JOURNAL_TOKEN must contain at least 32 bytes` |
| data_dir 未配置 | `journal 数据目录缺失` |
| 同目录已有写者 | `data directory is locked` |
| 找不到 flow-executor | `cannot locate flow-executor next to ...` |
| 配置了 cluster 但没开 feature | `journal.cluster requires a build with --features journal-cluster` |
| `mode=remote` 但缺 `[execution.remote]` | `execution.mode=remote 时必须配置 [execution.remote]` |
| `mode=in_process` | `execution.mode=in_process 已删除：本地执行只有 ipc` |
| `x_max` 越界 | `execution.x_max 必须在 1..=16` |
| WSS 只配了一半 | `wss_addr 与 agent_token 必须同时配置` |

---

## 3. 启动 agent（remote 模式）

```sh
flow-agent \
  --control-addr master.example.com:9700 \
  --agent-id agent-01 \
  --ca /etc/flow/ca.pem \
  --cert /etc/flow/agent-01.pem \
  --key /etc/flow/agent-01.key \
  --slots 8
```

- **证书 CN 必须与 `--agent-id` 一致**，否则握手被拒。
- agent 侧能力目录必须是 `[relay, resource_report, reconnect]` 的超集。
- `flow-executor` 同样要在 agent 同目录（或用 `--executor-bin` / `FLOW_EXECUTOR_BIN`）。
- WSS 场景（代理终止 TLS）：`--wss-url wss://... --token <共享令牌>`，
  此时 `agent_id` 与 token 一起做显式认证映射。

**优先级**：`CLI flag > env > flow.toml [agent] > 报错`。
`--data-addr` 已废弃——上联自 v4 单流化起就是单条 TLS 连接（v5 延续），该参数被
忽略（不报错，只为存量脚本兼容）。所谓「单流」是指控制语义与 data 语义的帧复用
同一条物理连接，靠接收侧的帧预算分类区分，**不是**有两条连接。

---

## 4. 关机与重启

### 4.1 优雅停止的顺序是load-bearing

`JournalBackend::close()` 的顺序**不能改**（`flow-backend/src/journal.rs`）：

```
1. shutdown.cancel()          # 停协调循环
2. await + take execution     # 等在飞派发
3. projector_stop.cancel()    # 停投影器
4. await + take projector
5. checkpoint()               # 写 checkpoint（失败只 warn，不阻塞退出）
6. journal.close()            # 关写者、刷段
7. cluster close              # 释放 PG 成员资格
8. projection.close()
```

**为什么第 3 步必须在第 5 步之前**：在飞的 `command()` / `run_cancel()` 可能阻塞在
`visible()` 里等投影追平。如果先写 checkpoint 再停投影器，每个等待者都会烧完
30 秒超时，`close()` 看起来像挂死。

**实务**：发 `SIGTERM`/`SIGINT`，等它自己退。**不要 `SIGKILL`**——虽然 journal 是
崩溃安全的（`SIGKILL` 后能恢复），但 `SIGKILL` 会跳过 checkpoint，下次启动要全量
重放，且执行器子进程会变孤儿。

### 4.2 重启后自动发生什么

1. `flock(data_dir)` 抢到排他锁 → 成为唯一写者；
2. 读 checkpoint + 全量重放到 `durable_lsn`；
3. `dirty_runs` 播种**全部非终态 run**；
4. 投影器从 `projected_lsn` 续跑，追平后才对外可见（`visible()` 阻塞到追平）；
5. 协调循环从折叠态继续推进。

**运维不需要做任何事**。`awaiting_resume` 的 run 会停在原处等人工裁决——
这是设计行为，不是卡住。

---

## 5. 离线维护（`flow-journal-tool`）

**所有子命令都先取排他锁**（`offline_lock` = 对 root 目录 `try_lock_exclusive`），
所以**必须在服务停止时运行**。

```sh
# 全量字节校验 + 已发布值闭环检查
flow-journal-tool verify --data-dir /var/lib/flow/journal

# 重建位置索引（一次性派生缓存，可随时删除）
flow-journal-tool rebuild-index --data-dir /var/lib/flow/journal

# 备份可恢复前缀到新目录
flow-journal-tool backup --data-dir <源> --destination <目标>

# 打印可恢复前缀；--confirm 才真正写入新目录
flow-journal-tool repair --data-dir <源> --destination <目标> [--confirm]
```

| 命令 | 语义 |
| --- | --- |
| `verify` | 全量扫描 + 每个已发布值以 `require_published=true` 重新读回 |
| `rebuild-index` | 重建 `indexes/locations.jsonl`（纯派生缓存） |
| `backup` | 复制**已验证**前缀，并在目的地**重新校验**一遍 |
| `repair` | **永不修改原文件**；只把可恢复前缀写进新目录 |

### 5.1 重建投影

投影损坏/丢失时（DESIGN §3.6：只有两张通用表，删了能重建）：

```sh
# 离线重建进新的 SQLite 文件（不执行任何工作流）
flow-journal-dev rebuild-projection --destination /tmp/projection-new.sqlite
```

`rebuild_projection` 持 `offline_lock`，拒绝损坏的 journal，建新文件（`create_new`，
权限 0600）并 fsync 父目录。

### 5.2 可以随时删除的派生数据

| 路径 | 删除后果 |
| --- | --- |
| `projection.sqlite` | 重启后从 journal 重建；期间读不可用 |
| `indexes/` | 重启后重建 |
| `observations/` | **节点日志永久丢失**，run 状态不受影响 |

**不可删**：`journal/`、`checkpoints/`。这两个是权威。

---

## 6. 故障处置

### 6.1 症状 → 原因 → 处置

| 症状 | 原因 | 处置 |
| --- | --- | --- |
| 起不来：`data directory is locked` | 同目录已有写者（或上次没退干净） | `ps` 找进程；确认无存活进程后重启。**不要**手动删锁文件 |
| 起不来：`projection cursor is ahead of authoritative journal` | journal 被回退/替换过 | **不要绕过**。确认哪个 journal 是新的；从备份恢复 |
| 起不来：`state checkpoint boundary mismatch` | checkpoint 与段不一致 | 会自动 warn `invalid checkpoint; replaying journal` 并全量重放；若仍失败则 journal 损坏，走 §6.3 |
| RPC 返回 `-32020 COMMITTED_NOT_VISIBLE` | 投影暂时落后 | 客户端重试。**偶发正常，持续出现说明投影器卡住**（查 `projection_error`） |
| 写全部返回 `-32021 limit` | 在飞 run 数达 `R_MAX = 1000` | 等 run 终态；不要调大（这是主进程状态内存约束） |
| 日志刷 `coordinator stalled: journal writer failed` | journal 写入**永久**失败 | 磁盘满/权限/IO 错误。**系统已停摆**，修磁盘后重启 |
| run 卡在 `awaiting_resume` | 外部操作结果不明 | **设计行为**。去外部系统查明，用 `run.adjudicate` 明确告诉引擎 |
| run 卡在 `running` 且某节点 `waiting{signal}` | 人工节点在等人 | 设计行为。`run.signal` 交付 |
| agent 连不上 | 证书 CN 与 `agent_id` 不符 / 网络 | 看握手日志；`STARTUP_TIMEOUT_MS = 10s` 内没 Welcome 即失败 |
| 远端派发全部 `LinkClosed` | 上联断 | agent 会自动重连 + Resume 对账；查对账裁决结果 |

### 6.2 `awaiting_resume` 处置流程

这是**唯一需要人工介入的状态**（DESIGN §C4）。步骤：

1. 找到 run 与节点：`run.status` / 节点 `operation.operation_id`；
2. **去外部系统查明真相**——那个 HTTP 请求到底发出去没有、结果是什么；
3. 按真相裁决：

```sh
# 情况 A：外部操作其实成功了，手工补录结果
flow-cli run adjudicate --run-id <id> --node-id <n> \
  --decision accept_output --output '<真实结果 JSON>' --reason '...'

# 情况 B：确认没产生副作用，重试
flow-cli run adjudicate --run-id <id> --node-id <n> \
  --decision retry --reason '确认未发送'

# 情况 C：确认失败，放弃该分支
flow-cli run adjudicate --run-id <id> --node-id <n> \
  --decision failed --reason '上游返回 500，已人工确认'
```

- `reason` ≤ 4096 字节；`decision` ∈ `accept_output | retry | failed`。
- `retry` 会把节点打回 `Pending`，协调循环以 `attempt + 1` 重新派发。
- **裁决前务必真的查过外部系统**。`retry` 是你在用运维权限覆盖「不自动重发」
  这条铁律——覆盖错了就是重复扣款。

### 6.3 journal 损坏

```
flow-journal-tool verify --data-dir <dir>          # 定位坏在哪
flow-journal-tool repair --data-dir <dir> --destination <new> --confirm
```

- `verify` 全量扫描 + 每个已发布值重新读回闭环；
- `repair` **永不改原文件**，只把可恢复前缀写进新目录；
- 然后用新目录起服务，或者从 `backup` 恢复。

**半条写入的行**（`sha256` 不匹配）是预期会遇到的——崩溃正好发生在写一半时。
系统不会猜内容，会明确报错。

---

## 7. 容量预算

### 7.1 内存

| 项 | 约束 | 依据 |
| --- | --- | --- |
| 在飞 run 数 | `R_MAX = 1000` | 主进程状态内存约束（**不是**协议量） |
| 在飞派发数 | `A_MAX = 16` | 协议活跃派发上界 |
| 执行器进程数 | `x_max`（1..=16，默认 4） | 进程数 × RSS |
| 单执行器 JS heap | 32 MiB | QuickJS 上限 |
| 单执行器输入面 | `INPUT_BUDGET = 8 MiB` | 传输聚合硬限 |
| 全局持久窗口 | `A_MAX × W_BYTES = 32 MiB` | 审计在途字节 |
| 单条审计记录 | `MAX_AUDIT_RECORD_BYTES = 512 KiB` | 必须 ≤ `W_BYTES` |
| 定义缓存 | `DEFINITION_CACHE_MAX = 128` | 终态 run 的定义条目随终态逐出 |

**`A_MAX` 不得用来论证主进程状态内存，`X_MAX` 不得用来论证审计吞吐。**
这两组量约束的是完全不同的事物（DESIGN §contract.rs 头部注释）。

### 7.2 磁盘

| 项 | 值 |
| --- | --- |
| 段文件 | 256 MiB 轮转 |
| 单行 | ≤ 1 MiB（`MAX_LINE_BYTES`） |
| inline 阈值 | ≤ 64 KiB（`INLINE_BYTES`） |
| 分块 | 256 KiB（`CHUNK_BYTES`） |
| 单值上限 | 256 MiB（`MAX_VALUE_BYTES`） |
| checkpoint | ≤ 16 MiB |
| 观测日志行 | ≤ 16 KiB（单行截断 8 KiB）；默认 10000 行/节点，硬上限 100000 |

**规划公式**：`journal 增长 ≈ Σ(节点输出 + 输入面 + 审计) × 事件膨胀`。
审计流量通常远大于业务值——每条 `AuditBatch` 都占一条 `audit_seq`。压测用
`flow-journal-bench` / `crates/backend-perf/` 实测，不要估算。

### 7.3 网络（remote 模式）

| 项 | 值 |
| --- | --- |
| 控制帧 | ≤ 128 KiB（`CONTROL_MAX_FRAME`） |
| data 语义帧 | ≤ 1 MiB（`DATA_MAX_FRAME`） |
| 传输块 | 256 KiB（`TRANSFER_CHUNK_BYTES`） |
| agent 每执行器队列 | 8 帧（`PER_EXECUTOR_QUEUE`） |
| agent 上联出队 | 256 帧 / 8 MiB（`UPLINK_OUT_QUEUE` / `UPLINK_OUT_BYTES`） |
| 公平轮转 | 64 KiB 或 4 帧/轮（`FAIR_TURN_BYTES` / `FAIR_TURN_FRAMES`） |
| agent 连接数上限 | **无常量**；并发由两道闸约束：全局 `Semaphore(A_MAX=16)` + 每 agent `slots_total` |
| 操作请求传输 | 8 MiB / 最多 16 个并发传输（`OPERATION_TRANSFER_LIMIT` / `_COUNT`） |
| 执行器输入聚合 | 8 MiB（`INPUT_BUDGET`） |

「单流」不意味着只有一种预算：控制语义与 data 语义的帧跑在同一条物理连接上，
由接收侧按消息类型分别按 128 KiB / 1 MiB 复核，超限即拒绝。

---

## 8. 安全边界

### 8.1 已做的

- 数据目录 0700、文件 0600；符号链接一律拒绝。
- 单写者靠 `flock` 排他（OS 层，不靠应用层约定）。
- 同一个 token，**两种承载方式，别混用**：
  - **WS RPC**（`journal_v2`）：token 是 params 对象里的 `_token` **字段**，
    不是 HTTP 头。`admit_call` 对**每个**方法（含读面与订阅）都校验，通过后把
    `_token` 从 params 里删掉再分发。
  - **HTTP**（下载 `/runs/{id}/values/{oid}`、webhook `POST /hooks/{id}`）：
    走 `Authorization: Bearer <token>` 头，大小写敏感的 `"Bearer "` 前缀。
  - 两种都用 `constant_time_eq` 比较（先比长度再 XOR 累加，无提前退出）。
- webhook 端点 Bearer + `Idempotency-Key`（≤128 字节）、body ≤ 8 MiB。
- 密钥参数只存名称，真值走 `FLOW_SECRET_<名称>` env，**永不落 journal 明文**；
  凭据只在签发 `OperationPermit` 时解析，不进授权事实。
- JS 沙箱无文件系统/网络/`require`。
- 客户端 token 缺失/不符 → `-32001 unauthorized`（订阅是 reject，HTTP 是 401）。

### 8.2 没做的（不要假设有）

| 项 | 状态 |
| --- | --- |
| 多租户授权 / 用户体系 | **未实现**。token 是单用户凭据 |
| RPC 传输加密（TLS on WS） | **未实现**。缺省只听 loopback |
| 速率限制 / 防重放 | **未实现**（`request_id` 幂等不是限流） |
| 审计日志（谁在什么时候调了什么） | **未实现**。`Command` 事件只记产品命令，不含调用方身份 |

**对外暴露必须自备外层**：TLS 终止 + 认证网关。`journal.addr` / `http_addr`
绑到非 loopback 地址时，流量是明文的。

---

## 9. 升级

### 9.1 v1 → v2 已完成

2026-10-10 起 JSONL v2 是唯一后端。**历史数据不做迁移，已舍弃**。
`StorageBackend` enum 只剩 `Journal` 一个变体，`sqlite`/`postgres` 已删除。

升级路径：**新起一个空的 journal 目录**，重新发布定义。不要试图搬旧数据。

旧数据保全（只读，可选）：

```sh
flow-journal-dev import-legacy --source <v1 数据目录> --database <v1 sqlite>
```

产出是一个只读 baseline 目录（`LegacyImport` 事件标记），供追溯查询，不参与执行。

### 9.2 协议版本

`PROTOCOL_VERSION = 5`（v5 postcard 单流帧：`u32 body_len | postcard(Message)`）。
**master 与 executor 必须同版本**——握手 `Hello`/`Welcome` 的 `version` 字段不符
直接失败，不协商、不降级、不逐帧校验（v5 起版本门集中在握手）。

改 `flow-engine/src/execution_protocol/contract.rs` 的任何常量都构成协议变更，
必须 bump `PROTOCOL_VERSION`。升级 = **同批替换全部二进制**
（`flow-journal-server` + `flow-executor` + `flow-agent`）。

> **在途状态（2026-10-13）**：工作区正在做 IPC8（v4 → v5）。该目录当前有未提交
> 改动且 `cargo build -p flow-engine` 不过（错误全在 `execution_protocol/`）；
> pristine HEAD 可编译且是 v4。**在 IPC8 落地前不要跨版本混布二进制**——v4 与 v5
> 的帧格式不兼容（JSON 信封 vs postcard），握手层会拒绝，但那之前连接可能已因
> 帧解析失败而中断。

### 9.3 升级步骤

```sh
# 1. 停服务（等优雅退出，见 §4.1）
# 2. 备份
flow-journal-tool backup --data-dir <dir> --destination <backup>
# 3. 替换全部二进制（同目录！）
# 4. 校验
flow-journal-tool verify --data-dir <dir>
# 5. 起服务，看启动行的 local_addr=/http_addr=
# 6. 确认没有 run 卡在意外状态
flow-cli run list --status awaiting_resume
```

---

## 10. 常用命令速查

```sh
# 服务
dist/journal/run-server.sh

# 维护（服务停止时）
flow-journal-tool verify --data-dir <dir>
flow-journal-tool backup --data-dir <dir> --destination <dst>
flow-journal-tool repair --data-dir <dir> --destination <dst> --confirm
flow-journal-tool rebuild-index --data-dir <dir>
flow-journal-dev rebuild-projection --destination <new.sqlite>

# 开发
flow-journal-dev run --definition wf.json --input in.json
flow-journal-dev resume          # 只按已提交事实恢复安全的活
flow-journal-dev status          # 打印恢复态，不启动执行

# agent
flow-agent --control-addr master:9700 --agent-id a1 --ca ca.pem --cert a1.pem --key a1.key --slots 8

# 基准
flow-journal-bench
```

---

## 11. 事故检查单

按顺序过一遍，多数「引擎坏了」其实是这三类之一。

```
□ 1. 服务起来了吗？
     - 没有 local_addr= 启动行 → 起失败，错误信息就是原因（§2.4 表）
     - data directory is locked → 有残留进程

□ 2. 投影追平了吗？
     - RPC 间歇 -32020 → 投影落后，重试
     - 持续 -32020 → 投影器卡死，查 projection_error / 磁盘

□ 3. 写者健康吗？
     - 日志有 "coordinator stalled: journal writer failed" → 磁盘问题，已停摆

□ 4. run 停在哪？
     - awaiting_resume → 需要人工裁决（§6.2），不是故障
     - running + 节点 waiting{signal} → 人工节点在等，不是故障
     - running + 节点 waiting{delay|retry} → 定时未到，查 wake_at
     - running + 节点 waiting{child} → 子 run 未终态
     - running 且无等待 → 协调循环问题，看 coordinator_ticks 是否在涨

□ 5. remote 模式额外看：
     - agent 连上了吗（AgentWelcome 收到了吗）
     - CapacityReport 在更新吗（slots_total 在变吗）
     - 有 Resume 对账的 ReconcileRequired 吗（那需要人工核对）
```

**`coordinator_ticks` 与 `coordinator_last_tick_ms` 是最有用的两个诊断字段**：
前者不涨 = 协调循环死了；后者不动 = 循环卡住。二者都可以从诊断接口读到。
