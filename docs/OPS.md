# flow 运维手册

> 状态：**当前代码实际语义**（2026-10-23）。设计与语义见 [DESIGN.md](DESIGN.md)；
> 本文只讲**怎么部署、怎么跑、坏了怎么办**。业务接口、回归和证据资产工具见 [EXECUTION.md](EXECUTION.md)。
>
> 本文所有数字都能在代码里对上。凡代码里没有的能力，本文一律标注「未实现」，
> 不按路线图描述。

> 2026-10-16 单机保证以 [ADR 0002](adr/0002-sqlite-persistence.md)
> 为基线；当前不提供日志复制或 master 高可用。

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
wss_addr = null                   # 代理终止 TLS 场景；与 [[execution.remote.agents]] 成对
# [[execution.remote.agents]]      # token → agent_id 映射表（每 agent 一张凭证，不共享）
# id = "agent-01"
# token = "…"                      # id/token 在表内都不得重复（token→身份必须一对一）
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

```

**`deny_unknown_fields`**：配置里出现已删除的键（如 `[server].rpc_addr`、
`FLOW_ADDR`）会**显式报错**，不会静默忽略。存量配置升级时必须先清理。

cluster 注册中心已移除。升级时删除整个 `[journal.cluster]` 段、部署参数
`--features journal-cluster` 及 `FLOW_CLUSTER_DATABASE_URL` 环境变量。旧 RPC
`journal.cluster.status/nodes/remove` 返回 `-32601`（method not found），前端不再提供集群页面。
现有 `flow.sqlite` 继续原样使用，无需格式迁移；旧 PG 注册数据保留作归档，由运维另行清理。
本次升级不会连接或删除旧 PG 表。远程 agent 的配置与连接方式保持不变。

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
| 存量配置包含 `[journal.cluster]` | “unknown field cluster”；删除整个配置段后重启 |
| `mode=remote` 但缺 `[execution.remote]` | `execution.mode=remote 时必须配置 [execution.remote]` |
| `mode=in_process` | `execution.mode=in_process 已删除：本地执行只有 ipc` |
| `x_max` 越界 | `execution.x_max 必须在 1..=16` |
| WSS 只配了一半 | `wss_addr 与 agents 必须同时配置` |
| 凭据表重复 id/token | `execution.remote.agents 重复的 agent id：…` / `…重复的 token…` |

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
- WSS 场景（代理终止 TLS）：`--wss-url wss://... --token <本 agent 的令牌>`；
  master 侧按 `[[execution.remote.agents]]` 的 token→agent_id 映射表反查身份，
  自报的 `x-flow-agent-id` 头只做一致性断言（不匹配即 401）。

**优先级**：`CLI flag > env > flow.toml [agent] > 报错`。
`--data-addr` 已废弃——上联自 v4 单流化起就是单条 TLS 连接（v5 延续），该参数被
忽略（不报错，只为存量脚本兼容）。所谓「单流」是指控制语义与 data 语义的帧复用
同一条物理连接，靠接收侧的帧预算分类区分，**不是**有两条连接。

---

## 4. 关机与重启

### 4.1 优雅停止

停止协调循环并等待执行任务退出，接续 owner 已持有的提交，关闭写入线程，释放成员资格，
最后关闭只读查询连接。没有独立 projector，不需要等异步可见性。

发送 SIGTERM/SIGINT 并等待退出。SIGKILL 后 SQLite 恢复已提交事务；正常重启不依赖
应用 checkpoint 或完整历史重放。子进程生命周期及未知外部操作仍按执行协议处理。

### 4.2 重启

取得 flock，打开 flow.sqlite/WAL，加载控制元数据与非终态执行，播种 dirty runs，
提交新 master epoch 后继续安全工作。已授权而结果未知的操作停在 awaiting_resume，
必须明确裁决，不能借重启重新发送。

## 5. 离线维护（flow-journal-tool）

CLI 先取数据目录排他锁，须在服务停止时运行：

```sh
flow-journal-tool verify <data_dir>
flow-journal-tool rebuild-index <data_dir>
flow-journal-tool backup <data_dir> <new_destination>
flow-journal-dev --data-dir <data_dir> check-projection
```

- verify：SQLite integrity/foreign-key 检查、事实与当前实体摘要、已发布正文闭环检查。
- rebuild-index：SQLite 原生 REINDEX；不创建应用位置索引。
- backup：一致快照包含当前状态、事实、回执与正文；校验完成后发布独立主文件。
  目标必须不存在。不要在线只复制 flow.sqlite；活跃 WAL 也是提交数据的一部分。
- check-projection：停写后用 reducer 从 LSN 1 重放全部事实，把应得的
  `current_entities` 与库中当前行逐行比较；报告 body/`last_lsn`/缺失/多余行。
  它只读事实、不就地修复；不一致时保留原库作为证据。

未发布值不能被业务引用；值连续性、总字节和摘要只在写入时校验一次。
不提供就地 rebuild-projection 或日志前缀 repair。损坏数据保留证据，从经过 verify 的
一致备份恢复；不能把不完整事实前缀当成当前执行状态。

### 5.1 文件职责

| 路径 | 职责 |
| --- | --- |
| flow.sqlite、活跃 WAL | 唯一业务权威，不可当缓存删除 |
| SQLite shm | SQLite 自行管理，不在运行中手工操作 |
| observations/ | 独立观测日志，删除后不可恢复，不影响业务状态 |

不生成 journal/checkpoints/indexes、execution-index.sqlite 或 projection.sqlite。

---

## 6. 故障处置

### 6.1 症状 → 原因 → 处置

| 症状 | 原因 | 处置 |
| --- | --- | --- |
| 起不来：`data directory is locked` | 同目录已有写者（或上次没退干净） | `ps` 找进程；确认无存活进程后重启。**不要**手动删锁文件 |
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

### 6.3 数据库损坏

停服务、保留原库和 WAL，运行 `flow-journal-tool verify <data_dir>` 定位结构、事实或正文错误。
从校验通过的一致备份恢复到独立目录，再启动服务。不截断原库，不自动重新发送未知副作用。

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

| 项 | 边界 |
| --- | --- |
| 业务数据库 | flow.sqlite，WAL + synchronous=FULL |
| 接受事务编码预算 | ≤1 MiB |
| inline 阈值 | ≤64 KiB |
| BLOB 分块 | ≤256 KiB/块，单值≤256 MiB |
| 当前实体 | 覆盖更新，不保存永久整态版本 |
| 历史/正文/回执 | 本次保留全部，不引入 TTL 或 GC |
| observations | 独立的容量与条数限额 |

命令大输入和定义先进入匿名临时文件，暂存总预算 16 MiB、单值最多 8 MiB。writer 逐块读取，值块、发布与最终命令共享一次 COMMIT；拒绝或重复请求不会发布暂存值，取消／崩溃会释放未提交的暂存文件。历史事实与外部调用捕获证据继续完整保留。

容量测量分开统计当前实体、事实、BLOB、索引、WAL 和空闲页。长读和大值会影响 WAL
回收与备份成本。checkpoint 是 SQLite WAL 维护，不再产生应用状态快照。

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

### 9.1 SQLite 存储格式

本地业务库为 flow.sqlite，格式版本 4。旧 JSONL/派生库目录不自动转换。SQLite v3 使用 `flow-journal-tool upgrade OLD NEW` 离线复制到新目录，源目录不修改；升级后执行 `check-projection` 再切换数据目录。完整结构与恢复契约见 [ADR 0002](adr/0002-sqlite-persistence.md)。

### 9.2 协议版本

`PROTOCOL_VERSION = 7`（postcard 单流帧：`u32 body_len | postcard(Message)`）。
**master 与 executor/agent 必须同版本**——执行器握手 `Hello`/`Welcome` 与
agent 上联握手 `AgentHello`/`AgentWelcome` 的 `version` 字段不符直接失败，
不协商、不降级、不逐帧校验（版本门集中在握手）。postcard 的变体索引是
位置编码，没有版本门的跨版本配对会静默错解成另一个消息类型。

改 `flow-engine/src/execution_protocol/contract.rs` 的任何常量都构成协议变更，
必须 bump `PROTOCOL_VERSION`。升级 = **同批替换全部二进制**
（`flow-journal-server` + `flow-executor` + `flow-agent`）。
v7 的执行上下文和 regression 隔离要求同批替换全部二进制，旧执行器/agent 在握手处拒绝。

### 9.3 升级步骤

```sh
# 1. 停服务（等优雅退出，见 §4.1）
# 2. 备份
flow-journal-tool backup <dir> <backup>
# 3. 替换全部二进制（同目录！）
# 4. 校验
flow-journal-tool verify <dir>
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
flow-journal-tool verify <dir>
flow-journal-tool backup <dir> <dst>
flow-journal-tool rebuild-index <dir>
flow-journal-dev --data-dir <dir> check-projection

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

## Harness Agent（内置 agent 模式）部署与安全边界

### 分发

harness agent 模式**内联在 flow-executor 内执行**（conga-host 无头装配，
无独立 runner 二进制）：分发面与 command 模式完全一致，无需额外二进制。
executor 依赖 conga 固定 rev（见 `crates/flow-executor/Cargo.toml`），rev
变更需全量回归 agent e2e；master/executor 仍按 `RUNTIME_ID` 锁步升级。

### 配置与密钥

- 模型配置在 `[harness.agent]`（base_url/model 成对启用；api/proxy/限额可选）。
  **run 启动时冻结进执行快照**：重启后改配置只影响新 run；回归候选继承
  baseline 的冻结配置。Settings 页保存后需重启生效。
- API key 只以密钥名出现在节点参数（`api_key`，`x-secret`）；真值来自密钥
  管理体系（stored 优先、`FLOW_SECRET_*` 兜底），经授权许可（内存）以**参数**
  传入 conga-host 无头装配。**任何环境变量都不携带 key**：workdir 的
  `.conga/hooks.json` 钩子、bash 工具的子进程都读不到。
- `CONGA_CONFIG_DIR`/`CONGA_PROJECT_DIR` 由无头装配的 RAII 钉根守卫钉进
  本次 run 的临时根（任务结束恢复）：settings.json 在 flow 路径上**永不被读**
  （stream_fn 注入使加载分支不可达），workdir 的 `.env`/`.conga/config.toml`
  不被自动装配。执行器串行处理任务，钉根期间无并发读者。

### 生命周期与资源预算

- agent 节点使用一次性 executor，结果提交后也回收整个会话/Job；普通节点继续复用执行器。
- `timeout_ms` 从取得许可开始覆盖初始化、运行、清理和 outcome 落账；取消/超时保留不确定结果，不自动重放。
  独立系统线程在截止/取消后给予 5 秒宽限（检查粒度 250ms），到期退出 executor。父端另有不受心跳延长的截止时间，触发有界取消排空和会话级 kill/wait/reap。
- agent 并发至多 `ceil(派发上限 / 2)`。`x_max=4` 时至多 2 个；`x_max=1` 只能串行。
  远程执行也受全局 16 个派发上限约束，增加 worker 不会自动提高该上限。
- Linux/macOS 每 250ms 采样会话 RSS 与进程数，预算 1 GiB / 64 个进程；越界或采集失败时退出。
  RSS 累加包含共享页，属于保守预算；采样不能阻止瞬时分配。Unix 工具继承单文件 256 MiB 硬限制。
- CPU 总量、总磁盘、总模型费用以及主动脱离会话的 daemon，需由部署级容器、文件系统配额及供应商额度约束。
  Windows 等其他平台本轮只提供生命周期看门狗，不能套用 Linux/macOS 的 RSS/PID 保证。

### 环境与保留

- HOME、XDG、CARGO_HOME 继承执行器环境白名单，**不指向临时根**。工具可能读取 git/ssh/registry 等配置；
  需要隔离时用独立 worker 用户与受限容器。CONGA_CONFIG_DIR/CONGA_PROJECT_DIR 单独钉定到本次操作临时根。
- FullAuto 允许读写、联网和 shell；随机 workdir 是临时工作目录，不是 OS 安全沙箱。
- 正常路径尽力删除临时根；强制退出可能留有 `flow-harness-*` 目录。只在确认相关 executor 已回收后清理这些目录。
- 工作流每个对象在协调器常驻最新版本与最高已发布版本；旧版本按需从 SQLite 读取。
  软删除保留对象和历史，已启动 run 的冻结定义与回执仍有效。
- 当前事实、正文及幂等回执不自动 TTL/GC。制定磁盘与备份预算，使用 `flow-journal-tool backup` 做一致备份，
  用 `verify`/独立目录恢复演练核对。归档删除前必须证明活动运行、冻结子流程、审计和回执的引用闭包完整，不能直接删旧 BLOB 或事实。

### 可重复的 agent 混合负载验收

```sh
cargo build --locked --offline --release -p flow-executor -p backend-perf --bin flow-executor --bin flow-capacity
./target/release/flow-capacity agent-soak /tmp/flow-agent-soak /tmp/flow-agent-soak.json 180 ./target/release/flow-executor
```

使用全新目录。负载包含普通脚本、日志、HTTP、delay、人工任务、取消及内联 agent 的两轮模型/真实 bash 工具调用。
provider 为本地确定性 mock，不消耗真实模型费用。JSON 写明环境、时长、提交/成功/取消/失败数、provider 调用数、延迟、进程树资源、重启与持久化诊断。
默认最多 4 个并发 run、每 200ms 尝试提交一次；这是可复现的回归场景，不是饱和容量或生产 SLO。
生产发布还需在目标平台按实际模型响应、工作目录体量、并发和持续时间复测。
