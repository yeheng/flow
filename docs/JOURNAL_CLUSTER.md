# Journal 多节点注册与管理

每个节点独立保存 journal。JSONL 继续保存工作流、run、executor 编排与审计的
权威事实；PG 是节点注册、journal 归属、期望状态、实例租约及上报进度的权威。
默认构建无需 PG。共享业务库与跨节点 run 编排方案仍是另一份设计提案。

## 启用

```sh
cargo build -p flow-rpc --bin flow-journal-server --features journal-cluster --locked
# 完整执行工具打包：
./scripts/build-journal.sh --features flow-rpc/journal-cluster
```

各节点使用不同数据目录与 node_id，共用 PG 数据库和 cluster_id。各自使用现有
`ipc` 或 `remote` 执行模式（`in_process` 仅非 unix 过渡态；T7 起 unix 上已删除）。
配置示例：

```toml
[journal]
addr = "0.0.0.0:9802"
http_addr = "0.0.0.0:9803"
data_dir = "/var/lib/flow/journal-a"

[journal.cluster]
cluster_id = "production"
node_id = "journal-a"
database_url_env = "FLOW_CLUSTER_DATABASE_URL"
rpc_url = "wss://journal-a.example.com/rpc"
http_url = "https://journal-a.example.com"
heartbeat_ms = 5000
lease_ms = 30000
```

```sh
export FLOW_CLUSTER_DATABASE_URL='postgres://flow:password@pg-host/flow'
export FLOW_JOURNAL_TOKEN='replace-with-a-long-random-token-at-least-32-bytes'
target/debug/flow-journal-server --config /path/to/flow.toml
```

DSN 只从指定环境变量读取，不出现在 config.get 中。PG 用户需要访问和创建
`flow_journal_cluster_*` 表的权限；启动在事务级 advisory lock 下创建 v1 元数据表。
数据库只向受信任服务节点开放；生产配置 PG、RPC 和 HTTP 的 TLS。

广告地址未提供时使用实际监听地址，支持端口 0。监听通配地址时必须提供可访问的
rpc_url/http_url。管理页只显示地址，不转发 token。配置存在但未编译 feature 时
启动失败。桌面内嵌服务拒绝此配置；管理页在浏览器连接该服务端时使用。

## 注册与同步

- `(cluster_id,node_id)` 同时只允许一个有效实例。每次注册生成 incarnation_id，
  epoch 加一，活跃租约期间重复启动失败。journal ID 与 node ID 永久一一绑定，
  不能给旧节点替换空目录，也不能克隆同一 journal 到另一个节点。
- PG 时钟判断租约。一条 UPDATE 原子更新租约、健康状态、revision 和快照，
  匹配 incarnation_id、epoch、enabled 与未过期租约。旧、过期或剔除实例不能续租。
- 快照包含 durable_lsn、applied_lsn（内存）、projected_lsn（SQLite）、master_epoch、
  run 状态计数和故障信息。进度和 master_epoch 不可倒退，内存/投影游标不超过
  durable_lsn；大整数传十进制字符串。启动先重建落后的投影，再注册。
- 状态为 online、degraded、offline、stopped、removed。offline 根据 PG 查询时间
  与租约计算，保留最后快照。正常关闭尽力上报最终快照并释放为 stopped；崩溃或
  PG 不可达时由租约过期显示 offline。重启需等旧租约过期，并使用原 journal。
- 注册/心跳请求开始时记录 monotonic 时间，成功后保守更新本地写入截止时间。
  过期 gate 不可复活。一次同步失败或超时就撤销 gate、取消 executor 编排并停止
  RPC/HTTP，不能自动降级为单机运行。SQL 与连接池获取超时受 heartbeat_ms 限制。
- 写线程在入队、append 前及 fsync 后检查 gate。PG 与磁盘不是原子事务：刚持久化
  但未返回成功的命令需重启后用幂等键确认。已发出的外部副作用可能完成，不能
  宣称 exactly-once，也不能在其他节点自动重试这些 run。

## 管理 API 与页面

沿用节点 token 认证；读接口无需 request_id。

| 方法 | 参数 | 结果 |
| --- | --- | --- |
| journal.cluster.status | 无 | enabled；启用时含 cluster_id、node_id |
| journal.cluster.nodes | after（node_id）、limit（上限 256） | PG server_time、节点快照、next_after |
| journal.cluster.remove | node_id、expected_epoch（字符串）、request_id | removed、node_id、revision |

剔除命令在 PG 存请求与回执，跨 API 节点重试同一 request_id 返回同一结果；同键
不同参数冲突。expected_epoch 防止旧页面剔除新实例，不使用心跳变化的 revision。
成功提交永久 removed 并撤销租约；目标节点下次心跳失败后停机。journal/node 绑定与
tombstone 不自动清理，不能重新注册被剔除身份。

`/cluster` 展示节点健康、归属、实例/epoch、地址、三个进度、精确落后量与 PG 心跳
时间，每 5 秒刷新当前页。失败保留旧数据并提示。剔除有确认、未终结 run 提示与
保留命令 ID 的超时重试。未配置时显示说明。

这次不包含日志复制、跨节点业务路由、run 迁移或自动故障接管。业务页面显示当前
节点数据；相同 cron 在多个 journal 会各自执行。不能用轮询负载均衡假定业务对象
共享，不能自动转移节点目录；跨节点调度和 executor HA 属于后续工作。

## 验证

隔离数据库设置 `FLOW_CLUSTER_TEST_DATABASE_URL` 后运行：

```sh
cargo test -p flow-journal --locked
cargo test -p flow-backend --features journal-cluster --test journal_cluster --locked -- --ignored
cargo test -p flow-rpc --features journal-cluster --test journal_cluster --locked -- --ignored
cargo clippy -p flow-rpc -p flow-backend --all-targets --features journal-cluster --locked -- -D warnings
cd web
npm test
npm run lint
npm run build
```

PG 测试明确 ignored，缺少数据库不能误报已验证。CI 的 journal-cluster 任务用真实
PG 显式执行，覆盖并发注册、永久归属、旧实例/命令、精确进度、回退拒绝、两个产品
进程、远程剔除、心跳超时与停机。
