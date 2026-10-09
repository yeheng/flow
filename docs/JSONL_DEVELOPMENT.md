# JSONL 一期开发入口

此路径用于开发与独立部署验证；一期非断电验收已通过，真实断电按用户要求排除。不要将旧 SQLite 数据目录直接交给新入口。v2 已是缺省后端（journal 唯一权威；v1 sqlite 后端已删除）：`flow-server` 直接以 v2 服务全部 v1 RPC 面（映射层 `flow-backend/src/journal_arm.rs`，切换记录见 [SQLITE_V1_TO_V2_MIGRATION.md](SQLITE_V1_TO_V2_MIGRATION.md)）。

## 二期 IPC 执行模式（本地子进程）

二期将进程内节点执行替换为受管理的本地执行子进程（模板/script/condition/HTTP 全部在执行器子进程（`flow-executor`）内执行；journal 语义与进程内模式逐字节同构）。

```sh
cargo build --workspace                # 产品二进制：flow-server / flow-executor / flow-agent / flow-cli / flow-journal-*
FLOW_EXECUTION_MODE=ipc cargo run -p flow-backend --bin flow-journal-dev -- --data-dir ./target/v2-ipc run --definition ./workflow.json --input ./input.json
# 执行器默认由主进程在自身同目录召唤 flow-executor（部署形态）；开发时
# target/ 下两二进制同目录，locate() 同样命中；独立部署用
# FLOW_EXECUTOR_BIN=/path/to/flow-executor 覆盖。
FLOW_EXECUTION_MODE=ipc cargo run -p flow-rpc --bin flow-journal-server
```

- 默认 `in_process`（一期行为）；`ipc` 模式二进制缺失/版本不兼容直接失败，不静默回退。
- **本机 release 构建**：若遇 `dlopen ... mis-aligned LINKEDIT string pool`（Xcode 21 ld 对部分
  proc-macro dylib 产生未对齐符号表，机器级 bug），用
  `scripts/release.sh cargo <command>` 包装（改用 rustc 自带 rust-lld + 26.5
  SDK）；默认构建不变。
- 执行器定位：`FLOW_EXECUTOR_BIN` 显式路径 → 当前可执行文件同目录的兄弟 `flow-executor`。
- 常量（X_max 默认 4/A_max 16/W 2 MiB/帧上限等）见 `flow_engine::execution_protocol::contract`。
- 同数据集单写者；一个 run 只绑定一种执行模式；master epoch 随 IPC 启动持久提交。
- 取消/强杀执行器：缺口与不确定副作用按一期规则保留（uncertain 等待/Integrity 标记），重启不重发已授权操作。

## 三期远程执行模式（agent 中继）

主进程增加远程执行端口（`FLOW_EXECUTION_MODE=remote`）：每台执行机运行
`flow-agent`（mTLS 上联，CN=agent_id），agent 管理本机执行器（`flow-executor`）并
有界公平中继；审计/确认仍端到端来自主进程 journal（agent 不产生权威
ACK/Permit）。断线后 agent 保留执行器并退避重连，Resume 清单由主进程按
日志裁决（AlreadyCommitted/UploadOnly/SubmitExistingResult/CancelAndDrain/
ReconcileRequired）；drain 用于升级下线。部署/证书/边界见 `docs/AGENT_OPS.md`。
测试：`cargo test -p flow-backend --test journal_remote --test journal_remote_tls --test journal_remote_reconnect --test journal_remote_ops`。

本地运行一个定义：

```sh
cargo run -p flow-backend --bin flow-journal-dev -- --data-dir ./target/v2-data run --definition ./workflow.json --input ./input.json
cargo run -p flow-backend --bin flow-journal-dev -- --data-dir ./target/v2-data resume
cargo run -p flow-backend --bin flow-journal-dev -- --data-dir ./target/v2-data status
```

`status` 只归约日志，不运行节点。`resume` 只继续可安全恢复的工作，已授权但缺少 Outcome 的操作进入 uncertain 等待。脚本/条件节点的纯计算重试保留原准备输入和绝对退避时间；不会自动重发未知外部操作。

开发 RPC 使用独立二进制：设置 `FLOW_JOURNAL_DATA_DIR`、至少 32 字节的随机 `FLOW_JOURNAL_TOKEN`，执行 `cargo run -p flow-rpc --bin flow-journal-server`。默认地址 `127.0.0.1:9802`，可通过 `FLOW_JOURNAL_ADDR` 修改为另一本机地址。每个 JSON-RPC 对象参数都必须含 `_token`，不要把该参数或完整请求记录到访问日志。

支持 workflow.create/update/publish/delete、run.start/cancel/signal/adjudicate、command.status、workflow.get、run.get、run.events.page、run.audit.page、schedule.change、webhook.change。cron/webhook 自动触发已接入；另支持 workflow.list/run.list、legacy.get/list、run.observations.page 与 run.subscribe。浏览器使用 /journal 页面，CLI 使用 journal 子命令。

写请求**必须**提供稳定的 `request_id`（缺失/空白即拒单）：无幂等键的客户端重试等于双 run/双 workflow。同身份重用不同业务参数返回 Conflict。重试必须使用相同业务参数。成功结果为包含 `committed`、`commit_cursor`、`request_id`、`result`、`visible` 的回执。错误 `-32020 COMMITTED_NOT_VISIBLE` 的 `data` 是已提交回执：不能当作未提交再次创建，应调用 `command.status` 查询相同 scope/request_id，或用原身份重试。`workflow.create` 的 scope 为 `workflow.create`；手动启动为 `run.start:manual:`；取消/信号/裁决为 `run.cancel:<run_id>`、`run.signal:<run_id>`、`run.adjudicate:<run_id>`。命令回执的幂等窗口为最近 8192 条（超窗旧回执按提交序淘汰，超窗重试会生成新命令）。

分页参数含 run_id、可选 cursor、limit（1–256）。后续页使用返回的完整 cursor；空 events 配合非空 next_cursor 表示继续扫描，不代表 run 结束。状态读取返回 StoredValue 与 snapshot_cursor。当前单用户令牌允许读取该数据目录全部 run，不能将此模式当作多租户授权。

人工接受未知操作的输出使用 run.adjudicate，参数为 run_id、node_id、operation_id、reason、output、request_id。必须匹配当前 uncertain 操作。它记录人工决定并推进节点，不创建声称外部响应已被捕获的 OperationOutcome，也不清除原尝试的 unknown 完整性标记。

离线维护（先关闭持有数据目录锁的服务）：

```sh
cargo run -p flow-journal --bin flow-journal-tool -- verify ./target/v2-data
cargo run -p flow-journal --bin flow-journal-tool -- rebuild-index ./target/v2-data
cargo run -p flow-journal --bin flow-journal-tool -- backup ./target/v2-data ./target/v2-backup
cargo run -p flow-backend --bin flow-journal-dev -- --data-dir ./target/v2-data rebuild-projection --destination ./target/rebuilt.sqlite
```

repair 默认仅报告候选前缀并以非零退出码结束；只有显式 `--confirm` 才写入新目录，原数据保留。候选后缀可能包含曾被确认的提交，不能将 repair 当作无损清理。新投影目标必须不存在；失败时保留目标用于排查，不将其当作完成的投影。投影重建不执行用户代码或网络请求。

流式下载默认监听 `127.0.0.1:9803`（`FLOW_JOURNAL_HTTP_ADDR`），路径为 `/runs/<run_id>/values/<output_id>`，使用 `Authorization: Bearer <FLOW_JOURNAL_TOKEN>`。引用必须出现在指定 run 的当前投影中；采用该投影的 LSN 作为读取上界。最多 8 个下载，每个 4 块有界队列，断连释放读取任务，读取损坏以流错误终止，响应不缓存。历史引用在固定投影上界内流式扫描日志定位；不支持 Range 断点续传。

最新负载、故障矩阵与限制见 [非断电验收报告](JSONL_PHASE1_ACCEPTANCE.md) 和 [F17](refactor-evidence/F17.md)。本轮已接入 LLM/email、迟到审计、订阅/客户端与 LegacyImport，1000 在飞等待容量已测。1 GiB 大值历史恢复已测；指定混合业务负载与非断电故障矩阵已补测通过。


浏览器打开 /journal，显式输入 RPC/HTTP 地址与令牌。LLM/email 的 api_key 配置保存凭证名称，对应服务进程的 FLOW_SECRET_<名称>；解析后的 Bearer 值不进入授权事实。webhook 调用 POST /hooks/<key>，携带同一 Bearer 令牌及稳定 Idempotency-Key；body 为 JSON 输入。配置源可通过 schedule.change/webhook.change 写入，cron 使用 cron_expr。

CLI 示例：

```sh
cargo run -p flow-cli -- --url ws://127.0.0.1:9802 journal call workflow.create --params ./request.json
cargo run -p flow-cli -- --url ws://127.0.0.1:9802 journal events RUN_ID --audit
cargo run -p flow-cli -- --url ws://127.0.0.1:9802 journal download RUN_ID OUTPUT_ID ./new-value.bin
cargo run -p flow-backend --bin flow-journal-dev -- --data-dir ./new-v2 import-legacy --source ./old-root --database ./old-root/flow.db
```

导入前停止旧服务；导入锁定源目录并校验源清单，目标须为独立目录。legacy.get/list 返回只读历史基线和报告；旧 run 不自动续跑，导入触发器默认禁用。旧日志中不存在的实际输入报告 missing_unrecoverable。导入失败可以用相同源和目标重试；源发生变化则拒绝。备份中未发布的 .flow-copy-* 文件不属于权威记录，可在确认无导入进程后人工清理。

JSONL 新写入后只能回退到能读取新 journal 的兼容二进制；不能重新启用旧 SQLite 数据库覆盖新事实。此轮未进行生产切换。
