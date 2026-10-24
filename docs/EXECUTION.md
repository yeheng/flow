**Flow 执行、业务调用和回归合同**

当前实现保留 SQLite 单写者和一个 reducer。执行协议为 v7，持久化格式为 SQLite v4，证据包格式为 v1。HA、通用动态 Agent 循环不属于本次实现。

**定义版本与兼容**

`workflow.update` 保存未指定 `semantics` 的新定义时写入 `semantics: 2`。历史定义缺少该字段仍按 v1 解释；显式传 `semantics: 1` 可以创建兼容版本。编辑器保存已有定义时保留语义和输入/输出 schema。

- v1：全部前驱必须成功，跳过会继续向下游传播。
- v2：默认等待全部入边确定，执行选中的成功路径；未选分支为 inactive，失败仍阻断下游。全部入边 inactive 时传播 inactive。没有有效 end 完成时失败。
- `node.params.join` 可显式选择 `selected` 或 `all`。`selected` 不会在第一个前驱成功时提前启动。
- v2 启动时固定完整子流程依赖。子节点 `params.version` 可指定已发布版本；省略时在根 run 启动时固定最新发布版本。v2 不接受动态模板选择子流程身份。
- 时间、随机种子、子流程定义和执行兼容版本记入运行快照，子 run 继承。v2 的 `Date.now()`、无参数 Date、Math.random 使用该上下文；本地日期读写转 UTC，字符串日期要求 ISO 日期或显式时区。重试和 regression 使用相同上下文。
- 活跃 v2 run 的运行时身份不匹配时拒绝启动执行，不把它改成业务失败。历史仍可离线读取；升级前应完成这些运行，或使用匹配的版本继续。
- `flow_engine::RUNTIME_ID` 是显式执行兼容版本 `RUNTIME_VERSION`（当前 `flow-execution-v1`）的稳定摘要，不随注释、测试或无关依赖变化。修改节点语义、执行快照解释、表达式/依赖行为或执行协议且无法保证兼容时必须提升 `flow-engine/build.rs` 中的兼容版本；发布前运行工作区和远程执行回归。
- `flow_engine::BUILD_ID` 单独记录源码与锁定依赖的构建指纹，写入执行器审计的 `engine_build`，用于溯源，不用于恢复门禁。历史使用构建哈希的运行仍只允许匹配的旧执行器处理；本次没有将其自动认定为兼容新版本。


保存与导入使用相同的定义准备规则。与最新版本相同的定义复用原版本及值引用，包括不同请求身份的并发保存；新请求仍取得自己的幂等回执。`workflow.import` 按名称匹配时，只更新唯一的未删除对象；多个匹配返回冲突（`-32012`），不发布定义值或修改任何工作流。

工作流元数据写入与依赖冻结通过读写锁协调。冻结只复制当前运行实际依赖的已发布版本，最多 128 个唯一版本，不复制无关工作流、已删除流程或全部版本历史；冻结期间保持同一元数据视图。

节点模板在后端合并更新字段后，使用共享规则校验名称、分类、节点参数及边端点并归一化片段。RPC 的完整片段归一化复用同一规则；部分更新可以单独修改 nodes 或 edges，但合并后的片段必须完整合法。

**输入输出合同**

定义可带 `input_schema`、`output_schema`。支持明确的子集：type、properties、required、additionalProperties、items、enum、min/maxLength、minimum/maximum、min/maxItems，以及 title/description/default 注解。不支持的关键字在保存时拒绝。根流程与子流程的输入均在创建 run 前验证（子流程校验已固定版本的 schema）；输出不合约时 run 失败。完整 schema 验证预算为 8 MiB。

**业务接口**

原来的 `/hooks/{id}` 及管理 RPC 保持原有鉴权和回执合同。业务调用使用独立入口：

1. 管理端用 `webhook.change` 创建或配置一个 hook，字段为 `workflow_id`、`enabled`、`invoke_version`、`invoke_key_hash`。后两项必须同时有效：固定已发布版本，以及业务密钥的 SHA-256 十六进制摘要。业务密钥至少 32 字节，由调用方安全保存，不在管理 token 与业务客户端间复用。
2. `POST /invoke/{id}`：`Authorization: Bearer <业务密钥>`，`Idempotency-Key` 必填，`X-Correlation-Id` 可选（缺省沿用幂等键），JSON body 为输入。
3. 成功返回 HTTP 202，含 `run_id`、`run_token`、`result_url`、`events_url` 和持久回执。重复请求返回相同 run 与 capability；同键异参返回 409。输入不合法返回 400，容量拒绝返回 429。
4. 使用 `run_token` 访问 `GET /invocations/{run_id}`，读取 status、complete、output、通用错误码 error、workflow_version、correlation_id。详细失败原因走管理面；该 token 不能管理流程或读取其他 run。
5. `GET /invocations/{run_id}/events` 返回固定快照的业务进度分页，不包含私有定义、准备参数、原始请求或中间正文；完整证据通过管理面查看。后续页传 JSON 编码的 `cursor`。快照读完后，使用 `after_lsn=<snapshot_cursor.lsn>` 继续轮询新事实。最终结果正文通过 `GET /invocations/{run_id}/values/{output_id}` 流式读取，拒绝下载私有定义和中间正文。
6. `POST /invocations/{run_id}/signals/{node_id}`、`POST /invocations/{run_id}/cancel` 使用同一个 run token，写入仍须幂等键。业务 signal 仅允许 human_task/input，不能通过旧 signal 别名裁决外部操作。

业务路由支持带 Authorization 的跨源预检，不使用浏览器 cookie；响应 `Cache-Control: no-store`。大于 8 MiB 的结果返回 413 和 JSON 下载描述符，再使用正文下载接口。

人工节点的早到 signal 在当前 run 的持久 inbox 中保存，节点进入等待后消费；每个 DAG 节点只有一个输入槽，重复身份去重，另一个信号竞争已占用槽返回冲突。该接口不承担无限消息队列职责。

**模型、工具和外部操作**

原生 `llm` 节点是一次非流式 HTTP JSON 模型调用：`params.url`、`params.body` 必填；支持 headers、credential、timeout_ms、response_schema。请求使用 POST，原始响应和结果持久记录。`body` 保持供应商原始 JSON，不把任意模型协议冒充通用标准。输出与 HTTP 节点一样是 `{status, headers, body}`。

受管理 harness 在超时、取消及正常结束时回收后代进程；执行器退出时，父进程还会回收其会话内的 harness 进程组。Unix 使用执行器会话和 harness 进程组，Windows 使用启动前绑定的 Job Object。主动脱离会话/作业的 daemon、跨主进程与执行器同时强杀的系统故障，仍需部署级进程容器或服务管理器约束；这不改变未知外部结果必须裁决的合同。

`harness` 节点有两种模式（`mode` 参数，缺省 `command`）。`command` 运行用户自备的外部 agent CLI。`agent` 在 executor 内联调用 conga-host 的无头装配，无独立 runner 二进制；typed 配置直入，API key 经 OperationPermit 的内存字段传给 provider，不使用 stdin 或环境变量。conga/conga-host 以源码形式随仓库 vendored（`crates/conga`、`crates/conga-host`，见 `crates/VENDOR.md`），构建不依赖外部 Git 源。

agent 从取得授权许可开始使用一个生命周期截止时间，覆盖临时目录准备、host 初始化、模型/工具回合、session 清理和 outcome 落账。取消或超时后停止等待，保留已有授权与结果证据；没有完整 outcome 的操作进入 Uncertain，不自动重放工具副作用。独立系统线程在截止时间或取消后给予 5 秒宽限（检查粒度 250ms），到期强制退出 executor，避免同步 host 代码阻塞 Tokio 时继续靠心跳占槽。

agent 使用一次性 executor：成功结果正常提交后，本地池仍回收整个执行器会话；远程 agent 每次派发本来就独占一个执行器。父进程 kill/wait/reap 会清理会话/Job 内的工具后代，进程级环境及 host 状态不进入下一任务。普通节点继续复用空闲 executor。服务 `close()` 等待执行端口停止及跟踪会话完成回收，包含空闲和预热的 executor。调度器限制 agent 同时占用至多 `ceil(派发上限 / 2)` 个槽；上限大于 1 时给普通节点保留容量，上限为 1 时串行执行。

Linux/macOS 看门狗每 250ms 检查 executor 会话各进程的 RSS 总和与数量，超过 1 GiB 或 64 个进程则退出；Unix 还设置继承到工具的 256 MiB 单文件硬上限。这是运行预算：RSS/PID 采样可能短时超量，RSS 会保守累加共享页，单文件限额不等于总磁盘限额，CPU 总量与总 token/费用没有独立硬限额。其他平台仅提供独立生命周期看门狗，资源隔离需由 worker 的 Job/容器补齐。主动脱离会话的进程和临时目录总量仍需部署级约束。

workdir 可选，缺省为本次操作的随机临时工作目录；显式目录须在授权前通过存在性校验。临时目录和 FullAuto 都不构成 OS 安全沙箱。HOME、CARGO_HOME 与 XDG 路径仍按执行器白名单继承，工具可能访问其中的配置。conga 配置/项目状态单独钉定到本次操作临时根，不读取用户 conga settings。模型配置在 run 启动时冻结，回归使用 baseline 配置并执行零外部调用。

agent 仍是一个受管理 operation，内部模型/工具调用不逐个持久化，证据粒度为 `trace_granularity=process`。强制终止无法撤回已发生的外部副作用，也可能留下临时目录；清理与保留策略不能冒充逐工具恢复保证。

agent 的中间进度实时写入同一节点的观测日志：每轮开始/结束、模型请求次数与响应状态、
provider 返回的 token 用量、工具名称与调用 ID、执行耗时及成功/失败。
运行页「活动」卡片与日志视图均可查看，无需等待节点完成。
进度日志不记录 prompt、模型文本/思考内容、工具参数或结果正文；
复用现有有界队列与丢弃计数，属于可丢弃观测，不构成逐工具审计或恢复检查点。

模型决定应由脚本/条件节点读取，业务工具由独立 HTTP 或子流程节点执行。每个模型调用和工具操作拥有独立的请求、授权和结果证据；可以对整个场景回归。`harness` 继续提供进程级证据，其内部调用不会被假定为独立受控工具操作。节点能力接口公开 `trace_granularity`。

所有外部调用默认 `effect_policy: manual`：已授权但没有完整结果时等待人工核对。显式设置 `effect_policy: repeatable` 且配置 `retry.max_attempts` 后，协调器可以提交 `OperationRetryAuthorized` 再进行下一次尝试，实际请求仍使用已准备的参数。该声明只适合重复调用始终安全的接口；有限有效期的幂等协议或需要人工核对的写操作继续使用 manual。平台不会根据 GET 方法自动推断安全，也不承诺任意外部系统 exactly-once。最大尝试次数仍受 100 次硬上限约束。

**Regression**

新增 RPC：

```json
{"method":"run.regress","params":{"baseline":"原run_id","workflow_id":"可选候选流程","version":2,"request_id":"稳定请求身份"}}
{"method":"run.compare","params":{"baseline":"原run_id","candidate":"候选run_id"}}
```

baseline 必须已终结且带执行上下文，子 run 也必须终结。候选运行创建自己的 run，使用候选定义和固定依赖，继承 baseline 的时间/种子、人工输入和外部录制结果。节点路径使用 JSON 字符串数组编码，避免节点 ID 中的分隔符造成身份碰撞。

执行器在构造实际请求后严格比较摘要，缺少 fixture 或请求与所选 fixture 不匹配即失败。协调器也独立拒绝 regression 的外部操作许可，因此不会回退到真实 HTTP、邮件或命令。此模式不验证新 prompt 调用模型后的质量；新模型评估应作为另一次明确授权的真实运行。

`run.regress` 还接受可选 `fixtures` 映射，键为规范 JSON 节点路径，值为 `{fingerprint, outcome}`。覆盖必须显式提供，正文引用必须已在当前数据集中发布；可使用另一真实运行录制的结果。覆盖路径写入运行证据，不会隐式更改 baseline。

`run.compare` 比较最终状态、错误、输出、逐节点结果、外部请求摘要和对应子 run，返回 complete、passed、differences；最多报告 64 处差异，超出时 truncated 为 true。

```sh
flow-cli journal regress BASELINE_RUN --version 2 --timeout 120
flow-cli journal compare BASELINE_RUN CANDIDATE_RUN
```

CLI 输出差异 JSON；一致退出 0，差异退出 4，等待超时退出 1。管理凭据仍通过 `FLOW_JOURNAL_TOKEN` 提供。

**证据资产与升级**

服务停止后可导出一个 run 与已创建子 run 的引用闭包：

```sh
flow-journal-dev --data-dir DATA export-run RUN_ID NEW_BUNDLE_DIRECTORY
flow-journal-dev --data-dir DATA inspect-bundle BUNDLE_DIRECTORY
```

包包含 manifest.json、保留原始位置的 events.jsonl、正文文件及摘要。引用中的流程定义、依赖和录制响应一并携带；不包含其他无关 run，也不依赖可丢弃 observations。正文按摘要派生的文件名保存，原始 output_id 不直接作为路径。包为完整内部证据，包含业务原始输入/输出，不自动脱敏。

检查会校验事件顺序、边界、摘要、引用闭包与正文，随后使用同一个 reducer 还原只读运行状态。还原过程使用本地连续事务序号，原始 LSN 保存在 JSONL 中；不会启动执行器或撤回外部效果。当前工具不把包直接合并到运行中的业务库。导出上限为 64 MiB 事件、1 GiB 正文、1000 个 run、10000 个引用。未完成导出不会发布 manifest；已有目标目录不覆盖。

SQLite v4 沿用 v3 的 SQL 表结构，但通过格式门阻止旧程序忽略新版执行语义。v3 使用非破坏性的离线复制升级：

```sh
flow-journal-tool upgrade OLD_DATA_DIRECTORY NEW_DATA_DIRECTORY
flow-journal-dev --data-dir NEW_DATA_DIRECTORY check-projection
```

升级不修改源目录，复制后校验事实、当前行、正文和摘要，再发布新库。停止旧服务、完成升级检查后，将服务的数据目录切换到新目录。原目录继续保留，不同时运行源目录与升级后的目录。新版本正常启动不会全量重放，也不会静默迁移未知格式；损坏时显式拒绝，不自动覆盖。

业务事实、回执和引用正文仍全部保留；没有引入 TTL 或 GC。16 个在飞 dispatch、1000 个非终态 run 的预算保持不变。

**CLI 不确定交付恢复**

常规写命令可传全局 `--request-id KEY`。同一操作重试必须保留同一个键和原参数；传输失败会在 stderr 输出请求身份和命令 scope，不改变 stdout 的 JSON/管道合同。对提供持久回执的命令，使用 `command.status` 查询该 scope 与 request_id，再决定是否重试。`config.update`、`secrets.set/delete` 没有业务 journal 命令回执，应读取当前配置或密钥状态核对。

```sh
flow-cli --request-id order-20261019-42 run start WORKFLOW_ID --input '{"id":42}' --detach
# 将 {"scope":"run.start:manual:","request_id":"order-20261019-42"} 写入 status.json
flow-cli journal call command.status --params status.json
```
