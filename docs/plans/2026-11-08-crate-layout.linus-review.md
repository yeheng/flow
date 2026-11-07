**状态：设计评审稿，供确认方向；尚未迁移代码。**

2026-11-08 修订：明确 `flow-plugin-runtime` 的归属；改为赞成将 store 合入 journal、将插件协议与 IPC 合成 SDK；保留 RPC 独立。上一版将模块边界直接等同于 crate 边界，拆分过度。以下目录与判断为修订后的目标方案。

评审范围：根 workspace、`crates/`、`agent-plugin/`、`llm-plugin/`，以及测试、CI、构建和插件部署的依赖关系。依据当前源码及本地 Cargo 输出，不把旧设计稿中的“已完成”当作实现证据。

**【品味评分】凑合。**

存储写者、只读查询、协调器、RPC 客户端的职责边界有价值，但不是每个职责都需要独立 crate。问题集中在测试基建归属、插件协议与实现混装，以及 workspace 边界不明确。目录分组和有依据的 package 合并都值得做。

**【核心判断】值得做，但先修依赖，再移动目录。**

- `crates/flow-test/` 作为测试目录，下面保留四个 package：赞成。
- `plugins/harness/`、`plugins/llm/` 各自容纳插件实现：赞成。
- `crates/flow-internal-plugins/` 作为一个编译进执行器的实现库：赞成，分节点迁移。
- 四种测试全部塞进一个 package、每种内置节点各建一个 crate：反对。
- 将 `flow-store` 合入 `flow-journal`，读写能力在模块/API 层分离：赞成。
- 将 `proc-ipc`、`plugin-proto` 合为 `flow-plugin-sdk`，内部保留协议/传输模块：赞成。
- `flow-executor` 演化并改名为 `flow-plugin-runtime`；保留独立 `flow-backend`、`flow-rpc`。

**【致命问题与改进方向】**

1. **[P1] 执行器为使用协议类型，把整个 agent 实现带进了构建依赖。**

   [flow-executor/Cargo.toml](../../crates/flow-executor/Cargo.toml) 的 `agent-plugin` 普通依赖，指向同时依赖 `agent-core`、`agent-host` 的包。[agent-plugin/src/lib.rs](../../agent-plugin/agent-plugin/src/lib.rs) 既定义 `RunTurnParams`、`TurnOutcome`、`WireEvent`，又实现事件投影、摘要和运行时句柄。

   `cargo tree --offline -p flow-executor --edges normal --depth 2` 明确包含 `agent-core` 和 `agent-host`。这证明编译依赖隔离没有完成；它不证明整个 agent 运行时在执行器进程里执行，也不能据此断言最终二进制保留了所有实现代码。

   修复：提取轻量 `flow-harness-protocol` package，只含 serde DTO、方法名和应用错误码。宿主与插件实现共同依赖它。`AgentEvent → WireEvent`、`TurnSummary → TurnOutcome` 等转换留在插件实现。通用 SDK 的 protocol 模块不装具体 agent 业务类型。

2. **[P2] 测试套件拥有了其他工具需要的公共基建。**

   [backend-perf/Cargo.toml](../../crates/backend-perf/Cargo.toml) 依赖 `backend-e2e`；多个性能场景导入 `backend_e2e::common`。被测客户端、进程管理和 fixtures 应由共享基建拥有，测试套件应只拥有用例。

   `backend-e2e/src/common/` 对服务端实现的实际 Rust 引用是 `flow_rpc::journal_v2::is_write_method`；该函数已经重导出自 `flow_dto::rpc`。改用现有契约入口后，共享代码可以迁到 `flow-test-support`，无需为了它新增 harness crate，也无需让 support 依赖 backend/rpc/engine 实现。

   [remote-e2e/Cargo.toml](../../crates/remote-e2e/Cargo.toml) 同样声明 `backend-e2e` 依赖，但当前 `src/`、`tests/` 没有使用 `backend_e2e`；注释声称共享 wire helpers，与代码不符。这条遗留依赖应删除，不能据注释再造一层抽象。

3. **[P2] “独立插件 workspace”的说法与 Cargo 实际成员不一致。**

   根 `members` 显式列出 16 项，但根 `cargo metadata --offline --locked --no-deps` 返回 19 个成员，其中包含 `agent-plugin`、`agent-core`、`agent-host`。从 `agent-plugin/Cargo.toml` 调用 metadata 又得到插件自己的三个成员和自己的 target 目录。LLM 则只在独立 workspace 中出现。

   同一套源码可以从不同 workspace/lockfile 上下文解析，CI 又分别维护构建入口；[flow-test-support/src/io.rs](../../crates/flow-test-support/src/io.rs) 还在测试运行时按需执行两种插件的 cargo build。

   建议仓库内这些 Rust package 统一进入根 workspace，显式列出成员、共用根锁文件。保留插件独立版本和独立进程。当前没有独立仓库交付的硬约束足以抵消这套重复构建机制；将来确实拆仓时再建立独立 workspace。Tauri 有平台工具链和桌面壳边界，继续保留其独立 workspace。

4. **[P2] `task.rs` 中的节点实现和执行协议状态混在同一个对象里，不能整块搬家。**

   [TaskRunner](../../crates/flow-executor/src/task.rs) 同时持有 master 消息通道、ACK、审计窗口、Transfer 装配、取消状态和节点执行方法。若把它移到 `flow-internal-plugins`，再让该库导入 executor 的 `TaskError`、`AuditStream`，就会出现反向依赖或循环。

   修复：迁出纯参数处理、计算和具体操作的实现；授权请求、审计持久确认、恢复裁决配合、Transfer 和 Result 提交留在 executor。先迁 Script/Condition，再处理 HttpCall/Email/命令模式；不要一次迁走整个 `TaskRunner`。

5. **[P2] 路径是运行约定的一部分，移动目录会直接破坏测试自举。**

   `flow-test-support/src/io.rs` 多处使用 `CARGO_MANIFEST_DIR.parent().parent()` 推导仓库根。移到 `crates/flow-test/test-support` 后，这个结果会变成 `crates/flow-test`。插件自举的 manifest 路径、CI、桌面脚本和文档也有旧路径。`flow-engine/build.rs` 还按固定源码目录计算 BUILD_ID。

   修复：目录迁移必须同批更新这些调用点；仓库根发现集中到一个 helper，以 workspace 特征定位，不继续复制固定层数。跨 package 的二进制路径由构建入口准备并显式传递，保留同 profile 产物定位；避免测试中递归调用 cargo 和误用旧二进制。

**【runtime、journal、RPC、SDK 的修订结论】**

`flow-plugin-runtime` 是现有 `flow-executor` 的演化，不新增一个与 executor 并列的宿主包。它位于 `crates/flow-plugin-runtime/`：承接 master 的派发协议，编排内置处理器与外部插件，拥有审计流、Transfer、取消、插件发现及进程回收。backend 拥有权威调度、许可裁决和恢复状态机，runtime 配合这些裁决执行。内置实现库被迁出后再完成 package/目录改名，不需要等待所有简单控制流都变成插件。对外二进制 `flow-executor` 可以先保留，避免同时破坏环境变量、定位器和 existing deployments；改二进制名是独立兼容迁移任务。

`flow-store` 不是一个通用可替换 store。源码直接依赖 journal 的 `FORMAT`、`EntityChange`、`entity_digest`、值预算和同库表结构；当前产品调用方在 backend，测试直接调用不构成另一种存储产品。用户所指“同一个 journal 存储子系统”成立，但当前 Rust crate 的实际调用方向是 backend 同时持有 writer 和 Store，并非 journal 调用 Store。

将其合入 `flow-journal::query`，可将 `Store` 改为 `Reader`，保留私有只读连接池、`read_only(true)`、同一读事务内读取状态与 LSN、身份/摘要校验。writer 继续独占权威写入。合并 package 不合并连接、不新增通用可写 DB 句柄，也不同时把 sqlx 改写成 rusqlite。新增单一 `query` feature，将 sqlx 查询依赖置于该功能下；backend 开启它，只有 codec/value 或 writer 需求的消费者不必开启。SQLite schema 与读写实现由同一 package 管理，减少跨 crate 暴露内部细节。

`flow-rpc` 保持独立：它实际提供 JSON-RPC 方法、认证/错误映射、webhook、下载 HTTP 和服务装配。服务端 bin、Tauri 桌面直接使用同一份 `module_product`、router 和状态结构；CLI 只依赖 DTO、在线调用这些接口。合入 backend 会让协调器 API 包承担 jsonrpsee/axum 产品适配职责；合入 CLI 会破坏轻量客户端边界。无需新增 flow-server/flow-api crate，也无需为这轮整理改 RPC 包名。cron 的去重/创建 run 等规则由 backend 负责，入口适配层负责驱动与请求转换。

`proc-ipc` 和 `plugin-proto` 当前实际由 runtime、Harness/LLM 插件及其互操作测试配套使用，没有发现独立发布的纯协议 Rust 消费方。合并为 `flow-plugin-sdk`，内部采用 `protocol::{envelope,jsonrpc,handshake}` 与 `ipc::{channel,spawn,child,env,challenge}`。protocol 模块不依赖 ipc 模块、仍接受通用字节流；跨语言契约继续以 wire.md 与 golden vectors 为准。模块边界足够表达这一层次。SDK 不包含插件注册表、master 协议、审计窗口和调度策略，也不依赖 runtime/backend/engine。将来出现真实独立消费者，再决定是否分包或增加 ipc feature；当前不为假想复用增加开关。

**【建议目录：最终命名】**

```text
Cargo.toml                         # Rust 核心、测试、插件的唯一根 workspace
Cargo.lock
crates/
  flow-plugin-sdk/                 # 合并现有 proc-ipc 与 plugin-proto
    src/
      protocol/                   # 帧、JSON-RPC 子集、握手
      ipc/                        # 通道、spawn、进程树回收
  flow-dto/                        # 产品共享类型、RPC 方法目录
  flow-config/                     # 配置与 manifest 解析
  flow-journal/                    # 权威写入 + 同库只读查询
    src/
      writer.rs
      query.rs                    # 原 flow-store；Reader 私有只读池
  flow-engine/                     # 定义、校验、状态归约及现有共享契约
  flow-backend/                    # 协调、调度、恢复、产品命令
  flow-rpc/                        # RPC/HTTP 产品入口
  flow-cli/                        # 纯 RPC 客户端
  flow-agent/                      # 远程中继及本机进程池
  flow-plugin-runtime/             # 现 flow-executor；唯一插件宿主
  flow-internal-plugins/           # 一个 package；执行器内静态调用
    Cargo.toml
    src/
      lib.rs
      script.rs
      condition.rs
      http.rs
      email.rs
      command.rs
  flow-test/                       # 纯目录，无 Cargo.toml、无聚合 facade
    test-support/                  # package: flow-test-support
    e2e/                           # package: flow-e2e
    e2e-remote/                     # package: flow-remote-e2e
    performance/                   # package: flow-perf
plugins/
  harness/                         # 纯分组目录
    README.md
    VENDOR.md
    protocol/                      # 新 package: flow-harness-protocol
    agent-core/                    # 保留独立 package 和版本
    agent-host/                    # 保留独立 package 和版本
    plugin/                        # package: flow-harness-plugin
      Cargo.toml
      plugin.toml
      src/
  llm/
    plugin/                        # package: flow-llm-plugin
      Cargo.toml
      plugin.toml
      src/
web/src-tauri/                     # 保留独立 workspace
```

目录展示最终 package 命名：自有包统一 `flow-` 前缀，依赖导入对应 `flow_plugin_sdk` 等 Rust 名称。结构迁移阶段先保留旧 package 名；集中命名阶段再更新 `cargo -p`、CI、脚本和文档。二进制名与 package 名是不同契约：现有 `flow-executor`、`flow-agent-plugin`、`flow-llm-plugin`、`flow-perf`、`flow-capacity` 先保留。第三方裁剪来源的 `agent-core`、`agent-host` 保留上游身份，不为前缀统一改名；外部依赖也不在命名规则内。

**【关键洞察：依赖应按职责流向】**

```text
flow-rpc → flow-backend → flow-journal::{writer,query}
flow-plugin-runtime → flow-internal-plugins → flow-engine / flow-dto 等共享契约
flow-plugin-runtime → flow-harness-protocol ← flow-harness-plugin → agent-host → agent-core
flow-plugin-runtime / flow-harness-plugin / flow-llm-plugin → flow-plugin-sdk

flow-e2e / flow-remote-e2e / flow-perf → flow-test-support → flow-dto
产品包 --仅 dev-dependencies--> flow-test-support
```

以上是关键约束，不是完整依赖图。具体要求：

- support 可使用通用测试库和轻量产品契约，禁止依赖 backend/rpc/executor/engine 实现及任何测试套件。其内部按 `io`、`provider`、`server`、`client`、`fixtures` 分模块即可。
- 测试套件之间没有普通依赖。remote 保持单独 package，便于单独开启 `fault-injection` 和运行分布式矩阵。
- 独立 package 不保证 `cargo test --workspace` 的 feature 隔离；Cargo 仍会统一同一依赖的 feature。发布构建须明确选择产品包，避免测试套件的普通依赖把 `fault-injection` 带入产物。
- executor 通过轻量协议依赖插件，不通过实现库依赖 agent runtime。LLM 当前 lib 几乎为空，没有实际双方共享的类型时，不为了目录对称再造 `llm-protocol`。
- builtin 库不依赖 executor，不导入 `AuditStream`、master 通道或 `JournalBackend`。允许依赖现有 engine 的模型、日志和共享契约；engine 不能再反向重导出 builtin，避免循环。
- SDK 内保留 protocol 与 ipc 的单向模块约束；协议函数不接受只能由 SDK spawn 得到的专用通道。具体插件的业务 DTO 独立于插件运行时实现。

**【内置实现到底包含什么】**

这里的 internal plugins 是静态实现库，沿用用户提议的名字；它不提供动态 ABI，不经插件 JSON-RPC，也不为内置节点新增子进程。

| 现有逻辑 | 建议归属 | 边界 |
| --- | --- | --- |
| Script、Condition、JS 求值和模板计算实现 | flow-internal-plugins | 保持预算、确定性上下文、日志和错误行为 |
| HttpCall、Email 的请求构造、执行和结果解析 | flow-internal-plugins，后续分批迁移 | executor 先完成许可流程；副作用后结果未知的分类不变 |
| Harness command 模式的具体命令处理 | flow-internal-plugins，后续迁移 | 进程取消/回收要求保留，宿主继续拥有派发生命周期 |
| Harness agent 模式 | plugins/harness/plugin | 当前真正迁到外部进程的是这一支，不是全部 Harness |
| LLM 出站操作 | plugins/llm/plugin | 已有独立进程；宿主保留授权和 outcome 审计 |
| Start、End、Delay、Input、HumanTask、SubWorkflow 的编排 | executor，配合 backend 的权威状态机 | 不为简单控制流额外制造插件层 |
| 秘密解析、许可请求、audit ACK、Transfer、恢复 outcome 复用 | executor | 实现库不得绕过这些步骤 |

迁移时采用普通函数和小的输入/输出结构。Script/Condition 共用一个求值实现。请求类操作可以分为“构造并校验请求”“执行已许可操作”“解析结果”，由 executor 编排。不要为这几个内置模块先发明 `PluginManager`、多层 trait、通用 Context 容器。

`flow-engine::exec` 中被调度器使用的超时常量应继续属于共享契约，不能随执行实现一起搬走后迫使 backend 依赖 builtin。`expr` 的实际执行实现可迁出；`template.rs` 的工作流片段归一化属于定义规则，保留在 engine。同名“模板”不是同一职责。

**【其他 crate 要不要合并】**

| 候选 | 判断 | 理由 |
| --- | --- | --- |
| 四个测试 package | 目录归组，保留 package | 通用基建、单机契约、远程故障、性能工具有不同依赖和运行入口 |
| flow-journal + flow-store | 合并 package，分开读写 API | 同一 schema、摘要和格式版本；私有只读池可以在 query 模块内继续保证能力边界 |
| proc-ipc + plugin-proto | 合并为 flow-plugin-sdk | 当前同一组配套消费者；用模块保持协议与传输分层 |
| flow-backend + flow-rpc | 保留 | 协调器和网络产品入口不同；桌面可以装配同一后端 |
| flow-dto + flow-engine | 保留 | CLI 当前只需轻量 DTO，合并会把引擎依赖推给客户端 |
| flow-config + flow-dto | 保留 | TOML/环境/文件配置和线上数据契约不同 |
| agent-core + agent-host | 本轮保留 | 核心循环与工具/MCP/skills/会话装配的依赖确实不同，VENDOR 升级边界仍有价值 |
| flow-engine 的全部内容 | 先迁执行实现，暂不再拆 | 该包仍混合协议、模型、状态、密钥、观测；先解决已证实的执行边界，后续再评估抽出 execution-protocol |

独立 crate 的价值由依赖与所有权决定，不由行数决定。约 500 行的 DTO 比一个包含所有测试和重依赖的“统一包”更有存在理由。

**【Linus 式落地任务清单】**

1. **提取 harness 方法协议。**
   目标与原因：切断 executor 对 agent runtime 的编译依赖。范围：新增 protocol package、agent-plugin lib/main、executor 的 agent_plugin/agent_progress、相关 Cargo manifests。做法：仅迁 wire 类型/常量，运行时投影函数留在实现包；两端改用同一契约。验证：运行 agent_isolation、agent_lifecycle、agent_provider、agent_workdir，保留隐私投影与错误码断言，并检查 `cargo tree -p flow-executor --edges normal`。验收：树中无 agent-core/agent-host/agent-plugin，实现仍通过真实插件进程调用。禁止修改 wire 字段、错误码、事件隐私边界或插件二进制名。

2. **归并测试目录及共享基建。**
   目标与原因：使套件只拥有用例，性能工具无需依赖 E2E 包。范围：四个测试目录、Cargo 路径、backend-e2e/common、性能场景 imports、文档与路径 helper。做法：先将 common 的服务端方法引用改为 flow-dto；再迁公共 server/client/fixtures 到 support；移除 perf→e2e 和 remote→e2e；最后迁目录并更新仓库根定位。验证：单机/远程套件、SIGKILL 恢复、CLI 往返、性能工具现有小规模场景，覆盖自定义 target-dir。验收：原 package 测试入口可用，suite 无相互依赖，support 不依赖被测实现。禁止删除故障注入用例、用 sleep 代替 ready 协议或为省目录合并四个 package。

3. **统一插件目录及 workspace 归属。**
   目标与原因：消除双重 workspace 上下文和重复自举。范围：plugins 新路径、根/插件 Cargo manifests 与锁文件、CI、构建脚本、测试二进制准备入口、VENDOR/README。做法：单独提交目录迁移；再单独提交 workspace 收敛，显式列出插件成员，移除插件嵌套 workspace 和冗余锁文件；保留显式版本与原依赖 feature，不顺手批量升级；统一预构建入口并删除测试中的 cargo build。验证：metadata 成员、fmt/clippy、根全量测试、插件自测、插件与执行器目标架构一致、同目录发现及 manifest 覆盖。验收：一个根锁文件管理核心/测试/插件，测试运行无需调用 cargo，所有部署件有明确构建与打包来源。禁止移除 Tauri workspace、改插件 id/二进制名、把 `cargo build --workspace` 当作产品发布选择器。

4. **逐步提取内置节点实现。**
   目标与原因：让 task.rs 回到派发与持久协议编排。范围：新增 flow-internal-plugins、executor/task.rs、engine/expr.rs/exec.rs 及其相关测试；不涉及 RPC 或数据库 schema。做法：先迁 Script/Condition/求值器并迁移原单测；再逐项提取 HTTP/Email/command 纯实现；继续共用既有预算、日志和确定性上下文；共享超时契约留在 engine。验证：脚本数值/超时/模板/确定性、条件分支、HTTP 与命令取消、已有 outcome 恢复不重复执行、授权后未知结果、远端大值与故障矩阵。验收：builtin 不依赖 executor，不持有权威写入或协议状态；节点结果与错误分类保持一致。禁止把 TaskRunner 整体搬家、建立循环依赖或把所有控制节点强制插件化。

5. **补齐源码身份、部署与架构文档。**
   目标与原因：搬迁不能让 BUILD_ID 漏记新实现，也不能让现有部署入口失效。范围：engine/build.rs、设计文档、CI/release、桌面 externalBin 构建与打包配置。做法：把新内置实现及宿主使用的协议/SDK 源码与 manifest 纳入溯源哈希；明确插件独立版本的部署记录；更新目录与依赖说明，逐项核对现有二进制清单。验证：修改内置实现会改变 BUILD_ID，纯搬迁不改变 RUNTIME_ID 的兼容版本；从构建包实际验证执行器/插件发现、CLI 入口及恢复用例。验收：旧路径无有效调用，构建身份覆盖新实现，现有运行快照仍按原兼容语义恢复。禁止为了目录变化提升执行兼容版本、改 RPC/v7 wire/SQLite 格式或把所有插件版本折成一个宿主版本。

6. **将查询模块合入 journal。**
   目标与原因：由一个 package 拥有同一 SQLite schema 的读写规则，移除伪通用存储层。范围：flow-store/src/store.rs 与 tests/queries.rs、flow-journal manifest/lib、新 query 模块、backend 调用点、测试 Cargo 依赖。做法：先机械迁移并保留 SQL/错误语义；通过 query feature 引入 sqlx；更新 backend 和测试调用；再移除 flow-store package，集中调整类型名为 Reader。验证：原查询测试、索引计划、摘要损坏/foreign journal 拒绝、响应预算、状态与游标一致性、导入/恢复回归；检查关闭 query 后的 codec/value 与 writer 构建。验收：不存在 flow-store package，Reader 无写入 API、连接池私有且只读，原格式与事务语义不变。禁止合并读写连接、改数据库格式、同步更换 SQL 驱动或借合并让 query 依赖 backend。

7. **合并 SDK 并集中完成自有包命名。**
   目标与原因：减少配套基础包的数量，并明确 runtime 与 SDK 的不同职责。范围：proc-ipc/plugin-proto 代码和测试、全部插件/宿主 imports、根 members、CI 跨平台任务、Cargo.lock、文档和 build.rs 路径。做法：将两包机械迁入 flow-plugin-sdk 的 ipc/protocol 模块；更新导入与互操作测试路径；在内置实现迁出后将 executor package/目录改为 flow-plugin-runtime；按目标表统一自有 package 前缀，集中更新 cargo -p 入口。验证：golden vectors、Python e2e、插件握手/取消/令牌、三平台 IPC/进程回收、单机/远程执行，以及原二进制发现与打包。验收：SDK 无 Flow 业务实现依赖、protocol 不依赖 ipc、runtime 是唯一插件宿主；旧 package 命令引用均已更新，对外二进制入口仍有效。禁止改变 wire/vector 内容、删平台清理路径、把插件注册或审计放进 SDK，或同步重命名第三方 agent-core/agent-host。

任务按职责拆分，执行顺序建议为：1 → 2 → 3 → 6 → 4 → 7 → 5。每一步独立验证；结构调整与 package 命名不要混在同一个大提交中。

**【风险与验证边界】**

风险集中于锁文件收敛、路径定位、内置实现提取后的循环依赖，以及外部副作用的许可/恢复顺序。目录迁移与 workspace/执行重构分开提交，便于逐步验证和回退。

本次已完成 manifests、调用点、构建脚本及文档的静态审阅；成功运行三个现有 workspace 的 `cargo metadata --offline --locked --no-deps`，以及 executor 的普通依赖树检查。没有修改实现，没有运行运行时测试或性能基准。以上验收测试是后续实施要求，不是本次已通过的结果。
