# flow-web 前端拖拽流程编辑器

Vue 3 + Vite + TypeScript + vue-router + Tauri 2，支持浏览器与桌面应用，画布基于 @vue-flow/core。直连 flow-journal-server
（JSON-RPC 2.0 over WebSocket），覆盖 编辑 → 保存 → 发布 → 运行 → 实时监控 全流程。

## 页面结构

| 路由                      | 页面                                                                                  |
| ------------------------- | ------------------------------------------------------------------------------------- |
| `/`                       | 仪表盘：概览卡片（工作流数 / 运行数 / 成功率 / 进行中）+ 最近运行 + 按工作流成功率    |
| `/workflows`              | 工作流列表：新建 / 打开编辑器 / 触发器 / 运行记录 / 删除                              |
| `/workflows/:id`          | 编辑器：顶部条（保存/发布/运行）+ 左节点面板、中画布、右参数与运行面板                |
| `/workflows/:id/versions` | 版本历史：版本表格（状态/校验和/时间）+ 加载到编辑器 / 以此版本发布 + 两版结构化 diff |
| `/workflows/:id/triggers` | 触发器：定时调度（cron/input 行内编辑 + 下次触发，10s 轮询）与 webhook（hook URL 复制 / curl 示例 / 调用合同） |
| `/workflows/:id/runs`     | 该工作流的运行历史（3s 轮询）                                                         |
| `/runs`                   | 全局运行历史（同上组件，不过滤）                                                      |
| `/runs/:runId`            | 运行详情：信息头 + 只读 DAG（按 run 钉死的版本渲染、保留运行态着色）+ 实时时间线      |
| `/journal`                | 事件审计：journal v2 审计记录、原始快照与观测记录检查台（运维/调试面，可直连任意 journal 工作区）   |

服务部署模型为单 master／SQLite，可通过远程 agent 扩展执行容量。前端不再包含 cluster 注册或节点汇总页面。

全局通知走右上角 Toast，确认/输入走 Modal（替代原生 prompt/confirm）；
有未保存修改时离开编辑器路由会先确认。

## 启动

```bash
# 终端 1：后端（仓库根）
cargo build -p flow-executor
FLOW_JOURNAL_TOKEN='replace-with-at-least-32-random-characters' \
FLOW_JOURNAL_DATA_DIR="$PWD/.flow-journal" \
cargo run -p flow-rpc --bin flow-journal-server  # 默认 ws://127.0.0.1:9802

# 终端 2：前端
cd web
npm install
VITE_FLOW_TOKEN='replace-with-at-least-32-random-characters' npm run dev
# 默认 http://127.0.0.1:5173；token 与后端一致，也可在 UI 中填写
```

### 浏览器 / 桌面双入口

两种模式共用 `src/` 和服务端方法实现，调用层自动选择：

- **桌面**：应用启动时自动启动内嵌的 journal 服务，普通请求经 Tauri `invoke`
  直接调用 Rust 方法模块，订阅经 Tauri Channel 推送，无需另开后端进程或 RPC 端口。
- **浏览器**：经原有 JSON-RPC WebSocket API 连接独立的 `flow-journal-server`，支持远程部署。

桌面开发与安装包行为一致，执行 `npm run dev:desktop` 或打开 `Flow.app` 即可使用。

| 命令（在 `web/` 下执行） | 用途 |
| --- | --- |
| `npm run dev:browser` | 启动 Vite 并打开浏览器 |
| `npm run dev:desktop` | 启动 Vite 并打开 Tauri 桌面窗口 |
| `npm run build` | 浏览器静态产物 `dist/` |
| `npm run preview` | 本地预览浏览器构建 |
| `npm run build:desktop` | 构建当前平台的桌面安装包 |
| `npm run build:desktop -- --bundles app` | macOS 仅生成 `.app`，跳过 DMG |
| `npm run build:executor` | 单独构建执行器 sidecar（`src-tauri/bin/`） |

桌面开发需要 Rust stable 与平台构建依赖：macOS 安装 Xcode Command Line Tools；
Linux 安装 WebKitGTK 4.1 等 [Tauri 系统依赖](https://v2.tauri.app/start/prerequisites/)。
当前内嵌后端沿用仓库的 Unix 运行时依赖，已验证 macOS；Windows 仍需原生端到端验收。
浏览器前端构建只需要 Node.js。

`dev:desktop` 自动启动 Vite，不需要提前运行 `dev`；5173 被占用时会明确报错。
窗口打开后，也可以在浏览器访问同一开发服务（浏览器仍需独立 API 后端）。桌面使用 hash 路由（如
`/#/workflows`），浏览器保留 history 路由；浏览器生产部署需将未知页面路径回退到 `index.html`。

原生工程位于 `src-tauri/`，有独立 Cargo workspace / lockfile，避免后端构建引入桌面系统依赖。
默认桌面产物位于 `src-tauri/target/release/bundle/`（设置 `CARGO_TARGET_DIR` 时随之改变）。
安装包必须在对应平台构建；对外分发的签名、公证需由发布环境配置。

桌面通过 Tauri `externalBin` 携带 `flow-executor` sidecar；`dev:desktop` / `build:desktop`
会在启动前自动执行 `npm run build:executor`，因此执行器源码改动无需手动重建。
执行器与主进程必须使用匹配的执行协议；缺失或不兼容的 sidecar 会明确拒绝启动。

桌面数据默认存放在用户主目录的 flow 主目录 `~/.flow`（可用 `FLOW_HOME` 覆盖），
与 journal-server / harness 沙箱统一布局：工作流、运行与权威事件保存在
`~/.flow/journal/flow.sqlite`，密钥与保留的旧数据使用 `~/.flow/data/` 目录，
可选配置文件为 `~/.flow/flow.toml`。需要隔离开发数据时，在启动前
设置 `FLOW_DESKTOP_DATA_DIR=/绝对路径`（或 `FLOW_HOME`）。桌面不会自动导入
原服务器数据目录，也不会自动迁移旧的 Tauri 应用数据目录
（`~/Library/Application Support/com.flow.workflow.desktop/`）。

应用启动会恢复本地运行并启动定时调度；无 `flow.toml` 时默认开启执行器预热
（`execution.prewarm = true`）：协调器就绪后立刻拉起一个空闲 `flow-executor`，
首次运行的节点派发省去进程启动 + 握手延迟；预热失败只告警，首次派发仍会懒加载。
退出时停止订阅、HTTP 和调度任务，关闭 Journal 并回收后端运行时。未完成的运行按
现有后端恢复规则在下次启动处理，退出不会将它们标为取消。
同一数据目录的第二个实例会拒绝启动，避免多个进程同时写入。

webhook 仍需接收 HTTP 请求，桌面仅在 `127.0.0.1` 随机端口开放此入口，触发器页面展示
实际地址（每次启动可能变化）。事件审计页在桌面直接打开本地数据，无需地址和令牌；
完整值下载走原生保存对话框和 Rust 流式写入。浏览器下载仍使用 `showSaveFilePicker`，
不支持该 API 时提示使用 CLI。

传输适配位于 `src/rpc/`，内嵌服务和 Tauri 命令位于 `src-tauri/src/`。桌面始终使用
本地服务；以下 `config.json` 和 `VITE_FLOW_*` 地址设置仅作用于浏览器。

浏览器 flow-journal-server 地址可用环境变量覆盖（需在 vite 启动前设置）：

```bash
VITE_FLOW_RPC=ws://127.0.0.1:9802 npm run dev      # JSON-RPC WebSocket
VITE_FLOW_HTTP=http://127.0.0.1:9803 npm run dev   # webhook HTTP 入口（触发器页展示 hook URL 用）
```

部署时也可不改构建：在 web 根目录放 `config.json`（见 `public/config.example.json`），
启动时拉取一次，优先级高于 `VITE_FLOW_*`：

```json
{ "rpcUrl": "wss://flow-server.example.com", "httpUrl": "https://flow-hooks.example.com" }
```

## 设置页与统一配置

「设置」页（`/#/settings`）分两块：

- **统一配置**：读写服务端 `flow.toml`（分层：CLI `--config` > 环境变量 > 配置文件
  > 内置默认值；示例见仓库根 `flow.config.example.toml`）。修改**重启后生效**，
  页面会标注哪些分区仍被环境变量覆盖（如 `FLOW_JOURNAL_ADDR`）。桌面端隐藏独立
  RPC 监听地址类分区，桌面配置文件位于应用数据目录 `flow.toml`；
- **密钥**：工作流 x-secret 参数引用的密钥可在此增删（AES-GCM 加密落盘于数据
  目录 `secrets.json` + 主密钥 `secret.key`，值永不出服务端）。执行时优先用这里
  的密钥，其次回落 `FLOW_SECRET_*` 环境变量；来自环境变量的密钥只能显示、不能删。

## 节点面板与业务节点（drawio 式）

编辑器左侧是 drawio 式节点面板：**搜索框 + 可折叠分组**。节点类型按功能分组
（控制/计算/集成/人工/AI/通知…），组头点击折叠/展开（折叠态按组持久化）；搜索时
自动展开并隐藏空组，同时匹配节点类型与业务节点。

**业务节点**：预配置好的节点或子图（含参数与内部连线），保存后出现在面板最上的
「业务节点」区（按分类分组），可直接拖拽或点击插入任意流程：

- 选中一个节点（或若干节点/整个子图）→ 右键「存为业务节点」或面板「存为业务节点」按钮 →
  填名称与分类（分类可复用已有值）→ 保存；
- 顶部条「存为业务节点」把当前工作流打包成 `sub_workflow` 单节点业务节点（分类「子流程」）；
- 业务节点持久化在服务端 `node_templates` 表，跨流程、跨设备可用；面板项 hover 可重命名/删除
  （已插入的节点不受影响）。插入时 id 重生成、按 max_instances 检查、入 undo 栈；
- 需要 run 溯源的完整子流程仍建议用 `sub_workflow` 节点（可钻取）。

## 工程化

```bash
npm run build    # vue-tsc --noEmit && vite build
npm run lint     # ESLint 9 flat config（typescript-eslint + eslint-plugin-vue）
npm run format   # Prettier
npm test         # vitest（monitor 事件流、toast、RPC 客户端、undo/redo、预校验、复制粘贴、分页合并、仪表盘聚合）
cargo test --manifest-path src-tauri/Cargo.toml # 内嵌运行、订阅、持久化与会话清理
npm run test:e2e # Playwright e2e（首次需 npx playwright install chromium）
```

e2e 基础设施（`playwright.config.ts` + `e2e/`）：global setup 编译并拉起一个**隔离的
flow-server**（临时目录 sqlite，`FLOW_ADDR=19311` / `FLOW_HTTP_ADDR=19312`，不碰
`data/flow.db`），vite 由 webServer 托管在 19313 并注入 `VITE_FLOW_RPC/VITE_FLOW_HTTP`；
teardown 杀进程删目录。种子数据直接走 JSON-RPC WebSocket（`e2e/helpers.ts`，Node 22
内置 WebSocket），用例间以随机名称隔离、串行执行（仪表盘断言全局 `run.stats`）。
覆盖：工作流列表新建、编辑器画布/保存、运行链路（含来源归因与详情时间线）、
触发器页（webhook 复制/POST 触发、非法 cron 报错）、仪表盘与 `run.stats` 对账、
编辑器右键交互（菜单增删/粘贴、按住右键拖拽连线与平移画布）。

## 编辑器交互（P1）

- **Undo/Redo**：Cmd/Ctrl+Z 撤销、Cmd/Ctrl+Shift+Z 或 Ctrl+Y 重做；快照式历史栈（上限 100），
  覆盖增删节点/边、连线、参数编辑（按 node+field 合并连续输入）、节点拖拽（整段合并为一条）、
  粘贴、自动布局；undo 回到上次保存的快照时「未保存」标记自动消失（脏标记是保存点的派生值）。
- **多选与复制粘贴**：Shift+左键拖框选、Cmd/Ctrl+点击多选；Cmd/Ctrl+C 复制选中节点及其内部边，
  Cmd/Ctrl+V 粘贴（重新生成节点 id、位置逐次偏移 32px、内部边重连、外部边不复制、
  受 max_instances 限制）；Delete/Backspace 删除选中。快捷键在输入框聚焦时不劫持。
- **右键交互**：原地右键弹自定义菜单——节点上（编辑参数/复制/创建副本/存为业务节点/删除，
  sub_workflow 另有钻取）、边上（删除连线）、空白处（添加节点子菜单按类别分组、粘贴到落点、
  自动布局、适应视图）；按住右键拖动：起点在节点上进入连线模式（多出端口按鼠标就近选择，
  悬停的合法目标节点绿色高亮，非法落点静默取消），起点在空白处平移画布。位移小于 4px
  视为点击弹菜单，浏览器原生右键菜单全程抑制。
- **前端预校验**：画布实时校验（150ms debounce），规则对齐服务端 Definition::validate
  （单 start/至少一个 end/DAG/可达性/condition 端口/max_instances/required 参数）；
  出错节点画布标红，顶部条显示错误计数，点击展开列表、点条目定位节点；
  保存/发布先过本地校验，有错不打 RPC（服务端校验仍是最终裁决）。
- **自动布局**：顶部条按钮，对全部节点做拓扑分层布局，可撤销。

## 版本管理（P2）

- **版本历史页** `/workflows/:id/versions`：版本表格（状态、校验和、创建时间），
  行操作「加载到编辑器」「以此版本发布」；下方两个版本选择器展示结构化 diff
  （新增/删除/变更节点按字段级 from→to、边增删，位置变化不算变更）。
- **回滚语义**：后端 publish 只给目标版本行置 published，`latest_published` 恒取
  MAX(version)，已发布指针无法回拨。因此「以此版本发布」= 复制该版定义 →
  `workflow.update` 生成新草稿 → 发布新版本（UI 文案如实说明，不是拨回指针）。
- 「加载到编辑器」把该版本定义载入作为编辑基础，保存时正常生成新版本；
  顶部条显示「基于旧版本，最新 vN」提示，避免误以为在改最新版。
- 版本列表数据来自 RPC `workflow.versions`（倒序，只回元数据列，definition 按需走
  `workflow.get` 带 version 参数拉取）。

## 触发器与仪表盘（P3）

- **触发器页** `/workflows/:id/triggers`：
  - 定时调度：新建表单（cron 5 字段 + 可选 JSON input）+ 列表（cron 表达式、
    下次触发时间本地化显示、启停、删除）。`next_fire_at` 由服务端算好
    （`schedule.create`/`schedule.list` 返回），前端不解析 cron；非法 cron 由后端
    -32010 经 toast 展示；页面激活期间 10s 轮询；
  - 调度编辑：行内「编辑」改 cron + 触发 input（JSON 文本域），保存走
    `schedule.update`（`api.updateSchedule`），只提交改动项——input 保持原样 = 不改
    （`undefined`），清空 = 删除输入（`null`），改成新 JSON = 替换；非法 JSON 前端拦截，
    保存成功后 toast 并刷新列表；
  - Webhook：新建按钮 + 列表（完整 hook URL 一键复制、调用示例展开、启停、删除）。
    hook URL 基址 = `VITE_FLOW_HTTP`（默认 `http://127.0.0.1:9803`），因为
    flow-server 的 HTTP 端口前端无法自知。POST body（JSON）即 run 的 input；
  - 调用合同（每张 webhook 卡片的折叠区）：
    - **版本固定**：下拉只列 `workflow.versions` 中 `status === "published"` 的版本，
      保存走 `webhook.change` patch（`api.updateWebhookInvoke`）固定 `invoke_version`；
    - **调用密钥**：「生成密钥」（32 字节 hex）或「粘贴已有密钥」（≥32 字节）两种入口；
      明文只显示一次（复制按钮 + 「服务端只存哈希，丢失需重设」警示），保存只提交
      `invoke_key_hash` = 明文的 SHA-256 hex；重设密钥覆盖旧键是不可逆操作，先确认弹窗；
    - **展示策略**：`webhook.list` 不回显 `invoke_version`/`invoke_key_hash`，未回显时
      显示「保存后立即生效，列表接口暂不回显」；服务端将来回显则直接显示
      「已固定 vN / 密钥已设置」+ 哈希前缀；
    - **调用示例**（主次按合同状态）：已配置调用合同时以 `POST /invoke/{token}`
      （Bearer 调用密钥 + 必填 `Idempotency-Key` + 可选 `X-Correlation-Id`，body=run 输入）
      为主示例 → 202 受理回执（`run_id`/`run_token`/`result_url`/`events_url`/`receipt`）。
      202 只表示受理≠业务完成；重试复用同一 `Idempotency-Key` 保证只触发一次；`run_token`
      只绑定该次 run，用于 `GET /invocations/{run_id}` 查结果（413 时返回 `output_ref`+
      `download_url`），不要外发管理令牌。`POST /hooks/{token}` 标注为运维/管理调用
      （管理 token），非业务主入口。
- **仪表盘** `/`：工作流总数、运行总数、成功率、进行中四张卡片 + 最近运行 +
  按工作流成功率。统计走 `run.stats`（服务端 GROUP BY 精确计数），派生值纯函数
  在 `src/state/dashboard.ts`（vitest 覆盖）；最近运行表只是展示窗口，走
  `run.list(limit 10)`。
- **运行归因**：runs 表带 `source`（手动 / 定时调度 / Webhook / 子流程）与
  `source_detail`（schedule id / webhook token）。运行记录页有「来源」列与
  来源筛选下拉；运行详情信息头显示来源，定时调度来源的 run 附「触发器」页链接。

## 回归运行与结果对比（P1）

- 运行详情（终态）顶部「回归」：以当前 run 为基线，选择**同一工作流**的候选版本启动新 run
  （`run.regress`）。候选复用基线记录的时钟/种子、人工输入与外部操作结果，不会重新发起
  HTTP/harness 副作用；请求摘要与基线不一致的节点会显式失败。候选版本默认取最新已发布版本。
- 回归候选的来源是 `manual` + `source_detail=regression:<baseline>`：详情头显示「回归自基线」，
  运行列表「来源」列带「回归」回链。
- 候选终结后详情头亮出「与基线比较」入口（`run.compare`，需手动点开）：展示运行状态、
  最终输出与逐节点结果的差异；
  `StoredValue` 大值引用以可行动占位展示，不泄漏内部指针。也可在运行详情手动「对比基线」
  输入任意基线 run id。
- 跨工作流回归会被限制为基线工作流：外部操作 fixture 按节点身份匹配，换工作流基本必然 miss。
- 基线 run 详情的「回归候选」折叠区按来源标记扫描本工作流最近 500 条运行，列出候选并可一键
  「与它对比」；对比覆盖递归子 run（`children.*`）、错误与请求指纹（`nodes.*.request`）差异，
  差异超过 64 条时提示截断。
- 回归弹窗「高级：覆盖外部操作结果」可显式替换录制 fixture（键是节点路径的紧凑 JSON 数组、
  fingerprint 是 64 位请求指纹、outcome 写业务值）；覆盖会落为事实，仅用于测试场景。

## 大值与列表降级（P1）

- 产品读面把 journal 值物化成业务值时有 8MB 预算：超预算时运行详情自动降级为原始信封渲染
  （时间线/日志不可用），输入/输出给「下载完整值」入口（桌面原生保存对话框 / 浏览器流式保存，
  不支持时整块下载）。
- 运行列表整页超预算时降级为原始快照扫描（手动刷新；单次最多扫描 8192 条，被截断会明示）。
- 运行列表的状态/来源筛选写入 URL：链接可分享、后退/刷新还原。

## 人工裁决 uncertain 操作（P1）

- 外部操作（http_call/harness/email）在崩溃/断链后进入「已授权但结果未知」的 uncertain 等待：
  系统绝不自动重发。运行面板显示「外部操作结果待确认」，必须人工核对后裁决：
  接受产出（可填人工认定的产出 JSON）、授权重发（明示可能重复副作用）、判定失败。
- 裁决理由必填（≤4096 字节），经 `run.adjudicate` 落为 journal 事实；它是重发的唯一授权凭据，
  自动重试只覆盖脚本/条件等可安全重算的场景。

## 使用

1. `/workflows` 点「新建」，画布自动放入 start → end 最小定义；
2. 从节点面板拖入节点，或画布空白处右键「添加节点」；手柄拖拽或按住右键从节点拖出连线
   （condition 有 真/假 两个出口，标签标在边上）；
3. 点击节点在右侧编辑参数——表单由 `nodetypes.list` 返回的 JSON Schema
   （`params_schema`，含 x-widget: code/json/workflow-picker）递归渲染；
4. 「保存」（或 Ctrl/Cmd+S）生成 draft 版本（服务端强制 validate），「发布」后可「运行」；
5. 运行时画布按节点状态着色（运行中蓝 / 完成绿 / 失败红 / 重试中黄 / 跳过灰），
   运行中节点的下游边流动动画；右侧时间线实时更新，行 hover/click 与画布节点联动；
   human_task 等待时可交付信号，运行中可取消；uncertain 外部操作在运行面板人工裁决。
6. sub_workflow 子流程节点：参数面板用下拉选择目标工作流（未发布/不存在有警告），
   右键菜单「钻取子流程」进入目标工作流（画布上方面包屑可逐级返回）；运行时节点和时间线上的
   「子 run →」链接可切换监控；编辑器内钻取用「← 父 run」返回，
   运行详情页内则跳转 `/runs/:childRunId`。

### 可观察性（节点日志与输入面）

- 节点日志（script 的 console.*、http_call 请求/响应行、condition 求值、重试/接管/等待等
  引擎叙事）写入独立的 observation store：有损、可丢弃、**不是 journal 权威事实**，
  与状态事件不是同一条管道。前端经 `run.observations.page` 按 seq 增量拉取。
- 前端两处呈现：**日志页签**（整个 run 的时间轴，级别/节点/文本过滤、跟随模式，顶部明示
  丢弃计数/历史不完整）与**节点检查器**（点画布节点或日志行打开：输入面 = 模板展开后
  params（已脱敏）、输出、错误、按 attempt 分组的节点日志）。
- `run.timeline` 节点条目带 `input`（输入面快照）；固定敏感键
  （authorization/token/password/cookie…）在展示层脱敏，`run.get` 的数据面不变。

其他画布能力：右下 Minimap、吸附网格（16px）、旧定义缺 position 时按拓扑分层兜底布局。

## 工作流 JSON 的人工输入与导入

在 definition 的 `input_schema` 中声明运行输入，`required` 指定必填字段。
点击运行时，界面读取实际执行的已发布版本，缺少输入或输入不符合约束时弹出表单；
填写后才创建运行。字段支持 `title`、`description`、`default`、`enum` 和类型约束。
例如：

```json
{
  "input_schema": {
    "type": "object",
    "required": ["project", "count"],
    "properties": {
      "project": { "type": "string", "title": "项目名称", "minLength": 1 },
      "count": { "type": "integer", "title": "数量", "minimum": 1, "default": 5 }
    },
    "additionalProperties": false
  },
  "nodes": [
    { "id": "start", "type": "start" },
    { "id": "end", "type": "end" }
  ],
  "edges": [{ "from": "start", "to": "end" }]
}
```

执行中的 `input` 节点缺少人工输入时自动弹出字段表单；取消后可在运行面板点击「填写表单」继续。
导入先补齐缺少的节点必填参数，再由服务端校验；缺少密钥时弹出密码表单，保存到密钥管理后重新校验。
通过校验后，创建或更新版本及发布在同一事务中完成；取消、校验失败均不会留下空工作流。
删除工作流采用逻辑删除：列表隐藏、触发器移除、禁止新运行，已有定义版本和运行历史保留，重启后仍可查看。
