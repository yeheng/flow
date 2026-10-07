# flow-web 前端拖拽流程编辑器

Vue 3 + Vite + TypeScript + vue-router + Tauri 2，支持浏览器与桌面应用，画布基于 @vue-flow/core。直连 flow-server
（JSON-RPC 2.0 over WebSocket），覆盖 编辑 → 保存 → 发布 → 运行 → 实时监控 全流程。

## 页面结构

| 路由                      | 页面                                                                                  |
| ------------------------- | ------------------------------------------------------------------------------------- |
| `/`                       | 仪表盘：概览卡片（工作流数 / 运行数 / 成功率 / 进行中）+ 最近运行 + 按工作流成功率    |
| `/workflows`              | 工作流列表：新建 / 打开编辑器 / 触发器 / 运行记录 / 删除                              |
| `/workflows/:id`          | 编辑器：顶部条（保存/发布/运行）+ 左节点面板、中画布、右参数与运行面板                |
| `/workflows/:id/versions` | 版本历史：版本表格（状态/校验和/时间）+ 加载到编辑器 / 以此版本发布 + 两版结构化 diff |
| `/workflows/:id/triggers` | 触发器：定时调度（cron + 下次触发，10s 轮询）与 webhook（hook URL 复制 / curl 示例）  |
| `/workflows/:id/runs`     | 该工作流的运行历史（3s 轮询）                                                         |
| `/runs`                   | 全局运行历史（同上组件，不过滤）                                                      |
| `/runs/:runId`            | 运行详情：信息头 + 只读 DAG（按 run 钉死的版本渲染、保留运行态着色）+ 实时时间线      |

全局通知走右上角 Toast，确认/输入走 Modal（替代原生 prompt/confirm）；
有未保存修改时离开编辑器路由会先确认。

## 启动

```bash
# 终端 1：后端（仓库根）
cargo run -p flow-app -- server       # 默认 ws://127.0.0.1:9800

# 终端 2：前端
cd web
npm install
npm run dev                          # 默认 http://127.0.0.1:5173
```

### 浏览器 / 桌面双入口

两种模式共用 `src/` 和服务端方法实现，调用层自动选择：

- **桌面**：应用启动时自动启动内嵌的 flow-server 服务，普通请求经 Tauri `invoke`
  直接调用 Rust 方法模块，订阅经 Tauri Channel 推送，无需另开后端进程或 RPC 端口。
- **浏览器**：经原有 JSON-RPC WebSocket API 连接独立的 `flow server`，支持远程部署。

桌面开发与安装包行为一致，执行 `npm run dev:desktop` 或打开 `Flow.app` 即可使用。

| 命令（在 `web/` 下执行） | 用途 |
| --- | --- |
| `npm run dev:browser` | 启动 Vite 并打开浏览器 |
| `npm run dev:desktop` | 启动 Vite 并打开 Tauri 桌面窗口 |
| `npm run build` | 浏览器静态产物 `dist/` |
| `npm run preview` | 本地预览浏览器构建 |
| `npm run build:desktop` | 构建当前平台的桌面安装包 |
| `npm run build:desktop -- --bundles app` | macOS 仅生成 `.app`，跳过 DMG |

桌面开发需要 Rust stable 与平台构建依赖：macOS 安装 Xcode Command Line Tools；
Linux 安装 WebKitGTK 4.1 等 [Tauri 系统依赖](https://v2.tauri.app/start/prerequisites/)。
当前内嵌后端沿用仓库的 Unix 运行时依赖，已验证 macOS；Windows 还需要后端适配。
浏览器前端构建只需要 Node.js。

`dev:desktop` 自动启动 Vite，不需要提前运行 `dev`；5173 被占用时会明确报错。
窗口打开后，也可以在浏览器访问同一开发服务（浏览器仍需独立 API 后端）。桌面使用 hash 路由（如
`/#/workflows`），浏览器保留 history 路由；浏览器生产部署需将未知页面路径回退到 `index.html`。

原生工程位于 `src-tauri/`，有独立 Cargo workspace / lockfile，避免后端构建引入桌面系统依赖。
默认桌面产物位于 `src-tauri/target/release/bundle/`（设置 `CARGO_TARGET_DIR` 时随之改变）。
安装包必须在对应平台构建；对外分发的签名、公证需由发布环境配置。

桌面数据默认存放在 Tauri 应用数据目录（macOS 为
`~/Library/Application Support/com.flow.workflow.desktop/`），普通工作流保存在 `flow/`，
JSONL 工作区保存在独立的 `journal/`，不会混用两套数据。需要隔离开发数据时，在启动前
设置 `FLOW_DESKTOP_DATA_DIR=/绝对路径`。桌面不会自动导入原服务器数据目录。

应用启动会恢复本地运行并启动定时调度；退出时停止订阅、HTTP 和调度任务，关闭 Journal
并回收后端运行时。未完成的运行按现有后端恢复规则在下次启动处理，退出不会将它们标为取消。
同一数据目录的第二个实例会拒绝启动，避免多个进程同时写入。

webhook 仍需接收 HTTP 请求，桌面仅在 `127.0.0.1` 随机端口开放此入口，触发器页面展示
实际地址（每次启动可能变化）。JSONL 工作区在桌面直接打开本地数据，无需地址和令牌；
完整值下载走原生保存对话框和 Rust 流式写入。浏览器下载仍使用 `showSaveFilePicker`，
不支持该 API 时提示使用 CLI。

传输适配位于 `src/rpc/`，内嵌服务和 Tauri 命令位于 `src-tauri/src/`。桌面始终使用
本地服务；以下 `config.json` 和 `VITE_FLOW_*` 地址设置仅作用于浏览器。

浏览器 flow-server 地址可用环境变量覆盖（需在 vite 启动前设置）：

```bash
VITE_FLOW_RPC=ws://127.0.0.1:9800 npm run dev      # JSON-RPC WebSocket
VITE_FLOW_HTTP=http://127.0.0.1:9801 npm run dev   # webhook HTTP 入口（触发器页展示 hook URL 用）
```

部署时也可不改构建：在 web 根目录放 `config.json`（见 `public/config.example.json`），
启动时拉取一次，优先级高于 `VITE_FLOW_*`：

```json
{ "rpcUrl": "wss://flow-server.example.com", "httpUrl": "https://flow-hooks.example.com" }
```

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
触发器页（webhook 复制/POST 触发、非法 cron 报错）、仪表盘与 `run.stats` 对账。

## 编辑器交互（P1）

- **Undo/Redo**：Cmd/Ctrl+Z 撤销、Cmd/Ctrl+Shift+Z 或 Ctrl+Y 重做；快照式历史栈（上限 100），
  覆盖增删节点/边、连线、参数编辑（按 node+field 合并连续输入）、节点拖拽（整段合并为一条）、
  粘贴、自动布局；undo 回到上次保存的快照时「未保存」标记自动消失（脏标记是保存点的派生值）。
- **多选与复制粘贴**：Shift+左键拖框选、Cmd/Ctrl+点击多选；Cmd/Ctrl+C 复制选中节点及其内部边，
  Cmd/Ctrl+V 粘贴（重新生成节点 id、位置逐次偏移 32px、内部边重连、外部边不复制、
  受 max_instances 限制）；Delete/Backspace 删除选中。快捷键在输入框聚焦时不劫持。
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
  - Webhook：新建按钮 + 列表（完整 hook URL 一键复制、curl 示例展开、启停、删除）。
    hook URL 基址 = `VITE_FLOW_HTTP`（默认 `http://127.0.0.1:9801`），因为
    flow-server 的 HTTP 端口前端无法自知。POST body（JSON）即 run 的 input。
- **仪表盘** `/`：工作流总数、运行总数、成功率、进行中四张卡片 + 最近运行 +
  按工作流成功率。统计走 `run.stats`（服务端 GROUP BY 精确计数），派生值纯函数
  在 `src/state/dashboard.ts`（vitest 覆盖）；最近运行表只是展示窗口，走
  `run.list(limit 10)`。
- **运行归因**：runs 表带 `source`（手动 / 定时调度 / Webhook / 子流程）与
  `source_detail`（schedule id / webhook token）。运行记录页有「来源」列与
  来源筛选下拉；运行详情信息头显示来源，定时调度来源的 run 附「触发器」页链接。

## 使用

1. `/workflows` 点「新建」，画布自动放入 start → end 最小定义；
2. 从节点面板拖入或直接点击添加节点（面板按类别分组），拖动手柄连线
   （condition 有 真/假 两个出口，标签标在边上）；
3. 点击节点在右侧编辑参数——表单由 `nodetypes.list` 返回的 JSON Schema
   （`params_schema`，含 x-widget: code/json/workflow-picker）递归渲染；
4. 「保存」（或 Ctrl/Cmd+S）生成 draft 版本（服务端强制 validate），「发布」后可「运行」；
5. 运行时画布按节点状态着色（运行中蓝 / 完成绿 / 失败红 / 重试中黄 / 跳过灰），
   运行中节点的下游边流动动画；右侧时间线实时更新，行 hover/click 与画布节点联动；
   human_task 等待时可交付信号，运行中可取消。

### 可观察性（节点日志与输入面）

- 后端把节点日志（script 的 console.*、http_call 请求/响应行、condition 求值、
  重试/接管/等待等引擎叙事）作为 `node_log` 事件写进同一条事件流——与状态事件
  同一 seq 空间、同一订阅回放，无第二套日志管道（设计见 `docs/observability-design.md`）。
- 前端两处呈现：**日志页签**（整个 run 的时间轴，级别/节点/文本过滤、跟随模式）
  与**节点检查器**（点画布节点或日志行打开：输入面 = 模板展开后 params（已脱敏）、
  输出、错误、按 attempt 分组的节点日志）。
- `run.timeline` 节点条目带 `input`（输入面快照）；固定敏感键
  （authorization/token/password/cookie…）在展示层脱敏，`run.get` 的数据面不变。
6. sub_workflow 子流程节点：参数面板用下拉选择目标工作流（未发布/不存在有警告），
   双击节点钻取子流程（画布上方面包屑可逐级返回）；运行时节点和时间线上的
   「子 run →」链接可切换监控；编辑器内钻取用「← 父 run」返回，
   运行详情页内则跳转 `/runs/:childRunId`。

其他画布能力：右下 Minimap、吸附网格（16px）、旧定义缺 position 时按拓扑分层兜底布局。
