# 常驻插件与 Web UI

入口：设置 → 插件服务。服务绑定运行后，该运行详情页自动显示插件 tab；切回运行视图或切换运行会销毁对应页面桥，业务执行不受影响。管理员可启动、停止服务并打开插件界面。关闭页面不停止服务，后端关闭会回收插件进程树。退出后的实例不会隐式重放请求；再次启动产生新 instance。旧 instance 的调用被拒绝。

## 插件声明

```toml
[plugin]
id = "resident-echo"
version = "0.1.0"
entry = "bin/resident_echo"
protocol = 2
node_types = []

[service]
methods = ["echo"]
peers = ["other-plugin"]
web_ui = "web-ui/index.html"
```

service-only 插件可不声明节点类型和 methods.invoke；兼具节点能力时，仍保留原节点声明及执行协议。服务是单独进程角色，不占节点执行槽。`methods` 为对外调用白名单，`peers` 为可访问其他服务的插件 ID 白名单。启动服务当前要求管理员身份，未开放普通用户的细粒度服务授予。

插件安装目录仍是 Flow 主目录的 plugins/<plugin-id>。可运行样例在 `examples/resident-echo/`，执行文件由以下命令生成：

```sh
cargo build --offline -p flow-plugin-sdk --example resident_echo
```

将样例的 plugin.toml、web-ui/ 和构建产物（放到 bin/resident_echo）放入插件目录。示例通过真实 IPC 处理多个请求并产生 echo.received 事件。

## 会话与消息

复用 SDK JSON-RPC 信封及帧协议，握手显式协商 `interaction_events_v1` 能力。宿主先发 initialize，插件响应。SDK 的 `duplex::pump` 提供一个读取者和一个写入者；请求、响应、通知可并发流动，不会因发送消息丢弃读取到一半的帧。常驻服务不替换现有单次节点 invoke。

宿主为插件/UI绑定身份，不接受页面伪造目标实例；消息 DTO 提供 Endpoint、Command/Event/Query/Response。临时服务通知带源端点、序号及事件 envelope。历史节点事件继续使用 Journal 的事实格式，由受限宿主适配器读取。

每服务最多 64 个在途请求、64 条发送队列、16 个反向宿主请求。方法和消息身份最长 128 字节，调用 payload 最大 64 KiB，单次调用超时 30 秒。超时或断线报告结果可能未知，不自动重试有副作用的动作。

### 宿主管理 RPC

- `services.list` → `{services:[{plugin_id,instance,alive,ui}]}`。
- `services.start {plugin_id,binding?:{run_id,node_id}}` → `{instance}`。绑定必须指向当前已有的活动节点；服务运行中不能重新绑定，应先停止。
- `services.stop {plugin_id,instance}` → 停止并等待进程回收，撤销 UI 资源地址。
- `services.call {plugin_id,instance,method,params}` → `{value: 方法结果}`，避免插件数据被误认成宿主命令回执；不隐式重试。
- `services.subscribe {plugin_id,instance,after:"0"}`：Flow 原生订阅，推送 services.event；取消使用 services.unsubscribe。先捕获实时订阅再补读历史，去重后交付。
- `services.events {plugin_id,instance,after:"0"}` → `{cursor,events,alive}`，序号使用无损十进制字符串。

start/stop/call/events 当前都要求管理员；list 仅包含运行元数据。服务通知为临时有界流，保留最近 128 条，游标超出窗口报 resync_required，不伪装成可靠历史。消费者可重新打开界面重建会话，不能依赖这些通知恢复业务。

### 插件 → 宿主 / 其他插件

插件应在收到宿主 `flow/ready` 后建立订阅，并在主处理期间持续读取反向 JSON-RPC 请求与通知：

- `flow/subscribe {subscription,stream:"peer"|"node",params:{...}}`：订阅 peer 的临时事件或绑定节点的持久事件；params 使用下述 events 查询参数。subscription 为插件选定的非空 ID（最长128），因此可以在请求回执到达前关联事件。宿主通过 flow/event 通知推送，结束或失败时发送 flow/stream_closed。最多16个并行订阅。
- `flow/unsubscribe {subscription}`：取消自身订阅并释放任务。
- `flow/services {}`：只列出 peers 中已启动的服务及当前 instance。
- `flow/call {plugin_id,instance,method,params}`：调用声明的 peer；目标自己的 methods 白名单仍生效。
- `flow/events {plugin_id,instance,after:"0"}`：补读声明 peer 的临时事件，调用方可轮询并推进游标。
- `flow/node.get {}`：查询启动时绑定的节点状态及运行序号。
- `flow/node.events {cursor?,since_lsn?:"0"}`：分页读取绑定节点的持久 Journal 事件。翻页沿用 next_cursor；快照完成后使用 snapshot_cursor.lsn 作为下次 since_lsn。空页也须推进游标。
- `flow/node.signal {request_id,payload}`：向绑定的 input/human_task 节点投递持久信号。request_id 稳定重试，宿主按来源插件和绑定范围生成回执身份。执行尝试身份在预检及最终提交时校验，旧绑定不能回答新尝试。

未绑定节点时禁止访问节点方法；消息中的 run_id/node_id 不能覆盖绑定。未声明 peer、未知宿主方法、陈旧实例均拒绝。节点信号复用既有 Journal 事务和幂等回执，没有增加第二套权威状态或外部消息队列。

## web-ui 数据桥

当前支持单个自包含 HTML，最大 256 KiB。路径必须在插件目录内，禁止绝对路径和目录越界。启动时读取并固定该实例的内容；不是可任意读取插件目录的静态文件服务器。外置 JS/CSS/字体资产和历史 UI bundle 管理尚未加入。

宿主通过临时只读资源句柄提供页面；句柄不具备 RPC 权限。页面在 `sandbox="allow-scripts"` iframe 中运行，资源响应附加 CSP（禁用连接、表单和默认资源）、no-store、no-referrer、nosniff。数据桥不传递宿主 token、Tauri 权限或全局状态。浏览器隔离不代表插件可执行文件受到 OS 沙箱保护，服务仍在宿主账号下运行。

HTML 在加载时监听宿主连接，接收专属 MessagePort：

```js
window.addEventListener("message", event => {
  if (event.source !== parent || event.data?.type !== "flow-ui-connect"
      || event.data.version !== 1 || event.ports.length !== 1) return;
  const port = event.ports[0];
  port.onmessage = event => console.log(event.data);
  port.postMessage({id:"request-1",method:"echo",params:{text:"hello"}});
});
```

宿主使用 Flow 原生订阅向页面推送，不通过页面定时请求模拟。返回 `{id,result}` 或 `{id,error}`；事件消息为 `{type:"event",sequence,source,event}`。服务停止/事件流断开会通知 disconnected。桥只调用选中服务的当前 instance，页面不能选择其他目标。页面销毁或重载时关闭旧 port，迟到结果不投递给新页面。

桥限制每页每秒 32 个请求、最多 8 个在途请求及消息大小。沙箱不开放原生表单提交，交互按钮应使用 type=button 和脚本消息。不要把宿主密钥放进 HTML 或通知 payload。

## 验证

```sh
cargo test --offline -p flow-backend -p flow-config -p flow-plugin-sdk -p flow-rpc --lib
cargo build --offline -p flow-plugin-sdk --example resident_echo
cargo run --offline -p flow-backend --example resident_smoke -- target/debug/examples/resident_echo
npm --prefix web run test
npm --prefix web run typecheck
npm --prefix web run lint
npm --prefix web run build
npm --prefix web run test:e2e -- resident-services.spec.ts
```

浏览器用独立 FLOW_HOME 和临时插件目录，覆盖真实子进程双向通信、跨插件转发、越权与陈旧身份、iframe 父页面隔离、页面关闭后服务存活、资源撤销、绑定节点事件读取及幂等回答。

当前边界：手动监管和重启；临时服务通知不持久化；持久业务交互通过既有节点信号实现；不支持进程内存透明恢复、历史自定义 UI 包留存。桌面 CSP 已接线，但原生 Tauri 窗口尚未做本次端到端验证。


桌面验证前置：测试二进制不位于主工作区执行器旁，应设置 `FLOW_EXECUTOR_BIN` 为已经构建的 flow-executor 绝对路径再运行 `cargo test --offline --manifest-path web/src-tauri/Cargo.toml`。本次已验证桌面编译与三项原生服务集成测试，原生窗口的视觉/交互仍未单独验收。
