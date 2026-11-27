# flow playwright 插件

[playwright](https://playwright.dev) 节点：在工作流里跑一段浏览器自动化脚本。
部署后节点面板出现「Playwright 浏览器」节点（集成分组）；节点参数里配置
JS 脚本，run 时插件进程拉起 `node runtime/wrapper.js`（cwd = 插件根），
用 Playwright 打开浏览器执行脚本，输出落 journal 供下游节点消费。

```
start ──▶ playwright ──▶ script（读 nodes.pw.result）──▶ end
```

## 布局

```
plugins/playwright/
  plugin/            # Rust 插件进程（flow-playwright-plugin）：协议壳 + 超时执法
    plugin.toml      # manifest：节点类型 / 参数表单 / 权限声明
    src/lib.rs       # 契约常量 + 执行核心（spawn node、整组击杀、信封解析）
    src/main.rs      # 通道引导 + initialize 握手 + 会话循环
    tests/self_test.rs
  runtime/           # Node 执行环境（部署时随插件拷走）
    wrapper.js       # 执行器：stdin 收参数 → 跑脚本 → stdout 哨兵行回信封
    package.json     # 依赖 playwright（npm install 装进部署目录，不入库不入内容哈希）
```

## 安装

```sh
# 1. 构建（仓库根）
cargo build -p flow-playwright-plugin

# 2. 部署到 <flow_home>/plugins/playwright/（幂等脚本，已装则跳过）
scripts/install-plugins.sh

# 3. 装 Node 依赖与浏览器（在部署目录里，一次性）
cd ~/.flow/plugins/playwright/runtime
npm install                     # 网络不畅可加 --ignore-scripts 跳过 fsevents 编译
npx playwright install chromium # 只下无头壳可加 --only-shell

# 4. 授予插件（管理员）：Web 设置页操作，或 JSON-RPC
#    {"method": "permissions.grant", "params": {"plugin_id": "playwright", "request_id": "<id>"}}
```

浏览器下载慢/卡住时：直连 `cdn.playwright.dev` 在部分网络下极慢，可改用
npmmirror 的 chrome-for-testing 镜像手动落位（版本号按报错提示替换）：

```sh
curl -L -o shell.zip "https://cdn.npmmirror.com/binaries/chrome-for-testing/<ver>/mac-arm64/chrome-headless-shell-mac-arm64.zip"
unzip shell.zip -d ~/Library/Caches/ms-playwright/chromium_headless_shell-<rev>/
touch ~/Library/Caches/ms-playwright/chromium_headless_shell-<rev>/INSTALLATION_COMPLETE
```

要求：本机 `node`（PATH 可见，执行器环境白名单含 PATH/HOME）。

## 节点参数

| 参数 | 类型 | 必填 | 缺省 | 说明 |
| --- | --- | --- | --- | --- |
| `script` | code(js) | 是 | — | JS 函数体，见下方契约 |
| `browser` | enum | 否 | `chromium` | `chromium` / `firefox` / `webkit` |
| `headless` | boolean | 否 | `true` | 有头模式需要完整浏览器（`npx playwright install chromium` 不带 `--only-shell`） |
| `timeout_ms` | number | 否 | `60000` | 1000..600000；**插件侧强制执行**（到点整组击杀 node+浏览器，runtime 对插件节点不掐表） |

### 脚本契约

脚本是一段**带 `return` 的 JS 函数体**（async），拿到四个全局：

- `page` / `context` / `browser`：Playwright 的 `Page` / `BrowserContext` / `Browser`
- `console`：被捕获进 logs（`log/info/warn/error/debug`），页面 `console.*` 与未捕获异常同样进 logs

```js
// 脚本示例：打开页面取标题与正文首段
await page.goto('https://example.com');
return {
  title: await page.title(),
  first: await (await page.locator('p').first().textContent()),
};
```

返回值必须是可 JSON 序列化的（`undefined` → `null`，BigInt → 字符串）；
循环引用报错不静默丢数据。进程工作目录 = 插件根（`~/.flow/plugins/playwright`），
脚本里的相对路径从这里解析；`require('playwright')` 也可直接用。

### 输出（下游引用）

节点输出形如：

```jsonc
nodes.pw = {
  "status": "success",          // 或 "failed"（failed 时节点失败，阻断下游）
  "browser": "chromium",
  "headless": true,
  "duration_ms": 1823,
  "logs": [{"level": "log", "text": "..."}],   // 上限 64 KiB，超限从旧到新截
  "result": { "title": "Example Domain" }      // 脚本 return 值
}
```

下游 script 节点直接 `nodes.pw.result.title`。

## 工作流权限块（必写）

playwright 节点是副作用节点：定义必须带顶层 `permissions`，且要覆盖插件
声明的必需能力 `pw-exec`（exec，`program = "node"`，
`argv_exact = ["runtime/wrapper.js"]`）。workflow 层镜像写法（`executable`
填执行机上的 node 绝对路径，`which node` 查）：

```jsonc
{
  "name": "playwright-demo",
  "definition": {
    "semantics": 2,
    "permissions": {
      "version": 1,
      "mode": "execute",
      "allow": [
        {
          "id": "run-node",
          "action": "exec",
          "executable": "/opt/homebrew/bin/node",
          "argv_exact": ["runtime/wrapper.js"],
          "approval": "auto"
        }
      ]
    },
    "nodes": [
      { "id": "start", "type": "start" },
      { "id": "pw", "type": "playwright", "name": "取标题", "params": {
        "script": "await page.goto('https://example.com');\nreturn { title: await page.title() };",
        "timeout_ms": 30000
      }},
      { "id": "end", "type": "end" }
    ],
    "edges": [
      { "from": "start", "to": "pw" },
      { "from": "pw", "to": "end" }
    ]
  }
}
```

插件未部署 / 未授予 / 定义没写权限块 → `run.start` 拒绝（`-32020`）。

## 已知约束（读一遍再写脚本）

1. **脚本里别写 JS 模板字面量 `` `${x}` ``**：引擎对插件节点参数做全量
   `${}` 模板展开（展开失败 = 节点直接失败）。反面是 feature：上游数据可以
   注进脚本文本，如 `"await page.goto('${nodes.fetch.body.url}')"`。要字面
   `$` 就用 `'$' + '{x}'` 拼接。
2. **非沙箱**：脚本是全权 Node 代码（能读文件、发请求、起进程）。权限模型
   按副作用节点对待；无 OS 沙箱的平台不承诺进程内细粒度执法（net/file 上限
   是 manifest 信息性声明）。
3. **输出 ≤ 8 MiB**（journal 捕获预算）：超限报 `-32021`，logs 先截、
   `result` 不截（不静默给你半截数据）。
4. **超时单钟**：只有插件掐表，到点 SIGKILL 整个进程组（node + 浏览器），
   信封不会送达——超时的 run 拿到 `-32023`。
5. **stdout 是结果通道**：wrapper 在 stdout 末行打 `__FLOW_PW__<JSON>`；
   脚本里 `console.log` 已被捕获，但直接 `process.stdout.write` 会污染通道，
   别用。
6. 浏览器进程偶发残留（被 SIGKILL 时没机会清理）是已知代价；下次同版本
   安装会复用浏览器缓存目录，不受影响。

## 契约速查（错误码）

| code | 场景 |
| --- | --- |
| `-32020` | 脚本失败 / 浏览器崩溃 / wrapper 未回信封（不确定类） |
| `-32021` | 输出超过 8 MiB 捕获上限 |
| `-32022` | 参数畸形（防御纵深；发布期 manifest 表单先拦一道） |
| `-32023` | 超时（进程组已击杀） |
| `-32024` | 环境缺失：node 不在 PATH / runtime 未拷 / playwright 未 npm install |

注意：宿主侧（runtime `plugin_node`）当前把插件的错误响应统一收敛为
Uncertain 且不透传 message——排障看执行器 stderr 日志（插件把失败详情
也打在 stderr）。
