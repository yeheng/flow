# 插件线协议规范（wire.md）

> 状态：v1（`protocol_version = 1`）。
> 本文档与 `crates/plugin-proto/tests/vectors/` 共同构成**对外契约本体**；
> Rust crate `plugin-proto` 只是契约的一种实现，实现之间以本文档 + 向量为准。
> 向量**只增不改**；改本文档的任何 wire 语义 = bump 主版本。

## 0. 这份协议是什么

宿主与插件（或任何子进程 / 远端进程）在**一条可靠有序的字节流**上通信。
字节流从哪来不在本规范范围内：本地 socket、named pipe、stdio、TCP 都行。
本规范定义字节流里的帧与消息。

**本地 spawn 的配套约定**（参考实现 `proc-ipc` crate）：

- unix：宿主用 socketpair 建通道，在 exec 前把通道 dup2 到**固定号 3**，
  经环境变量 `PROC_IPC_FD`（恒为 `"3"`）告知子进程；其余继承 fd 一律
  CLOEXEC，环境变量不继承（白名单注入）。
- **通道 fd 是非阻塞的**（宿主侧异步运行时所需）。插件侧自行归一：
  同步实现把它设回阻塞，异步实现直接注册事件循环。
- 非 unix（Windows named pipe / 回连）：经 `PROC_IPC_ADDR` 回连 +
  `PROC_IPC_TOKEN` 令牌回验（名字本机可见，令牌防抢连）。

三条设计原则，读后面章节时随时对照：

1. **显式类型**：一帧是什么，帧头说了算（kind 字节）。不做「靠载荷结构猜类型」。
2. **版本门集中在握手**：握手时主版本不等就拒绝并关闭，不降级；逐帧不带版本号。
3. **未知即明确**：未知方法回 `-32601`，未知能力忽略，未知 kind 关连接。没有静默路径。

---

## 1. 帧格式（Envelope）

```
+--------+------------------+--------------------+
| u8 kind| u32 body_len (BE)| body (body_len B)  |
+--------+------------------+--------------------+
```

- `kind`：
  - `0x01` = **rpc**：body 是 UTF-8 编码的 JSON-RPC 2.0 消息（见 §2）。
  - `0x02` = **binary**：body 是原始字节，语义由上层的 rpc 消息约定（见 §4）。
  - 其他值：**协议错误，立即关闭连接**。未知 kind 不是「跳过」，是「断开」——
    跳过一个未知 kind 意味着双方对流的解释已经分叉，继续说下去只会更错。
- `body_len`：大端 u32。
- 单帧 body 上限 **1 MiB（1,048,576 字节）**。超限：拒绝该连接，不截断、不协商。
  更大的数据必须由上层切成多个 `0x02` 帧（建议每块 ≤ 256 KiB）。
- 没有 magic、没有校验和：承载已是可靠字节流，帧损坏只会表现为长度与
  内容不符，按 §1.1 处理。

### 1.1 流上的切分规则

- `body_len` 声明多少就读多少：读到不足声明长度就 EOF = **协议错误**
  （半帧），不是干净关闭。
- 帧头之前出现 EOF = 干净关闭。
- 实现必须能处理任意切分（半包/粘包是承载的现实，不是异常）。

### 1.2 为什么不做「帧内再压缩 / 再编码」

控制消息都很小；大二进制走 `0x02` 原始字节（对比 JSON base64 的 +33%
膨胀）。任何再编码都会把「一帧一种含义」变成「一帧两层解析」，跨语言
实现的出错面直接翻倍。

---

## 2. 消息格式（JSON-RPC 2.0 子集）

body（kind=0x01）是**单个 JSON 对象**，遵循 JSON-RPC 2.0，但收窄为子集：

### 2.1 三种消息

**Request**（期待响应）：

```json
{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"tool":"grep"}}
```

**Notification**（不期待响应；没有 `id` 字段）：

```json
{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":42}}
```

**Response**（`result` 与 `error` 恰居其一）：

```json
{"jsonrpc":"2.0","id":1,"result":{"ok":true}}
```

```json
{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}
```

### 2.2 收窄规则（违反 = `-32600` Invalid Request）

| 规则 | 理由 |
| --- | --- |
| 顶层数组（batch）**禁止** | 批次的部分失败语义各家分歧大，且无收益 |
| `id` 只允许 string / number（i64 范围） | 规范允许的怪异 id 类型只会增加实现面 |
| `id: null` 只允许出现在 **error 响应** | 对齐规范的「请求无法解析」兜底形态 |
| `jsonrpc` 必须恰好是 `"2.0"` | 没有协商空间 |
| `method` 必须是非空字符串 | 空方法名按结构非法处理 |
| Response 必须有 `id` 字段（可为 null，见上） | 无 id 的响应无法配对 |
| 未知方法 → `-32601`，**不得静默丢弃** | 静默会把「拼错方法名」变成「插件没反应」 |

### 2.3 错误码

| 码 | 名 | 用途 |
| --- | --- | --- |
| -32700 | Parse error | body 不是合法 JSON |
| -32600 | Invalid request | 违反 §2.2 任意一条 |
| -32601 | Method not found | 方法不存在 |
| -32602 | Invalid params | 参数结构非法 |
| -32603 | Internal error | 处理器内部错误 |
| -32001..=-32099 | Application | 应用自定义（**-32000 保留不用**，-32100 以下拒绝） |

`error.data` 可携带任意 JSON 上下文；协议层不解释它。

### 2.4 规范形（canonical form）

**解析侧**必须接受任意合法 JSON（键序无关、空白随意）。
**编码侧**应当输出规范形（golden vectors 钉住的形态）：

1. 信封字段固定序：`jsonrpc, id, method, params` / `jsonrpc, id, result` / `jsonrpc, id, error`；
2. `error` 对象固定序：`code, message, data`；
3. 其余嵌套对象（`params` / `result` / `error.data` 内部）按键**字典序（码点序）**排列；
4. 分隔符紧凑（无空白）、UTF-8、无 BOM；
5. 数值为最短往返表示；id 数值限定 i64。

这套规则任何语言 5 行代码可实现，且与参考实现的默认输出逐字节一致。

---

## 3. 握手（initialize）

连接建立后，**宿主先说话**。状态机：

```
host                          plugin
  |--- initialize (request) --->|
  |<---- initialize result -----|   主版本相等 → 继续
  |<---- error (-32001) --------|   主版本不等 → 双方关闭
  | [正常消息交换阶段]           |
  | [任一侧关闭通道 = 会话结束]  |
```

### 3.1 initialize 请求

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{
  "protocol_version":1,
  "capabilities":["binary_blocks"],
  "client_info":{"name":"example-host","version":"1.2.3"}}}
```

- `protocol_version`：u32，**高 16 位主版本，低 16 位次版本**。
  - 主版本不等 → 插件回 `-32001`（message 必须包含双方主版本号），双方关闭。
  - 次版本只用于诊断，不参与裁决。
- `capabilities`：宿主能力目录（见 §3.3）。
- `client_info` / `server_info`：实现标识（至少含 `name` 与 `version` 字符串）。

### 3.2 initialize 响应

```json
{"jsonrpc":"2.0","id":1,"result":{
  "protocol_version":1,
  "capabilities":["binary_blocks","reverse_requests"],
  "server_info":{"name":"example-plugin","version":"0.3.0"}}}
```

握手完成前，任何其他消息都是协议错误。

### 3.3 能力目录（闭集）

| 能力 | 含义 |
| --- | --- |
| `reverse_requests` | 宿主愿意接受插件发来的 **request**（反向调用） |
| `binary_blocks` | 本侧接受 `kind=0x02` 帧 |

- 能力名是**闭集**：未知名字在握手时按 `-32602` 拒绝（闭集让「双方对
  同一个名字的理解」永远一致；新增能力名 = bump 次版本）。
- 能力是**目录**不是开关：对方没声明的能力就不使用，不问、不猜、不降级重试。
- `reverse_requests` 只改变「谁可以发 request」；notification 与 response
  两个方向永远合法（response 是对已发 request 的义务）。

### 3.4 id 空间

- 每个方向各自维护请求 id 计数，**从 1 开始递增**；
- 不得复用 id（响应迟到时旧 id 复用会造成错配对）；
- 建议：宿主用 number，插件反向 request 用 string（`"r-1"`），排障时一眼分清方向。

### 3.5 会话令牌回验（回连承载）

本地回连承载（Windows named pipe / fs socket，见 §0）的名字对本机可见，
任何本地进程都能尝试连接。令牌把「被 spawn 的那个子进程」和「抢连者」
分开：

1. 宿主 spawn 时生成随机令牌，经 `PROC_IPC_TOKEN` 注入子进程环境；
2. 插件在 `initialize` 响应的 `result.session_token` 字段**回显**它；
3. 宿主核对：不等 → 回 `-32002` 并**立即杀掉会话**，不给重试机会
   （重试等于给抢连者猜令牌的机会窗口）。

**方向不可反**：宿主永不把令牌发给对端——发出去就等于把门钥匙进给
了抢连者。只有真正被 spawn 的子进程才可能从 env 里知道它。

unix fd 承载无需令牌（fd 本身就是能力）：双方均省略该字段，核对
按「双方缺失 = 通过」处理。规范形：字段缺省时不序列化（见向量
`initialize_response` 与 `initialize_response_with_token` 的差异）。

### 3.6 兼容性说明

---

## 4. 二进制块（kind=0x02）

- 语义由**配对的 rpc 消息**约定：上层协议用 rpc 商定一个传输标识与总量，
  再在**同一条流**上交替发送 `0x02` 帧。
- 每个 `0x02` 帧的 body 建议携带最小的自描述头（由上层定义，如
  `transfer_id + offset`）；本规范不规定块内结构，只保证**顺序可靠**。
- 建议 chunk ≤ 256 KiB；必须 < 1 MiB（§1 帧上限）。
- 未协商 `binary_blocks` 能力就发 `0x02` 帧 = 协议错误，关闭连接。

---

## 5. 生命周期

- **取消**：`{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":<被取消的 id>}}`。
  通知语义：不保证目标已收到；对端可以选择继续完成（响应照常返回），实现
  不得把「已取消」当成「已应答」。
- **关闭**：任一侧直接关闭字节流即可，无需告别消息（有别的 rpc 语义要
  表达「我要关了」是上层的自由）。
- **背压**：本协议的所有实现**必须有界排队**——读端停摆时，写端的发送
  缓冲必须有上限并阻塞发送（不是丢弃，不是无界增长）。字节流的天然
  背压就是防线，别在实现里把它绕掉（比如无界的本地发送队列）。

---

## 6. 兼容性承诺

| 变更 | 是否破坏 | 要求 |
| --- | --- | --- |
| 新增 JSON-RPC 方法 | 否 | 老实现自然回 `-32601` |
| 新增能力名 | 否（但需 bump 次版本） | 闭集校验会拒绝未知名，双方升级后互通 |
| 新增帧 kind | 否 | 老实现按协议错误关闭，行为已定义 |
| 改帧头 / 现有 kind 语义 / initialize 字段语义 | **是** | bump 主版本 + 全量新向量集 |

实现者自查清单：

1. 帧编解码对 `tests/vectors/` **逐字节一致**（含规范形再编码）；
2. 半包 / 粘包 / 超限 / 未知 kind 四种输入的行为符合 §1；
3. batch、非法 id、result+error 并存 → `-32600`；
4. 主版本不等 → `-32001` 且 message 含双方版本，随后关闭；
5. 无界队列不存在；读端停摆时写端会阻塞而不是涨内存。

---

## 7. 向量文件格式

`tests/vectors/*.json`：

```json
{
  "name": "initialize_request",
  "kind": "rpc",
  "message": { ... },          // rpc：语义 JSON（键序无关比较用）
  "bytes_hex": "01 000000.."   // 完整帧（kind + len + body）的 hex
}
```

binary 向量额外携带 `payload_hex`（body 的 hex）。其他语言的用法：
拷走整个目录，断言 (a) 你的解码器吃下 `bytes_hex` 得到 `message`/`payload`；
(b) 你的编码器对同一消息输出**完全相同**的 `bytes_hex`。

---

## 8. 参考实现

| 实现 | 位置 | 说明 |
| --- | --- | --- |
| Rust（宿主 + 插件两侧） | `crates/plugin-proto` | 帧编解码、JSON-RPC 子集、initialize；对向量逐字节断言 |
| Rust 传输层 | `crates/proc-ipc` | spawn + 通道交接 + fd 收敛 + 进程回收（§0 的配套约定） |
| Python（插件侧） | `examples/plugin-py/plugin_rpc.py` | 纯标准库；「第二种语言能独立复现向量字节」是规范成立的证据 |

Python 侧自检与用法：

```sh
python3 examples/plugin-py/test_vectors.py   # 向量逐字节校验
python3 examples/plugin-py/plugin_rpc.py     # 作为插件被宿主拉起
```

`crates/plugin-proto/tests/python_e2e.rs` 是跨语言端到端测试：Rust 宿主
拉起真实 python3 插件，完成握手、请求往返、-32601、通知忽略与进程回收。
机器上没有 python3 时该测试自动跳过（不算失败）。

维护规则：**改协议必须同时改三个地方**——本文档、向量集（只增不改）、
两个参考实现；做不到就别改。
