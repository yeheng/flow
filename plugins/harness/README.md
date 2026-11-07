# agent-plugin — harness agent 插件（独立项目）

harness agent 的**独立运行时项目**：agent 循环、provider、headless 装配
全部住在这里，以插件进程 `flow-agent-plugin` 的形态被宿主（flow-executor）
spawn，经 [plugin-proto](../docs/protocol/wire.md) JSON-RPC 对话。

```
plugins/harness/
  agent-core/    agent 循环、provider（openai-compat / anthropic）、消息与工具类型
  agent-host/    会话状态机、headless 装配、权限与清理
  plugins/harness/  flow-agent-plugin 服务进程（本文的契约实现方）
```

## 方法层契约（wire 之上的 `agent/*`）

通道与帧格式由 [proc-ipc](../crates/proc-ipc) 与
[plugin-proto](../crates/plugin-proto) 提供（initialize 握手、
`$/cancelRequest` 取消均为协议自带）。本插件在其上定义：

### `agent/runTurn`（request，宿主 → 插件）

```json
{
  "spec": {
    "base_url": "https://…", "model": "…", "api": "openai|anthropic",
    "proxy": null, "max_turns": 4, "max_tool_calls_per_turn": null,
    "max_tokens": null, "tool_timeout_secs": null
  },
  "api_key": "…", "prompt": "…", "workdir": "/…", "roots": "/…"
}
```

成功响应（result）：

```json
{ "text": "…", "stop_reason": "end_turn", "error": null, "exit_code": 0 }
```

- `exit_code` 语义与原 runner 契约一致：`0` 完成 / `1` 回合失败或
  max_turns 耗尽 / `130` 中止。判定规则「只看末条 assistant」（保守方向：
  把失败判成成功会掩盖工具副作用）。
- `text` 已按 7.5 MiB 上限截断（journal 派生 8 MiB 线内的自我截断）。

### 错误码（应用段，宿主据此分派恢复语义）

| 码 | 含义 | 宿主处置 |
| --- | --- | --- |
| -32010 | 装配/环境失败（规格非法、代理不可用、临时根不可建） | 确定性失败（Business），重试无意义 |
| -32011 | 回合失败（provider 错误、取消、通道中断） | 不确定失败（Uncertain） |

### `agent/event`（notification，插件 → 宿主，回合期间）

只携带**元信息**——隐私边界在投影时执行（正文/thinking/参数/结果体/
prompt 在 wire 上不存在，不是被过滤）：

| kind | 字段 |
| --- | --- |
| `turn_start` | — |
| `provider_request` | `model` |
| `provider_response` | `stop_reason`, `tools`, `input_tokens`, `output_tokens`, `ms` |
| `tool_start` | `id`, `name` |
| `tool_end` | `id`, `name`, `is_error`, `ms` |
| `turn_end` | `tool_results`, `errors`, `ms` |
| `agent_end` | — |
| `error` | —（详情只进最终响应） |

### 取消

`$/cancelRequest { "id": <runTurn 的请求 id> }` → 触发回合的 CancelSignal；
回合以 `-32011`（turn cancelled 类）收口。宿主侧整体回收靠进程组击杀
（插件与执行器同进程组）。

## 进程与环境契约

- 定位：宿主同目录兄弟 `flow-agent-plugin`，或 `FLOW_AGENT_PLUGIN_BIN`
  显式指定（该变量在执行器环境白名单里）。
- 环境：白名单快照（PATH/HOME/FLOW_HOME/…，与执行器同源，见
  flow-engine `execution_protocol::environment`）+ `RUST_LOG`；凭据类
  变量不进入。API key 只经请求参数过通道。
- 回连承载（非 unix）：`PROC_IPC_ADDR` 回连 + `PROC_IPC_TOKEN` 在
  initialize 响应回验（wire.md §3.5）。

## 构建与测试

```sh
# 共享宿主仓库 target（与 flow 二进制同目录，兄弟定位即生效）
CARGO_TARGET_DIR=target cargo build --manifest-path （根 workspace：cargo -p agent-plugin） --bins

cargo test --manifest-path （根 workspace：cargo -p agent-plugin）
# flow 侧端到端（真实插件进程 + mock provider）
cargo test -p backend-e2e --test agent_isolation --test agent_lifecycle \
                         --test agent_provider --test agent_workdir
```
