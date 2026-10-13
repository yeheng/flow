# IPC 协议性能基线（T01）

测量日期：2026-10-13。本报告是 `docs/IPC_REVIEW_OPTIMIZED_TASK_LIST.md` T01 的首份基线，只记录当前实现（T05/T06/T07/T08 落地后）的原始数值，不含任何优化前后对比结论——对比必须在同一命令、同一机器、同一负载下复测得出。

## 环境

| 项 | 值 |
| --- | --- |
| 机器 | Apple M2 Max, 32 GiB RAM |
| 系统 | Darwin 27.0.0 (macOS, arm64) |
| rustc | 1.99.0 (b940084d7 2026-09-28) |
| 构建参数 | `--release`，workspace 默认 profile（无 LTO/PGO 特调） |
| 协议 | v4 单流（`u32 body_len + u8 kind + u32 meta_len + JSON meta + raw`），`PROTOCOL_VERSION=4` |

## 复现命令

```sh
cargo run -p flow-engine --release --example ipc_baseline --locked
```

（示例先对每个场景做 roundtrip 语义等价断言，再计时；分配计数由内置全局分配器统计。）

## 结果（2026-10-13 第六次复测：v4 唯一协议）

```
flow-engine v4 协议基线（release，单线程编解码）
wire bytes: control=96 audit_batch(64×1KiB)=70425 max_chunk=262343

control_small (Heartbeat)    ops=  20000       1.09 µs/op     83.98 MiB/s  allocs/op=   19.0  alloc_bytes/op=    2406.0
audit_batch (64×1KiB)        ops=   2000     124.08 µs/op    541.28 MiB/s  allocs/op= 1830.0  alloc_bytes/op=  775050.0
transfer_max (256KiB chunk)  ops=   1000      25.43 µs/op   9839.02 MiB/s  allocs/op=   14.0  alloc_bytes/op= 1311809.0
transport_duplex throughput  ops=    500     142.10 µs/op    472.63 MiB/s  allocs/op= 2152.1  alloc_bytes/op= 822418.7
cancel_latency_under_flood     queued_frames_before_cancel=33  latency=1.90 ms
```

对比上一版 v3 基线（同机同命令）：transfer_max 由 205.22 µs/op 降至
**25.43 µs/op（≈8×）**、线字节 349 734 → **262 343（−25%）**；满队列 Cancel
排队延迟 8.54 → **1.90 ms**（帧更小、编码更快）。

（首测与第三测在运行间波动范围内一致，未因 T05/T06/T07/T08/T09 改动出现回退。）

## 场景说明与读数口径

- **control_small**：Heartbeat 单帧编解码（92 wire bytes）。µs/op 含 encode（信封 Value + 直写帧缓冲）与 decode（借用信封 + 类型化单遍解析）。
- **audit_batch**：64 条 × 1 KiB payload 的 AuditBatch（约 70.4 KiB wire）。allocs/op 高的主因是 `AuditRecord.payload` 本身为 `serde_json::Value`——JSON 树解析的固有分配，不是信封层开销。
- **transfer_max**：最大合法 TransferChunk（256 KiB 原始字节，raw blob 约 262 KiB wire）。alloc_bytes/op ≈ 1.3 MiB 反映帧缓冲 + 解码侧原始字节的合计（无 base64/大字符串）。
- **transport_duplex throughput**：真实 `FrameTransport`（duplex 承载，队列深 128）端到端收发审计批次的吞吐（含任务调度与通道交接）。
- **cancel_latency_under_flood**：发送队列预填 32 条最大传输帧后提交 Cancel，实测接收端看到 Cancel 的排队延迟（单流契约：Cancel 排在已入队帧之后，不可抢占）。该数值是调度事实，不是协议保证。

## 协议：v4 单流（唯一，已删除 v3）

帧格式：`u32 body_len | u8 kind | u32 meta_len | JSON meta | raw blob`。
带二进制体的类型（`TransferChunk`）载荷作为 raw blob 承载；其余类型 kind=0、
meta 即完整 JSON 信封。`TransferChunk.bytes` 内存表示为 `Vec<u8>`（不再
base64），v4 是唯一协议、无 v3 双轨。Journal 持久格式不变（仍是原 base64
ValueChunk）；WSS 承载复用同一帧格式。

实现与验收：
- `frame.rs`：`encode_frame` + `FrameDecoder`（长度头即校验上限、meta 边界
  校验、按通道复核 control/data 预算）。
- `message.rs`：kind/meta/blob 拆分与重组；`PROTOCOL_VERSION = 4`；无 base64
  路径（删除了 v3 信封的 TransferChunk 编码）。
- 测试：全 fixtures 往返、raw 不入 base64、未知 kind/meta 溢出/malformed 拒绝、
  半帧与粘帧、本地 IPC 大输入 raw 往返、远程 agent 大输入 raw 往返。
- 相对上一版 v3：transfer 编解码 ≈8×、线字节 −25%、满队列 Cancel 排队延迟
  −78%（见上）。

## 边界与已知留白

- 本基线不含：真实进程 spawn 成本、Journal 磁盘 sync 时间、mTLS 握手成本、跨机网络 RTT。这些由 `backend-perf`（journal 场景压测）与真实 TLS 套件覆盖，另按需补充。
- 收益复测口径：T05/T06/T07/T08 任何后续调整（或 T12 二进制 wire）必须用同一命令重跑本表并追加日期分段，不做外推。
- Cancel 延迟优化（如消息边界调度）进入条件：本表数值在实际负载下被证实构成问题。
