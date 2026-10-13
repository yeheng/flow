# IPC 协议性能基线（T01）

测量日期：2026-10-13。本报告是 `docs/IPC_REVIEW_OPTIMIZED_TASK_LIST.md` T01 的首份基线，只记录当前实现（T05/T06/T07/T08 落地后）的原始数值，不含任何优化前后对比结论——对比必须在同一命令、同一机器、同一负载下复测得出。

## 环境

| 项 | 值 |
| --- | --- |
| 机器 | Apple M2 Max, 32 GiB RAM |
| 系统 | Darwin 27.0.0 (macOS, arm64) |
| rustc | 1.99.0 (b940084d7 2026-09-28) |
| 构建参数 | `--release`，workspace 默认 profile（无 LTO/PGO 特调） |
| 协议 | v3 单流（`u32 BE length + u8 channel + JSON`），`PROTOCOL_VERSION=3` |

## 复现命令

```sh
cargo run -p flow-engine --release --example ipc_baseline --locked
```

（示例先对每个场景做 roundtrip 语义等价断言，再计时；分配计数由内置全局分配器统计。）

## 结果（2026-10-13 第三次复测，含 T12 分解）

```
flow-engine v3 协议基线（release，单线程编解码）
wire bytes: control=92 audit_batch(64×1KiB)=70421 max_chunk=349734

control_small (Heartbeat)    ops=  20000       1.25 µs/op     70.21 MiB/s  allocs/op=   16.0  alloc_bytes/op=    2250.0
audit_batch (64×1KiB)        ops=   2000     112.77 µs/op    595.54 MiB/s  allocs/op= 1827.0  alloc_bytes/op=  564542.0
transfer_max (256KiB chunk)  ops=   1000     205.22 µs/op   1625.26 MiB/s  allocs/op=   29.0  alloc_bytes/op= 2099085.0
transport_duplex throughput  ops=    500     151.38 µs/op    443.64 MiB/s  allocs/op= 2152.1  alloc_bytes/op= 823168.5
cancel_latency_under_flood     queued_frames_before_cancel=33  latency=8.54 ms

T12 breakdown (256KiB chunk): b64_decode=80.39 µs  sha256=108.35 µs  envelope_json=205.03 µs  base64_share=20%  wire_inflation=33%
```

（首测与第三测在运行间波动范围内一致，未因 T05/T06/T07/T08/T09 改动出现回退。）

## 场景说明与读数口径

- **control_small**：Heartbeat 单帧编解码（92 wire bytes）。µs/op 含 encode（信封 Value + 直写帧缓冲）与 decode（借用信封 + 类型化单遍解析）。
- **audit_batch**：64 条 × 1 KiB payload 的 AuditBatch（约 70.4 KiB wire）。allocs/op 高的主因是 `AuditRecord.payload` 本身为 `serde_json::Value`——JSON 树解析的固有分配，不是信封层开销。
- **transfer_max**：最大合法 TransferChunk（256 KiB 原始字节，base64 后约 349.7 KiB wire）。alloc_bytes/op ≈ 2 MiB 反映 base64 字符串 + 帧缓冲 + 解码侧字符串的合计。
- **transport_duplex throughput**：真实 `FrameTransport`（duplex 承载，队列深 128）端到端收发审计批次的吞吐（含任务调度与通道交接）。
- **cancel_latency_under_flood**：发送队列预填 32 条最大传输帧后提交 Cancel，实测接收端看到 Cancel 的排队延迟（单流契约：Cancel 排在已入队帧之后，不可抢占）。该数值是调度事实，不是协议保证。

## T12 进入条件判定（二进制大载荷 wire）

T12 的进入条件是「Base64/大载荷编码确为主要成本」。分解测量（256 KiB 传输块，`T12 breakdown` 行）：

| 分量 | µs/op | 占比 | 二进制 wire 能否消除 |
| --- | --- | --- | --- |
| base64 解码 | 80.39 | 20% | 是 |
| 整值 sha256 | 108.35 | 27% | **否**（T12 约束：最终内容摘要必须保留） |
| 信封 JSON 编解码 | 205.03 | 51% | 部分（大载荷字段变 raw，但仍保留 JSON metadata） |
| 线上字节膨胀 | — | +33% | 是（去掉 base64 的 4/3 膨胀） |

**结论：进入条件未满足，暂缓 T12。** Base64 只占单块处理的 20%；最大单项是信封 JSON（51%）与必需的 sha256（27%），都不是「base64→raw」能解决的。v4 会引入双版本维护/兼容/部署复杂度，而其能消除的收益上界约 20% CPU + 33% 线字节。继续监控：若 T08 字节预算或真实远程链路显示线字节膨胀实际构成瓶颈，再用本分解方法复评。

## 边界与已知留白

- 本基线不含：真实进程 spawn 成本、Journal 磁盘 sync 时间、mTLS 握手成本、跨机网络 RTT。这些由 `backend-perf`（journal 场景压测）与真实 TLS 套件覆盖，另按需补充。
- 收益复测口径：T05/T06/T07/T08 任何后续调整（或 T12 二进制 wire）必须用同一命令重跑本表并追加日期分段，不做外推。
- Cancel 延迟优化（如消息边界调度）进入条件：本表数值在实际负载下被证实构成问题。
