# 后端冷历史与长期运行证据

日期：2026-10-16。机器：同一台 macOS/aarch64 主机，Rust release，Tokio 4 worker。基线提交：`e58b9ab84a0a26d97a2c1f52ac20529385b2adaf`，由 git archive 提取到独立目录，应用源码不改，仅加入相同容量驱动及旧 API 适配。结果为本机实测，不是产品 SLO。

## 实施与计量范围

`flow-capacity` 提供 history / attempts / audits / soak / storage 模式。前三种提交合法合成 journal 事实，验证 reducer、索引和 checkpoint；soak 才是真实流程、IPC 和执行器。合成历史不能代替完整业务吞吐。RSS 是进程驻留内存；FD、线程、Tokio task 与子进程由探针采集。磁盘字节包含派生索引，允许随保留历史增长。

历史和重试的快照读，对照旧公开 `state()` 全量导出后取单 run 与新 `run_snapshot()`；这是生产调用路径迁移的收益，**不是 RPC run.get 的端到端延迟**。每档读 100 次，只能描述该样本。基线和修复版部分容量实验与 soak/编译并行，延迟不是隔离 CPU 的严谨排名；内存和结构规模证据仍有效。不报告未采集的分配次数或 CPU 指令数。

## 固定活跃 run=1，增长终态历史

每个终态 run 有 1 KiB prepared params、一个已发布值描述符，按 16 run 一批。均完成，业务错误/超时 0；checkpoint 失败单列。

| 终态 run | 基线 RSS MB | 修复 RSS MB | 修复 checkpoint 字节 | 基线/修复快照 P99 ms |
| --- | ---: | ---: | ---: | ---: |
| 1,000 | 129.38 | 22.97 | 240,952 | 3.225 / 0.283 |
| 10,000 | 1,131.02 | 45.79 | 1,959,847 | 43.576 / 0.332 |
| 100,000 | 8,309.77 | 48.04 | 1,960,012 | 427.267 / 0.335 |

修复版三档 checkpoint 全成功，resident_runs=1、resident_attempts=0、dirty_run_count=1；发布缓存从 1,000 增至 8,192 后封顶，FD=19、Tokio tasks=3。基线 10k checkpoint 超过 16 MiB 状态上限，100k 超过 8 MiB 发布目录上限，报告中旧 checkpoint 文件大小不能视作成功的新 checkpoint。

100k 重启：基线 51.46 s，修复 40.07 s。修复后 reducer/历史恢复有界，但 value-addresses 仍完整扫描重建，所以**重启不是常数时间**。目录字节基线 814,280,069，修复 2,106,194,034；磁盘放大是版本化索引的代价。当前没有声明重启或写入提速目标。

结果：[基线](evidence/backend-review-2026-10-16/flow-baseline-history-100k.json)、[修复](evidence/backend-review-2026-10-16/flow-cold-history-final-100k.json)。JSON 中超长 dirty ID 列表只保留前 8 个及原计数，未删资源样本。

## 单 run 重试与审计

重试按 32 次一批；每档 last_batch_ms 是该档最后一批提交，**不是分位数**。

| attempts | 基线/修复 RSS MB | 基线/修复最后批 ms | 修复常驻 attempts |
| --- | ---: | ---: | ---: |
| 1,000 | 30.47 / 17.50 | 10.62 / 11.87 | 1 |
| 10,000 | 1,446.89 / 19.71 | 19.27 / 11.70 | 1 |
| 100,000 | 11,916.69 / 19.94 | 84.34 / 12.33 | 1 |

基线 100k checkpoint 失败，重启 64.93 s；修复三档成功，重启 0.842 s。基线观测超过 10 GiB 后请求停止，但执行信号前进程自行完成；没有将停止请求写成实际中止。两版业务错误/超时均 0。

审计按最多 64 条一批；分位数取本档区间批次，跨档清空样本。

| audits | 基线/修复 RSS MB | 基线/修复 commit P99 ms | 修复投影积压 LSN |
| --- | ---: | ---: | ---: |
| 1,000 | 16.01 / 14.37 | 11.63 / 15.75 | 0 |
| 10,000 | 56.05 / 16.48 | 15.26 / 18.05 | 0 |
| 100,000 | 5,268.05 / 16.71 | 27.15 / 19.00 | 0 |

小规模提交增加了权威派生索引的开销；不声称所有延迟都降低。基线 100k 采样时 durable=1569、projected=837；修复两者均1569。比较磁盘字节时必须考虑这一积压，不能将基线尚未落盘的投影称作空间优势。批次成本、RSS 和代码中的 attempt 更新单元共同支持历史不再参与每次全量复制；没有推导未测的 CPU/分配数字。

结果：[重试基线](evidence/backend-review-2026-10-16/flow-baseline-attempts-100k.json)、[重试修复](evidence/backend-review-2026-10-16/flow-cold-attempts-100k.json)、[审计基线](evidence/backend-review-2026-10-16/flow-baseline-audits-100k.json)、[审计修复](evidence/backend-review-2026-10-16/flow-cold-audits-100k.json)。

## 30 分钟真实混合运行

实际单调计时 1,800.0066 s，池容量4，每秒计划5个 run，最多4个同时等待。轮转：256 KiB script 输出、2,000 行 console 洪泛、本机 HTTP、80 ms delay、human signal、取消1秒 delay。

提交8,966；成功7,472；预期取消1,494；失败0；超时0。端到端 run P50=241.37 ms、P95=432.95 ms、P99=654.38 ms。期间含其他基线进程/构建压力，不用这个 P99 制定隔离性能门槛。

361 个采样：master RSS 13.01–36.26 MB，FD峰值23，Tokio tasks峰值20，resident run峰值4、attempt峰值9；投影积压最多2 LSN。预热300秒后 RSS 线性拟合斜率0.179 MiB/分钟，不能声称完全平坦或无限期无泄漏。该阶段缓存/观察记录尚在增长；需更长测试区分 allocator 保留和缓存稳态。采样之外关闭前 RSS44.20 MB，关闭后52.26 MB，未虚报内存归还。

日志洪泛下观察入口累计丢弃2,713,023条，storage_dropped=0，业务0失败；这是允许丢失的诊断日志，不是 correctness 消息。history_incomplete 明确为true。关闭后子进程0、Tokio tasks=0、FD=9；重启3.885 s，历史完整可读。目录约1.61 GB是保留的日志/值/索引，不能当成内存泄漏。

第一次 soak 发现取消后的执行器被过早复用：晚到成功结果与已排队 Cancel 并存，下一任务收到上一任务的 Reject。已修为 outcome成功、session_healthy 且未发送Cancel才归还；失败/丢弃 lease 由取消令牌驱动 kill/reap，pool shutdown 等待 TaskTracker。修复后重做完整30分钟。新增真实单槽取消后再派发，以及满队列下关闭并回收真实子进程的回归测试。

结果：[完整样本](evidence/backend-review-2026-10-16/flow-cold-soak-final-1800.json)、[采样汇总](evidence/backend-review-2026-10-16/soak-summary.json)。

## 恢复、故障与读限制

新回归覆盖冷终态及旧 checkpoint 前缀、两派生库删除重建、旧 audit 重复/冲突、旧 attempt 的晚到 audit、8,448 次 publication 后旧 ID 重发及旧 Ref、当前执行快照与完整诊断导出。SQLite 历史版本带摘要，损坏派生行读取明确失败。产品元数据不读 definition，完整 timeline 8 MiB 总预算在物化业务值前拒绝，正常单值仍可完整下载。

既有专项覆盖双向 harness 洪泛/退出、传输停读/满队列/关闭、观察日志洪泛、静默 WSS 握手关闭、上联出站饱和仍处理入站、反复断线副作用授权/提交边界、迁移原文件只读。完整 workspace：398 passed、0 failed、11 ignored，日志无 panic。ignored 的大负载专项不算通过。

本次实测不覆盖 PG运行、Windows、Tauri/Web、真实断电；不宣称远程256连接、一慢多快、所有多版本大图/编译缓存、多订阅消费者及更长 soak 的完整容量矩阵已验收。R16 的本轮历史/attempt/audit/30分钟主证据已交付；这些部署扩展矩阵仍是单独验证项。没有产品 SLO，未凭空设置尾延迟通过线。

## 复现

```sh
cargo build --offline --release -p backend-perf --bin flow-capacity -p flow-executor --bin flow-executor
target/release/flow-capacity history /tmp/flow-history /tmp/flow-history.json 100000
target/release/flow-capacity attempts /tmp/flow-attempts /tmp/flow-attempts.json 100000
target/release/flow-capacity audits /tmp/flow-audits /tmp/flow-audits.json 100000
target/release/flow-capacity storage /tmp/flow-storage /tmp/flow-storage.json 10000
target/release/flow-capacity soak /tmp/flow-soak /tmp/flow-soak.json 1800 /absolute/path/to/flow-executor
```

每次使用全新目录。原始本机日志位于 `/private/tmp/flow-cold-*.log`；可移植的 JSON 证据随仓库保存。存储路线及迁移回退见 [ADR 0001](adr/0001-journal-storage-and-cluster.md)。
