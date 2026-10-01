# JSONL 一期非断电验收

日期：2026-10-01。用户授权：验证剩余验收项，明确排除真实断电。所有数据集、HTTP 服务、磁盘镜像与被终止进程均为测试专用；生产未切换。

本轮补充可复现测试，未修改业务实现。**本轮约定的非断电验收通过**：完整Rust回归266项通过，workspace check通过，web客户端11项通过；大容量/真实满盘/Chromium测试单独执行。早先标记未验收的项目在下表逐项映射；结果适用于列出的机器、数据分布和负载，不推断无限期运行、任意硬件或任意负载。

## 执行环境与口径

- 本机 macOS，Rust 工作区，独立 loopback mock HTTP/RPC；未调用真实邮件、LLM 服务。
- 性能使用 release：`CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none`；`/usr/bin/time -l` 测试进程 maximum resident set size，包含测试内服务与生产代码，排除编译器。
- 浏览器使用独立 headless Chromium。kimi-webbridge 启动后未连接扩展，因此未操作用户浏览器。浏览器指标为 GC 后 JS heap，不冒充浏览器全部进程 RSS。
- 实际 ENOSPC 使用独立32 MiB HFS+磁盘镜像。测试核对挂载设备，并将填充限制在40 MiB内；不填充工作目录所在磁盘。
- 混合业务负载预设120 run/30秒（4 run/s），HTTP/script/delay/signal/cancel各20%，A_max=16，公开创建 projected p99门槛250ms。此场景不是1000业务事务/s；writer吞吐另见F07。

## 已执行结果

| 项目 | 场景及核验 | 结果 |
| --- | --- | --- |
| 持续混合业务 | 120 run，24次真实本地HTTP；所有非取消run成功，取消run终止；1367事务/848781编码字节 | projected p99 **15.274 ms**，RSS **35.12 MiB**，通过 |
| A_max与同时释放 | 16个HTTP任务同时等待，响应各1 MiB，之后执行JS；同时释放全部响应 | peak_active=16，全部内容核对，4.303秒排空，RSS **100.17 MiB**，通过 |
| 多前驱预算 | 两个5 MiB前驱成功；汇合脚本不得执行；另一个9 MiB脚本输出拒绝 | 用户代码前拒绝10 MiB汇合；输出预算拒绝，RSS **55.88 MiB**，通过 |
| 无可信长度与转义扩张 | 45 MiB无Content-Length的原始NUL响应，转义后270 MiB | 在256 MiB输出预算处失败，保留完整raw Outcome，不发布不完整输出；RSS **33.33 MiB**，通过 |
| writer真实满盘 | 小镜像填充32505856字节后产生OS errno28，提交失败后继续提交仍失败 | durable_lsn不越过原回执；已确认前缀存在；歧义尾保留，经新目录repair可读，通过 |
| 观测/备份目标真实满盘 | 仅观测及备份位于满镜像，权威journal位于正常测试盘 | 观测storage_dropped增加，业务输出仍成功；备份返回失败且无完成manifest；源verify通过 |
| 同步故障窗口 | 数据sync第1..10次、目录sync第1..4次失败；另有短写、部分写、慢盘、满队列原生回归 | 不提前确认、不丢已确认前缀，失败后停止追加，通过 |
| 提交后投影前进程退出 | 子进程公开命令返回COMMITTED_NOT_VISIBLE后直接exit(70)，不运行Rust析构 | 新进程重放，原请求重试返回原cursor且只创建一次，通过 |
| 授权后SIGKILL | 服务已收到HTTP请求但未发响应，SIGKILL执行进程 | 恢复进入uncertain、Outcome仍缺失、没有第二次HTTP调用，通过 |
| 取消/外部结果竞争 | HTTP已接收，取消持久后才放行响应，再重启 | run保持cancelled，下游不派发、不重发，通过 |
| 1000在飞实际崩溃 | 1000个等待run被独立进程加载后SIGKILL，再启动恢复及取消 | create projected p99 17.691ms，恢复+取消15.601s，RSS225.61MiB，全部run可枚举并最终cancelled，通过 |
| 取消树中途重启 | 64个子run已创建，取消父run后立即关闭并重启，A_max=1续做 | 65个run全部cancelled，通过 |
| 观测隔离 | 损坏manifest/partial段、删除目录、段路径不可写、10000条console洪泛 | 业务输出一致，loss可识别，权威verify通过 |
| 慢订阅与迟到审计 | 单消息缓冲超过2秒不消费；独立命令仍可见；分页limit=2补齐；终态后追加LateAudit | run_seq完整连续，迟到证据送达，终态序号不变，通过 |
| Chromium长历史 | 实际EventWindow/JournalClient；RPC回调夹具注入20万条多字节事件，每批让出事件循环，10次重连 | 保留3241条/4193854字节，丢弃196759条旧展示记录，11次快照对齐，最新seq=200000；heap增长3264952字节，通过 |
| CLI长历史 | 真实CLI子进程连接分页RPC夹具，1000页/10万条，>100 MiB内容直接写stdout | 请求1000页，RSS **12.84 MiB**，通过 |
| 备份中轮转 | 4 KiB段，固定upper后边写边备份；备份恢复后继续追加 | 固定前缀一致、恢复可写、禁止覆盖目标，通过 |
| 迁移/兼容二进制恢复 | 导入旧库/partial原始日志；复制中断重入；导入后新写入；仅复制journal到新目录，由独立flow-journal-dev status启动 | 无SQLite/legacy-source仍精确重建全部导入基线和新增状态；源变化拒绝，通过 |

已有验收继续有效：1 GiB原始值全量恢复4.938秒、检查点恢复205.61ms、RSS28.39MiB；128MiB流式原始内容校验；父子孙单槽执行、幂等signal、delay原wake_at、投影快照事务回滚、权限和业务secret键不误删。

## 一期故障矩阵对应证据

按设计§14行顺序：

1. 半写/LF/摘要/版本：`flow-journal/tests/journal.rs` codec、short_writes。
2. 回执丢失/同键：backend journal 并发附着、断连重试；新增 committed_projection_crash。
3. 轮转/目录/缺段：journal sync_failure_matrix、rotation_chain；maintenance checkpoint不能掩盖缺段。
4. 投影原子性：store projection_snapshot_failure；backend invalid_multi_event_transaction；新增进程退出恢复。
5. 删除派生数据：backend concurrent_same_identity、checkpoint_corrupt；migration仅journal备份与独立二进制恢复。
6. 输入恢复：journal_execution pure_retry/script_actual_values；授权后SIGKILL场景已提交准备随日志保留；事务/半写夹具覆盖未提交准备不应用。
7. 单行授权与未知Outcome：journal事务原子性、backend引用屏障、授权后SIGKILL。
8. 取消/迟到：cancel_authorized_http、manual_resolution及LateAudit原回执去重。
9. 值闭包/扩张：publication_and_business_references、无长度扩张256MiB预算测试。
10. 汇合预算/重启：aggregate_predecessors；HTTP完成后delay重启不重发。
11. 父子/wake_at/signal：child_completion、three_generations、pure_retry、script_actual_values。
12. 取消树/1000：cancelled_fanout_tree；journal_capacity（包括额外SIGKILL子进程）。
13. 洪泛/慢盘/满盘：slow_disk_and_full_queues、真实ENOSPC两套测试、mixed/active16。
14. 暂停投影：paused_projection、RPC authenticated_commands、客户端原command.status。
15. 分页/慢客户端：page cursor绑定；slow_subscription；Chromium与CLI长历史；原RPC错误参数/大值下载。
16. 观测/敏感数据/权限：observation_failure及真实ENOSPC；adapter凭证测试和RPC下载授权。
17. 导入/回退规则：journal_import复制中断、缺失/冲突、源保护、新写入后journal-only二进制恢复。回退只允许兼容journal的版本，未使用旧SQLite覆盖新事实。

## 复现命令

短回归：

```sh
cargo test -p flow-engine -p flow-store -p flow-backend -p flow-journal -p flow-rpc -p flow-cli
cargo check --workspace --all-targets
```

性能（先构建，按Cargo输出的可执行路径运行；不要把子进程fixture作为独立验收执行）：

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none cargo test -p flow-backend --release --test journal_acceptance --no-run
/usr/bin/time -l target/release/deps/journal_acceptance-7278635bba97ae06 --ignored --exact sustained_mixed_business --nocapture
/usr/bin/time -l target/release/deps/journal_acceptance-7278635bba97ae06 --ignored --exact sixteen_active_tasks_and_simultaneous_release_are_bounded --nocapture
/usr/bin/time -l target/release/deps/journal_acceptance-7278635bba97ae06 --ignored --exact http_without_length_expansion_hits_output_budget_and_retains_outcome --nocapture
/usr/bin/time -l target/release/deps/journal_acceptance-7278635bba97ae06 --exact aggregate_predecessors_and_oversize_output_fail_within_budget --nocapture
```

真实ENOSPC使用新镜像，后两条测试顺序执行；完成后卸载：

```sh
hdiutil create -size 32m -fs HFS+ -volname FLOW_ACCEPTANCE -type SPARSE /tmp/flow-acceptance-enospc.sparseimage
mkdir -p /tmp/flow-acceptance-volume
hdiutil attach -nobrowse -mountpoint /tmp/flow-acceptance-volume /tmp/flow-acceptance-enospc.sparseimage
FLOW_ENOSPC_VOLUME=/tmp/flow-acceptance-volume cargo test -p flow-journal --test journal real_enospc_freezes_writer_and_preserves_acknowledged_prefix -- --ignored --nocapture
FLOW_ENOSPC_VOLUME=/tmp/flow-acceptance-volume cargo test -p flow-backend --test journal_acceptance enospc_observations_and_backup_target_do_not_damage_authority -- --ignored --nocapture
hdiutil detach /tmp/flow-acceptance-volume
```

浏览器（web/内；独立启动Vite后另一个终端运行脚本）：

```sh
npm run dev -- --port 19413 --strictPort --host 127.0.0.1
node scripts/journal-browser-acceptance.mjs
npm test -- --run src/api/journal.test.ts src/rpc/client.test.ts
```

原生CLI长历史测RSS测试仅在macOS运行；其他平台不将未测RSS计为通过。

## 边界

真实断电按用户要求未执行，不由SIGKILL替代。浏览器内存夹具和CLI分页夹具用于客户端资源验证；服务端真实journal的分页、权限、慢消费和迟到审计由独立集成测试验证。迁移演练使用当前兼容二进制的独立进程，未宣称旧版不兼容二进制可以回退。30秒混合负载不是多日soak；1000在飞、writer高提交率与16活跃任务分别验收，不互相换算。生产切换未执行。
