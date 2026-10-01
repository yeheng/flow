# JSONL 一期实现复核与补齐记录

日期：2026-10-01。范围：一期进程内执行；未实现 IPC/远程，未切换生产。

结论：本轮修复下列正确性问题，并接入此前缺失的适配器、触发器、迁移器、订阅与客户端。短回归和指定容量场景通过。**仍不能将 F01–F17 全部标 done**：完整验收矩阵还有下列未测项。

## Review 发现与处理

| 严重性 | 触发、影响 | 修复与证据 |
| --- | --- | --- |
| P1 | ValuePublished/业务引用可缺少有效完整块闭包，结果可能指向不可读取的值 | ValueCatalog 归约连续块/摘要/长度/发布；事务回滚恢复哈希。backend journal 测试覆盖缺块、错摘要、回滚、发布后追加及重启 |
| P1 | 单操作身份未完整绑定节点；HTTP 隐式重定向/重试可能在一次许可内发送额外请求 | Intent/Authorized 核对 node_id；禁用重定向与重试；请求构建在授权前，实际发送在授权后。适配器/非法请求测试 |
| P1 | 重复审计与结果不能附着；迟到证据可能与业务推进混淆 | 相同序号/结果返回原 LSN，异内容冲突；LateAudit 不推进 run_seq、不更改封口完整性。人工裁决/迟到证据测试 |
| P2 | 后端未利用 reducer 检查点，派生缓存恢复链不完整 | 恢复 State + 部分 SHA256 流状态，重放后缀，原子刷新投影；部分值跨检查点与损坏缓存重放测试 |
| P2 | 活跃额度饱和时逐 run 反复扫描 | 批量退回未准入 run；1..16 槽位限制。父子孙单槽、1000 等待 run 测试 |
| P2 | 查询/快照序号与浏览器缓存可能超限、丢精度或漏对齐 | SQL 取 body 前限额；序号十进制字符串；缓冲丢弃后再取快照；连接关闭停止重连。web 11 项与 RPC 测试 |
| P2 | 备份/下载中断会留下看似完整的目标文件 | 临时文件 sync 后 hard_link 无覆盖发布；迁移复制中断重入与真实 CLI 拒绝覆盖测试 |
| P2 | 观测 manifest 无界读取、丢弃计数仅全局 | 1 MiB manifest 限额；4096 scope 独立原子计数，不让 emit 等磁盘锁；scope 不可追溯时标不完整。观测洪泛/重启/scope 测试 |

相关实现：flow-journal/{value,maintenance,storage,writer,tail}、flow-engine/{journal_state,observation}、flow-backend/journal*、flow-store/projection、flow-rpc/journal*、flow-cli/journal、web/src/api/journal.ts 与 JournalView.vue。

## 新补齐的路径

- LLM/email：凭证名称持久化，FLOW_SECRET_<名称> 仅在发送时解析；请求与原始响应走同一授权、分块、Outcome 屏障。
- cron/webhook：配置复核、去重和 RunStarted 同一命令事务；配置删除后原投递重试仍附着原回执。
- LegacyImport：锁旧目录、源清单摘要、一致 SQLite 导出、原始日志字节（含 partial/corrupt 尾）与逐 run 缺失报告。历史 run 为只读基线，旧副作用不会自动重跑，导入触发器默认禁用。
- v2 RPC 订阅/分页/下载、浏览器 /journal、CLI journal call/events/download。COMMITTED_NOT_VISIBLE 查询原请求，不生成新写。
- 检查点恢复与投影快照原子替换；数据/目录 fsync 分别记录样本、总时长、最大时长及对数桶分布。

## 已执行验证

在工作区运行（socket 测试使用本机端口权限）：

```sh
cargo test -p flow-engine -p flow-store -p flow-backend -p flow-journal -p flow-rpc -p flow-cli
cargo check --workspace --all-targets
# web/ 中
npm test -- --run src/api/journal.test.ts src/rpc/client.test.ts
npm run build
```

最终完整 Rust 回归 254 项通过，0 失败；包括恢复、scope、三代单槽、CLI 与非法请求回归。容量用例单独执行，见 F17。浏览器 11 项通过，build 通过。复核中出现过测试 fixture 将 http_call 写成 http，已纠正；不是产品请求失败。

额外指定测试：journal_import 复制中断重入；journal_execution 实际 HTTP→script→delay→重启，纯重试、人工裁决、LLM/email；CLI 用真实子进程连接 v2 RPC，HTTP chunked 响应验证流式下载和不覆盖已有文件。

## 容量实测

本机 macOS，release 优化构建。系统 release strip 与 sqlx dylib 曾产生 LINKEDIT 错误，以下配置构建成功：

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none cargo run --release -p flow-journal --bin journal-bench -- /tmp/flow-review-bench-20261001
CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none cargo test -p flow-backend --release --test journal_capacity --no-run
/usr/bin/time -l target/release/deps/journal_capacity-35712f8523b5b929 --ignored --nocapture
```

| 场景 | 实测 | 判断 |
| --- | --- | --- |
| writer 低速20条，目标10 tx/s | control/audit p99 11.63/12.04 ms | 内置门槛通过 |
| writer 混合2000条，目标1000 tx/s | 994.14 tx/s，4.152 MiB/s，control/audit p99 18.15/17.55 ms | 含尾部排空；内置门槛通过 |
| writer 突发1000条 | 37.51 ms 排空，control/audit p99 34.24/36.19 ms | 内置门槛通过 |
| 1000 在飞 human wait，A_max=2 | create projected p99 13.712 ms；create+wait 84.35 s；恢复+取消15.35 s | 指定场景通过 |
| 1 GiB原始值（约1.33 GiB编码历史） | 全量恢复4.938s，检查点恢复205.61ms，流式校验128 MiB原始内容；最大RSS 29769728 B（28.39 MiB） | 最新release指定用例通过 |
| 上述1000 run 进程资源 | 最大 RSS 216055808 B（206.05 MiB），总运行100.02 s | 低于服务1 GiB预算 |

writer 三组逐条核对实际内容；混合2000用户事务/196用户数据同步，约10.2事务/同步。混合数据同步197样本含初始化，max8.504 ms；目录同步4样本，max4.742 ms。桶单位微秒。1 GiB测试运行命令为同一 capacity 二进制加 --ignored --nocapture gib_history_checkpoint_and_streamed_verify_remain_bounded，总耗时20.22s。1000 run 与长历史均已使用包含检查点/观测补丁的 release 二进制复测；这些场景仍不冒充持续混合业务指标。

## 仍需验收或明确限制

1. 持续混合业务负载的 projected p99/RSS 与长历史客户端 RSS 尚无完整实测；1000等待 run 不代替这些测试。
2. 系统级磁盘写满、所有目录同步/复制中断/取消树竞争故障窗口未全部跑完；故障钩子和关闭/重启不等于真实断电。目标硬件真实断电需部署环境测试。
3. scope 的队列/存储丢弃可追溯；整段保留删除、损坏或超过4096个scope时历史计数标不完整，不能报告为精确零。
4. 旧 UI/SQLite/PG 默认入口仍独立，新 UI 使用 /journal；LegacyImport 历史只读而非自动续跑。下载支持完整流，不支持 Range；历史导入大引用可用离线 journal 工具读取。
5. 未执行生产迁移/回退演练。新写入后只可使用兼容新 journal 的二进制或显式迁移回退，不能直接恢复旧库。
