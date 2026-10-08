# flow-agent 运维说明（三期）

## 部署

- 主进程（`flow-journal-server` / `flow-journal-dev`）启用远程模式：

  ```sh
  FLOW_EXECUTION_MODE=remote \
  FLOW_REMOTE_CONTROL_ADDR=127.0.0.1:9805 \
  FLOW_REMOTE_DATA_ADDR=127.0.0.1:9806 \
  FLOW_REMOTE_CA=/path/ca.pem FLOW_REMOTE_CERT=/path/server.pem \
  FLOW_REMOTE_KEY=/path/server.key \
  cargo run -p flow-rpc --bin flow-journal-server
  ```

  （环境变量入口经 `flow_backend::execution::remote_mode_from_env`；同数据
  集单写者、同 run 单执行模式约束与一期/二期一致。）
- 每台执行机部署 `flow-agent` + `flow-executor`（`scripts/build-pg.sh` 产物，
  两者同目录）：

  ```sh
  flow-agent --control-addr HOST:9805 --data-addr HOST:9806 \
    --agent-id agent-a --ca-cert ca.pem --cert agent-a.pem \
    --key agent-a.key --slots 4

  （执行器缺省取 flow-agent 同目录的兄弟 flow-executor；其他位置用
  `--executor-bin` 或 `FLOW_EXECUTOR_BIN` 指定。）
  ```

## 证书与身份

- PKI：内部 CA 签发 server（CN/SAN=flow-server）与每台 agent 证书
  （CN=agent_id）。mTLS：主进程验证 agent 客户端证书并把 **CN 映射为
  agent_id**（不信自报字段）；agent 验证 server 证书。
- data 连接绑定：AgentWelcome 下发一次性凭据，DataBind 校验同 TLS 主体 +
  agent_boot_id + link_session_id；凭据一次性、连接替换后旧绑定作废。
- 凭证：业务密钥以最小引用经 OperationPermit 按任务下发（主进程解析，
  不复制主进程环境到 agent；凭证值不进入授权事实）。

## 版本与升级

- 执行器二进制版本不兼容在本地握手处明确失败；主进程远程能力协商
  （AgentHello 能力交集）。升级流程：drain 目标 agent（等待在飞收尾或
  超时回收）→ 替换二进制 → 重启 agent（新 agent_boot_id，Resume 对账
  后按裁决继续/取消）。
- journal 兼容：三期未新增 journal 事件类型（会话/对账为内存协议状态）；
  一期/二期回退规则不变。

## 故障处理

- agent 失联：其上任务挂起等 Resume（attach_timeout 后失败封口，缺口
  与 uncertain 按一期规则保留）；其他机器任务不受影响。
- agent 崩溃/被杀：执行器随本地通道 EOF 退出；任务按 Lost 失败封口，
  重试由主进程新派发执行。
- 主进程重启（epoch 变化）：agent 重连 Resume 无绑定记录 →
  ReconcileRequired → 保守取消；已提交事实不受影响。
- drain：停止接新派发 → 取消在飞 → DrainComplete → 退出；之后新派发
  明确失败。

## 边界（必须向运维明示）

- **主进程是唯一提交者与可用性边界**：agent 只扩展计算容量，不扩展
  journal 耐久吞吐，不提供主进程高可用（三期 §1.8：不宣称 HA、无共享
  目录 fencing、无日志复制）。
- agent 断网期间执行中的任务会被持久窗口背压冻结（不提前确认、不无界
  积压）；agent 整机丢失时未确认数据可能丢失，主进程保留已确认前缀并
  标记 incomplete，不生成成功结果。
