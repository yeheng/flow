//! Adapters that invoke external plugin processes. The worker owns authorization,
//! audit, transfer, and result commitments; these adapters provide node execution.

use super::*;

use std::sync::Mutex;
use tokio::sync::oneshot;

/// The plugin session handles reverse RPCs serially, so one matching reply is enough.
#[derive(Default)]
pub(crate) struct ToolReplySlot {
    waiter: Mutex<Option<(String, oneshot::Sender<Message>)>>,
}

impl ToolReplySlot {
    fn register(&self, call_id: String) -> oneshot::Receiver<Message> {
        let (send, receive) = oneshot::channel();
        *self.waiter.lock().expect("tool reply slot") = Some((call_id, send));
        receive
    }

    pub(crate) fn deliver(&self, message: Message) {
        let Message::ToolPermissionDecision { tool_call_id, .. } = &message else {
            return;
        };
        let mut waiter = self.waiter.lock().expect("tool reply slot");
        if waiter
            .as_ref()
            .is_some_and(|(call_id, _)| call_id == tool_call_id)
        {
            if let Some((_, send)) = waiter.take() {
                let _ = send.send(message);
            }
        }
    }
}

impl TaskRunner {
    pub(super) async fn execute_plugin_node(
        &mut self,
        node: &Node,
        prepared: &Prepared,
        audit: &mut AuditStream,
        logger: &flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        match node.kind() {
            Some(NodeType::Llm) => self.llm_node(prepared, audit, logger).await,
            Some(NodeType::Harness) => self.agent_node(prepared, audit, logger).await,
            _ => self.plugin_node(node, prepared, audit, logger).await,
        }
    }

    pub(super) async fn verify_frozen_plugin(&self) -> Result<(), TaskError> {
        let Some(binding) = self.task.node.get("_flow_plugin") else {
            return Ok(());
        };
        let node_type = self.task.node["type"].as_str().unwrap_or_default();
        let plugin = self.plugins.resolve(node_type).ok_or_else(|| {
            TaskError::Business(format!("plugin for node type {node_type} is not installed"))
        })?;
        if binding["id"].as_str() != Some(plugin.plugin_id.as_str())
            || binding["content_hash"].as_str() != Some(plugin.content_hash.as_str())
        {
            return Err(TaskError::Business(format!(
                "plugin identity changed for node type {node_type}"
            )));
        }
        let plugin = plugin.clone();
        tokio::task::spawn_blocking(move || plugin.recheck_content_hash())
            .await
            .map_err(|error| TaskError::Business(error.to_string()))?
            .map_err(TaskError::Business)
    }

    /// llm 插件会话错误 → TaskError：Setup（环境）按 Invalid——但授权已
    /// 提交，许可已落 journal，环境失败同样意味着「副作用状态未知」，统一
    /// Uncertain 是保守正确的方向。
    fn llm_session_error(error: crate::plugin_session::PluginSessionError) -> TaskError {
        match error {
            crate::plugin_session::PluginSessionError::Setup(message)
            | crate::plugin_session::PluginSessionError::Invoke(message) => {
                TaskError::Uncertain(format!("llm plugin session failed: {message}"))
            }
        }
    }

    /// llm 节点（R4 插件化）：授权与审计在本进程，出站执行在插件。
    ///
    /// 与原 http_node 的 Llm 分支逐条对齐的不变量：
    /// - 请求 JSON（含 `adapter: "llm"`）是**指纹与回归 fixture 的基底**，
    ///   形状一个字节都不能漂，否则存量 fixture 全部失配；
    /// - T7：畸形请求（方法/URL/代理）必须在授权前失败——授权即承诺
    ///   「副作用可能已发生」，永远发不出去的请求不进 uncertain；
    /// - 重定向/重试禁用、限额流读、多值头合并在插件侧（llm-plugin
    ///   自测钉住），本侧对返回字节做长度+摘要互验后才落 outcome；
    /// - 授权后通道失败/出站失败/超限 = Uncertain（副作用状态未知）。
    pub(super) async fn llm_node(
        &mut self,
        prepared: &Prepared,
        audit: &mut AuditStream,
        logger: &flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        // 恢复路径：已有 Outcome 直接派生输出，不重发请求。
        if let Some(outcome_value) = self
            .task
            .previous_outcome
            .clone()
            .filter(|_| !self.is_regression())
        {
            logger.info("已有捕获的 operation outcome，不重复发起请求（恢复）");
            let outcome: StoredValue =
                serde_json::from_value(outcome_value).map_err(TaskError::from)?;
            let output = self.http_output(&outcome, NodeType::Llm, audit).await?;
            return Ok(ResultOutcome::Success {
                output,
                branch: None,
            });
        }
        let mut budget = INPUT_BUDGET;
        let mut params = self
            .materialize_stored(
                &prepared.params,
                &TransferKey::PreparedParams.to_wire(),
                &mut budget,
            )
            .await?;
        // 原生模型节点固定 POST（请求体是供应商 JSON）。
        params["method"] = json!("POST");
        let method = "POST";
        if !HTTP_METHODS.contains(&method) {
            return Err(TaskError::Invalid("invalid HTTP method".into()));
        }
        let url = params["url"]
            .as_str()
            .ok_or_else(|| TaskError::Business("HTTP URL missing".into()))?
            .to_owned();
        // T7 授权前校验：URL 与代理解析失败是确定性失败。
        reqwest::Url::parse(&url).map_err(|e| TaskError::Invalid(e.to_string()))?;
        if let Some(proxy) = params["proxy"].as_str().filter(|s| !s.trim().is_empty()) {
            reqwest::Proxy::all(proxy)
                .map_err(|e| TaskError::Business(format!("invalid proxy URL: {e}")))?;
        }
        // 请求 JSON：授权指纹与回归 fixture 的基底（形状冻结）。
        let request = json!({
            "method": method,
            "url": url,
            "headers": params.get("headers").cloned().unwrap_or(json!({})),
            "body": params.get("body").cloned().unwrap_or(Value::Null),
            "proxy": params.get("proxy").cloned().unwrap_or(Value::Null),
            "credential": params.get("credential").cloned().unwrap_or(Value::Null),
            "adapter": "llm",
        });
        if self.is_regression() {
            // 回归模式：零外部调用，只放行与录制基线逐字节一致的请求。
            let outcome = self.fixture_outcome(&request)?;
            let output = self.http_output(&outcome, NodeType::Llm, audit).await?;
            return Ok(ResultOutcome::Success {
                output,
                branch: None,
            });
        }
        // 授权：RequestOperation → OperationPermit（凭据在许可里，不进
        // 请求指纹与审计）。插件拿到的凭据只经通道过线。
        let secret_ref = request["credential"]["secret_ref"]
            .as_str()
            .map(str::to_owned);
        let credentials = self.authorize_operation(request).await?;
        let credential = secret_ref.and_then(|name| {
            flow_engine::execution_protocol::credential_for(&credentials, &name).map(str::to_owned)
        });
        // 注册表路由：llm → 插件（内置默认注册或 manifest 发现）。
        let plugin = match self.plugins.resolve("llm") {
            Some(plugin) => plugin.clone(),
            None => {
                return Err(TaskError::Business(
                    "llm 插件未注册：执行器启动时未找到默认二进制或 manifest（见启动日志）".into(),
                ))
            }
        };
        let url_label = flow_engine::redact_url(&url);
        logger.info(format!("→ {method} {url_label}"));
        let started = std::time::Instant::now();
        let timeout_ms = params["timeout_ms"]
            .as_u64()
            .unwrap_or(flow_engine::exec::DEFAULT_HTTP_TIMEOUT_MS)
            .clamp(1, flow_engine::exec::HTTP_TIMEOUT_MAX_MS);
        let invoke_params = json!({
            "method": method,
            "url": url,
            "headers": params.get("headers").cloned().unwrap_or(json!({})),
            "body": params.get("body").cloned().unwrap_or(Value::Null),
            "proxy": params.get("proxy").cloned().unwrap_or(Value::Null),
            "timeout_ms": timeout_ms,
            "credential": credential,
        });
        let mut session = crate::plugin_session::open(&plugin, true)
            .await
            .map_err(Self::llm_session_error)?;
        let outcome = crate::plugin_session::invoke(
            &mut session,
            &plugin.invoke_method,
            invoke_params,
            |_| {},
        )
        .await;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = session.shutdown().await;
                return Err(Self::llm_session_error(error));
            }
        };
        let _ = session.shutdown().await;
        let result = match outcome.error {
            None => outcome.result,
            Some(error) => {
                // 应用码分派：-32020/-32021/-32022 全部按 Uncertain 收口
                //（授权已提交：许可在 journal 里，副作用状态未知即不确定）。
                return Err(TaskError::Uncertain(format!(
                    "llm plugin egress failed: {}",
                    error.message
                )));
            }
        }
        .ok_or_else(|| TaskError::Uncertain("llm plugin returned empty result".into()))?;
        // 互验：body 长度 + 摘要（插件侧契约：二进制块先行、摘要后至）。
        let status = result["status"]
            .as_u64()
            .ok_or_else(|| TaskError::Uncertain("llm plugin result missing status".into()))?
            as u16;
        let body_bytes = result["body_bytes"].as_u64().unwrap_or(0);
        if body_bytes != outcome.binary.len() as u64 {
            return Err(TaskError::Uncertain(format!(
                "llm plugin body length mismatch: declared {body_bytes}, received {}",
                outcome.binary.len()
            )));
        }
        let expected_digest = result["body_digest"].as_str().unwrap_or("");
        let actual_digest = {
            use sha2::Digest;
            hex::encode(sha2::Sha256::digest(&outcome.binary))
        };
        if expected_digest != actual_digest {
            return Err(TaskError::Uncertain(
                "llm plugin body digest mismatch".into(),
            ));
        }
        let headers = result["headers"].clone();
        let raw = outcome.binary;
        // 响应元信息一行：状态码、耗时、体量；非 2xx/3xx 升级 warn。
        let response_line = format!(
            "← HTTP {status}（{}ms，body {}B）",
            started.elapsed().as_millis(),
            raw.len()
        );
        if status >= 400 {
            logger.warn(response_line);
        } else {
            logger.info(response_line);
        }
        // 捕获审计：与 http_node 完全同一套（store_bytes → 本地装配器
        // 登记 → outcome 先落账再派生输出）。
        let raw_stored = store_bytes(audit, &self.journal_id, &raw, ValueCodec::Bytes).await?;
        let body_ref = match &raw_stored {
            StoredValue::Ref(reference) => serde_json::to_value(reference)?,
            StoredValue::Inline(value) => value.clone(),
        };
        let body_output_id = match &raw_stored {
            StoredValue::Ref(reference) => reference.output_id.clone(),
            StoredValue::Inline(_) => String::new(),
        };
        if !body_output_id.is_empty() {
            self.transfers.insert_ready(
                TransferKey::OperationBody(body_output_id.clone()).to_wire(),
                raw,
            );
        }
        let outcome_value = StoredValue::inline(
            json!({"status": status, "headers": headers, "body_raw": body_ref}),
        )?;
        let outcome_seq = audit.push(
            "operation_outcome",
            json!({"outcome": serde_json::to_value(&outcome_value)?}),
        )?;
        audit.await_durable(outcome_seq, &self.cancel).await?;
        let output = self
            .http_output(&outcome_value, NodeType::Llm, audit)
            .await?;
        Ok(ResultOutcome::Success {
            output,
            branch: None,
        })
    }

    /// 动态节点类型的通用插件派发：materialize 参数 → invoke（manifest 声明
    /// 的方法）→ `{output}` 响应 → 落账输出。
    ///
    /// 通用契约（manifest `methods.invoke`）：
    /// - 请求 `{params: <节点参数>}`（已模板展开物化）；
    /// - 响应 `{output: <任意 JSON>}`（≤ 8 MiB）；分支输出不支持
    /// - 会话/协议失败按 Uncertain（授权外副作用可能已发生）。
    pub(super) async fn plugin_node(
        &mut self,
        node: &Node,
        prepared: &Prepared,
        audit: &mut AuditStream,
        logger: &flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        if let Some(outcome_value) = self
            .task
            .previous_outcome
            .clone()
            .filter(|_| !self.is_regression())
        {
            logger.info("已有捕获的 operation outcome，不重复调用插件（恢复）");
            let outcome: StoredValue =
                serde_json::from_value(outcome_value).map_err(TaskError::from)?;
            let output = match outcome {
                StoredValue::Inline(value) => value,
                StoredValue::Ref(_) => {
                    return Err(TaskError::Invalid("plugin outcome must be inline".into()))
                }
            };
            return Ok(ResultOutcome::Success {
                output: StoredValue::Inline(output),
                branch: None,
            });
        }
        let plugin = match self.plugins.resolve(&node.node_type) {
            Some(plugin) => plugin.clone(),
            None => {
                return Err(TaskError::Business(format!(
                    "节点类型 {} 的插件未注册（manifest 缺失或已下架，见启动日志）",
                    node.node_type
                )))
            }
        };
        let mut budget = INPUT_BUDGET;
        let params = self
            .materialize_stored(
                &prepared.params,
                &TransferKey::PreparedParams.to_wire(),
                &mut budget,
            )
            .await?;
        logger.info(format!("→ 插件 {}（{}）", plugin.plugin_id, node.node_type));
        // G4：动态插件节点与内置副作用节点同一条授权路径（09 §5）：
        // manifest 需求由 master 裁决，Allow 前 zero 副作用；params 全文
        // 进指纹（换参 = 新请求 = 重新批准）。请求携带插件内容身份（I35：
        // master 核验执行器二进制与授予快照一致）。
        let request = json!({
            "kind": "plugin_invoke",
            "node_type": node.node_type,
            "plugin": {
                "id": plugin.plugin_id,
                "content_hash": plugin.content_hash,
            },
            "params": params,
        });
        let credentials = self.authorize_operation(request).await?;
        // Widget::Secret 字段按名取值注入（I37：全部需求获准后才解析）。
        let mut params = params;
        if !credentials.is_empty() {
            for form in plugin
                .ui
                .as_ref()
                .map(|ui| ui.params_forms.as_slice())
                .unwrap_or_default()
            {
                if form.node_type != node.node_type {
                    continue;
                }
                for field in &form.fields {
                    if matches!(field.widget, flow_config::plugin_manifest::Widget::Secret) {
                        if let Some(name) = params.get(&field.key).and_then(Value::as_str) {
                            if let Some(value) =
                                flow_engine::execution_protocol::credential_for(&credentials, name)
                            {
                                params[&field.key] = Value::String(value.to_string());
                            }
                        }
                    }
                }
            }
        }
        let started = std::time::Instant::now();
        let timeout_ms = params["timeout_ms"]
            .as_u64()
            .unwrap_or(60_000)
            .clamp(1, flow_engine::exec::HTTP_TIMEOUT_MAX_MS);
        let invoke_params = json!({"params": params});
        let mut session = crate::plugin_session::open(&plugin, false)
            .await
            .map_err(Self::llm_session_error)?;
        let outcome = crate::plugin_session::invoke(
            &mut session,
            &plugin.invoke_method,
            invoke_params,
            |_| {},
        )
        .await;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = session.shutdown().await;
                return Err(Self::llm_session_error(error));
            }
        };
        let _ = session.shutdown().await;
        let result = outcome
            .result
            .ok_or_else(|| TaskError::Uncertain("plugin invoke failed".into()))?;
        let output_value = result["output"].clone();
        if output_value.is_null() {
            return Err(TaskError::Uncertain(
                "plugin response missing `output`".into(),
            ));
        }
        logger.info(format!(
            "← 插件 {}（{}ms）",
            plugin.plugin_id,
            started.elapsed().as_millis()
        ));
        let _ = timeout_ms;
        let output = store_value(audit, &self.journal_id, &output_value, 8 * 1024 * 1024).await?;
        Ok(ResultOutcome::Success {
            output,
            branch: None,
        })
    }

    /// 内联 harness agent。沿用统一授权、Outcome 与回归协议；本次任务
    /// 完成后回收整个 executor，不复用 host 的全局状态或工具后代。
    ///
    /// agent 是**一次性执行器**：它会改写进程级环境并派生工具后代，宿主
    /// 状态不得流向下一个任务。
    ///
    /// # Errors
    /// 配置缺失、凭据缺失等确定性失败以 `Business` 收口；超时或取消
    /// 以 `Uncertain` 收口（工具副作用可能已发生）。
    pub(super) async fn agent_node(
        &mut self,
        prepared: &Prepared,
        audit: &mut AuditStream,
        logger: &flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        // Rebuild the result from committed evidence when resuming an agent turn.
        if let Some(outcome_value) = self
            .task
            .previous_outcome
            .clone()
            .filter(|_| !self.is_regression())
        {
            logger.info("已有捕获的 operation outcome，不重复启动进程（恢复）");
            let outcome: StoredValue =
                serde_json::from_value(outcome_value).map_err(TaskError::from)?;
            let output = self.agent_output_from_outcome(&outcome, audit).await?;
            return Ok(ResultOutcome::Success {
                output,
                branch: None,
            });
        }
        let params = materialize_prepared_params(self, prepared).await?;
        let agent_params = flow_internal_plugins::agent_params(&params)
            .map_err(|e| TaskError::Business(e.message))?;
        let timeout_ms = agent_params
            .timeout_ms
            .clamp(1, flow_engine::exec::HARNESS_TIMEOUT_MAX_MS);
        // 配置缺失/畸形发生在授权之前：请求指纹必须包含冻结配置，没有它
        // 就无法授权，也就没有可落账的 operation——直接 Business。
        let config = flow_internal_plugins::frozen_agent_config(&self.task.node)
            .map_err(TaskError::Invalid)?
            .ok_or_else(|| {
                TaskError::Business(
                    "harness agent 配置未随派发下发（[harness.agent] 未配置，或 master \
                     版本过旧）；无法构造授权请求"
                        .into(),
                )
            })?;
        let request = flow_internal_plugins::agent_request(
            &flow_internal_plugins::AgentParams {
                prompt: agent_params.prompt.clone(),
                workdir: agent_params.workdir.clone(),
                api_key: agent_params.api_key.clone(),
                timeout_ms,
            },
            &config,
        );
        if self.is_regression() {
            // 0 runner / 0 网络：指纹比较用 baseline 冻结的配置构造。
            let outcome = self.fixture_outcome(&request)?;
            let output = self.agent_output_from_outcome(&outcome, audit).await?;
            return Ok(ResultOutcome::Success {
                output,
                branch: None,
            });
        }
        // workdir 运行期校验（授权前，仅对显式配置的路径）：保存期只查了
        // 字面量；存在性与 canonicalize 是本机事实，remote 模式下只有
        // executor 看得到。缺省（未配置）走 roots 下的临时沙箱，授权后创建。
        let configured_workdir = match &agent_params.workdir {
            Some(configured) => {
                let canonical = std::path::Path::new(configured)
                    .canonicalize()
                    .map_err(|e| {
                        TaskError::Business(format!(
                            "harness agent workdir 不可用（{configured}）：{e}；请清空 workdir 使用默认临时工作目录，或填写执行器所在机器上已存在的绝对目录，发布后启动新运行"
                        ))
                    })?;
                if !canonical.is_dir() {
                    return Err(TaskError::Business(format!(
                        "harness agent workdir 不是目录：{}",
                        canonical.display()
                    )));
                }
                Some(canonical)
            }
            None => None,
        };
        let secret_ref = request["credential"]["secret_ref"]
            .as_str()
            .map(str::to_owned);
        let credentials = self.authorize_operation(request).await?;
        let credential = secret_ref.and_then(|name| {
            flow_engine::execution_protocol::credential_for(&credentials, &name).map(str::to_owned)
        });
        // 生命周期截止时间从授权后起算：覆盖装配、回合、清理与落账，
        // 避免任一阶段无限延长节点占用。
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        // 看门狗独立于 Tokio：宿主同步代码堵死运行时时，协作式超时与
        // 心跳都会失效，只有独立线程能保证执行器最终退出。
        //
        // Independent of Tokio progress and heartbeats. If synchronous host code
        // blocks the runtime, the managed process still exits within a bounded grace.
        let _watchdog = flow_engine::process::AgentWatchdog::start(
            Duration::from_millis(timeout_ms),
            self.cancel.clone(),
        )
        .map_err(|error| TaskError::Uncertain(format!("agent watchdog unavailable: {error}")))?;
        // 沙箱根：flow 主目录下 `harness/flow-harness-<uuid>`（FLOW_HOME →
        // ~/.flow；与配置/journal 同住，不散落系统 temp）。home 不可用的
        // 退化环境回退系统 temp。运行结束整个 roots 被收走（见下）。
        let harness_root = flow_config::flow_home().unwrap_or_else(std::env::temp_dir);
        let roots = harness_root
            .join("harness")
            .join(format!("flow-harness-{}", uuid::Uuid::now_v7().simple()));
        // prompt/密钥不进日志：只记模型、工作目录与超时（缺省时目录是临时
        // 沙箱，标注 default 以示来源）。
        let default_workdir = configured_workdir.is_none();
        let workdir = match configured_workdir {
            Some(canonical) => canonical,
            None => {
                let scratch = roots.join("workdir");
                std::fs::create_dir_all(&scratch).map_err(|e| {
                    TaskError::Business(format!("harness agent 临时沙箱创建失败：{e}"))
                })?;
                scratch.canonicalize().map_err(|e| {
                    TaskError::Business(format!("harness agent 临时沙箱不可用：{e}"))
                })?
            }
        };
        logger.info(format!(
            "→ harness agent（model {}，workdir {}{}，timeout {timeout_ms}ms）",
            config.model,
            workdir.display(),
            if default_workdir {
                "（默认临时沙箱）"
            } else {
                ""
            }
        ));
        let Some(api_key) = credential.filter(|key| !key.trim().is_empty()) else {
            // 授权成功但凭据缺失：确定性失败，落 outcome B 让恢复派生稳定。
            let outcome = StoredValue::inline(json!({
                "agent_error": format!(
                    "密钥 {} 未配置或为空（设置页密钥管理；环境兜底 FLOW_SECRET_*）",
                    agent_params.api_key
                ),
            }))?;
            return self
                .commit_agent_failure(
                    outcome,
                    audit,
                    format!("harness agent credential missing: {}", agent_params.api_key),
                )
                .await;
        };
        let cancel = self.cancel.clone();
        let result = tokio::select! {
            biased;
            // biased：取消优先于回合完成。回合刚完成与取消同时就绪时，
            // 选取消会让已完成的工作被判 Uncertain——但工具副作用确实
            // 已经发生，保守方向是对的。
            _ = cancel.cancelled() => Err(TaskError::Uncertain("harness agent cancelled; result incomplete".into())),
            result = tokio::time::timeout_at(deadline, self.agent_run(
                &config,
                &agent_params.prompt,
                &api_key,
                &workdir,
                &roots,
                audit,
                logger,
            )) => result.unwrap_or_else(|_| Err(TaskError::Uncertain(
                "harness agent lifecycle deadline exceeded; result incomplete".into(),
            ))),
        };
        // 成功/失败/超时/取消都收走临时根；Windows 上文件锁可能让删除失败，
        // 重试一次，仍失败只能残留（在 flow 主目录 harness/ 下，可人工清理）。
        if std::fs::remove_dir_all(&roots).is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = std::fs::remove_dir_all(&roots);
        }
        result
    }

    /// 授权后的内联执行段：直接调用 flow-harness-agent-host 的 headless 装配（无
    /// runner 子进程、无 stdin/stdout 协议）。确定性失败（临时根不可建、
    /// 装配失败）落封闭 outcome；调用方统一约束总截止时间与取消。
    /// 超时/取消传播 Uncertain，随后回收整个执行器会话。
    ///
    /// # Errors
    /// 装配失败以 `Business`（经 `commit_agent_failure` 落 outcome）收口；
    /// 回合失败以 `Uncertain` 收口。
    #[allow(clippy::too_many_arguments)]
    async fn agent_run(
        &mut self,
        config: &flow_engine::AgentRunConfig,
        prompt: &str,
        api_key: &str,
        workdir: &std::path::Path,
        roots: &std::path::Path,
        audit: &mut AuditStream,
        logger: &flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        let started = std::time::Instant::now();
        // agent 运行时住在独立插件进程里（agent-plugin 项目）：spawn →
        // 握手 → runTurn → 事件泵 → 摘要（见 `agent_plugin.rs`）。装配/
        // 环境类失败是确定性失败 → outcome B；回合类失败 → Uncertain。
        // 回合的清理（会话资源回收）在插件侧完成后才回摘要。
        //
        // The caller bounds assembly, the turn, cleanup and outcome publication
        // with one deadline. Dropping this future is always followed by retiring
        // the entire executor session, including the plugin and tool process
        // groups (插件与执行器同进程组).
        //
        // 进度投影只写入有界观测通道：不记录 prompt、模型正文或工具参数
        // （隐私边界在插件侧投影时执行，详见 `agent_progress.rs`）。
        // 节点类型路由：harness → 插件注册（内置默认或 manifest 发现，
        // 见 plugin_registry）。注册缺失 = 环境问题，确定性失败。
        let plugin = match self.plugins.resolve("harness") {
            Some(plugin) => plugin.clone(),
            None => {
                let outcome = StoredValue::inline(json!({ "agent_error":
                    "harness 插件未注册：执行器启动时未找到默认二进制或 manifest（见启动日志）"
                }))?;
                return self
                    .commit_agent_failure(
                        outcome,
                        audit,
                        "harness plugin registration missing".into(),
                    )
                    .await;
            }
        };
        let mut progress = crate::agent_progress::AgentProgress::new(logger);
        // 受管回合（master 冻结了策略）：权限随 _flow_execution 下发；反向
        // 裁决经执行协议转 master（G5）。未受管（legacy 回合）不接通道。
        let execution = self.task.node.pointer("/_flow_execution");
        let managed = execution
            .and_then(|e| e.get("permissions_managed"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let permission = managed.then(|| flow_harness_protocol::TurnPermission {
            execution_mode: execution
                .and_then(|e| e.get("permissions_mode"))
                .and_then(Value::as_str)
                .unwrap_or("execute")
                .to_string(),
            policy_digest: execution
                .and_then(|e| e.get("policy_digest"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            managed: true,
        });
        let on_request = managed.then(|| {
            let outbound = self.outbound.clone();
            let dispatch_id = self.dispatch_id.clone();
            let cancel = self.cancel.clone();
            let tool_replies = self.tool_replies.clone();
            std::sync::Arc::new(
                move |request: flow_plugin_sdk::protocol::jsonrpc::Request| {
                    let outbound = outbound.clone();
                    let dispatch_id = dispatch_id.clone();
                    let cancel = cancel.clone();
                    let tool_replies = tool_replies.clone();
                    Box::pin(async move {
                        forward_permission_request(
                            outbound,
                            dispatch_id,
                            cancel,
                            tool_replies,
                            request,
                        )
                        .await
                    }) as futures::future::BoxFuture<'static, _>
                },
            ) as crate::agent_plugin::ReverseHandler
        });
        let parsed = match crate::agent_plugin::run_agent_turn(
            crate::agent_plugin::TurnRequest {
                plugin: &plugin,
                config,
                prompt,
                api_key,
                workdir,
                roots,
                permission,
                on_request,
            },
            |event| progress.on_wire_event(event),
        )
        .await
        {
            Ok(parsed) => parsed,
            Err(crate::agent_plugin::AgentPluginError::Assembly(message)) => {
                // 装配/环境失败与原内联路径同语义：先落 outcome 留痕，
                // 再以 Business 收口（重试无意义）。
                let outcome = StoredValue::inline(json!({ "agent_error": message }))?;
                return self
                    .commit_agent_failure(
                        outcome,
                        audit,
                        format!("harness agent assembly failed: {message}"),
                    )
                    .await;
            }
            Err(crate::agent_plugin::AgentPluginError::Turn(message)) => {
                return Err(TaskError::Uncertain(format!(
                    "harness agent turn failed: {message}"
                )));
            }
        };
        logger.info(format!(
            "← agent {}（{}ms，{} turns）",
            if parsed.exit_code == 0 {
                "完成"
            } else {
                "失败"
            },
            started.elapsed().as_millis(),
            progress.rounds()
        ));
        // error 行同时进 stderr（A6）：Business 文案不只剩退出码。
        let mut stderr = String::new();
        if let Some(error) = &parsed.error {
            stderr.push_str(&format!("[agent] {error}"));
        }
        let raw_stored = store_bytes(
            audit,
            &self.journal_id,
            parsed.text.as_bytes(),
            ValueCodec::Bytes,
        )
        .await?;
        let body_ref = match &raw_stored {
            StoredValue::Ref(reference) => serde_json::to_value(reference)?,
            StoredValue::Inline(value) => value.clone(),
        };
        if let StoredValue::Ref(reference) = &raw_stored {
            if !reference.output_id.is_empty() {
                self.transfers.insert_ready(
                    TransferKey::OperationBody(reference.output_id.clone()).to_wire(),
                    parsed.text.as_bytes().to_vec(),
                );
            }
        }
        let agent_meta = json!({
            "model": config.model,
            "stop_reason": parsed.stop_reason,
            "elapsed_ms": started.elapsed().as_millis() as u64,
        });
        let outcome = StoredValue::inline(json!({
            "exit_code": parsed.exit_code,
            "body_raw": body_ref,
            "stderr": stderr,
            "agent": agent_meta,
        }))?;
        let outcome_seq = audit.push(
            "operation_outcome",
            json!({"outcome": serde_json::to_value(&outcome)?}),
        )?;
        audit.await_durable(outcome_seq, &self.cancel).await?;
        let output = self.agent_output_from_outcome(&outcome, audit).await?;
        Ok(ResultOutcome::Success {
            output,
            branch: None,
        })
    }

    /// agent 模式的 Outcome → 节点输出（恢复与首跑共用同一派生）。
    /// 封闭形状：`spawn_error` / `agent_error` / 正常三选一。
    ///
    /// # Errors
    /// `spawn_error`/`agent_error` 归为 `Business`；原始文本超限归为
    /// `Business`（outcome 已留存）。
    async fn agent_output_from_outcome(
        &mut self,
        outcome: &StoredValue,
        audit: &mut AuditStream,
    ) -> Result<StoredValue, TaskError> {
        let outcome_value = match outcome {
            StoredValue::Inline(value) => value.clone(),
            StoredValue::Ref(_) => return Err(TaskError::Invalid("outcome must be inline".into())),
        };
        if let Some(error) = outcome_value["spawn_error"].as_str() {
            return Err(TaskError::Business(format!(
                "harness agent spawn failed: {error}"
            )));
        }
        if let Some(error) = outcome_value["agent_error"].as_str() {
            return Err(TaskError::Business(format!("harness agent error: {error}")));
        }
        let exit_code = outcome_value["exit_code"].as_i64().map(|code| code as i32);
        let stderr = outcome_value["stderr"].as_str().unwrap_or("").to_owned();
        let agent = outcome_value.get("agent").cloned().unwrap_or(Value::Null);
        let raw: flow_facts::ValueRef =
            serde_json::from_value(outcome_value["body_raw"].clone()).map_err(TaskError::from)?;
        if raw.total_bytes > 8 * 1024 * 1024 {
            return Err(TaskError::Business(
                "agent text exceeds 8 MiB; raw outcome retained".into(),
            ));
        }
        let stdout = self.raw_bytes(&raw).await?;
        let output = flow_internal_plugins::agent_output(
            exit_code,
            String::from_utf8_lossy(&stdout).into_owned(),
            stderr,
            agent,
        )
        .map_err(|e| TaskError::Business(format!("{}; outcome retained", e.message)))?;
        store_value(audit, &self.journal_id, &output, 8 * 1024 * 1024).await
    }

    /// 落一个封闭失败 outcome（B/C 形状）并以 Business 收口；恢复路径经
    /// [`Self::agent_output_from_outcome`] 重放同一失败文案。
    ///
    /// 先落 outcome 再报错：恢复期才能重放出同样的失败，而不是变成
    /// 一次「结果未知」的人工裁决。
    async fn commit_agent_failure(
        &mut self,
        outcome: StoredValue,
        audit: &mut AuditStream,
        message: String,
    ) -> Result<ResultOutcome, TaskError> {
        let outcome_seq = audit.push(
            "operation_outcome",
            json!({"outcome": serde_json::to_value(&outcome)?}),
        )?;
        audit.await_durable(outcome_seq, &self.cancel).await?;
        Err(TaskError::Business(message))
    }
}

/// 控制帧预算预检（postcard 编码长度）。
fn postcard_probe(message: &Message) -> std::result::Result<Vec<u8>, String> {
    postcard::to_allocvec(message).map_err(|e| e.to_string())
}

/// 反向工具裁决转发（G5）：`permission/request` → 执行协议
/// ToolPermissionRequest → master 裁决 → ToolPermissionDecision → JSON-RPC
/// 响应回插件。链路断/取消一律 fail closed（拒绝该次工具调用），
/// 不回退到本地放行；裁决时限归 master（approval TTL），执行器不设
/// 第二只钟。
async fn forward_permission_request(
    outbound: mpsc::Sender<Message>,
    dispatch_id: String,
    cancel: CancellationToken,
    tool_replies: Arc<ToolReplySlot>,
    request: flow_plugin_sdk::protocol::jsonrpc::Request,
) -> flow_plugin_sdk::protocol::jsonrpc::Message {
    use flow_plugin_sdk::protocol::handshake::{
        PermissionRequestParams, PermissionRequestResult, PERMISSION_REQUEST_METHOD,
    };
    use flow_plugin_sdk::protocol::jsonrpc::{Response, Response as RpcResponse};
    let id = request.id.clone();
    let error_response = |message: String| {
        flow_plugin_sdk::protocol::jsonrpc::Message::Response(RpcResponse {
            jsonrpc: "2.0",
            id: id.clone(),
            result: None,
            error: Some(flow_plugin_sdk::protocol::jsonrpc::Error {
                code: flow_plugin_sdk::protocol::jsonrpc::ErrorCode::Application(-32011),
                message,
                data: None,
            }),
        })
    };
    if request.method != PERMISSION_REQUEST_METHOD {
        return error_response(format!("unknown reverse method: {}", request.method));
    }
    let Some(params_value) = request.params else {
        return error_response("permission request missing params".into());
    };
    let params: PermissionRequestParams = match serde_json::from_value(params_value) {
        Ok(params) => params,
        Err(error) => return error_response(format!("permission params malformed: {error}")),
    };
    let request = Message::ToolPermissionRequest {
        dispatch_id: dispatch_id.clone(),
        tool_call_id: params.tool_call_id.clone(),
        tool: params.tool.clone(),
        params: params.params.clone(),
        cwd: params.cwd.clone(),
    };
    // 控制帧预算预检：工具参数必须整体装进控制帧（截断 = 拒绝，不静默）。
    let probe = postcard_probe(&request);
    match probe {
        Ok(bytes) if bytes.len() > flow_engine::execution_protocol::CONTROL_MAX_FRAME => {
            return error_response("工具参数超过控制帧预算（写入内容过大）".into())
        }
        Err(error) => return error_response(format!("tool request encode failed: {error}")),
        _ => {}
    }
    // Register before sending. Replays for a finished or different call are
    // discarded by the slot, and the approval deadline still belongs to the master.
    let reply = tool_replies.register(params.tool_call_id.clone());
    let message = tokio::select! {
        biased;
        _ = cancel.cancelled() => return error_response("cancelled; failing closed".into()),
        result = async {
            outbound.send(request).await.map_err(|_| ())?;
            reply.await.map_err(|_| ())
        } => match result {
            Ok(message) => message,
            Err(()) => return error_response("master link closed; failing closed".into()),
        }
    };
    let Message::ToolPermissionDecision {
        dispatch_id: target,
        tool_call_id,
        operation_id,
        allowed,
        reason,
    } = message
    else {
        return error_response("invalid tool reply".into());
    };
    if target != dispatch_id || tool_call_id != params.tool_call_id {
        return error_response("tool reply identity mismatch".into());
    }
    let result = PermissionRequestResult {
        allowed,
        reason,
        operation_id,
    };
    flow_plugin_sdk::protocol::jsonrpc::Message::Response(Response {
        jsonrpc: "2.0",
        id,
        result: Some(serde_json::to_value(result).unwrap_or(Value::Null)),
        error: None,
    })
}
