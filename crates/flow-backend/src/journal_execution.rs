use crate::journal::{JournalBackend, JournalError};
use crate::journal_commands::{id, node_event};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use flow_engine::journal_state::{Operation, Prepared, Run, Wait};
use flow_engine::{NodeLogger, NodeType};
use flow_journal::value::{Chunk, ValueCodec};
use flow_journal::{EventKind, StoredValue, ValueRef};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

type Result<T> = std::result::Result<T, JournalError>;
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

#[derive(Clone)]
pub(crate) struct Attempt {
    pub backend: Arc<JournalBackend>,
    pub run_id: String,
    pub node_id: String,
    pub dispatch_id: String,
}

impl Attempt {
    pub async fn begin(
        backend: Arc<JournalBackend>,
        run_id: String,
        node_id: String,
    ) -> Result<Self> {
        let dispatch_id = id();
        backend
            .internal(|state| {
                let run = state
                    .runs
                    .get(&run_id)
                    .ok_or_else(|| invalid("run not found"))?;
                if run.terminal() {
                    return Err(invalid("run terminal"));
                }
                let old = run.nodes.get(&node_id);
                let execution = old.map(|n| n.node_execution_id.clone()).unwrap_or_else(id);
                let attempt = old.map_or(1, |n| n.attempt + 1);
                let mut event = node_event(
                    run,
                    &node_id,
                    EventKind::DispatchStarted,
                    json!({"node_execution_id":execution,"attempt":attempt.to_string()}),
                    true,
                );
                event.dispatch_id = Some(dispatch_id.clone());
                Ok((vec![event], ()))
            })
            .await?;
        Ok(Self {
            backend,
            run_id,
            node_id,
            dispatch_id,
        })
    }
    pub async fn snapshot(&self) -> Result<Run> {
        self.backend
            .inspect(|s| s.runs.get(&self.run_id).cloned())
            .await
            .ok_or_else(|| invalid("run missing").into())
    }
    pub async fn audit(&self, kind: EventKind, payload: Value) -> Result<()> {
        self.backend
            .internal(|state| {
                let run = state
                    .runs
                    .get(&self.run_id)
                    .ok_or_else(|| invalid("run missing"))?;
                let node = run
                    .nodes
                    .get(&self.node_id)
                    .ok_or_else(|| invalid("dispatch missing"))?;
                if node.dispatch_id != self.dispatch_id {
                    return Err(invalid("stale dispatch"));
                }
                let mut event = node_event(run, &self.node_id, kind, payload, false);
                event.audit_seq = node.attempts[&self.dispatch_id].audit_seq + 1;
                Ok((vec![event], ()))
            })
            .await
    }
    pub async fn json(&self, value: Value) -> Result<StoredValue> {
        let bytes = flow_journal::codec::bounded_json(&value, 8 * 1024 * 1024)?;
        if bytes.len() <= flow_journal::INLINE_BYTES {
            return Ok(StoredValue::Inline(value));
        }
        let mut stream = Capture::new(self, ValueCodec::Json);
        for chunk in bytes.chunks(flow_journal::CHUNK_BYTES) {
            stream.push(chunk).await?;
        }
        Ok(StoredValue::Ref(stream.finish().await?))
    }
    pub async fn stream(
        &self,
        input: &mut (impl tokio::io::AsyncRead + Unpin),
    ) -> Result<StoredValue> {
        use tokio::io::AsyncReadExt;
        let mut capture = Capture::new(self, ValueCodec::Json);
        let mut bytes = vec![0; flow_journal::CHUNK_BYTES];
        loop {
            let n = input
                .read(&mut bytes)
                .await
                .map_err(flow_journal::Error::Io)?;
            if n == 0 {
                break;
            }
            capture.push(&bytes[..n]).await?;
        }
        Ok(StoredValue::Ref(capture.finish().await?))
    }
    pub async fn prepare(
        &self,
        node: &flow_engine::Node,
        predecessors: BTreeMap<String, StoredValue>,
    ) -> Result<Prepared> {
        let run = self.snapshot().await?;
        if let Some(prepared) = &run.nodes[&self.node_id].prepared {
            return Ok(prepared.clone());
        }
        let templates = serde_json::to_string(&node.params)?.contains("${");
        if !templates {
            let params = self.json(node.params.clone()).await?;
            let prepared = Prepared {
                input: run.input,
                predecessors,
                params,
                engine_build: env!("CARGO_PKG_VERSION").into(),
                node_semantics: 1,
            };
            self.audit(EventKind::InputPrepared, json!({"prepared":prepared}))
                .await?;
            return Ok(prepared);
        }
        let mut budget = 8 * 1024 * 1024;
        let input = self.materialize(&run.input, &mut budget).await?;
        let mut nodes = serde_json::Map::new();
        for (key, value) in &predecessors {
            nodes.insert(key.clone(), self.materialize(value, &mut budget).await?);
        }
        let mut params = node.params.clone();
        let mut opaque = Vec::new();
        if let Some(map) = params.as_object_mut() {
            for key in node.kind().map(NodeType::opaque_params).unwrap_or_default() {
                if let Some(value) = map.remove(key) {
                    opaque.push((key.to_string(), value));
                }
            }
        }
        let expanded = tokio::task::spawn_blocking(move || {
            flow_engine::expr::expand_templates_bounded(
                &params,
                &input,
                &Value::Object(nodes),
                Duration::from_secs(2),
            )
        })
        .await
        .map_err(|e| invalid(e.to_string()))?
        .map_err(|e| invalid(e.to_string()))?;
        let mut expanded = expanded;
        if let Some(map) = expanded.as_object_mut() {
            map.extend(opaque);
        }
        let params = self.json(expanded).await?;
        let prepared = Prepared {
            input: run.input,
            predecessors,
            params,
            engine_build: env!("CARGO_PKG_VERSION").into(),
            node_semantics: 1,
        };
        self.audit(EventKind::InputPrepared, json!({"prepared":prepared}))
            .await?;
        Ok(prepared)
    }
    async fn materialize(&self, value: &StoredValue, budget: &mut usize) -> Result<Value> {
        let n = usize::try_from(value.bytes()?).map_err(|_| invalid("value length overflow"))?;
        if n > *budget {
            return Err(flow_journal::Error::Limit("aggregate input exceeds 8 MiB".into()).into());
        }
        *budget -= n;
        let value = value.clone();
        let root = self.backend.journal.root().to_path_buf();
        let upper = self.backend.journal.durable_lsn();
        Ok(tokio::task::spawn_blocking(move || {
            flow_journal::value::materialize(&root, upper, &value, n)
        })
        .await
        .map_err(|e| invalid(e.to_string()))??)
    }
    pub async fn finish(&self, kind: EventKind, mut payload: Value) -> Result<()> {
        self.backend
            .internal(|state| {
                let run = state
                    .runs
                    .get(&self.run_id)
                    .ok_or_else(|| invalid("run missing"))?;
                let node = run
                    .nodes
                    .get(&self.node_id)
                    .ok_or_else(|| invalid("node missing"))?;
                if node.dispatch_id != self.dispatch_id {
                    return Err(invalid("stale result"));
                }
                if let Some(original) = &node.attempts[&self.dispatch_id].result {
                    let mut previous = original.payload.clone();
                    previous.as_object_mut().unwrap().remove("sealed_through");
                    if original.kind == kind && previous == payload {
                        return Ok((vec![], ()));
                    }
                    return Err(flow_journal::Error::Conflict(
                        "different result for sealed dispatch".into(),
                    ));
                }
                let seq = node.attempts[&self.dispatch_id].audit_seq
                    + u64::from(kind == EventKind::NodeCompleted);
                payload["sealed_through"] = json!(seq.to_string());
                let mut event = node_event(run, &self.node_id, kind, payload, true);
                if kind == EventKind::NodeCompleted {
                    event.audit_seq = seq;
                }
                Ok((vec![event], ()))
            })
            .await
    }
    pub async fn compute(
        &self,
        node: &flow_engine::Node,
        prepared: &Prepared,
        logger: NodeLogger,
    ) -> Result<(StoredValue, Option<bool>)> {
        let mut budget = 8 * 1024 * 1024;
        let input = self.materialize(&prepared.input, &mut budget).await?;
        let mut nodes = serde_json::Map::new();
        for (id, value) in &prepared.predecessors {
            nodes.insert(id.clone(), self.materialize(value, &mut budget).await?);
        }
        let params = self.materialize(&prepared.params, &mut budget).await?;
        let code = match node.kind() {
            Some(NodeType::Script) => params["code"].as_str().unwrap_or("").to_owned(),
            Some(NodeType::Condition) => {
                format!("return ({});", params["expr"].as_str().unwrap_or("false"))
            }
            _ => return Err(invalid("not a compute node").into()),
        };
        let timeout =
            Duration::from_millis(params["timeout_ms"].as_u64().unwrap_or(2000).min(30_000));
        let result = tokio::task::spawn_blocking(move || {
            flow_engine::expr::eval_body_bounded(
                &code,
                &input,
                &Value::Object(nodes),
                timeout,
                &logger,
            )
        })
        .await
        .map_err(|e| invalid(e.to_string()))?
        .map_err(|e| invalid(e.to_string()))?;
        let branch =
            (node.kind() == Some(NodeType::Condition)).then(|| flow_engine::exec::truthy(&result));
        Ok((self.json(result).await?, branch))
    }
    pub async fn wait(&self, wait: Wait) -> Result<()> {
        self.finish(EventKind::WaitRegistered, json!({"wait":wait}))
            .await
    }

    /// Request construction is derived from the committed prepared parameters. Authorization
    /// is durably recorded with intent before reqwest is called, and never reused after restart.
    pub async fn http(&self, prepared: &Prepared, kind: NodeType) -> Result<StoredValue> {
        let mut budget = 8 * 1024 * 1024;
        let mut params = self.materialize(&prepared.params, &mut budget).await?;
        let mut credential = None;
        if matches!(kind, NodeType::Llm | NodeType::Email) {
            let name = params["api_key"]
                .as_str()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| invalid("api_key secret reference required"))?
                .to_owned();
            credential = Some(
                flow_engine::secrets::get_secret(&name)
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| invalid(format!("secret reference {name} not configured")))?,
            );
            let (url, body) = if kind == NodeType::Llm {
                flow_engine::exec::llm_request(&params)
            } else {
                flow_engine::exec::email_request(&params)
            }
            .map_err(|e| invalid(e.message))?;
            params = json!({"method":"POST","url":url,"body":body,"timeout_ms":params["timeout_ms"],"credential":{"scheme":"bearer","secret_ref":name}});
        }
        let method = params["method"].as_str().unwrap_or("GET").to_uppercase();
        if !flow_engine::HTTP_METHODS.contains(&method.as_str()) {
            return Err(invalid("invalid HTTP method").into());
        }
        let url = params["url"]
            .as_str()
            .ok_or_else(|| invalid("HTTP URL missing"))?;
        let http_method =
            reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| invalid(e.to_string()))?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|e| invalid(e.to_string()))?;
        let mut request = client
            .request(http_method, url)
            .timeout(Duration::from_millis(
                params["timeout_ms"].as_u64().unwrap_or(30_000),
            ));
        if let Some(headers) = params["headers"].as_object() {
            for (key, value) in headers {
                request = request.header(
                    key,
                    value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string()),
                );
            }
        }
        if !params["body"].is_null() {
            request = request.json(&params["body"]);
        }
        if let Some(secret) = credential {
            request = request.bearer_auth(secret);
        }
        let outbound = request.build().map_err(|e| invalid(e.to_string()))?;
        let request = json!({"method":method,"url":url,"headers":params.get("headers").cloned().unwrap_or(json!({})),"body":params.get("body").cloned().unwrap_or(Value::Null),"credential":params.get("credential").cloned().unwrap_or(Value::Null)});
        let fingerprint = flow_journal::codec::digest(&flow_journal::codec::bounded_json(
            &request,
            8 * 1024 * 1024,
        )?);
        let stored =
            flow_journal::value::store_json(&self.backend.journal, request, 8 * 1024 * 1024)
                .await?;
        let operation = Operation {
            operation_id: id(),
            fingerprint,
            permit_id: id(),
            request: stored,
            outcome: None,
        };
        self.backend
            .internal(|state| {
                let run = state
                    .runs
                    .get(&self.run_id)
                    .ok_or_else(|| invalid("run missing"))?;
                let node = &run.nodes[&self.node_id];
                if run.terminal() || node.dispatch_id != self.dispatch_id {
                    return Err(invalid("operation cancelled/stale"));
                }
                if node.prepared.as_ref() != Some(prepared) {
                    return Err(invalid(
                        "operation input differs from committed preparation",
                    ));
                }
                if node.operation.is_some() {
                    return Err(invalid(
                        "operation already authorized; outcome must be reconciled, not resent",
                    ));
                }
                let intent = node_event(
                    run,
                    &self.node_id,
                    EventKind::OperationIntent,
                    json!({"operation":operation}),
                    true,
                );
                let mut authorized = node_event(
                    run,
                    &self.node_id,
                    EventKind::OperationAuthorized,
                    json!({"operation":operation}),
                    true,
                );
                authorized.run_seq += 1;
                Ok((vec![intent, authorized], ()))
            })
            .await?;
        if self.snapshot().await?.terminal() {
            return Err(invalid("cancelled after authorization; outcome uncertain").into());
        }
        let mut response = client
            .execute(outbound)
            .await
            .map_err(|e| invalid(format!("external outcome uncertain: {e}")))?;
        let status = response.status().as_u16();
        let headers: serde_json::Map<String, Value> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), json!(v.to_str().unwrap_or(""))))
            .collect();
        let mut capture = Capture::new(self, ValueCodec::Bytes);
        while let Some(bytes) = response
            .chunk()
            .await
            .map_err(|e| invalid(format!("partial response, outcome uncertain: {e}")))?
        {
            for chunk in bytes.chunks(flow_journal::CHUNK_BYTES) {
                capture.push(chunk).await?;
            }
        }
        let raw = capture.finish().await?;
        let outcome =
            StoredValue::inline(json!({"status":status,"headers":headers,"body_raw":raw}))?;
        self.audit(EventKind::OperationOutcome, json!({"outcome":outcome}))
            .await?;
        self.http_output(&outcome, kind).await
    }
    pub async fn http_output(&self, outcome: &StoredValue, kind: NodeType) -> Result<StoredValue> {
        use serde::Deserialize;
        use std::io::{Seek, SeekFrom};
        let mut budget = flow_journal::INLINE_BYTES;
        let outcome = self.materialize(outcome, &mut budget).await?;
        let status = outcome["status"]
            .as_u64()
            .ok_or_else(|| invalid("invalid HTTP outcome"))?;
        let raw: ValueRef = serde_json::from_value(outcome["body_raw"].clone())?;
        if matches!(kind, NodeType::Llm | NodeType::Email) {
            if status >= 400 {
                return Err(invalid(format!("HTTP {status}; outcome retained")).into());
            }
            if raw.total_bytes > 8 * 1024 * 1024 {
                return Err(flow_journal::Error::Limit(
                    "integration decoding exceeds 8 MiB; raw outcome retained".into(),
                )
                .into());
            }
            let root = self.backend.journal.root().to_path_buf();
            let upper = self.backend.journal.durable_lsn();
            let response: Value = tokio::task::spawn_blocking(move || {
                let mut bytes = flow_journal::codec::BoundedBuffer {
                    bytes: Vec::new(),
                    limit: 8 * 1024 * 1024,
                };
                flow_journal::value::read_value(&root, upper, &raw, true, &mut bytes)?;
                Ok::<_, flow_journal::Error>(serde_json::from_slice(&bytes.bytes)?)
            })
            .await
            .map_err(|e| invalid(e.to_string()))??;
            let output = if kind == NodeType::Llm {
                let content = response
                    .pointer("/choices/0/message/content")
                    .ok_or_else(|| {
                        invalid(
                            "LLM response missing choices[0].message.content; raw outcome retained",
                        )
                    })?;
                json!({"content":content,"model":response["model"],"usage":response["usage"]})
            } else {
                json!({"status":status,"id":response["id"]})
            };
            return self.json(output).await;
        }
        // Re-read captured bytes for JSON/text semantics. The temporary spool is bounded by
        // MAX_VALUE_BYTES and contains no unique authority; its reference is never published.
        let root = self.backend.journal.root().to_path_buf();
        let upper = self.backend.journal.durable_lsn();
        let candidate = raw.clone();
        let valid_json = tokio::task::spawn_blocking(move || {
            let mut spool = tempfile::tempfile()?;
            flow_journal::value::read_value(&root, upper, &candidate, true, &mut spool)?;
            spool.seek(SeekFrom::Start(0))?;
            let mut de = serde_json::Deserializer::from_reader(std::io::BufReader::new(spool));
            let valid = serde::de::IgnoredAny::deserialize(&mut de).is_ok() && de.end().is_ok();
            Ok::<_, flow_journal::Error>(valid)
        })
        .await
        .map_err(|e| invalid(e.to_string()))??;
        use flow_journal::value::JsonPart;
        let mut prefix = flow_journal::codec::bounded_json(
            &json!({"status":status,"headers":outcome["headers"]}),
            flow_journal::INLINE_BYTES,
        )?;
        prefix.pop();
        prefix.extend_from_slice(b",\"body\":");
        let parts = vec![
            JsonPart::Literal(prefix),
            if valid_json {
                JsonPart::RawJson(raw)
            } else {
                JsonPart::Text(raw)
            },
            JsonPart::Literal(b"}".to_vec()),
        ];
        let (mut reader, producer) =
            flow_journal::value::compose(self.backend.journal.root().into(), upper, parts);
        let result = self.stream(&mut reader).await;
        drop(reader);
        let production = producer.await.map_err(|e| invalid(e.to_string()))?;
        let output = result?;
        production?;
        if status >= 400 {
            return Err(invalid(format!("HTTP {status}; outcome retained")).into());
        }
        Ok(output)
    }
}

struct Capture<'a> {
    attempt: &'a Attempt,
    output_id: String,
    codec: ValueCodec,
    index: u64,
    total: u64,
    hash: Sha256,
    pending: Vec<u8>,
}
impl<'a> Capture<'a> {
    fn new(attempt: &'a Attempt, codec: ValueCodec) -> Self {
        Self {
            attempt,
            output_id: id(),
            codec,
            index: 0,
            total: 0,
            hash: Sha256::new(),
            pending: Vec::with_capacity(flow_journal::CHUNK_BYTES),
        }
    }
    async fn push(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if self.total + self.pending.len() as u64 + bytes.len() as u64
            > flow_journal::MAX_VALUE_BYTES
        {
            return Err(
                flow_journal::Error::Limit("captured value size; prefix retained".into()).into(),
            );
        }
        let mut bytes = bytes;
        while !bytes.is_empty() {
            let n = bytes
                .len()
                .min(flow_journal::CHUNK_BYTES - self.pending.len());
            self.pending.extend_from_slice(&bytes[..n]);
            bytes = &bytes[n..];
            if self.pending.len() == flow_journal::CHUNK_BYTES {
                self.flush().await?;
            }
        }
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let chunk = Chunk {
            output_id: self.output_id.clone(),
            chunk_index: self.index,
            data: STANDARD.encode(&self.pending),
        };
        self.attempt
            .audit(EventKind::ValueChunk, serde_json::to_value(chunk)?)
            .await?;
        self.hash.update(&self.pending);
        self.total += self.pending.len() as u64;
        self.pending.clear();
        self.index += 1;
        Ok(())
    }
    async fn finish(mut self) -> Result<ValueRef> {
        self.flush().await?;
        let value = ValueRef {
            journal_id: self.attempt.backend.journal.id().into(),
            output_id: self.output_id,
            codec: self.codec,
            version: 1,
            chunk_count: self.index,
            total_bytes: self.total,
            digest: hex::encode(self.hash.finalize()),
        };
        self.attempt
            .audit(EventKind::ValuePublished, serde_json::to_value(&value)?)
            .await?;
        Ok(value)
    }
}
