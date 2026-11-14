//! Host-owned resident session. Calls are transient, bounded, and never retried implicitly.
use flow_plugin_sdk::protocol::{
    envelope::{read_frame, write_frame},
    handshake::{
        check_version, initialize_request, verify_session_token, Capability, InitializeParams,
        InitializeResult,
    },
};
use flow_plugin_sdk::{Id, Message, Request, Session, SpawnConfig};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
pub type HostHandler = Arc<
    dyn Fn(String, Value) -> futures::future::BoxFuture<'static, Result<Value, String>>
        + Send
        + Sync,
>;
type Reply = oneshot::Sender<Result<Value, String>>;
struct PendingGuard {
    id: String,
    pending: Arc<Mutex<BTreeMap<String, Reply>>>,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}

fn host_error(id: Id, message: &str) -> Message {
    Message::Response(flow_plugin_sdk::Response {
        jsonrpc: "2.0",
        id,
        result: None,
        error: Some(flow_plugin_sdk::Error::new(
            flow_plugin_sdk::ErrorCode::Application(-32001),
            message,
        )),
    })
}

/// Read and write the plugin stream until it closes, the stop token fires, or the
/// outgoing channel closes. A limit or a bad notification stays inside this call.
async fn serve<S>(
    stream: S,
    outgoing: mpsc::Receiver<Message>,
    host: Option<HostHandler>,
    stop: CancellationToken,
    pending: Arc<Mutex<BTreeMap<String, Reply>>>,
    history: Arc<Mutex<(u64, std::collections::VecDeque<Value>)>>,
    events: broadcast::Sender<Value>,
    responses: mpsc::Sender<Message>,
    instance: String,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (incoming, mut messages) = mpsc::channel(64);
    let dispatch = async {
        let mut tasks = tokio::task::JoinSet::<Message>::new();
        loop {
            let message = tokio::select! {
                message = messages.recv() => match message {
                    Some(message) => message,
                    None => return,
                },
                result = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Ok(response)) = result {
                        if responses.send(response).await.is_err() {
                            return;
                        }
                    }
                    continue;
                }
            };
            match message {
                Message::Response(response) => {
                    if let Id::String(id) = response.id {
                        if let Some(reply) = pending.lock().unwrap().remove(&id) {
                            let _ = reply.send(match response.error {
                                Some(error) => Err(error.message),
                                None => Ok(response.result.unwrap_or(Value::Null)),
                            });
                        }
                    }
                }
                Message::Notification(notification) => {
                    let payload = notification.params.unwrap_or(Value::Null);
                    let preview = json!({"topic": notification.method, "payload": payload});
                    if serde_json::to_vec(&preview)
                        .map(|bytes| bytes.len())
                        .unwrap_or(usize::MAX)
                        > 65536
                    {
                        continue;
                    }
                    let mut history = history.lock().unwrap();
                    let sequence = history.0 + 1;
                    let source = flow_dto::interaction::Endpoint {
                        component: "plugin-service".into(),
                        instance: instance.clone(),
                        scope: "service".into(),
                    };
                    let message = flow_dto::interaction::InteractionMessage {
                        id: format!("{}:{sequence}", instance),
                        kind: flow_dto::interaction::MessageKind::Event,
                        topic: notification.method,
                        correlation_id: None,
                        payload,
                    };
                    if message.validate().is_err() {
                        continue;
                    }
                    history.0 = sequence;
                    let event = json!({"sequence": sequence.to_string(), "source": source, "event": message});
                    history.1.push_back(event.clone());
                    if history.1.len() > 128 {
                        history.1.pop_front();
                    }
                    drop(history);
                    let _ = events.send(event);
                }
                Message::Request(request) => {
                    if tasks.len() >= 16 {
                        if responses
                            .send(host_error(request.id, "host request limit"))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }
                    let host = host.clone();
                    tasks.spawn(async move {
                        let result = match host {
                            Some(host) => {
                                host(request.method, request.params.unwrap_or(Value::Null)).await
                            }
                            None => Err("host method forbidden".into()),
                        };
                        let result = result.and_then(|value| {
                            if serde_json::to_vec(&value).map_err(|e| e.to_string())?.len() > 65536
                            {
                                return Err("host response too large; use a smaller page".into());
                            }
                            Ok(value)
                        });
                        match result {
                            Ok(value) => Message::Response(flow_plugin_sdk::Response {
                                jsonrpc: "2.0",
                                id: request.id,
                                result: Some(value),
                                error: None,
                            }),
                            Err(error) => host_error(request.id, &error),
                        }
                    });
                }
            }
        }
    };
    tokio::select! {
        _ = flow_plugin_sdk::duplex::pump(stream, outgoing, incoming) => {}
        _ = dispatch => {}
        _ = stop.cancelled() => {}
    }
}

pub struct ResidentService {
    pub instance: String,
    pub binding: std::sync::OnceLock<(String, String, String)>,
    methods: BTreeSet<String>,
    sender: mpsc::Sender<Message>,
    pending: Arc<Mutex<BTreeMap<String, Reply>>>,
    pub events: broadcast::Sender<Value>,
    subscriptions: Mutex<BTreeMap<String, tokio::task::JoinHandle<()>>>,
    history: Arc<Mutex<(u64, std::collections::VecDeque<Value>)>>,
    stop: CancellationToken,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl ResidentService {
    pub async fn start(config: SpawnConfig, methods: Vec<String>) -> Result<Arc<Self>, String> {
        Self::start_with_host(config, methods, None).await
    }
    pub async fn start_with_host(
        config: SpawnConfig,
        methods: Vec<String>,
        host: Option<HostHandler>,
    ) -> Result<Arc<Self>, String> {
        Self::start_with_host_lease(config, methods, host, None).await
    }
    async fn start_with_host_lease(
        config: SpawnConfig,
        methods: Vec<String>,
        host: Option<HostHandler>,
        lease: Option<flow_config::plugin_fs::RootLease>,
    ) -> Result<Arc<Self>, String> {
        #[cfg(not(unix))]
        if config.challenge.is_none() {
            return Err("service requires a session challenge".into());
        }
        let expected_token = config.challenge.as_ref().map(|c| c.as_str().to_owned());
        let mut session = Session::spawn(config).await.map_err(|e| e.to_string())?;
        drop(lease);
        let handshake = async {
            let params = InitializeParams {
                protocol_version: flow_plugin_sdk::PROTOCOL_VERSION,
                capabilities: vec![Capability::InteractionEventsV1],
                client_info: json!({"name":"flow-resident-service","interaction_protocol":1}),
            };
            let request =
                Message::Request(initialize_request(Id::String("initialize".into()), &params));
            write_frame(
                session.channel_mut(),
                &flow_plugin_sdk::Frame {
                    kind: flow_plugin_sdk::FrameKind::Rpc,
                    payload: request.to_vec().unwrap(),
                },
            )
            .await
            .map_err(|e| e.to_string())?;
            let response = read_frame(session.channel_mut())
                .await
                .map_err(|e| e.to_string())?
                .as_rpc()
                .map_err(|e| e.to_string())?;
            let Message::Response(response) = response else {
                return Err("handshake response required".into());
            };
            if response.id != Id::String("initialize".into()) || response.error.is_some() {
                return Err("service handshake rejected".into());
            }
            let result: InitializeResult =
                serde_json::from_value(response.result.ok_or("missing handshake result")?)
                    .map_err(|e| e.to_string())?;
            check_version(result.protocol_version)?;
            verify_session_token(expected_token.as_deref(), &result).map_err(|e| e.message)?;
            if !result
                .capabilities
                .contains(&Capability::InteractionEventsV1)
            {
                return Err("interaction_protocol 1 required".into());
            }
            Ok::<_, String>(())
        };
        match tokio::time::timeout(Duration::from_secs(10), handshake).await {
            Ok(Ok(())) => {}
            result => {
                let _ = session.shutdown().await;
                return Err(format!("service handshake failed: {result:?}"));
            }
        }
        let (sender, outgoing) = mpsc::channel(64);
        let (events, _) = broadcast::channel(128);
        let pending = Arc::new(Mutex::new(BTreeMap::<String, Reply>::new()));
        let history = Arc::new(Mutex::new((0u64, std::collections::VecDeque::new())));
        let stop = CancellationToken::new();
        let service = Arc::new(Self {
            binding: std::sync::OnceLock::new(),
            subscriptions: Mutex::new(BTreeMap::new()),
            history: history.clone(),
            instance: uuid::Uuid::now_v7().to_string(),
            methods: methods.into_iter().collect(),
            sender,
            pending: pending.clone(),
            events: events.clone(),
            stop: stop.clone(),
            task: tokio::sync::Mutex::new(None),
        });
        let instance = service.instance.clone();
        let responses = service.sender.clone();
        let task = tokio::spawn(async move {
            serve(
                session.channel_mut(),
                outgoing,
                host,
                stop.clone(),
                pending.clone(),
                history,
                events,
                responses,
                instance,
            )
            .await;
            let _ = session.shutdown().await;
            for (_, reply) in std::mem::take(&mut *pending.lock().unwrap()) {
                let _ = reply.send(Err("service disconnected; outcome may be unknown".into()));
            }
            stop.cancel();
        });
        *service.task.lock().await = Some(task);
        Ok(service)
    }
    fn installed_config(entry: std::path::PathBuf) -> Result<SpawnConfig, String> {
        let mut config = SpawnConfig::new(entry);
        config.env = flow_engine::execution_protocol::environment::EXECUTOR_ENV_WHITELIST
            .iter()
            .filter_map(|name| std::env::var_os(name).map(|value| ((*name).into(), value)))
            .collect();
        #[cfg(not(unix))]
        {
            config.challenge =
                Some(flow_plugin_sdk::Challenge::generate().map_err(|e| e.to_string())?);
        }
        Ok(config)
    }
    pub async fn start_installed(
        entry: std::path::PathBuf,
        methods: Vec<String>,
    ) -> Result<Arc<Self>, String> {
        Self::start(Self::installed_config(entry)?, methods).await
    }
    pub async fn start_installed_with_host(
        entry: std::path::PathBuf,
        methods: Vec<String>,
        host: HostHandler,
    ) -> Result<Arc<Self>, String> {
        Self::start_with_host(Self::installed_config(entry)?, methods, Some(host)).await
    }
    pub async fn start_installed_with_host_lease(
        entry: std::path::PathBuf,
        methods: Vec<String>,
        host: HostHandler,
        lease: flow_config::plugin_fs::RootLease,
    ) -> Result<Arc<Self>, String> {
        Self::start_with_host_lease(
            Self::installed_config(entry)?,
            methods,
            Some(host),
            Some(lease),
        )
        .await
    }
    pub fn events_since(&self, instance: &str, after: u64) -> Result<Value, String> {
        if instance != self.instance {
            return Err("stale service instance".into());
        }
        let history = self.history.lock().unwrap();
        if after > history.0 {
            return Err("resync_required".into());
        }
        let retained = history.1.len();
        let missing = history.0.saturating_sub(after) > retained as u64;
        // after 0 is a fresh subscriber. Hand back the retained window and say
        // when older events are already gone. A nonzero cursor that fell out of
        // the window still has to resync; it claimed to have consumed that prefix.
        if missing && after != 0 {
            return Err("resync_required".into());
        }
        let available = if missing {
            retained
        } else {
            (history.0 - after) as usize
        };
        Ok(json!({
            "cursor": history.0.to_string(),
            "events": history.1.iter().skip(retained - available).cloned().collect::<Vec<_>>(),
            "alive": self.alive(),
            "gap": missing,
        }))
    }
    pub fn notify_ready(&self) -> Result<(), String> {
        self.sender
            .try_send(Message::Notification(flow_plugin_sdk::Notification {
                jsonrpc: "2.0",
                method: "flow/ready".into(),
                params: Some(json!({"instance":self.instance})),
            }))
            .map_err(|_| "service unavailable".into())
    }
    pub fn resource_token(&self) -> String {
        uuid::Uuid::now_v7().to_string()
    }
    pub fn alive(&self) -> bool {
        !self.stop.is_cancelled()
    }
    pub async fn call(&self, instance: &str, method: &str, params: Value) -> Result<Value, String> {
        if instance != self.instance {
            return Err("stale service instance".into());
        }
        if !self.alive() {
            return Err("service unavailable".into());
        }
        if !self.methods.contains(method) {
            return Err("method forbidden".into());
        }
        if serde_json::to_vec(&params)
            .map_err(|e| e.to_string())?
            .len()
            > 65536
        {
            return Err("payload too large".into());
        }
        let id = uuid::Uuid::now_v7().to_string();
        flow_dto::interaction::InteractionMessage {
            id: id.clone(),
            kind: flow_dto::interaction::MessageKind::Command,
            topic: method.into(),
            correlation_id: None,
            payload: params.clone(),
        }
        .validate()
        .map_err(str::to_owned)?;
        let (reply, result) = oneshot::channel();
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.len() >= 64 {
                return Err("service request limit".into());
            }
            pending.insert(id.clone(), reply);
        }
        let _guard = PendingGuard {
            id: id.clone(),
            pending: self.pending.clone(),
        };
        let message = Message::Request(Request {
            jsonrpc: "2.0",
            id: Id::String(id.clone()),
            method: method.into(),
            params: Some(params),
        });
        if self.sender.try_send(message).is_err() {
            self.pending.lock().unwrap().remove(&id);
            return Err("service queue unavailable".into());
        }
        let result = tokio::time::timeout(Duration::from_secs(30), result).await;
        self.pending.lock().unwrap().remove(&id);
        result
            .map_err(|_| "service timeout; outcome may be unknown".to_string())?
            .map_err(|_| "service disconnected".to_string())?
    }
    pub fn follow(
        &self,
        id: String,
        handler: HostHandler,
        method: String,
        mut params: Value,
    ) -> Result<String, String> {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        if !self.alive() {
            return Err("service unavailable".into());
        }
        if id.is_empty() || id.len() > 128 || id.chars().any(char::is_control) {
            return Err("invalid subscription identity".into());
        }
        subscriptions.retain(|_, task| !task.is_finished());
        if subscriptions.len() >= 16 {
            return Err("subscription limit reached".into());
        }
        if subscriptions.contains_key(&id) {
            return Err("subscription already exists".into());
        }
        let subscription = id.clone();
        let sender = self.sender.clone();
        let stop = self.stop.clone();
        let task = tokio::spawn(async move {
            loop {
                let result = tokio::select! {_ = stop.cancelled()=>return,result=handler(method.clone(),params.clone())=>result};
                let page = match result {
                    Ok(page) => page,
                    Err(error) => {
                        let _ =
                            sender.try_send(Message::Notification(flow_plugin_sdk::Notification {
                                jsonrpc: "2.0",
                                method: "flow/stream_closed".into(),
                                params: Some(json!({"subscription":subscription,"error":error})),
                            }));
                        return;
                    }
                };
                for event in page["events"].as_array().into_iter().flatten() {
                    let message = Message::Notification(flow_plugin_sdk::Notification {
                        jsonrpc: "2.0",
                        method: "flow/event".into(),
                        params: Some(json!({"subscription":subscription,"event":event})),
                    });
                    if sender.try_send(message).is_err() {
                        let _ =
                            sender.try_send(Message::Notification(flow_plugin_sdk::Notification {
                                jsonrpc: "2.0",
                                method: "flow/stream_closed".into(),
                                params: Some(json!({"subscription": subscription, "error": "event queue full"})),
                            }));
                        return;
                    }
                }
                if method == "flow/events" {
                    params["after"] = page["cursor"].clone();
                    if page["alive"] == false {
                        break;
                    }
                } else if !page["next_cursor"].is_null() {
                    params["cursor"] = page["next_cursor"].clone();
                    continue;
                } else {
                    if page["terminal"] == true {
                        break;
                    }
                    params.as_object_mut().map(|p| p.remove("cursor"));
                    params["since_lsn"] = page["snapshot_cursor"]["lsn"].clone();
                }
                tokio::select! {_=stop.cancelled()=>return,_=tokio::time::sleep(Duration::from_millis(100))=>{}}
            }
            let _ = sender.try_send(Message::Notification(flow_plugin_sdk::Notification {
                jsonrpc: "2.0",
                method: "flow/stream_closed".into(),
                params: Some(json!({"subscription":subscription,"reason":"stream_ended"})),
            }));
        });
        subscriptions.insert(id.clone(), task);
        Ok(id)
    }
    pub fn unfollow(&self, id: &str) -> bool {
        if let Some(task) = self.subscriptions.lock().unwrap().remove(id) {
            task.abort();
            true
        } else {
            false
        }
    }
    pub async fn shutdown(&self) {
        self.stop.cancel();
        let subscriptions = std::mem::take(&mut *self.subscriptions.lock().unwrap());
        for (_, task) in subscriptions {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }
}
impl Drop for ResidentService {
    fn drop(&mut self) {
        self.stop.cancel();
        for task in self.subscriptions.get_mut().unwrap().values() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_plugin_sdk::protocol::envelope::{read_frame, write_frame};
    use flow_plugin_sdk::{Frame, FrameKind};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn fixture(events: u64, sender: mpsc::Sender<Message>) -> Arc<ResidentService> {
        let (events_tx, _) = broadcast::channel(8);
        let mut history = (0u64, std::collections::VecDeque::new());
        for sequence in 1..=events {
            history.0 = sequence;
            history
                .1
                .push_back(json!({"sequence": sequence.to_string()}));
            if history.1.len() > 128 {
                history.1.pop_front();
            }
        }
        Arc::new(ResidentService {
            instance: "instance".into(),
            binding: std::sync::OnceLock::new(),
            methods: BTreeSet::new(),
            sender,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            events: events_tx,
            subscriptions: Mutex::new(BTreeMap::new()),
            history: Arc::new(Mutex::new(history)),
            stop: CancellationToken::new(),
            task: tokio::sync::Mutex::new(None),
        })
    }

    #[test]
    fn fresh_cursor_keeps_the_retained_window_and_marks_the_gap() {
        let (sender, _outgoing) = mpsc::channel(1);
        let service = fixture(200, sender);
        let page = service.events_since("instance", 0).unwrap();
        assert_eq!(page["gap"], json!(true));
        assert_eq!(page["cursor"], json!("200"));
        let events = page["events"].as_array().unwrap();
        assert_eq!(events.len(), 128);
        assert_eq!(events[0]["sequence"], json!("73"));
        assert!(service.events_since("instance", 71).is_err());
        let caught_up = service.events_since("instance", 72).unwrap();
        assert_eq!(caught_up["gap"], json!(false));
        assert_eq!(caught_up["events"].as_array().unwrap().len(), 128);
        assert_eq!(caught_up["events"][0]["sequence"], json!("73"));
        let current = service.events_since("instance", 200).unwrap();
        assert_eq!(current["gap"], json!(false));
        assert_eq!(current["cursor"], json!("200"));
        assert_eq!(current["events"].as_array().unwrap().len(), 0);
        assert!(service.events_since("instance", 201).is_err());
        assert!(service.events_since("other", 0).is_err());
        let (sender, _outgoing) = mpsc::channel(1);
        let small = fixture(2, sender);
        let page = small.events_since("instance", 0).unwrap();
        assert_eq!(page["gap"], json!(false));
        assert_eq!(page["events"].as_array().unwrap().len(), 2);
        let (sender, _outgoing) = mpsc::channel(1);
        let exact = fixture(128, sender);
        let page = exact.events_since("instance", 0).unwrap();
        assert_eq!(page["gap"], json!(false));
        assert_eq!(page["cursor"], json!("128"));
        assert_eq!(page["events"].as_array().unwrap().len(), 128);
        let (sender, _outgoing) = mpsc::channel(1);
        let empty = fixture(0, sender);
        let page = empty.events_since("instance", 0).unwrap();
        assert_eq!(page["gap"], json!(false));
        assert_eq!(page["cursor"], json!("0"));
        assert_eq!(page["events"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn full_event_queue_ends_the_subscription_only() {
        let (sender, mut outgoing) = mpsc::channel(1);
        sender
            .try_send(Message::Notification(flow_plugin_sdk::Notification {
                jsonrpc: "2.0",
                method: "filled".into(),
                params: None,
            }))
            .unwrap();
        let service = fixture(0, sender);
        let handler: HostHandler = Arc::new(|_, _| {
            Box::pin(async {
                Ok(json!({"events": [json!({"n": 1})], "cursor": "1", "alive": true}))
            })
        });
        service
            .follow(
                "sub".into(),
                handler,
                "flow/events".into(),
                json!({"after": "0"}),
            )
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let finished = service
                .subscriptions
                .lock()
                .unwrap()
                .get("sub")
                .is_some_and(|task| task.is_finished());
            if finished {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("subscription stayed alive");
            }
            tokio::task::yield_now().await;
        }
        assert!(service.alive());
        let _ = outgoing.try_recv();
    }

    #[tokio::test]
    async fn excess_host_request_is_rejected_without_stopping_the_service() {
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            let (host_io, mut plugin) = tokio::io::duplex(1024 * 1024);
            let (sender, outgoing) = mpsc::channel(64);
            let pending = Arc::new(Mutex::new(BTreeMap::new()));
            let history = Arc::new(Mutex::new((0u64, std::collections::VecDeque::new())));
            let (events, _) = broadcast::channel(8);
            let stop = CancellationToken::new();
            let block = Arc::new(AtomicBool::new(true));
            let host: HostHandler = Arc::new({
                let block = block.clone();
                move |method, _params| {
                    let block = block.clone();
                    Box::pin(async move {
                        if method == "ping" {
                            return Ok(json!("pong"));
                        }
                        while block.load(Ordering::SeqCst) {
                            tokio::task::yield_now().await;
                        }
                        Ok(json!("done"))
                    })
                }
            });
            let serve_task = tokio::spawn({
                let stop = stop.clone();
                async move {
                    serve(
                        host_io,
                        outgoing,
                        Some(host),
                        stop,
                        pending,
                        history,
                        events,
                        sender,
                        "instance".into(),
                    )
                    .await;
                }
            });
            for index in 0..17 {
                let request = Message::Request(Request {
                    jsonrpc: "2.0",
                    id: Id::String(format!("h{index}")),
                    method: "block".into(),
                    params: Some(json!({})),
                });
                write_frame(
                    &mut plugin,
                    &Frame {
                        kind: FrameKind::Rpc,
                        payload: request.to_vec().unwrap(),
                    },
                )
                .await
                .unwrap();
            }
            let mut saw_limit = false;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < deadline && !saw_limit {
                let frame =
                    match tokio::time::timeout(Duration::from_millis(200), read_frame(&mut plugin))
                        .await
                    {
                        Ok(frame) => frame.unwrap(),
                        Err(_) => continue,
                    };
                let Message::Response(response) = frame.as_rpc().unwrap() else {
                    continue;
                };
                if response
                    .error
                    .as_ref()
                    .is_some_and(|error| error.message.contains("host request limit"))
                {
                    saw_limit = true;
                }
            }
            assert!(saw_limit, "17th host request did not return an error");
            assert!(!stop.is_cancelled());
            block.store(false, Ordering::SeqCst);
            let request = Message::Request(Request {
                jsonrpc: "2.0",
                id: Id::String("ping".into()),
                method: "ping".into(),
                params: Some(json!({})),
            });
            write_frame(
                &mut plugin,
                &Frame {
                    kind: FrameKind::Rpc,
                    payload: request.to_vec().unwrap(),
                },
            )
            .await
            .unwrap();
            let mut saw_pong = false;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < deadline && !saw_pong {
                let frame =
                    match tokio::time::timeout(Duration::from_millis(200), read_frame(&mut plugin))
                        .await
                    {
                        Ok(frame) => frame.unwrap(),
                        Err(_) => continue,
                    };
                let Message::Response(response) = frame.as_rpc().unwrap() else {
                    continue;
                };
                if response.id == Id::String("ping".into()) {
                    assert_eq!(response.result, Some(json!("pong")));
                    saw_pong = true;
                }
            }
            assert!(saw_pong, "service stopped answering after the limit error");
            assert!(!stop.is_cancelled());
            stop.cancel();
            let _ = serve_task.await;
        })
        .await;
        result.expect("excess host request hung the service");
    }

    #[tokio::test]
    async fn dropped_notifications_do_not_gap_the_sequence_or_stop_the_service() {
        let (host_io, mut plugin) = tokio::io::duplex(1024 * 1024);
        let (sender, outgoing) = mpsc::channel(64);
        let pending = Arc::new(Mutex::new(BTreeMap::new()));
        let history = Arc::new(Mutex::new((0u64, std::collections::VecDeque::new())));
        let seen = history.clone();
        let (events, _) = broadcast::channel(8);
        let stop = CancellationToken::new();
        let serve_task = tokio::spawn({
            let stop = stop.clone();
            async move {
                serve(
                    host_io,
                    outgoing,
                    None,
                    stop,
                    pending,
                    history,
                    events,
                    sender,
                    "instance".into(),
                )
                .await;
            }
        });
        for note in [
            Message::Notification(flow_plugin_sdk::Notification {
                jsonrpc: "2.0",
                method: "x".repeat(200),
                params: Some(json!({})),
            }),
            Message::Notification(flow_plugin_sdk::Notification {
                jsonrpc: "2.0",
                method: "echo.received".into(),
                params: Some(json!("x".repeat(70_000))),
            }),
            Message::Notification(flow_plugin_sdk::Notification {
                jsonrpc: "2.0",
                method: "echo.received".into(),
                params: Some(json!({"ok": true})),
            }),
        ] {
            write_frame(
                &mut plugin,
                &Frame {
                    kind: FrameKind::Rpc,
                    payload: note.to_vec().unwrap(),
                },
            )
            .await
            .unwrap();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let stored = seen.lock().unwrap().1.iter().cloned().collect::<Vec<_>>();
            if stored.len() == 1 {
                assert_eq!(stored[0]["sequence"], json!("1"));
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("valid notification was not stored, history={stored:?}");
            }
            tokio::task::yield_now().await;
        }
        assert!(!stop.is_cancelled());
        stop.cancel();
        let _ = serve_task.await;
    }
}
