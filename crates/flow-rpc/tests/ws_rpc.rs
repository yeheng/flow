use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use jsonrpsee::core::client::{ClientT, SubscriptionClientT};
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

/// 真机跑 flow-server 进程：崩溃恢复只能靠真的杀进程来证明。
struct ServerProc {
    child: Child,
    addr: SocketAddr,
    data_dir: PathBuf,
    db: PathBuf,
}

impl ServerProc {
    fn spawn(data_dir: PathBuf, db: PathBuf) -> ServerProc {
        let addr = free_port();
        let child = Command::new(env!("CARGO_BIN_EXE_flow-server"))
            .env("FLOW_DATA_DIR", &data_dir)
            .env("FLOW_DB", &db)
            .env("FLOW_ADDR", addr.to_string())
            .env("RUST_LOG", "info")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("启动 flow-server 失败");
        let proc = ServerProc {
            child,
            addr,
            data_dir,
            db,
        };
        wait_ready(addr);
        proc
    }

    fn kill(&mut self) {
        self.child.kill().expect("kill 失败");
        self.child.wait().expect("wait 失败");
    }

    async fn client(&self) -> WsClient {
        connect(self.addr).await
    }
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn wait_ready(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("flow-server {addr} 在 20s 内未就绪");
}

async fn connect(addr: SocketAddr) -> WsClient {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match WsClientBuilder::default()
            .request_timeout(Duration::from_secs(60))
            .build(format!("ws://{addr}"))
            .await
        {
            Ok(client) => return client,
            Err(err) => {
                if Instant::now() > deadline {
                    panic!("连接 {addr} 失败：{err}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// 按设计约定：所有方法使用具名参数（JSON-RPC 的 params 为对象）。
fn named(value: Value) -> ObjectParams {
    let mut params = ObjectParams::new();
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                params.insert(&key, item).unwrap();
            }
        }
        Value::Null => {}
        other => panic!("具名参数必须是对象：{other}"),
    }
    params
}

async fn call<T: serde::de::DeserializeOwned>(client: &WsClient, method: &str, params: Value) -> T {
    client
        .request(method, named(params))
        .await
        .unwrap_or_else(|e| panic!("调用 {method} 失败：{e}"))
}

async fn call_err(client: &WsClient, method: &str, params: Value) -> String {
    match client.request::<Value, _>(method, named(params)).await {
        Ok(value) => panic!("{method} 本应失败，实际返回 {value}"),
        Err(err) => err.to_string(),
    }
}

struct Workspace {
    proc: ServerProc,
    client: WsClient,
}

impl Workspace {
    async fn start() -> Workspace {
        let root = std::env::temp_dir().join(format!("flow-rpc-test-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let proc = ServerProc::spawn(root.clone(), root.join("flow.db"));
        let client = proc.client().await;
        Workspace { proc, client }
    }

    /// 重启服务端进程，复用同一份 data_dir / db（模拟宕机后重启）。
    async fn restart(&mut self) {
        self.proc.kill();
        let proc = ServerProc::spawn(self.proc.data_dir.clone(), self.proc.db.clone());
        self.client = proc.client().await;
        self.proc = proc;
    }

    async fn publish_workflow(&self, name: &str, definition: Value) -> (String, i64) {
        let created: Value = call(&self.client, "workflow.create", json!({"name": name})).await;
        let workflow_id = created["workflow_id"].as_str().unwrap().to_string();
        let updated: Value = call(
            &self.client,
            "workflow.update",
            json!({"workflow_id": workflow_id, "definition": definition}),
        )
        .await;
        let version = updated["version"].as_i64().unwrap();
        call::<Value>(
            &self.client,
            "workflow.publish",
            json!({"workflow_id": workflow_id, "version": version}),
        )
        .await;
        (workflow_id, version)
    }

    async fn start_run(&self, workflow_id: &str, input: Value) -> String {
        let started: Value = call(
            &self.client,
            "run.start",
            json!({"workflow_id": workflow_id, "input": input}),
        )
        .await;
        started["run_id"].as_str().unwrap().to_string()
    }

    async fn wait_run_status(&self, run_id: &str, expected: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let got: Value = call(&self.client, "run.get", json!({"run_id": run_id})).await;
            let status = got["run"]["status"].as_str().unwrap().to_string();
            if status == expected {
                return got;
            }
            if Instant::now() > deadline {
                panic!("run {run_id} 未在 30s 内变为 {expected}，当前 {status}（{got}）");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_event<F>(&self, run_id: &str, predicate: F)
    where
        F: Fn(&Value) -> bool,
    {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let events: Value = call(&self.client, "run.events", json!({"run_id": run_id})).await;
            if events["events"]
                .as_array()
                .map(|list| list.iter().any(&predicate))
                .unwrap_or(false)
            {
                return;
            }
            if Instant::now() > deadline {
                panic!("等待事件超时：{events}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.proc.data_dir);
    }
}

fn simple_def() -> Value {
    json!({
        "nodes": [
            {"id": "n1", "type": "start", "name": "输入", "position": {"x": 0, "y": 0}},
            {"id": "n2", "type": "script", "name": "计算", "params": {"code": "return { total: input.amount * 3 };"}},
            {"id": "n3", "type": "end", "name": "输出"}
        ],
        "edges": [{"from": "n1", "to": "n2"}, {"from": "n2", "to": "n3"}]
    })
}

#[tokio::test]
async fn workflow_crud_over_websocket() {
    let ws = Workspace::start().await;
    let client = &ws.client;

    let created: Value = call(client, "workflow.create", json!({"name": "订单流程"})).await;
    let workflow_id = created["workflow_id"].as_str().unwrap().to_string();

    // 非法定义（有环）在 update 阶段就被拦下
    let cyclic = json!({
        "nodes": [
            {"id": "a", "type": "start"},
            {"id": "b", "type": "script", "params": {"code": "return 1;"}},
            {"id": "c", "type": "script", "params": {"code": "return 2;"}},
            {"id": "done", "type": "end"}
        ],
        "edges": [
            {"from": "a", "to": "b"},
            {"from": "b", "to": "done"},
            {"from": "b", "to": "c"},
            {"from": "c", "to": "b"}
        ]
    });
    let err = call_err(
        client,
        "workflow.update",
        json!({"workflow_id": workflow_id, "definition": cyclic}),
    )
    .await;
    assert!(err.contains("环"), "{err}");

    let updated: Value = call(
        client,
        "workflow.update",
        json!({"workflow_id": workflow_id, "definition": simple_def()}),
    )
    .await;
    assert_eq!(updated["version"], json!(1));

    // 未发布不允许执行
    let err = call_err(client, "run.start", json!({"workflow_id": workflow_id})).await;
    assert!(err.contains("没有已发布版本"), "{err}");

    call::<Value>(
        client,
        "workflow.publish",
        json!({"workflow_id": workflow_id, "version": 1}),
    )
    .await;

    let fetched: Value = call(client, "workflow.get", json!({"workflow_id": workflow_id})).await;
    assert_eq!(fetched["status"], json!("published"));
    assert_eq!(fetched["definition"]["nodes"][1]["id"], json!("n2"));

    let list: Value = call(client, "workflow.list", json!({})).await;
    assert_eq!(list["workflows"].as_array().unwrap().len(), 1);
    assert_eq!(list["workflows"][0]["published_version"], json!(1));

    let node_types: Value = call(client, "nodetypes.list", json!({})).await;
    let types: Vec<&str> = node_types["node_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["type"].as_str().unwrap())
        .collect();
    assert!(
        types.contains(&"http_call") && types.contains(&"human_task"),
        "{types:?}"
    );

    // 删除：有 run 之后拒绝
    let run_id = ws.start_run(&workflow_id, json!({"amount": 1})).await;
    ws.wait_run_status(&run_id, "succeeded").await;
    let err = call_err(
        client,
        "workflow.delete",
        json!({"workflow_id": workflow_id}),
    )
    .await;
    assert!(err.contains("拒绝删除"), "{err}");
}

#[tokio::test]
async fn run_executes_and_timeline_is_readable() {
    let ws = Workspace::start().await;
    let (workflow_id, _) = ws.publish_workflow("流程", simple_def()).await;

    // 订阅拿推送
    let (tx, mut rx) = mpsc::channel::<Value>(64);
    let mut sub = ws
        .client
        .subscribe::<Value, _>("run.subscribe", named(json!({})), "run.unsubscribe")
        .await
        .unwrap();
    tokio::spawn(async move {
        while let Some(Ok(event)) = sub.next().await {
            if tx.send(event).await.is_err() {
                break;
            }
        }
    });

    let run_id = ws.start_run(&workflow_id, json!({"amount": 14})).await;
    let run = ws.wait_run_status(&run_id, "succeeded").await;
    assert_eq!(run["run"]["output"], json!({"total": 42}));
    assert_eq!(run["live"], json!(false));

    let timeline: Value = call(&ws.client, "run.timeline", json!({"run_id": run_id})).await;
    assert_eq!(timeline["status"], json!("succeeded"));
    assert_eq!(timeline["phase"], json!("succeeded"));
    let nodes = timeline["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3, "时间线按定义顺序列出全部节点：{timeline}");
    assert_eq!(nodes[0]["id"], json!("n1"));
    assert_eq!(nodes[1]["state"], json!("completed"));
    assert_eq!(nodes[1]["output"], json!({"total": 42}));
    assert!(nodes[1]["duration_ms"].as_u64().is_some());
    assert_eq!(nodes[1]["attempts"], json!(1));

    let events: Value = call(&ws.client, "run.events", json!({"run_id": run_id})).await;
    let kinds: Vec<&str> = events["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        vec![
            "run_started",
            "node_started",
            "node_completed",
            "node_started",
            "node_completed",
            "node_started",
            "node_completed",
            "run_completed"
        ]
    );

    // 推送应包含该 run 的事件
    let mut pushed = Vec::new();
    while let Ok(event) = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
        match event {
            Some(value) => {
                if value["run_id"] == json!(run_id) {
                    pushed.push(value["type"].as_str().unwrap().to_string());
                    if value["type"] == json!("run_completed") {
                        break;
                    }
                }
            }
            None => break,
        }
    }
    assert!(pushed.contains(&"run_completed".to_string()), "{pushed:?}");
}

#[tokio::test]
async fn human_task_is_resolved_by_signal_over_websocket() {
    let ws = Workspace::start().await;
    let def = json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "approve", "type": "human_task", "name": "审批", "params": {"prompt": "请审批"}},
            {"id": "n3", "type": "end"}
        ],
        "edges": [{"from": "n1", "to": "approve"}, {"from": "approve", "to": "n3"}]
    });
    let (workflow_id, _) = ws.publish_workflow("审批流程", def).await;
    let run_id = ws.start_run(&workflow_id, json!({"amount": 7})).await;

    ws.wait_event(&run_id, |e| {
        e["type"] == json!("node_started") && e["node_id"] == json!("approve")
    })
    .await;

    let got: Value = call(&ws.client, "run.get", json!({"run_id": run_id})).await;
    assert_eq!(
        got["run"]["status"],
        json!("running"),
        "等信号期间不能是终态"
    );

    call::<Value>(
        &ws.client,
        "run.signal",
        json!({"run_id": run_id, "node_id": "approve", "payload": {"approved_by": "alice"}}),
    )
    .await;

    let run = ws.wait_run_status(&run_id, "succeeded").await;
    assert_eq!(run["run"]["output"], json!({"approved_by": "alice"}));
}

#[tokio::test]
async fn restart_resumes_run_interrupted_mid_delay() {
    let mut ws = Workspace::start().await;
    let def = json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "slow", "type": "delay", "params": {"ms": 3000}},
            {"id": "n3", "type": "end"}
        ],
        "edges": [{"from": "n1", "to": "slow"}, {"from": "slow", "to": "n3"}]
    });
    let (workflow_id, _) = ws.publish_workflow("慢流程", def).await;
    let run_id = ws.start_run(&workflow_id, json!({"amount": 1})).await;

    // 等到延迟节点已经开始执行，然后强杀进程——这就是崩溃现场
    ws.wait_event(&run_id, |e| {
        e["type"] == json!("node_started") && e["node_id"] == json!("slow")
    })
    .await;
    ws.restart().await;

    // 重启后应自动续跑：残留的纯节点以新的 attempt 重放
    let run = ws.wait_run_status(&run_id, "succeeded").await;
    assert_eq!(run["run"]["output"], json!({"slept_ms": 3000}), "{run}");

    let timeline: Value = call(&ws.client, "run.timeline", json!({"run_id": run_id})).await;
    assert_eq!(timeline["nodes"][1]["attempts"], json!(2), "{timeline}");

    let events: Value = call(&ws.client, "run.events", json!({"run_id": run_id})).await;
    let replays = events["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["type"] == json!("node_started") && e["node_id"] == json!("slow"))
        .count();
    assert_eq!(replays, 2, "重放必须留下新的事件：{events}");
}

#[tokio::test]
async fn restart_asks_for_human_adjudication_on_side_effect_node() {
    // 一个只接受连接、永不响应的本地监听器：让 http_call 确定性地挂在请求里
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let hanging = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => held.push(stream),
                Err(_) => break,
            }
        }
    });

    let mut ws = Workspace::start().await;
    let def = json!({
        "nodes": [
            {"id": "n1", "type": "start"},
            {"id": "pay", "type": "http_call", "name": "下单",
             "params": {"method": "POST", "url": format!("http://{hanging}/pay"), "body": {"amount": "${input.amount}"}}},
            {"id": "n3", "type": "end"}
        ],
        "edges": [{"from": "n1", "to": "pay"}, {"from": "pay", "to": "n3"}]
    });
    let (workflow_id, _) = ws.publish_workflow("支付流程", def).await;
    let run_id = ws.start_run(&workflow_id, json!({"amount": 99})).await;

    ws.wait_event(&run_id, |e| {
        e["type"] == json!("node_started") && e["node_id"] == json!("pay")
    })
    .await;
    // 请求已发出但未收到响应时杀进程：副作用是否发生不可知
    ws.restart().await;

    let got = ws.wait_run_status(&run_id, "awaiting_resume").await;
    assert_eq!(got["live"], json!(true), "run 仍在引擎里等待裁决");

    let timeline: Value = call(&ws.client, "run.timeline", json!({"run_id": run_id})).await;
    assert_eq!(timeline["status"], json!("awaiting_resume"));
    assert_eq!(timeline["nodes"][1]["state"], json!("running"));

    // 引擎不会替用户猜：由人确认这次副作用成功了
    call::<Value>(
        &ws.client,
        "run.signal",
        json!({
            "run_id": run_id,
            "node_id": "pay",
            "payload": {"action": "succeeded", "output": {"status": 200, "body": {"paid": true}}}
        }),
    )
    .await;

    let run = ws.wait_run_status(&run_id, "succeeded").await;
    assert_eq!(
        run["run"]["output"],
        json!({"status": 200, "body": {"paid": true}})
    );
}

#[tokio::test]
async fn nodetypes_list_exposes_json_schema_and_sub_workflow() {
    let ws = Workspace::start().await;
    let node_types: Value = call(&ws.client, "nodetypes.list", json!({})).await;
    let list = node_types["node_types"].as_array().unwrap();

    // 旧 params 数组已移除，统一为 params_schema
    for t in list {
        assert!(t.get("params").is_none(), "{} 仍带旧 params 数组", t["type"]);
        assert_eq!(t["params_schema"]["type"], json!("object"), "{}", t["type"]);
    }

    let script = list.iter().find(|t| t["type"] == "script").unwrap();
    assert_eq!(script["params_schema"]["required"], json!(["code"]));
    assert_eq!(
        script["params_schema"]["properties"]["code"]["x-widget"],
        json!("code")
    );
    assert_eq!(
        script["params_schema"]["properties"]["timeout_ms"]["default"],
        json!(2000)
    );

    let http = list.iter().find(|t| t["type"] == "http_call").unwrap();
    assert_eq!(
        http["params_schema"]["properties"]["method"]["enum"],
        json!(flow_engine::HTTP_METHODS)
    );
    assert_eq!(
        http["params_schema"]["properties"]["headers"]["x-widget"],
        json!("json")
    );

    let sub = list.iter().find(|t| t["type"] == "sub_workflow").unwrap();
    assert_eq!(sub["params_schema"]["required"], json!(["workflow_id"]));
    assert_eq!(
        sub["params_schema"]["properties"]["workflow_id"]["x-widget"],
        json!("workflow-picker")
    );
}

#[tokio::test]
async fn sub_workflow_runs_child_and_links_timeline() {
    let ws = Workspace::start().await;

    // 子工作流：透传输入并包装
    let (child_id, _) = ws
        .publish_workflow(
            "子流程",
            json!({
                "nodes": [
                    {"id": "s", "type": "start"},
                    {"id": "n", "type": "script", "params": {"code": "return { got: input };"}},
                    {"id": "e", "type": "end"}
                ],
                "edges": [{"from": "s", "to": "n"}, {"from": "n", "to": "e"}]
            }),
        )
        .await;

    // 父工作流：start → sub_workflow → end
    let (parent_id, _) = ws
        .publish_workflow(
            "父流程",
            json!({
                "nodes": [
                    {"id": "s", "type": "start"},
                    {"id": "sub", "type": "sub_workflow", "params": {"workflow_id": child_id}},
                    {"id": "e", "type": "end"}
                ],
                "edges": [{"from": "s", "to": "sub"}, {"from": "sub", "to": "e"}]
            }),
        )
        .await;

    let run_id = ws.start_run(&parent_id, json!({"amount": 5})).await;
    let run = ws.wait_run_status(&run_id, "succeeded").await;
    // 子 run 输入 = 父 run 输入；子 run 输出透传为父 run 输出
    assert_eq!(run["run"]["output"], json!({"got": {"amount": 5}}));

    // 时间线带上确定性 child_run_id，且子 run 真实存在并已成功
    let timeline: Value = call(&ws.client, "run.timeline", json!({"run_id": run_id})).await;
    let sub = timeline["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["id"] == "sub")
        .unwrap();
    let child_run_id = sub["child_run_id"].as_str().unwrap();
    assert_eq!(child_run_id, format!("{run_id}:sub:1"));

    let child = ws.wait_run_status(child_run_id, "succeeded").await;
    assert_eq!(child["run"]["output"], json!({"got": {"amount": 5}}));
    assert_eq!(child["run"]["workflow_id"], json!(child_id));
}

#[tokio::test]
async fn sub_workflow_missing_workflow_id_is_rejected_at_save() {
    let ws = Workspace::start().await;
    let created: Value = call(&ws.client, "workflow.create", json!({"name": "坏流程"})).await;
    let workflow_id = created["workflow_id"].as_str().unwrap();
    let err = call_err(
        &ws.client,
        "workflow.update",
        json!({
            "workflow_id": workflow_id,
            "definition": {
                "nodes": [
                    {"id": "s", "type": "start"},
                    {"id": "sub", "type": "sub_workflow"},
                    {"id": "e", "type": "end"}
                ],
                "edges": [{"from": "s", "to": "sub"}, {"from": "sub", "to": "e"}]
            }
        }),
    )
    .await;
    assert!(err.contains("workflow_id"), "{err}");
}
