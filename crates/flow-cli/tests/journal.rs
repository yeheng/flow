use flow_backend::journal::JournalBackend;
use serde_json::{json, Value};
use tokio::process::Command;

#[tokio::test]
async fn v2_binary_calls_pages_and_downloads_without_overwriting() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = std::env::temp_dir().join(format!("flow-cli-v2-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&root).unwrap();
    let backend = JournalBackend::open(&root.join("data"), Default::default())
        .await
        .unwrap();
    let token = "c".repeat(32);
    let (server, addr) = flow_rpc::journal_v2::serve(
        backend.clone(),
        token.clone(),
        "127.0.0.1:0".parse().unwrap(),
    )
    .await
    .unwrap();
    let url = format!("ws://{addr}");
    let params = root.join("params.json");
    std::fs::write(
        &params,
        json!({"name":"cli","request_id":"stable"}).to_string(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_flow-cli"))
        .env("FLOW_JOURNAL_TOKEN", &token)
        .args([
            "--url",
            &url,
            "journal",
            "call",
            "workflow.create",
            "--params",
        ])
        .arg(&params)
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    let receipt: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["visible"], true);
    let workflow = receipt["result"]["workflow_id"].as_str().unwrap();
    backend.workflow_update(workflow,json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}),None).await.unwrap();
    backend.workflow_publish(workflow, 1, None).await.unwrap();
    let run = backend
        .run_start(workflow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run = run.result["run_id"].as_str().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_flow-cli"))
        .env("FLOW_JOURNAL_TOKEN", &token)
        .args(["--url", &url, "journal", "events", run])
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    let records: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(!records.is_empty());
    // Exercise the actual streaming client against a chunked HTTP response.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = format!("http://{}", listener.local_addr().unwrap());
    let responder = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            socket.read(&mut request).await.unwrap();
            socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n").await.unwrap();
        }
    });
    let destination = root.join("download.bin");
    for expected in [true, false] {
        let output = Command::new(env!("CARGO_BIN_EXE_flow-cli"))
            .env("FLOW_JOURNAL_TOKEN", &token)
            .args(["--url", &url, "journal", "download", run, "value"])
            .arg(&destination)
            .args(["--http", &http])
            .output()
            .await
            .unwrap();
        assert_eq!(output.status.success(), expected, "{:?}", output);
        assert_eq!(std::fs::read(&destination).unwrap(), b"abcdef");
    }
    responder.await.unwrap();
    server.stop().unwrap();
    server.stopped().await;
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}
