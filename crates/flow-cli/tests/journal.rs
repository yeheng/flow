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
    let output = Command::new(flow_test_support::io::flow_bin())
        .arg("cli")
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
    let output = Command::new(flow_test_support::io::flow_bin())
        .arg("cli")
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
            let _ = socket.read(&mut request).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n").await.unwrap();
        }
    });
    let destination = root.join("download.bin");
    for expected in [true, false] {
        let output = Command::new(flow_test_support::io::flow_bin())
            .arg("cli")
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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn long_paginated_history_streams_to_stdout_with_bounded_rss() {
    use std::process::Stdio;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let count = Arc::new(AtomicUsize::new(0));
    let mut module = jsonrpsee::RpcModule::new(count.clone());
    module.register_method("run.events.page",|params,ctx,_|{
        let p:Value=params.parse().unwrap();assert_eq!(p["_token"],"long-history-token");
        let page=p["cursor"]["page"].as_u64().unwrap_or(0);
        let events:Vec<Value>=(0..100).map(|i|json!({"lsn":(page*100+i+1).to_string(),"event_index":0,"event":{"run_seq":(page*100+i+1).to_string(),"kind":"input_prepared","payload":"x".repeat(1024)}})).collect();
        ctx.fetch_add(1,Ordering::Relaxed);
        json!({"events":events,"next_cursor":if page<999{json!({"page":page+1})}else{Value::Null},"snapshot_cursor":{"journal_id":"fixed","lsn":"100000"}})
    }).unwrap();
    let server = jsonrpsee::server::Server::builder()
        .build("127.0.0.1:0")
        .await
        .unwrap();
    let url = format!("ws://{}", server.local_addr().unwrap());
    let handle = server.start(module);
    let output = Command::new("/usr/bin/time")
        .args(["-l", flow_test_support::io::flow_bin().to_str().unwrap()])
        .args(["cli", "--url", &url, "journal", "events", "run"])
        .env("FLOW_JOURNAL_TOKEN", "long-history-token")
        .stdout(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    assert_eq!(count.load(Ordering::Relaxed), 1000);
    let stats = String::from_utf8(output.stderr).unwrap();
    let rss = stats
        .lines()
        .find(|line| line.contains("maximum resident set size"))
        .expect("RSS measurement requires unsandboxed test")
        .split_whitespace()
        .next()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    println!("CLI 100000 records, 1000 pages, >100 MiB payload; peak_rss={rss} bytes");
    assert!(rss < 128 * 1024 * 1024, "{stats}");
    handle.stop().unwrap();
    handle.stopped().await;
}
