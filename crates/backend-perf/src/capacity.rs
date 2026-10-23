//! Resource/history/soak evidence. Run release builds; JSON reports include actual durations.
use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::{Run, RunStatus, WaitKind};
use flow_journal::{Event, EventKind, StoredValue};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let mode = args.first().map(String::as_str).unwrap_or("help");
    if mode == "storage-crash-child" {
        return storage_crash_child(
            args.get(1).ok_or("missing child storage kind")?,
            Path::new(args.get(2).ok_or("missing child root")?),
        )
        .await;
    }
    if args.len() < 3 {
        return Err("usage: flow-capacity history|attempts|storage ROOT REPORT [COUNT]; flow-capacity soak|agent-soak ROOT REPORT SECONDS EXECUTOR".into());
    }
    let root = PathBuf::from(&args[1]);
    std::fs::create_dir_all(&root)?;
    let report = match mode {
        "history" => history(&root, args.get(3).map_or(Ok(100_000), |s| s.parse())?).await?,
        "audits" => audits(&root, args.get(3).map_or(Ok(100_000), |s| s.parse())?).await?,
        "attempts" => attempts(&root, args.get(3).map_or(Ok(100_000), |s| s.parse())?).await?,
        "soak" | "agent-soak" => {
            soak(
                &root,
                args.get(3).ok_or("missing seconds")?.parse()?,
                PathBuf::from(args.get(4).ok_or("missing executor")?),
                mode == "agent-soak",
            )
            .await?
        }
        "storage" => storage(&root, args.get(3).map_or(Ok(10_000), |s| s.parse())?).await?,
        _ => return Err("unknown mode".into()),
    };
    let report = json!({"profile":if cfg!(debug_assertions){"debug"}else{"release"},"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"generated_at":chrono::Utc::now(),"legacy_baseline":cfg!(feature="legacy-baseline"),"result":report});
    std::fs::write(&args[2], serde_json::to_vec_pretty(&report)?)?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
async fn snapshot(b: &JournalBackend, id: &str) -> Result<Option<Run>> {
    #[cfg(feature = "legacy-baseline")]
    {
        Ok(b.state().await.unwrap().runs.get(id).cloned())
    }
    #[cfg(not(feature = "legacy-baseline"))]
    {
        Ok(b.run_snapshot(id).await?)
    }
}
fn wait_kind(wait: &flow_engine::journal_state::Wait) -> WaitKind {
    #[cfg(feature = "legacy-baseline")]
    {
        // The persisted shape is shared by both revisions; this adapter also
        // compiles when the comparison feature is enabled on the current tree.
        serde_json::from_value(serde_json::to_value(wait).unwrap()["kind"].clone()).unwrap()
    }
    #[cfg(not(feature = "legacy-baseline"))]
    {
        wait.kind()
    }
}
fn disk(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| {
            if e.path().is_dir() {
                disk(&e.path())
            } else {
                e.metadata().map_or(0, |m| m.len())
            }
        })
        .sum()
}
fn process(pid: i32) -> Value {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut info: libc::proc_taskinfo = std::mem::zeroed();
        let n = libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTASKINFO,
            0,
            (&mut info as *mut libc::proc_taskinfo).cast(),
            std::mem::size_of_val(&info) as i32,
        );
        let mut fds = vec![0u8; 128 * 1024];
        let fdbytes = libc::proc_pidinfo(
            pid,
            libc::PROC_PIDLISTFDS,
            0,
            fds.as_mut_ptr().cast(),
            fds.len() as i32,
        );
        json!({"pid":pid,"rss_bytes":if n>0{Some(info.pti_resident_size)}else{None},"threads":if n>0{Some(info.pti_threadnum)}else{None},"fds":if fdbytes>=0{Some(fdbytes as usize/std::mem::size_of::<libc::proc_fdinfo>())}else{None}})
    }
    #[cfg(target_os = "linux")]
    {
        let pages = std::fs::read_to_string(format!("/proc/{pid}/statm"))
            .ok()
            .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok());
        json!({"pid":pid,"rss_bytes":pages.map(|p|p*unsafe{libc::sysconf(libc::_SC_PAGESIZE)} as u64),"fds":std::fs::read_dir(format!("/proc/{pid}/fd")).ok().map(|d|d.count())})
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        json!({"pid":pid,"rss_bytes":null,"fds":null})
    }
}
#[cfg(target_os = "linux")]
fn child_pids(pid: i32) -> std::io::Result<Vec<i32>> {
    let mut pids = std::collections::BTreeSet::new();
    // Children belong to the thread that spawned them, not necessarily the leader.
    for thread in std::fs::read_dir(format!("/proc/{pid}/task"))? {
        let path = thread?.path().join("children");
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for raw in text.split_whitespace() {
            pids.insert(
                raw.parse()
                    .map_err(|_| std::io::Error::other("invalid child PID"))?,
            );
        }
    }
    Ok(pids.into_iter().collect())
}
#[cfg(target_os = "macos")]
fn child_pids(pid: i32) -> std::io::Result<Vec<i32>> {
    unsafe {
        let count = libc::proc_listchildpids(pid, std::ptr::null_mut(), 0);
        if count < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut pids = vec![0i32; count as usize + 1024];
        let n = libc::proc_listchildpids(
            pid,
            pids.as_mut_ptr().cast(),
            std::mem::size_of_val(pids.as_slice()) as i32,
        );
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(pids.into_iter().filter(|pid| *pid > 0).collect())
    }
}
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn child_pids(_: i32) -> std::io::Result<Vec<i32>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "process enumeration unavailable",
    ))
}
fn resources() -> Value {
    let pid = std::process::id() as i32;
    let mut todo = vec![pid];
    let mut seen = std::collections::BTreeSet::from([pid]);
    let mut children = Vec::new();
    let mut errors = Vec::new();
    while let Some(parent) = todo.pop() {
        match child_pids(parent) {
            Ok(pids) => {
                for child in pids {
                    if seen.len() >= 4096 {
                        errors.push("process count exceeds 4096".into());
                        break;
                    }
                    if seen.insert(child) {
                        children.push(process(child));
                        todo.push(child);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {} // process exited during sampling
            Err(e) => errors.push(format!("pid {parent}: {e}")),
        }
    }
    json!({"master":process(pid),"children":children,"children_collection":{"supported":cfg!(any(target_os="macos",target_os="linux")),"complete":errors.is_empty(),"errors":errors},"tokio_tasks":tokio::runtime::Handle::current().metrics().num_alive_tasks()})
}
fn scoped(
    kind: EventKind,
    id: &str,
    run_seq: u64,
    dispatch: Option<&str>,
    audit: u64,
    payload: Value,
) -> Event {
    let mut e = Event::new(kind, payload);
    e.run_id = Some(id.into());
    e.run_seq = run_seq;
    e.node_id = dispatch.map(|_| "s".into());
    e.dispatch_id = dispatch.map(str::to_owned);
    e.audit_seq = audit;
    e
}
fn definition() -> Value {
    json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]})
}
fn run_start(id: &str, workflow: &str) -> Event {
    scoped(
        EventKind::RunStarted,
        id,
        1,
        None,
        0,
        json!({"run_id":id,"workflow_id":workflow,"workflow_version":1,"definition":StoredValue::Inline(definition()),"input":StoredValue::Inline(Value::Null),"source":"capacity-fixture","source_detail":null,"parent":null,"depth":0,"created_at":"2026-10-15T00:00:00Z","status":"running","output":null,"error":null,"last_run_seq":"0","nodes":{}}),
    )
}
fn dispatch(id: &str, n: u64, seq: u64) -> Event {
    scoped(
        EventKind::DispatchStarted,
        id,
        seq,
        Some(&format!("{id}-d{n}")),
        0,
        json!({"node_execution_id":format!("{id}-s"),"attempt":n.to_string()}),
    )
}
fn prepared(id: &str, dispatch: &str) -> Event {
    scoped(
        EventKind::InputPrepared,
        id,
        0,
        Some(dispatch),
        1,
        json!({"prepared":{"input":StoredValue::Inline(Value::Null),"predecessors":{},"params":StoredValue::Inline(json!({"pad":"x".repeat(1024)})),"engine_build":"capacity-fixture","node_semantics":1}}),
    )
}
async fn workflow(b: &JournalBackend) -> Result<String> {
    let r = b.workflow_create("capacity", None).await?;
    let id = r.result["workflow_id"]
        .as_str()
        .ok_or("workflow id")?
        .to_owned();
    b.workflow_update(&id, definition(), None).await?;
    b.workflow_publish(&id, 1, None).await?;
    Ok(id)
}
fn summary(samples: &[f64]) -> Value {
    if samples.is_empty() {
        return Value::Null;
    }
    let mut v = samples.to_vec();
    v.sort_by(f64::total_cmp);
    json!({"count":v.len(),"p50_ms":v[(v.len()-1)/2],"p95_ms":v[(v.len()*95).div_ceil(100).saturating_sub(1)],"p99_ms":v[(v.len()*99).div_ceil(100).saturating_sub(1)],"max_ms":v.last()})
}
async fn history(root: &Path, count: usize) -> Result<Value> {
    let b = JournalBackend::open(root, Default::default()).await?;
    let workflow = workflow(&b).await?;
    b.append(vec![run_start("active", &workflow)]).await?;
    let started = Instant::now();
    let mut reports = Vec::new();
    let mut first_receipt = None;
    let mut first_audit = None;
    let mut index = 0;
    while index < count {
        let end = [1_000, 10_000, 100_000, count]
            .into_iter()
            .filter(|n| *n > index)
            .fold((index + 16).min(count), usize::min);
        let mut events = Vec::new();
        for i in index..end {
            let id = format!("history-{i:08}");
            let d = format!("{id}-d1");
            let r = flow_journal::ValueRef {
                journal_id: b.journal.id().into(),
                output_id: format!("value-{i:08}"),
                codec: flow_journal::value::ValueCodec::Utf8,
                version: 1,
                chunk_count: 0,
                total_bytes: 0,
                digest: flow_journal::codec::digest(b""),
            };
            events.push(Event::new(EventKind::ValuePublished, json!(r)));
            events.push(run_start(&id, &workflow));
            events.push(dispatch(&id, 1, 2));
            let prep = prepared(&id, &d);
            if i == 0 {
                first_audit = Some(prep.clone());
            }
            events.push(prep);
            events.push(scoped(EventKind::NodeCompleted,&id,3,Some(&d),2,json!({"output":StoredValue::Ref(r.clone()),"sealed_through":"2","integrity":"complete"})));
            events.push(scoped(
                EventKind::RunCompleted,
                &id,
                4,
                None,
                0,
                json!({"output":StoredValue::Ref(r)}),
            ));
        }
        let lsn = b.append(events).await?;
        if first_receipt.is_none() {
            first_receipt = Some(lsn)
        }
        index = end;
        if [1_000, 10_000, 100_000, count].contains(&index) {
            let checkpoint_start = Instant::now();
            let checkpoint = b.checkpoint().await;
            let checkpoint_ms = checkpoint_start.elapsed().as_secs_f64() * 1000.;
            let mut reads = Vec::new();
            for _ in 0..100 {
                let t = Instant::now();
                assert!(snapshot(&b, "history-00000000").await?.is_some());
                reads.push(t.elapsed().as_secs_f64() * 1000.);
            }
            let sample = json!({"runs":index,"elapsed_secs":started.elapsed().as_secs_f64(),"resources":resources(),"diagnostics":b.diagnostics().await,"checkpoint_ok":checkpoint.is_ok(),"checkpoint_error":checkpoint.err().map(|e|e.to_string()),"checkpoint_ms":checkpoint_ms,"checkpoint_bytes":std::fs::metadata(root.join("checkpoints/latest.json")).map_or(0,|m|m.len()),"disk_bytes":disk(root),"read":summary(&reads)});
            println!("{sample}");
            reports.push(sample);
        }
    }
    let original = first_audit.ok_or("no audit")?;
    assert_eq!(
        b.append(vec![original.clone()]).await?,
        first_receipt.ok_or("no receipt")?
    );
    let mut late = scoped(
        EventKind::LateAudit,
        "history-00000000",
        0,
        Some("history-00000000-d1"),
        3,
        json!({"evidence":"after eviction"}),
    );
    b.append(vec![late.clone()]).await?;
    late.payload = json!({"evidence":"changed"});
    assert!(b.append(vec![late]).await.is_err());
    assert_eq!(
        snapshot(&b, "history-00000000")
            .await?
            .ok_or("history gone")?
            .nodes["s"]
            .attempts["history-00000000-d1"]
            .audit_seq,
        3
    );
    b.close().await?;
    drop(b);
    let ready = Instant::now();
    let b = JournalBackend::open(root, Default::default()).await?;
    let restart_ms = ready.elapsed().as_secs_f64() * 1000.;
    assert_eq!(b.append(vec![original]).await?, first_receipt.unwrap());
    let diagnostics = b.diagnostics().await;
    b.close().await?;
    drop(b);
    Ok(
        json!({"mode":"history","payload":"verified synthetic facts, 1 KiB prepared params and one publication per terminal run","terminal_runs":count,"fixed_active_runs":1,"samples":reports,"restart_ms":restart_ms,"restart_resources":resources(),"restart_diagnostics":diagnostics,"errors":0,"timeouts":0}),
    )
}
async fn attempts(root: &Path, count: usize) -> Result<Value> {
    let b = JournalBackend::open(root, Default::default()).await?;
    let w = workflow(&b).await?;
    b.append(vec![
        run_start("retry", &w),
        dispatch("retry", 1, 2),
        prepared("retry", "retry-d1"),
    ])
    .await?;
    let start = Instant::now();
    let mut reports = Vec::new();
    let mut seq = 2;
    let mut index = 1;
    while index <= count {
        let mut events = Vec::new();
        let end = [1_001, 10_001, 100_001, count + 1]
            .into_iter()
            .filter(|n| *n > index)
            .fold((index + 32).min(count + 1), usize::min);
        let t = Instant::now();
        for n in index..end {
            seq += 1;
            events.push(scoped(EventKind::NodeFailed,"retry",seq,Some(&format!("retry-d{n}")),0,json!({"error":"capacity retry","sealed_through":if n==1{"1"}else{"0"},"retry_wake_at":1})));
            if n < count {
                seq += 1;
                events.push(dispatch("retry", n as u64 + 1, seq));
            }
        }
        b.append(events).await?;
        let batch_ms = t.elapsed().as_secs_f64() * 1000.;
        index = end;
        let finished = end - 1;
        if [1_000, 10_000, 100_000, count].contains(&finished) {
            let cp = b.checkpoint().await;
            let sample = json!({"checkpoint_ok":cp.is_ok(),"checkpoint_error":cp.err().map(|e|e.to_string()),"attempts":finished,"elapsed_secs":start.elapsed().as_secs_f64(),"last_batch_ms":batch_ms,"resources":resources(),"diagnostics":b.diagnostics().await,"disk_bytes":disk(root)});
            println!("{sample}");
            reports.push(sample);
        }
    }
    b.close().await?;
    drop(b);
    let t = Instant::now();
    let b = JournalBackend::open(root, Default::default()).await?;
    let run = snapshot(&b, "retry").await?.ok_or("retry gone")?;
    assert_eq!(run.nodes["s"].attempt, count as u32);
    let diagnostic = b.diagnostics().await;
    let restart_ms = t.elapsed().as_secs_f64() * 1000.;
    b.close().await?;
    Ok(
        json!({"mode":"attempts","count":count,"samples":reports,"restart_ms":restart_ms,"restart_diagnostics":diagnostic,"errors":0,"timeouts":0}),
    )
}

async fn execute(
    b: Arc<JournalBackend>,
    workflow: String,
    kind: usize,
    index: u64,
) -> Result<RunStatus> {
    let receipt = b
        .run_start(
            &workflow,
            None,
            json!({"index":index}),
            "manual",
            None,
            None,
        )
        .await?;
    let id = receipt.result["run_id"]
        .as_str()
        .ok_or("run id")?
        .to_owned();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut delivered = false;
    loop {
        if Instant::now() >= deadline {
            return Err(format!("run timeout: {id}").into());
        }
        let run = snapshot(&b, &id).await?.ok_or("run disappeared")?;
        if run.terminal() {
            if run.status == RunStatus::Failed || run.status == RunStatus::AwaitingResume {
                return Err(format!("run {id} ended as {}: {:?}", run.status, run.error).into());
            }
            return Ok(run.status);
        }
        if !delivered {
            if kind == 4
                && run
                    .nodes
                    .get("n")
                    .and_then(|n| n.wait.as_ref())
                    .is_some_and(|w| wait_kind(w) == WaitKind::Signal)
            {
                b.run_signal(&id, "n", json!({"ok":true}), None).await?;
                delivered = true;
            }
            if kind == 5 && run.nodes.contains_key("n") {
                b.run_cancel(&id, None).await?;
                delivered = true;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
async fn soak(root: &Path, seconds: u64, executor: PathBuf, include_agents: bool) -> Result<Value> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let http = tokio::spawn(async move {
        let mut requests = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                incoming=listener.accept()=>{let Ok((mut stream,_))=incoming else{break};requests.spawn(async move {let mut buf=[0u8;4096];let _=stream.read(&mut buf).await;tokio::time::sleep(Duration::from_millis(80)).await;let _=stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}").await;});},
                _=requests.join_next(),if !requests.is_empty()=>{}
            }
        }
    });
    let b = JournalBackend::open(root, Default::default()).await?;
    let provider = if include_agents {
        let provider = flow_test_support::provider::spawn_mock_openai(vec![
            flow_test_support::provider::Turn::ToolThenFinal {
                command: "echo agent-soak".into(),
                text: "agent completed".into(),
                delay_ms: 80,
            },
        ])
        .await;
        std::env::set_var("FLOW_SECRET_CAPACITY_AGENT", "mock-capacity-key");
        b.install_harness_agent(Some(flow_engine::AgentRunConfig {
            base_url: provider.base_url(),
            model: "mock-capacity-model".into(),
            max_turns: Some(4),
            ..Default::default()
        }))?;
        Some(provider)
    } else {
        None
    };
    b.start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
        flow_backend::execution::IpcOptions {
            executor: flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
                executor,
            ),
            x_max: 4,
            tag: None,
        },
    ))
    .await?;
    let mut workflows = Vec::new();
    for (kind, params) in [
        (
            "script",
            json!({"code":"return {blob: 'x'.repeat(256 * 1024)};"}),
        ),
        (
            "script",
            json!({"code":"for (let i=0;i<2000;i++) console.info('sample-'+i); return {ok:true};"}),
        ),
        ("http_call", json!({"url":format!("http://{addr}/")})),
        ("delay", json!({"ms":80})),
        ("human_task", json!({})),
        ("delay", json!({"ms":1000})),
    ] {
        let r = b.workflow_create("soak", None).await?;
        let id = r.result["workflow_id"]
            .as_str()
            .ok_or("workflow")?
            .to_owned();
        b.workflow_update(&id,json!({"nodes":[{"id":"s","type":"start"},{"id":"n","type":kind,"params":params},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),None).await?;
        b.workflow_publish(&id, 1, None).await?;
        workflows.push(id);
    }
    if include_agents {
        let imported = b.workflow_import("agent-soak", json!({"semantics":2,"nodes":[
            {"id":"s","type":"start"}, {"id":"n","type":"harness","params":{
                "mode":"agent","prompt":"run the tool, then finish", "api_key":"CAPACITY_AGENT", "timeout_ms":30_000
            }}, {"id":"e","type":"end"}
        ],"edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}), true, None).await?;
        workflows.push(
            imported.result["workflow_id"]
                .as_str()
                .ok_or("agent workflow")?
                .to_owned(),
        );
    }
    let start = Instant::now();
    let mut launch = tokio::time::interval(Duration::from_millis(200));
    launch.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut monitor = tokio::time::interval(Duration::from_secs(5));
    monitor.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut jobs = tokio::task::JoinSet::new();
    let mut samples = Vec::new();
    let mut latencies = Vec::new();
    let (mut submitted, mut succeeded, mut cancelled, mut failed, mut timeouts) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut errors = Vec::new();
    while start.elapsed().as_secs() < seconds || !jobs.is_empty() {
        tokio::select! {
            _=launch.tick(),if start.elapsed().as_secs()<seconds && jobs.len()<4=>{
                let kind=submitted as usize%workflows.len();let w=workflows[kind].clone();let b=b.clone();let index=submitted;
                jobs.spawn(async move {let now=Instant::now();(execute(b,w,kind,index).await,now.elapsed().as_secs_f64()*1000.)});submitted+=1;
            },
            result=jobs.join_next(),if !jobs.is_empty()=>{
                match result.ok_or("job disappeared")? {
                    Ok((Ok(RunStatus::Succeeded),ms))=>{succeeded+=1;latencies.push(ms)},
                    Ok((Ok(RunStatus::Cancelled),ms))=>{cancelled+=1;latencies.push(ms)},
                    Ok((Ok(status),_))=>{failed+=1;if errors.len()<64{errors.push(status.to_string())}},
                    Ok((Err(e),_))=>{failed+=1;if e.to_string().contains("timeout"){timeouts+=1}
                    if errors.len()<64{errors.push(e.to_string())}},
                    Err(e)=>{failed+=1;if errors.len()<64{errors.push(e.to_string())}},
                }
            },
            _=monitor.tick()=>{
                let now=Instant::now();let query=flow_backend::journal_views::list_runs(&b,None,None,None,None,20).await;
                let read_ms=now.elapsed().as_secs_f64()*1000.;if let Err(e)=query{failed+=1;if errors.len()<64{errors.push(e.to_string())}}
                let sample=json!({"elapsed_secs":start.elapsed().as_secs_f64(),"submitted":submitted,"succeeded":succeeded,"cancelled":cancelled,"failed":failed,"timeouts":timeouts,"errors":errors,"resources":resources(),"diagnostics":b.diagnostics().await,"observation_loss":b.observations.as_ref().map(|o|o.loss()),"list_read_ms":read_ms});println!("{sample}");samples.push(sample);
            }
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    b.checkpoint().await?;
    let before_close = resources();
    b.close().await?;
    drop(b);
    http.abort();
    let _ = http.await;
    let restart = Instant::now();
    let b = JournalBackend::open(root, Default::default()).await?;
    let restart_ms = restart.elapsed().as_secs_f64() * 1000.;
    let restored = b.diagnostics().await;
    b.close().await?;
    drop(b);
    let provider_calls = provider.as_ref().map_or(0, |p| p.hits());
    if let Some(provider) = provider {
        provider.stop().await;
    }
    let report = json!({"mode":if include_agents {"agent-soak"} else {"soak"},"inline_agent":include_agents,"provider_calls":provider_calls,"provider":"local deterministic mock","dispatch_limit":4,"max_in_flight_runs":4,"launch_interval_ms":200,"requested_secs":seconds,"elapsed_secs":elapsed,"submitted":submitted,"succeeded":succeeded,"cancelled":cancelled,"failed":failed,"timeouts":timeouts,"errors":errors,"latency":summary(&latencies),"samples":samples,"before_close":before_close,"after_close":resources(),"restart_ms":restart_ms,"restart_diagnostics":restored,"disk_bytes":disk(root)});
    if failed > 0 {
        std::fs::write(
            root.join("failed-soak.json"),
            serde_json::to_vec_pretty(&report)?,
        )?;
        return Err(format!("soak had {failed} failures: {errors:?}").into());
    }
    Ok(report)
}
async fn storage(root: &Path, count: usize) -> Result<Value> {
    use sqlx::sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
    };
    let mut results = Vec::new();
    for batch in [1usize, 64] {
        let dir = root.join(format!("journal-{batch}"));
        let j = flow_journal::Journal::open(&dir, Default::default()).await?;
        let mut latency = Vec::new();
        let start = Instant::now();
        for i in (0..count).step_by(batch) {
            let events = (i..(i + batch).min(count))
                .map(|n| {
                    Event::new(
                        EventKind::LegacyImport,
                        json!({"key":format!("bench-{n}"),"payload":"x".repeat(1024)}),
                    )
                })
                .collect();
            let now = Instant::now();
            j.submit(format!("tx-{i}"), events, flow_journal::QueueClass::Control)
                .await?;
            latency.push(now.elapsed().as_secs_f64() * 1000.);
        }
        let elapsed = start.elapsed().as_secs_f64();
        let upper = j.durable_lsn();
        let identity = j.id().to_owned();
        j.close().await?;
        drop(j);
        let replay = Instant::now();
        let mut tail = flow_journal::tail::TailReader::new(&dir, &identity);
        let mut events = 0;
        while let Some((tx, _)) = tail.next(upper)? {
            events += tx
                .events
                .iter()
                .filter(|e| e.kind == EventKind::LegacyImport)
                .count();
        }
        assert_eq!(events, count);
        results.push(json!({"storage":"journal","batch":batch,"events":count,"write_secs":elapsed,"commit":summary(&latency),"replay_ms":replay.elapsed().as_secs_f64()*1000.,"disk_bytes":disk(&dir),"resources":resources()}));
        let path = root.join(format!("events-{batch}.sqlite"));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Full),
            )
            .await?;
        sqlx::query("CREATE TABLE events(seq INTEGER PRIMARY KEY,payload TEXT NOT NULL)")
            .execute(&pool)
            .await?;
        let start = Instant::now();
        let mut latency = Vec::new();
        for i in (0..count).step_by(batch) {
            let now = Instant::now();
            let mut tx = pool.begin().await?;
            for n in i..(i + batch).min(count) {
                sqlx::query("INSERT INTO events VALUES(?,?)")
                    .bind(n as i64)
                    .bind(
                        json!({"key":format!("bench-{n}"),"payload":"x".repeat(1024)}).to_string(),
                    )
                    .execute(&mut *tx)
                    .await?;
            }
            tx.commit().await?;
            latency.push(now.elapsed().as_secs_f64() * 1000.);
        }
        let elapsed = start.elapsed().as_secs_f64();
        pool.close().await;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::new().filename(&path).read_only(true))
            .await?;
        let replay = Instant::now();
        let mut seq = -1i64;
        let mut events = 0;
        loop {
            let rows: Vec<(i64, String)> =
                sqlx::query_as("SELECT seq,payload FROM events WHERE seq>? ORDER BY seq LIMIT 256")
                    .bind(seq)
                    .fetch_all(&pool)
                    .await?;
            if rows.is_empty() {
                break;
            }
            for (id, body) in rows {
                let _: Value = serde_json::from_str(&body)?;
                seq = id;
                events += 1;
            }
        }
        assert_eq!(events, count);
        results.push(json!({"storage":"sqlite-events-full-wal","batch":batch,"events":count,"write_secs":elapsed,"commit":summary(&latency),"replay_ms":replay.elapsed().as_secs_f64()*1000.,"disk_bytes":std::fs::metadata(&path)?.len(),"resources":resources()}));
        pool.close().await;
    }
    let mut crashes = Vec::new();
    for kind in ["journal", "sqlite"] {
        crashes.push(storage_crash_probe(root, kind).await?);
    }
    Ok(
        json!({"crash_prefix_probes":crashes,"mode":"storage-comparison","scope":"append and bounded replay of identical 1 KiB logical facts; not a full workflow-engine migration or power-loss proof","results":results,"errors":0,"timeouts":0}),
    )
}

async fn audits(root: &Path, count: usize) -> Result<Value> {
    let b = JournalBackend::open(root, Default::default()).await?;
    let w = workflow(&b).await?;
    b.append(vec![
        run_start("audit", &w),
        dispatch("audit", 1, 2),
        prepared("audit", "audit-d1"),
    ])
    .await?;
    let start = Instant::now();
    let mut reports = Vec::new();
    let mut index = 0;
    let mut latencies = Vec::new();
    while index < count {
        let end = [1_000, 10_000, 100_000, count]
            .into_iter()
            .filter(|n| *n > index)
            .fold((index + 64).min(count), usize::min);
        let events = (index..end)
            .map(|n| {
                scoped(
                    EventKind::LateAudit,
                    "audit",
                    0,
                    Some("audit-d1"),
                    n as u64 + 2,
                    json!({"evidence":"x".repeat(64)}),
                )
            })
            .collect();
        let now = Instant::now();
        b.append(events).await?;
        latencies.push(now.elapsed().as_secs_f64() * 1000.);
        index = end;
        if [1_000, 10_000, 100_000, count].contains(&index) {
            let cp = b.checkpoint().await;
            let sample = json!({"audits":index,"elapsed_secs":start.elapsed().as_secs_f64(),"batch_commit":summary(&latencies),"checkpoint_ok":cp.is_ok(),"checkpoint_error":cp.err().map(|e|e.to_string()),"resources":resources(),"diagnostics":b.diagnostics().await,"disk_bytes":disk(root)});
            println!("{sample}");
            reports.push(sample);
            latencies.clear();
        }
    }
    let current = snapshot(&b, "audit").await?.ok_or("audit missing")?;
    assert_eq!(
        current.nodes["s"].attempts["audit-d1"].audit_seq,
        count as u64 + 1
    );
    b.close().await?;
    Ok(json!({"mode":"audits","audits":count,"samples":reports,"errors":0,"timeouts":0}))
}

async fn storage_crash_child(kind: &str, root: &Path) -> Result<()> {
    std::fs::create_dir_all(root)?;
    if kind == "journal" {
        let j = flow_journal::Journal::open(root, Default::default()).await?;
        for n in 0..128 {
            j.submit(
                format!("committed-{n}"),
                vec![Event::new(EventKind::LegacyImport, json!({"key":n}))],
                flow_journal::QueueClass::Control,
            )
            .await?;
        }
        let pending = j.clone();
        tokio::spawn(async move {
            let _ = pending
                .submit(
                    "unacknowledged".into(),
                    vec![Event::new(EventKind::LegacyImport, json!({"key":128}))],
                    flow_journal::QueueClass::Control,
                )
                .await;
        });
        tokio::task::yield_now().await;
        std::fs::write(root.join("ready"), b"128 acknowledged facts")?;
        std::future::pending::<()>().await;
    } else {
        use sqlx::sqlite::{
            SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
        };
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(root.join("events.sqlite"))
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Full),
            )
            .await?;
        sqlx::query("CREATE TABLE events(seq INTEGER PRIMARY KEY)")
            .execute(&pool)
            .await?;
        for n in 0..128 {
            let mut tx = pool.begin().await?;
            sqlx::query("INSERT INTO events VALUES(?)")
                .bind(n)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        let mut tx = pool.begin().await?;
        sqlx::query("INSERT INTO events VALUES(128)")
            .execute(&mut *tx)
            .await?;
        std::fs::write(root.join("ready"), b"128 acknowledged facts")?;
        std::future::pending::<()>().await;
    }
    Ok(())
}
async fn storage_crash_probe(root: &Path, kind: &str) -> Result<Value> {
    let root = root.join(format!("crash-{kind}"));
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .arg("storage-crash-child")
        .arg(kind)
        .arg(&root)
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(Duration::from_secs(20), async {
        while !root.join("ready").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    child.kill().await?;
    let status = child.wait().await?;
    assert!(!status.success());
    let recovered = if kind == "journal" {
        let j = flow_journal::Journal::open(&root, Default::default()).await?;
        let mut tail = flow_journal::tail::TailReader::new(&root, j.id());
        let mut ids = Vec::new();
        while let Some((tx, _)) = tail.next(j.durable_lsn())? {
            for e in tx.events {
                if e.kind == EventKind::LegacyImport {
                    ids.push(e.payload["key"].as_u64().ok_or("bad recovered fact")?);
                }
            }
        }
        assert_eq!(&ids[..128], &(0..128).collect::<Vec<_>>()[..]);
        j.close().await?;
        ids.len() as i64
    } else {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new().filename(root.join("events.sqlite")),
            )
            .await?;
        let ids: Vec<i64> = sqlx::query_scalar("SELECT seq FROM events ORDER BY seq")
            .fetch_all(&pool)
            .await?;
        assert_eq!(ids, (0..128).collect::<Vec<_>>());
        pool.close().await;
        ids.len() as i64
    };
    Ok(
        json!({"storage":kind,"acknowledged":128,"recovered":recovered,"acknowledged_prefix_intact":true,"fault":"SIGKILL with next transaction unacknowledged/open","power_loss_tested":false}),
    )
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod resource_tests {
    #[test]
    fn sampling_includes_a_live_child() {
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exec sleep 30"])
            .spawn()
            .unwrap();
        let pids = super::child_pids(std::process::id() as i32);
        let _ = child.kill();
        let _ = child.wait();
        assert!(pids.unwrap().contains(&(child.id() as i32)));
    }
}
