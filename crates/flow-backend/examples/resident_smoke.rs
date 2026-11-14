use flow_backend::resident_service::ResidentService;
use serde_json::json;
#[tokio::main]
async fn main() {
    let entry = std::env::args()
        .nth(1)
        .expect("resident_echo executable required");
    let service = ResidentService::start_installed(
        std::path::PathBuf::from(&entry),
        vec!["echo".into(), "crash".into()],
    )
    .await
    .unwrap();
    let instance = service.instance.clone();
    assert!(service.call("stale", "echo", json!({})).await.is_err());
    assert!(service
        .call(&instance, "forbidden", json!({}))
        .await
        .is_err());
    let (a, b) = tokio::join!(
        service.call(&instance, "echo", json!({"a":1})),
        service.call(&instance, "echo", json!({"b":2}))
    );
    assert_eq!(a.unwrap(), json!({"a":1}));
    assert_eq!(b.unwrap(), json!({"b":2}));
    assert_eq!(
        service.events_since(&instance, 0).unwrap()["events"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(service.events_since(&instance, 999).is_err());
    let target = service.clone();
    let host: flow_backend::resident_service::HostHandler =
        std::sync::Arc::new(move |method, params| {
            let target = target.clone();
            Box::pin(async move {
                if method != "flow/call" || params["plugin_id"] != "echo" {
                    return Err("peer forbidden".into());
                }
                target
                    .call(
                        params["instance"].as_str().ok_or("instance required")?,
                        params["method"].as_str().ok_or("method required")?,
                        params["params"].clone(),
                    )
                    .await
            })
        });
    let relay = ResidentService::start_installed_with_host(
        std::path::PathBuf::from(&entry),
        vec!["relay".into()],
        host,
    )
    .await
    .unwrap();
    let result = relay
        .call(
            &relay.instance,
            "relay",
            json!({"plugin_id":"echo","instance":instance,"method":"echo","params":{"via":"peer"}}),
        )
        .await
        .unwrap();
    assert_eq!(result, json!({"via":"peer"}));
    assert!(relay
        .call(&relay.instance, "relay", json!({"plugin_id":"forbidden"}))
        .await
        .is_err());
    relay.shutdown().await;
    assert!(service.call(&instance,"crash",json!({})).await.is_err());
    service.shutdown().await;
    assert!(!service.alive());
    assert!(service.call(&instance, "echo", json!({})).await.is_err());
    let replacement=ResidentService::start_installed(std::path::PathBuf::from(&entry),vec!["echo".into()]).await.unwrap();
    assert_ne!(replacement.instance,instance);
    assert!(replacement.call(&instance,"echo",json!({})).await.is_err());
    replacement.shutdown().await;
    println!("resident smoke: concurrent requests, events, stale identity, allowlist and shutdown passed");
}
