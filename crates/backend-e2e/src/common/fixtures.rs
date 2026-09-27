//! 定义构造器（fixtures）+ 本地 HTTP stub 服务。
//!
//! HTTP stub 全部是确定性的「本地 TcpListener」（DESIGN.md §13 测试约定）：
//! 卡住 = 接受连接后不响应；断流 = 声明 Content-Length 但发一半即关闭；
//! 不依赖任何不可路由地址，也不依赖外部服务。

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---- 定义构造器 ----

/// start → script → end。
pub fn linear_def(code: &str) -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "n1", "type": "script", "name": "脚本", "params": {"code": code}},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "n1"},
            {"from": "n1", "to": "end"}
        ]
    })
}

/// start → condition → (true → t → end_t) / (false → f → end_f)。
/// 两个 end 是互斥分支的收尾（AND-join 合同不让它们合流，见 DESIGN §6.2）。
pub fn condition_def(expr: &str, true_code: &str, false_code: &str) -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "cond", "type": "condition", "name": "条件",
             "params": {"expr": expr}},
            {"id": "t", "type": "script", "name": "真分支", "params": {"code": true_code}},
            {"id": "f", "type": "script", "name": "假分支", "params": {"code": false_code}},
            {"id": "end_t", "type": "end", "name": "真结束"},
            {"id": "end_f", "type": "end", "name": "假结束"}
        ],
        "edges": [
            {"from": "start", "to": "cond"},
            {"from": "cond", "to": "t", "port": "true"},
            {"from": "cond", "to": "f", "port": "false"},
            {"from": "t", "to": "end_t"},
            {"from": "f", "to": "end_f"}
        ]
    })
}

/// start → condition；false 分支是 delay → human_task → end_f 的多层下游，
/// true 分支直接 end。用于钉住「跳过必须推进到不动点」。
pub fn skip_chain_def(expr: &str) -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "cond", "type": "condition", "name": "条件", "params": {"expr": expr}},
            {"id": "end_t", "type": "end", "name": "真结束"},
            {"id": "d", "type": "delay", "name": "等待", "params": {"ms": 30}},
            {"id": "h", "type": "human_task", "name": "人工"},
            {"id": "end_f", "type": "end", "name": "假结束"}
        ],
        "edges": [
            {"from": "start", "to": "cond"},
            {"from": "cond", "to": "end_t", "port": "true"},
            {"from": "cond", "to": "d", "port": "false"},
            {"from": "d", "to": "h"},
            {"from": "h", "to": "end_f"}
        ]
    })
}

/// start → cond；true → s1 → join_end；false → s2 → join_end。
/// AND-join：一条入边 Unsatisfied 就整节点跳过。
pub fn join_skip_def(expr: &str) -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "cond", "type": "condition", "name": "条件", "params": {"expr": expr}},
            {"id": "s1", "type": "script", "name": "左", "params": {"code": "return 'l';"}},
            {"id": "s2", "type": "script", "name": "右", "params": {"code": "return 'r';"}},
            {"id": "join_end", "type": "end", "name": "汇合结束"}
        ],
        "edges": [
            {"from": "start", "to": "cond"},
            {"from": "cond", "to": "s1", "port": "true"},
            {"from": "cond", "to": "s2", "port": "false"},
            {"from": "s1", "to": "join_end"},
            {"from": "s2", "to": "join_end"}
        ]
    })
}

/// start → delay → end。
pub fn delay_def(ms: u64) -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "d", "type": "delay", "name": "等待", "params": {"ms": ms}},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "d"},
            {"from": "d", "to": "end"}
        ]
    })
}

/// start → human_task → end。
pub fn human_def() -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "h", "type": "human_task", "name": "人工", "params": {"prompt": "请审批"}},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "h"},
            {"from": "h", "to": "end"}
        ]
    })
}

/// start → http_call → end。
pub fn http_def(method: &str, url: &str) -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "call", "type": "http_call", "name": "请求",
             "params": {"method": method, "url": url}},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "call"},
            {"from": "call", "to": "end"}
        ]
    })
}

/// 全参数 http_call（headers / body / 重试策略 / 超时）。
pub fn http_def_full(params: Value) -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "call", "type": "http_call", "name": "请求", "params": params},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "call"},
            {"from": "call", "to": "end"}
        ]
    })
}

/// start → s1 → end_a；start → s2 → end_b（两个 end → 输出按节点 id 映射）。
pub fn multi_end_def() -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "s1", "type": "script", "name": "一", "params": {"code": "return 1;"}},
            {"id": "s2", "type": "script", "name": "二", "params": {"code": "return 2;"}},
            {"id": "end_a", "type": "end", "name": "结束一"},
            {"id": "end_b", "type": "end", "name": "结束二"}
        ],
        "edges": [
            {"from": "start", "to": "s1"},
            {"from": "start", "to": "s2"},
            {"from": "s1", "to": "end_a"},
            {"from": "s2", "to": "end_b"}
        ]
    })
}

/// start → s1, s2 → end（AND-join 两汇都满足 → end 输出为前驱映射）。
pub fn multi_pred_def() -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "s1", "type": "script", "name": "一", "params": {"code": "return 'a';"}},
            {"id": "s2", "type": "script", "name": "二", "params": {"code": "return 'b';"}},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "s1"},
            {"from": "start", "to": "s2"},
            {"from": "s1", "to": "end"},
            {"from": "s2", "to": "end"}
        ]
    })
}

/// start → script(抛错) → end。
pub fn failing_def(code: &str, retry: Option<Value>) -> Value {
    let mut params = json!({ "code": code });
    if let Some(retry) = retry {
        params["retry"] = retry;
    }
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "boom", "type": "script", "name": "炸", "params": params},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "boom"},
            {"from": "boom", "to": "end"}
        ]
    })
}

/// start → sub_workflow(→ child_wf) → end。
pub fn sub_def(child_wf: &str) -> Value {
    json!({
        "nodes": [
            {"id": "start", "type": "start", "name": "开始"},
            {"id": "sub", "type": "sub_workflow", "name": "子流程",
             "params": {"workflow_id": child_wf}},
            {"id": "end", "type": "end", "name": "结束"}
        ],
        "edges": [
            {"from": "start", "to": "sub"},
            {"from": "sub", "to": "end"}
        ]
    })
}

/// 把 sub_workflow 定义里的目标 workflow_id 填上（深度链建流时用）。
pub fn set_sub_wf(def: &mut Value, child_wf: &str) {
    for node in def["nodes"].as_array_mut().unwrap() {
        if node["type"] == json!("sub_workflow") {
            node["params"]["workflow_id"] = json!(child_wf);
        }
    }
}

/// 深度上限用例：n 层链，第 i 层调用第 i+1 层，最内层是普通线性流。
/// 返回时 sub_workflow 的目标 id 是占位符，调用方创建完全部工作流后
/// 用 [`set_sub_wf`] 逐层回填，再 update/publish。
pub fn deep_chain_defs(depth: usize) -> Vec<Value> {
    let mut defs = Vec::with_capacity(depth);
    for level in 0..depth {
        if level + 1 == depth {
            defs.push(linear_def("return { leaf: true };"));
        } else {
            defs.push(sub_def("__CHILD__"));
        }
    }
    defs
}

// ---- HTTP stub ----

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

fn response_bytes(status: u16, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        reason(status),
        body.len()
    )
    .into_bytes()
}

/// 读一个 HTTP 请求（请求头 + 按 Content-Length 读 body），返回 (头文本, body)。
async fn read_request(stream: &mut TcpStream) -> Option<(String, Vec<u8>)> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let head_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 64 * 1024 {
            return None;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let content_length = head
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let mut body = buf[head_end..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    Some((head, body))
}

/// 本地 HTTP stub。Drop 时 abort accept 循环。
pub struct StubHttp {
    pub addr: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
    handle: tokio::task::JoinHandle<()>,
    /// hang 模式下持有的连接（不响应，进程结束自然释放）。
    _held: Option<Arc<Mutex<Vec<TcpStream>>>>,
}

impl StubHttp {
    pub fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    async fn bind() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定 stub 端口失败");
        let addr = listener.local_addr().expect("读取 stub 端口失败");
        (listener, addr)
    }

    /// 按请求序号依次返回 statuses 中的状态码（最后一个重复）；body 为 JSON。
    pub async fn json_sequence(statuses: Vec<u16>) -> StubHttp {
        let (listener, addr) = Self::bind().await;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let counter = Arc::new(AtomicUsize::new(0));
        let rec = requests.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let Some((head, _body)) = read_request(&mut stream).await else {
                    continue;
                };
                rec.lock().unwrap().push(head);
                let index = counter.fetch_add(1, Ordering::SeqCst);
                let status = statuses[index.min(statuses.len() - 1)];
                let body = json!({ "attempt": index + 1 }).to_string();
                let _ = stream.write_all(&response_bytes(status, &body)).await;
                let _ = stream.flush().await;
                let _ = stream.shutdown().await;
            }
        });
        StubHttp {
            addr,
            requests,
            handle,
            _held: None,
        }
    }

    /// 固定状态码 + JSON body。
    pub async fn fixed(status: u16, body: Value) -> StubHttp {
        let (listener, addr) = Self::bind().await;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let rec = requests.clone();
        let payload = body.to_string();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let Some((head, _body)) = read_request(&mut stream).await else {
                    continue;
                };
                rec.lock().unwrap().push(head);
                let _ = stream.write_all(&response_bytes(status, &payload)).await;
                let _ = stream.flush().await;
                let _ = stream.shutdown().await;
            }
        });
        StubHttp {
            addr,
            requests,
            handle,
            _held: None,
        }
    }

    /// 接受连接后永远不响应（http_call 确定性地挂在请求里）。
    pub async fn hang() -> StubHttp {
        let (listener, addr) = Self::bind().await;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let held = Arc::new(Mutex::new(Vec::new()));
        let rec = requests.clone();
        let stash = held.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                // 只读到请求头即止：连接已建立、请求已写入，随后无限期挂住
                if let Some((head, _)) = read_request(&mut stream).await {
                    rec.lock().unwrap().push(head);
                }
                stash.lock().unwrap().push(stream);
            }
        });
        StubHttp {
            addr,
            requests,
            handle,
            _held: Some(held),
        }
    }

    /// 声明 Content-Length: declared，只发 sent 字节就断开（响应体断流 → retryable）。
    pub async fn truncate(declared: usize, sent: &str) -> StubHttp {
        let (listener, addr) = Self::bind().await;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let rec = requests.clone();
        let payload = sent.to_string();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let Some((head, _body)) = read_request(&mut stream).await else {
                    continue;
                };
                rec.lock().unwrap().push(head);
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n"
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(payload.as_bytes()).await;
                let _ = stream.flush().await;
                let _ = stream.shutdown().await;
            }
        });
        StubHttp {
            addr,
            requests,
            handle,
            _held: None,
        }
    }
}

impl Drop for StubHttp {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// 节点 id → 时间线节点的便捷查找。
pub fn timeline_node<'a>(timeline: &'a Value, node_id: &str) -> &'a Value {
    timeline["nodes"]
        .as_array()
        .unwrap_or_else(|| panic!("timeline 没有 nodes：{timeline}"))
        .iter()
        .find(|n| n["id"] == json!(node_id))
        .unwrap_or_else(|| panic!("timeline 里没有节点 {node_id}：{timeline}"))
}
