//! Tauri 桌面端入口：内嵌 flow 后端，把 JSON-RPC 暴露给渲染层。
//!
//! 桌面端与服务端共享同一个 `flow_rpc` 方法模块，差别只在入口形态：
//! 桌面端是**进程内调用**，服务端是 WebSocket。这保证「产品面只有一份
//! 实现」，不会出现桌面能做的服务端做不了的情况。
//!
//! 生命周期要点：
//!
//! - 后端在 `setup` 阶段启动（此时还没有窗口），失败即启动失败——
//!   与其让用户对着一个连不上的界面，不如启动就报错。
//! - 窗口关闭/刷新时必须关闭对应会话，否则订阅推送会指向已消失的窗口。
//! - 退出时显式 `shutdown` 后端，确保 SQLite 写者被干净关闭。
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;
mod service;
#[cfg(test)]
mod tests;

use service::Host;
use std::sync::{Arc, OnceLock};
use tauri::Manager;

/// 应用入口：装配 Tauri 应用、内嵌后端并处理退出。
fn main() {
    // 日志初始化：环境变量优先，缺省 info。失败不致命——没有日志只是
    // 少了排查线索，不该因此阻止应用启动。
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
    // 后端句柄需要在 setup 闭包与退出路径之间共享；用 OnceLock 而不是
    // 局部变量，因为 Tauri 的 builder 链式调用无法把值同时交给两处。
    let host = Arc::new(OnceLock::<Arc<Host>>::new());
    let setup_host = host.clone();
    let app = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .setup(move |app| {
            // 统一数据根：FLOW_DESKTOP_DATA_DIR（测试/多实例）→ flow 主目录
            // （FLOW_HOME → ~/.flow，与 journal-server/harness 沙箱同一布局）；
            // 无 home 的退化环境才回退 Tauri app_data_dir。
            let root = std::env::var_os("FLOW_DESKTOP_DATA_DIR")
                .map(std::path::PathBuf::from)
                .or_else(flow_config::flow_home);
            let root = match root {
                Some(path) => {
                    // ~/.flow 可能首次使用尚不存在；目录锁/数据库要求 root 在位
                    std::fs::create_dir_all(&path).map_err(std::io::Error::other)?;
                    path
                }
                None => app.path().app_data_dir()?,
            };
            // 后端启动失败直接让应用启动失败：桌面端没有「服务没起来但
            // 界面还能用」的形态。
            let backend = Host::start(root).map_err(std::io::Error::other)?;
            let _ = setup_host.set(backend.clone());
            app.manage(backend);
            Ok(())
        })
        .on_page_load(|webview, payload| {
            // 页面开始加载 = 前端重启，会话全部作废。
            if matches!(payload.event(), tauri::webview::PageLoadEvent::Started) {
                if let Some(host) = webview.try_state::<Arc<Host>>() {
                    host.sessions.close_window(webview.label());
                }
            }
        })
        .on_window_event(|window, event| {
            // 窗口销毁时关闭该窗口的全部会话，中止未结束的订阅推送。
            if matches!(event, tauri::WindowEvent::Destroyed) {
                if let Some(host) = window.try_state::<Arc<Host>>() {
                    host.sessions.close_window(window.label());
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            commands::flow_info,
            commands::flow_open_client,
            commands::flow_close_client,
            commands::flow_call,
            commands::flow_subscribe,
            commands::flow_unsubscribe,
            commands::flow_download
        ])
        .build(tauri::generate_context!())
        .expect("failed to start Flow and its local backend");
    let code = app.run_return(|_, _| {});
    // 显式停机：让后端线程收尾、SQLite 写者干净关闭，再退出进程。
    if let Some(host) = host.get() {
        if let Err(error) = host.shutdown() {
            eprintln!("flow-journal shutdown: {error}");
        }
    }
    std::process::exit(code);
}
