#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;
mod service;
#[cfg(test)]
mod tests;

use service::Host;
use std::sync::{Arc, OnceLock};
use tauri::Manager;

fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
    let host = Arc::new(OnceLock::<Arc<Host>>::new());
    let setup_host = host.clone();
    let app = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .setup(move |app| {
            let root = std::env::var_os("FLOW_DESKTOP_DATA_DIR")
                .map(std::path::PathBuf::from)
                .unwrap_or(app.path().app_data_dir()?);
            let backend = Host::start(root).map_err(std::io::Error::other)?;
            let _ = setup_host.set(backend.clone());
            app.manage(backend);
            Ok(())
        })
        .on_page_load(|webview, payload| {
            if matches!(payload.event(), tauri::webview::PageLoadEvent::Started) {
                if let Some(host) = webview.try_state::<Arc<Host>>() {
                    host.sessions.close_window(webview.label());
                }
            }
        })
        .on_window_event(|window, event| {
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
    if let Some(host) = host.get() {
        if let Err(error) = host.shutdown() {
            eprintln!("flow-server shutdown: {error}");
        }
    }
    std::process::exit(code);
}
