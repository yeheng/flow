//! TLS 客户端配置（agent 上联 mTLS）。
//!
//! agent 上联 master 时走**双向 TLS**：master 用 CA 校验 agent 证书，
//! agent 用 CA 校验 master 证书。双边都认证，才能保证「对端确实是
//! 声明的那个节点」——单边 TLS 只证明 master 是真的，agent 侧就成了
//! 任何人都能连的开放端口。
//!
//! 本模块只负责**把 PEM 材料装配成 rustls 配置**，不涉及连接、重连与
//! 协议语义（那些在 `runtime` / `relay`）。
//!
//! 装配失败一律归为 `InvalidData` 的 IO 错误：证书材料有问题属于
//! 配置错误，不是临时故障，重试没有意义。

use std::io::Cursor;
use std::sync::Arc;

/// 安装 rustls 的加密提供者。
///
/// 用 ring：agent 与 master 必须用同一套算法实现，否则握手会在
/// 算法协商阶段失败，而那时的报错完全看不出根因。
fn ensure_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// 从 PEM 文件构建 mTLS 客户端配置（校验服务端证书 + 提供本机身份）。
///
/// 三个文件分别对应：CA（信任锚）、本机证书、本机私钥。
pub fn client_config_from_files(
    ca: &std::path::Path,
    cert: &std::path::Path,
    key: &std::path::Path,
) -> std::io::Result<tokio_rustls::TlsConnector> {
    ensure_provider();
    client_config_from_pem(
        &std::fs::read(ca)?,
        &std::fs::read(cert)?,
        &std::fs::read(key)?,
    )
}

/// 从内存中的 PEM 材料构建 mTLS 客户端配置。
///
/// 关键约束：**CA 不得为空**。空的信任锚意味着「信任任何服务端证书」，
/// 那等于没有 TLS——因此这里显式拒绝，而不是让握手阶段才失败。
pub fn client_config_from_pem(
    ca: &[u8],
    cert: &[u8],
    key: &[u8],
) -> std::io::Result<tokio_rustls::TlsConnector> {
    use rustls_pemfile::{certs, private_key};
    let mut roots = rustls::RootCertStore::empty();
    // CA 文件可能含多张证书（中间 CA + 根 CA），全部加入信任锚。
    for der in certs(&mut Cursor::new(ca))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(bad)?
    {
        roots
            .add(rustls::pki_types::CertificateDer::from(der.to_vec()))
            .map_err(bad)?;
    }
    // 空信任锚 = 不校验服务端身份，必须在这里就拒绝。
    if roots.is_empty() {
        return Err(bad("CA PEM contains no certificates"));
    }
    let client_certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        certs(&mut Cursor::new(cert))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(bad)?
            .into_iter()
            .collect();
    // 私钥缺失时明确报错：没有身份就没有 mTLS，退化成单向 TLS 等于
    // 放弃了对 agent 端的认证。
    let client_key = private_key(&mut Cursor::new(key))
        .map_err(bad)?
        .ok_or_else(|| bad("no private key in PEM"))?;
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(client_certs, client_key)
        .map_err(bad)?;
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

/// 把任意可显示的错误收敛为 `InvalidData` 的 IO 错误。
fn bad<E: std::fmt::Display>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
}
