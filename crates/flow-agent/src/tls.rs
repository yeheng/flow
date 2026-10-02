//! TLS 客户端配置（agent 上联 mTLS）。

use std::io::Cursor;
use std::sync::Arc;

/// 从 PEM 文件构建 mTLS 客户端配置（校验服务端证书 + 提供本机身份）。
fn ensure_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

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

pub fn client_config_from_pem(
    ca: &[u8],
    cert: &[u8],
    key: &[u8],
) -> std::io::Result<tokio_rustls::TlsConnector> {
    use rustls_pemfile::{certs, private_key};
    let mut roots = rustls::RootCertStore::empty();
    for der in certs(&mut Cursor::new(ca))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(bad)?
    {
        let _ = roots.add(rustls::pki_types::CertificateDer::from(der.to_vec()));
    }
    let client_certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        certs(&mut Cursor::new(cert))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(bad)?
            .into_iter()
            .collect();
    let client_key = private_key(&mut Cursor::new(key))
        .map_err(bad)?
        .ok_or_else(|| bad("no private key in PEM"))?;
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(client_certs, client_key)
        .map_err(bad)?;
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

fn bad<E: std::fmt::Display>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
}
