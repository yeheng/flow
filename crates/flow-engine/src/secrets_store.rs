//! 持久化密钥的加密文件存储（web 设置页管理的密钥落这里）。
//!
//! 两条边界（与 [`crate::secrets`] 的机制边界一致）：
//! - 名称列表对前端公开，**真值永不出进程内存**——RPC 永远不回显 value；
//! - 真值 AES-256-GCM 加密落盘，主密钥在 `<data_dir>/secret.key`（随机
//!   32 字节，unix 下 0600）。拿到磁盘上两个文件之一拿不到另一个，密文
//!   不可解——这防的是「备份/同步盘整目录泄露」，不是本机 root。
//!
//! 与环境变量（`FLOW_SECRET_*`）的关系：**stored 优先，env 兜底**。界面
//! 删除某个 stored 密钥后，同名 env 变量（若设过）重新生效。

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use serde::{Deserialize, Serialize};

use crate::secrets::SecretSource;

/// 单条密文的落盘形态（nonce 每条随机，hex 编码）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    nonce: String,
    value: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct FileBody {
    version: u32,
    secrets: BTreeMap<String, Entry>,
}

pub struct SecretFileStore {
    /// secrets.json 路径（原子写：tmp + rename）。
    path: PathBuf,
    cipher: Aes256Gcm,
    inner: Mutex<BTreeMap<String, Entry>>,
}

impl SecretFileStore {
    /// 打开（或初始化）`<dir>` 下的密钥存储：secret.key + secrets.json。
    pub fn open(dir: &Path) -> Result<SecretFileStore, String> {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("创建密钥目录 {} 失败：{e}", dir.display()))?;
        let key_path = dir.join("secret.key");
        let key: [u8; 32] = match std::fs::read(&key_path) {
            Ok(bytes) => bytes
                .try_into()
                .map_err(|_| format!("密钥主文件 {} 长度非法（须 32 字节）", key_path.display()))?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let mut key = [0u8; 32];
                use aes_gcm::aead::rand_core::RngCore;
                OsRng.fill_bytes(&mut key);
                write_private(&key_path, &key)?;
                key
            }
            Err(err) => return Err(format!("读取 {} 失败：{err}", key_path.display())),
        };
        let path = dir.join("secrets.json");
        let inner = match std::fs::read(&path) {
            Ok(bytes) => {
                let body: FileBody = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("解析 {} 失败：{e}", path.display()))?;
                body.secrets
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(err) => return Err(format!("读取 {} 失败：{err}", path.display())),
        };
        Ok(SecretFileStore {
            path,
            cipher: Aes256Gcm::new((&key).into()),
            inner: Mutex::new(inner),
        })
    }

    fn encrypt(&self, value: &str) -> Result<Entry, String> {
        let mut nonce_bytes = [0u8; 12];
        use aes_gcm::aead::rand_core::RngCore;
        OsRng.fill_bytes(&mut nonce_bytes);
        let ciphertext = self
            .cipher
            .encrypt(Nonce::from_slice(&nonce_bytes), value.as_bytes())
            .map_err(|e| format!("密钥加密失败：{e}"))?;
        Ok(Entry {
            nonce: hex::encode(nonce_bytes),
            value: hex::encode(ciphertext),
        })
    }

    fn decrypt(&self, entry: &Entry) -> Option<String> {
        let nonce = hex::decode(&entry.nonce).ok()?;
        let ciphertext = hex::decode(&entry.value).ok()?;
        let plaintext = self
            .cipher
            .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
            .ok()?;
        String::from_utf8(plaintext).ok()
    }

    fn persist(&self, secrets: &BTreeMap<String, Entry>) -> Result<(), String> {
        let body = FileBody {
            version: 1,
            secrets: secrets.clone(),
        };
        let json =
            serde_json::to_vec_pretty(&body).map_err(|e| format!("密钥文件序列化失败：{e}"))?;
        write_private(&self.path, &json)
    }

    /// 写入（或覆盖）一个密钥。值不出本进程。
    pub fn set(&self, name: &str, value: &str) -> Result<(), String> {
        let entry = self.encrypt(value)?;
        let mut secrets = self.inner.lock().unwrap();
        secrets.insert(name.to_string(), entry);
        let result = self.persist(&secrets);
        if result.is_err() {
            // 落盘失败回滚内存，避免「界面显示已保存、重启后消失」
            secrets.remove(name);
        }
        result
    }

    /// 删除一个密钥；返回是否真的删了（env 来源的名字不在这里，返回 false）。
    pub fn delete(&self, name: &str) -> Result<bool, String> {
        let mut secrets = self.inner.lock().unwrap();
        let Some(removed) = secrets.remove(name) else {
            return Ok(false);
        };
        let result = self.persist(&secrets);
        if result.is_err() {
            secrets.insert(name.to_string(), removed);
        }
        result.map(|()| true)
    }
}

impl SecretSource for SecretFileStore {
    fn get(&self, name: &str) -> Option<String> {
        let secrets = self.inner.lock().unwrap();
        secrets.get(name).and_then(|entry| self.decrypt(entry))
    }

    fn names(&self) -> Vec<String> {
        self.inner.lock().unwrap().keys().cloned().collect()
    }
}

/// 0600 写入（unix；其他平台尽力而为）。tmp + rename 原子落盘。
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode_0600()
            .open(&tmp)
            .map_err(|e| format!("写入 {} 失败：{e}", tmp.display()))?;
        file.write_all(bytes)
            .map_err(|e| format!("写入 {} 失败：{e}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| format!("落盘 {} 失败：{e}", path.display()))
}

#[cfg(unix)]
trait Mode0600 {
    fn mode_0600(&mut self) -> &mut Self;
}
#[cfg(unix)]
impl Mode0600 for std::fs::OpenOptions {
    fn mode_0600(&mut self) -> &mut Self {
        use std::os::unix::fs::OpenOptionsExt;
        self.mode(0o600)
    }
}
#[cfg(not(unix))]
trait Mode0600 {
    fn mode_0600(&mut self) -> &mut Self;
}
#[cfg(not(unix))]
impl Mode0600 for std::fs::OpenOptions {
    fn mode_0600(&mut self) -> &mut Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "flow-secrets-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn set_get_delete_roundtrip_survives_reopen() {
        let dir = tempdir();
        let store = SecretFileStore::open(&dir).unwrap();
        store.set("OPENAI_KEY", "s3cret-value").unwrap();
        assert_eq!(store.get("OPENAI_KEY").as_deref(), Some("s3cret-value"));
        assert_eq!(store.names(), vec!["OPENAI_KEY".to_string()]);

        // 密文不落明文
        let raw = std::fs::read_to_string(dir.join("secrets.json")).unwrap();
        assert!(!raw.contains("s3cret-value"));

        // 重开（模拟重启）后仍在
        drop(store);
        let store = SecretFileStore::open(&dir).unwrap();
        assert_eq!(store.get("OPENAI_KEY").as_deref(), Some("s3cret-value"));

        assert!(store.delete("OPENAI_KEY").unwrap());
        assert_eq!(store.get("OPENAI_KEY"), None);
        assert!(!store.delete("OPENAI_KEY").unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wrong_key_file_cannot_decrypt() {
        let dir = tempdir();
        let store = SecretFileStore::open(&dir).unwrap();
        store.set("K", "v").unwrap();
        drop(store);
        // 换一把主密钥：解密失败视为未配置（None），不炸
        std::fs::write(dir.join("secret.key"), [7u8; 32]).unwrap();
        let store = SecretFileStore::open(&dir).unwrap();
        assert_eq!(store.get("K"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn works_through_secret_source_trait() {
        let dir = tempdir();
        let store = SecretFileStore::open(&dir).unwrap();
        store.set("A", "1").unwrap();
        let source: std::sync::Arc<dyn SecretSource> = std::sync::Arc::new(store);
        assert_eq!(source.get("A").as_deref(), Some("1"));
        assert_eq!(source.names(), vec!["A".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
