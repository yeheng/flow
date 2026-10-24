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
//!
//! 为什么落盘形态要单独设计：主密钥与密文分开存、分开授权，才能把
//! 「整目录被同步/备份泄露」与「拿到密文」两件事拆开——单文件存明文或
//! 密钥与密文同文件，都只会退化成「泄露即全泄」。

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
    /// 该条密文的随机 nonce（hex）。
    nonce: String,
    /// 密文（hex）。
    value: String,
}

/// 密钥文件的整体形态。
///
/// 带 `version` 是为了将来换算法或换 KDF 时能识别旧文件；当前只有
/// v1，写入时固定填 1。
#[derive(Debug, Default, Serialize, Deserialize)]
struct FileBody {
    /// 文件格式版本。
    version: u32,
    /// 名称 → 密文条目。
    secrets: BTreeMap<String, Entry>,
}

/// 基于文件的加密密钥存储。
///
/// 线程安全靠内部 `Mutex` 串行化「改内存 + 落盘」：并发写同一目录时，
/// 否则会出现内存与文件分叉（后写覆盖先写）。
pub struct SecretFileStore {
    /// secrets.json 路径（原子写：tmp + rename）。
    path: PathBuf,
    /// AES-256-GCM 加密器（密钥在进程内存中，仅此一处持有）。
    cipher: Aes256Gcm,
    /// 名称 → 密文条目。
    inner: Mutex<BTreeMap<String, Entry>>,
}

impl SecretFileStore {
    /// 打开（或初始化）`<dir>` 下的密钥存储：secret.key + secrets.json。
    ///
    /// 主密钥缺失时随机生成并落盘：每个密钥条目用独立随机 nonce 加密，
    /// 密钥与密文分属两个文件，单独拿到其一都无法还原真值。
    ///
    /// # Errors
    /// 目录不可创建、主密钥长度非法、密钥文件不可读或解析失败时返回
    /// 错误——解析失败不静默重来：宁可启动失败，也不要带着空清单继续，
    /// 那会让界面显示「密钥已清空」。
    pub fn open(dir: &Path) -> Result<SecretFileStore, String> {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("创建密钥目录 {} 失败：{e}", dir.display()))?;
        let key_path = dir.join("secret.key");
        let key: [u8; 32] = match std::fs::read(&key_path) {
            // 长度必须正好 32 字节（AES-256）：截断或超长都说明文件被改过。
            Ok(bytes) => bytes
                .try_into()
                .map_err(|_| format!("密钥主文件 {} 长度非法（须 32 字节）", key_path.display()))?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // 首次初始化：随机主密钥，0600 落盘（unix）。
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
            // 首次启动还没有密钥文件：空清单起步。
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(err) => return Err(format!("读取 {} 失败：{err}", path.display())),
        };
        Ok(SecretFileStore {
            path,
            cipher: Aes256Gcm::new((&key).into()),
            inner: Mutex::new(inner),
        })
    }

    /// 加密一条真值，返回可落盘的密文条目。
    ///
    /// 每条独立随机 nonce：同一密钥多次加密的密文不同，泄露一条密文
    /// 推不出其他条，也看不出「哪些条目值相同」。
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

    /// 解密一条密文；任何异常（hex 非法、长度不对、认证失败）都返回
    /// `None`，与「未配置」同义。
    ///
    /// 认证失败包含「主密钥换了一把」的情况：此时不该把损坏当成致命错误，
    /// 只需让该密钥表现为未配置，界面可以重新写入。
    fn decrypt(&self, entry: &Entry) -> Option<String> {
        let nonce: [u8; 12] = hex::decode(&entry.nonce).ok()?.try_into().ok()?;
        let ciphertext = hex::decode(&entry.value).ok()?;
        let plaintext = self
            .cipher
            .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
            .ok()?;
        String::from_utf8(plaintext).ok()
    }

    /// 把整份密钥表原子落盘。
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
    ///
    /// 写入对内存与文件是「全成或全不成」：落盘失败即回滚内存状态。
    pub fn set(&self, name: &str, value: &str) -> Result<(), String> {
        let entry = self.encrypt(value)?;
        let mut secrets = self.inner.lock().unwrap();
        let previous = secrets.insert(name.to_string(), entry);
        let result = self.persist(&secrets);
        if result.is_err() {
            // 落盘失败回滚内存，避免「界面显示已保存、重启后消失」
            if let Some(previous) = previous {
                secrets.insert(name.to_string(), previous);
            } else {
                secrets.remove(name);
            }
        }
        result
    }

    /// 删除一个密钥；返回是否真的删了（env 来源的名字不在这里，返回 false）。
    ///
    /// 返回值让调用方能区分「删掉了」与「本来就没有」：界面据此提示，
    /// 而不是把两者都报成成功。
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
    /// 取真值：解密失败与未配置同义（`None`）。
    fn get(&self, name: &str) -> Option<String> {
        let secrets = self.inner.lock().unwrap();
        secrets.get(name).and_then(|entry| self.decrypt(entry))
    }

    /// 列出已配置的名称（不含值）。
    fn names(&self) -> Vec<String> {
        self.inner.lock().unwrap().keys().cloned().collect()
    }
}

/// 0600 写入（unix；其他平台尽力而为）。tmp + rename 原子落盘。
///
/// tmp + rename 是为了避免「写一半的密钥文件」：崩溃留下的半文件会被
/// 解析成整体损坏，rename 则保证读者只看到旧版本或新版本。
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    // 临时文件名带唯一后缀：并发写不会互相覆盖临时文件。
    let tmp = parent.join(format!(
        ".flow-secret-{}.tmp",
        crate::execution_protocol::transport::fresh_boot_id()
    ));
    let result = (|| {
        {
            // create_new：绝不覆盖已存在的临时文件，避免写到别人的中间态。
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode_0600()
                .open(&tmp)
                .map_err(|e| format!("写入 {} 失败：{e}", tmp.display()))?;
            file.write_all(bytes)
                .map_err(|e| format!("写入 {} 失败：{e}", tmp.display()))?;
            // 先 fsync 再 rename：rename 只保证目录项原子，数据未落盘时
            // 崩溃仍会得到空文件。
            file.sync_all()
                .map_err(|e| format!("同步 {} 失败：{e}", tmp.display()))?;
        }
        std::fs::rename(&tmp, path).map_err(|e| format!("落盘 {} 失败：{e}", path.display()))?;
        #[cfg(unix)]
        // 同步目录：rename 的目录项本身也要落盘，否则崩溃后可能看不到新文件。
        std::fs::File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(|e| format!("同步目录 {} 失败：{e}", parent.display()))?;
        Ok(())
    })();
    if result.is_err() {
        // 失败清理临时文件：否则目录里会堆满半写的密钥碎片。
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// unix 下的 0600 打开选项。
#[cfg(unix)]
trait Mode0600 {
    /// 把文件权限设为 0600（仅属主可读写）。
    fn mode_0600(&mut self) -> &mut Self;
}
#[cfg(unix)]
impl Mode0600 for std::fs::OpenOptions {
    /// 设置 POSIX mode 0600（仅属主可读写）。
    fn mode_0600(&mut self) -> &mut Self {
        use std::os::unix::fs::OpenOptionsExt;
        self.mode(0o600)
    }
}
/// 非 unix 平台的占位实现：权限语义由平台默认策略承担。
#[cfg(not(unix))]
trait Mode0600 {
    /// 不做任何权限设置（平台不支持 POSIX mode）。
    fn mode_0600(&mut self) -> &mut Self;
}
#[cfg(not(unix))]
impl Mode0600 for std::fs::OpenOptions {
    /// 不设置权限：非 unix 平台没有 POSIX mode。
    fn mode_0600(&mut self) -> &mut Self {
        self
    }
}

/// 加密存储的落盘一致性测试。
#[cfg(test)]
mod tests {
    use super::*;

    /// 造一个本次运行独占的临时目录（并发测试不互相干扰）。
    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "flow-secrets-test-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// 落盘失败必须回滚内存：界面不能显示「已保存」而重启后消失。
    #[test]
    fn failed_writes_restore_existing_and_absent_entries() {
        let dir = tempdir();
        let store = SecretFileStore::open(&dir).unwrap();
        store.set("K", "original").unwrap();
        std::fs::rename(dir.join("secrets.json"), dir.join("saved.json")).unwrap();
        std::fs::create_dir(dir.join("secrets.json")).unwrap();
        assert!(store.set("K", "replacement").is_err());
        assert_eq!(store.get("K").as_deref(), Some("original"));
        assert!(store.set("NEW", "value").is_err());
        assert_eq!(store.get("NEW"), None);
        assert!(store.delete("K").is_err());
        assert_eq!(store.names(), vec!["K".to_owned()]);
        std::fs::remove_dir(dir.join("secrets.json")).unwrap();
        std::fs::rename(dir.join("saved.json"), dir.join("secrets.json")).unwrap();
        let reopened = SecretFileStore::open(&dir).unwrap();
        assert_eq!(reopened.get("K").as_deref(), Some("original"));
        store.set("K", "replacement").unwrap();
        assert_eq!(
            SecretFileStore::open(&dir).unwrap().get("K").as_deref(),
            Some("replacement")
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// 损坏条目不得 panic、不得毒化整个存储：只让该条目表现为未配置。
    #[test]
    fn corrupt_entries_never_panic_or_poison_the_store() {
        let dir = tempdir();
        let store = SecretFileStore::open(&dir).unwrap();
        store.set("VALID", "value").unwrap();
        for nonce in [
            "".to_owned(),
            "0".to_owned(),
            "00".repeat(11),
            "00".repeat(12),
            "00".repeat(13),
            "invalid hex".to_owned(),
        ] {
            store.inner.lock().unwrap().insert(
                "BAD".into(),
                Entry {
                    nonce,
                    value: "00".into(),
                },
            );
            store.persist(&store.inner.lock().unwrap()).unwrap();
            let reopened = SecretFileStore::open(&dir).unwrap();
            assert_eq!(reopened.get("BAD"), None);
            assert_eq!(reopened.get("VALID").as_deref(), Some("value"));
            assert_eq!(reopened.names().len(), 2);
            reopened.set("BAD", "fixed").unwrap();
            assert_eq!(reopened.get("BAD").as_deref(), Some("fixed"));
            assert!(reopened.delete("BAD").unwrap());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// 写入→读取→删除的往返，以及重启后仍在；磁盘上不得出现明文。
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

    /// 换一把主密钥后旧密文不可解：表现为「未配置」而不是崩溃。
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

    /// 存储要能经 `SecretSource` trait 使用（进程入口就是按 trait 装的）。
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
