//! Versioned environment bundles. Credentials are persisted only through the encrypted store.
use crate::{secrets::SecretSource, secrets_store::SecretFileStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path, sync::Mutex};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentBinding {
    pub id: String,
    pub revision: u64,
    pub config: Value,
}
#[derive(Clone, Serialize, Deserialize)]
struct Revision {
    name: String,
    config: Value,
    secrets: BTreeMap<String, String>,
}
#[derive(Default, Clone, Serialize, Deserialize)]
struct Data {
    environments: BTreeMap<String, Vec<Revision>>,
}
pub struct EnvironmentStore {
    encrypted: SecretFileStore,
    data: Mutex<Data>,
}
impl EnvironmentStore {
    pub fn open(path: &Path) -> Result<Self, String> {
        let encrypted = SecretFileStore::open(path)?;
        let data = match encrypted.get("ENVIRONMENTS") {
            Some(value) => serde_json::from_str(&value).map_err(|_| "invalid environment data")?,
            None if encrypted.names().is_empty() => Data::default(),
            None => return Err("cannot decrypt environment data".into()),
        };
        Ok(Self {
            encrypted,
            data: Mutex::new(data),
        })
    }
    pub fn list(&self) -> Vec<Value> {
        self.data.lock().unwrap().environments.iter().map(|(id, versions)| {
            let r = versions.last().unwrap();
            serde_json::json!({"id":id,"name":r.name,"revision":versions.len(),"config":r.config,"secrets":r.secrets.keys().collect::<Vec<_>>()})
        }).collect()
    }
    pub fn save(
        &self,
        id: &str,
        name: &str,
        config: Value,
        secret: Option<(&str, Option<&str>)>,
        expected: u64,
    ) -> Result<EnvironmentBinding, String> {
        if id == "default"
            || id.is_empty()
            || id.len() > 64
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err("invalid environment id".into());
        }
        if name.trim().is_empty()
            || name.len() > 128
            || !config.is_object()
            || serde_json::to_vec(&config)
                .map_err(|e| e.to_string())?
                .len()
                > 65536
        {
            return Err("invalid environment name or config".into());
        }
        if let Some((key, value)) = secret {
            if key.is_empty()
                || key.len() > 64
                || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                || value.is_some_and(|v| v.is_empty() || v.len() > 8192)
            {
                return Err("invalid secret name or value".into());
            }
        }
        let mut guard = self.data.lock().unwrap();
        let mut next = guard.clone();
        let versions = next.environments.entry(id.into()).or_default();
        if versions.len() as u64 != expected {
            return Err("environment revision conflict".into());
        }
        let mut r = versions.last().cloned().unwrap_or(Revision {
            name: name.into(),
            config: config.clone(),
            secrets: BTreeMap::new(),
        });
        r.name = name.into();
        r.config = config.clone();
        if let Some((key, value)) = secret {
            if let Some(value) = value {
                r.secrets.insert(key.into(), value.into());
            } else {
                r.secrets.remove(key);
            }
        }
        versions.push(r);
        let revision = versions.len() as u64;
        self.encrypted.set(
            "ENVIRONMENTS",
            &serde_json::to_string(&next).map_err(|e| e.to_string())?,
        )?;
        *guard = next;
        Ok(EnvironmentBinding {
            id: id.into(),
            revision,
            config,
        })
    }
    pub fn delete(&self, id: &str, expected: u64) -> Result<(), String> {
        let mut guard = self.data.lock().unwrap();
        let mut next = guard.clone();
        let versions = next.environments.get(id).ok_or("environment not found")?;
        if versions.len() as u64 != expected {
            return Err("environment revision conflict".into());
        }
        next.environments.remove(id);
        self.encrypted.set(
            "ENVIRONMENTS",
            &serde_json::to_string(&next).map_err(|e| e.to_string())?,
        )?;
        *guard = next;
        Ok(())
    }

    pub fn bind(&self, id: &str) -> Result<EnvironmentBinding, String> {
        let guard = self.data.lock().unwrap();
        let versions = guard.environments.get(id).ok_or("environment not found")?;
        Ok(EnvironmentBinding {
            id: id.into(),
            revision: versions.len() as u64,
            config: versions.last().unwrap().config.clone(),
        })
    }
    pub fn credentials(
        &self,
        binding: &EnvironmentBinding,
    ) -> Result<BTreeMap<String, String>, String> {
        self.data
            .lock()
            .unwrap()
            .environments
            .get(&binding.id)
            .and_then(|v| {
                binding
                    .revision
                    .checked_sub(1)
                    .and_then(|i| v.get(i as usize))
            })
            .map(|v| v.secrets.clone())
            .ok_or_else(|| "environment revision missing".into())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn isolation_versions_and_reopen() {
        let path = std::env::temp_dir().join(format!("flow-env-{}", uuid::Uuid::now_v7()));
        let s = EnvironmentStore::open(&path).unwrap();
        let a = s
            .save(
                "dev",
                "Development",
                serde_json::json!({"url":"dev"}),
                Some(("KEY", Some("old"))),
                0,
            )
            .unwrap();
        s.save(
            "dev",
            "Development",
            serde_json::json!({"url":"new"}),
            Some(("KEY", Some("new"))),
            1,
        )
        .unwrap();
        let b = s
            .save("prod", "Production", serde_json::json!({}), None, 0)
            .unwrap();
        assert_eq!(s.credentials(&a).unwrap()["KEY"], "old");
        assert!(s.credentials(&b).unwrap().is_empty());
        assert!(s
            .save("dev", "bad", serde_json::json!({}), None, 1)
            .is_err());
        assert!(!serde_json::to_string(&s.list()).unwrap().contains("old"));
        drop(s);
        assert_eq!(
            EnvironmentStore::open(&path)
                .unwrap()
                .credentials(&a)
                .unwrap()["KEY"],
            "old"
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}
