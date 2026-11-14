//! Cooperative cross-process publication leases. Never unlink or replace the lock file.
use fs2::FileExt;
use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

pub fn plugins_root() -> Option<PathBuf> {
    std::env::var_os("FLOW_PLUGINS_DIR")
        .map(PathBuf::from)
        .or_else(|| crate::flow_home().map(|home| home.join("plugins")))
}
pub struct RootLease {
    file: File,
}
impl RootLease {
    fn open(root: &Path, exclusive: bool) -> io::Result<Self> {
        let root = root.canonicalize()?;
        let path = root.join(".publish.lock");
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = std::fs::symlink_metadata(&path)?;
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "publication lock must be a regular file",
                    ));
                }
                OpenOptions::new().read(true).write(exclusive).open(&path)?
            }
            Err(error) => return Err(error),
        };
        if exclusive {
            FileExt::lock_exclusive(&file)?;
        } else {
            FileExt::lock_shared(&file)?;
        }
        Ok(Self { file })
    }
    /// Blocking: callers on an async runtime must acquire through spawn_blocking.
    pub fn shared(root: &Path) -> io::Result<Self> {
        Self::open(root, false)
    }
    pub fn exclusive(root: &Path) -> io::Result<Self> {
        Self::open(root, true)
    }
}
impl Drop for RootLease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn readers_exclude_publication_and_release_on_drop() {
        let root = std::env::temp_dir().join(format!(
            "flow-root-lease-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let a = RootLease::shared(&root).unwrap();
        let b = RootLease::shared(&root).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join(".publish.lock"))
            .unwrap();
        assert_eq!(
            FileExt::try_lock_exclusive(&file).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(a);
        assert!(FileExt::try_lock_exclusive(&file).is_err());
        drop(b);
        FileExt::try_lock_exclusive(&file).unwrap();
        FileExt::unlock(&file).unwrap();
        drop(file);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[cfg(unix)]
    #[test]
    fn rejects_symbolic_lock_file() {
        let root = std::env::temp_dir().join(format!("flow-lock-link-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::os::unix::fs::symlink("missing", root.join(".publish.lock")).unwrap();
        assert!(RootLease::shared(&root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
