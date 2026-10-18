use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

fn files(root: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}
fn main() {
    let root = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("../..");
    let mut paths = Vec::new();
    for dir in ["crates/flow-engine/src", "crates/flow-executor/src"] {
        println!("cargo:rerun-if-changed={}", root.join(dir).display());
        files(&root.join(dir), &mut paths);
    }
    for name in [
        "Cargo.lock",
        "Cargo.toml",
        "crates/flow-engine/Cargo.toml",
        "crates/flow-executor/Cargo.toml",
        "crates/flow-engine/build.rs",
    ] {
        paths.push(root.join(name));
    }
    paths.sort();
    let mut hash = Sha256::new();
    for path in paths {
        println!("cargo:rerun-if-changed={}", path.display());
        let name = path.strip_prefix(&root).unwrap().to_string_lossy();
        let bytes = std::fs::read(&path).unwrap();
        hash.update((name.len() as u64).to_le_bytes());
        hash.update(name.as_bytes());
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    let identity: String = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
    println!("cargo:rustc-env=FLOW_RUNTIME_ID={identity}");
}
