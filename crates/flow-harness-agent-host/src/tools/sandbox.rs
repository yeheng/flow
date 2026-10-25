//! Filesystem confinement for the `bash` tool, enabled by CONGA_SANDBOX=1.
//! Fail-closed: if confinement cannot be applied, the command is refused.

/// Generate a Seatbelt (sandbox-exec) SBPL profile: allow everything broadly,
/// deny file writes everywhere except cwd / tmp / var/tmp. Pure function; the
/// CALLER canonicalizes paths (Seatbelt matches by resolved real path, so a
/// symlinked root like /var/tmp would never match as a literal).
#[cfg(target_os = "macos")]
fn seatbelt_profile(cwd: &str, tmp: &str, var_tmp: &str) -> String {
    format!(
        "(version 1)\n\
         (allow default)\n\
         (deny file-write*)\n\
         (allow file-write* (subpath \"{cwd}\"))\n\
         (allow file-write* (subpath \"{tmp}\"))\n\
         (allow file-write* (subpath \"{var_tmp}\"))\n"
    )
}

/// Read the sandbox flag from the ToolContext env map (host-populated from
/// the process env). Exact "1" only — no truthy-string guessing.
pub(crate) fn sandbox_enabled(env: &std::collections::HashMap<String, String>) -> bool {
    env.get("CONGA_SANDBOX").map(String::as_str) == Some("1")
}

/// Verify Seatbelt can actually apply a RESTRICTIVE profile on this machine.
///
/// Apple has been locking `sandbox-exec` down: on recent macOS any profile
/// containing a `deny` rule is refused with `sandbox_apply: Operation not
/// permitted` (exit 71), while a pure-allow profile still applies. Wrapping
/// commands in a sandbox that silently cannot confine is worse than no
/// sandbox — the operator believes they are protected — so probe the real
/// capability once and let the caller refuse to run.
///
/// The probe is generic (it asks "can this machine deny at all?", not "is
/// this particular profile valid?") so the verdict can be cached for the
/// process; per-profile rejections still surface as a failing command.
/// Cached because it spawns a process.
#[cfg(target_os = "macos")]
pub(crate) fn seatbelt_usable() -> Result<(), String> {
    static PROBE: std::sync::OnceLock<Result<(), String>> = std::sync::OnceLock::new();
    PROBE
        .get_or_init(|| {
            // /usr/bin/true (not /bin/true — that path does not exist on macOS)
            // with all stdio nulled: it performs no file writes, so on a machine
            // where Seatbelt works the deny rule is simply never triggered.
            match std::process::Command::new("sandbox-exec")
                .arg("-p")
                .arg("(version 1)(allow default)(deny file-write*)")
                .arg("/usr/bin/true")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
            {
                Ok(s) if s.success() => Ok(()),
                Ok(s) => Err(format!(
                    "macOS refused to apply the sandbox profile (sandbox-exec exited {}); \
                 this system cannot confine the bash tool, so CONGA_SANDBOX=1 is unavailable",
                    s.code().unwrap_or(-1)
                )),
                Err(e) => Err(format!(
                    "sandbox self-check could not run sandbox-exec: {e}"
                )),
            }
        })
        .clone()
}

/// Apply filesystem confinement to `cmd`. MUST be called before cwd/env are
/// set on `cmd` (the macOS branch rewrites program+args wholesale).
/// Err = fail-closed: the caller must refuse to run the command.
pub(crate) fn confine(
    cmd: &mut tokio::process::Command,
    cwd: &std::path::Path,
) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        // Capability check first: a platform that cannot deny at all must be
        // reported as "sandbox unavailable", not as a failed command.
        seatbelt_usable()?;
        let cwd_c = cwd
            .canonicalize()
            .map_err(|e| format!("sandbox: cwd not accessible: {e}"))?;
        // Premise: this whitelist is read from the PROCESS env on the
        // assumption that hosts populate ToolContext.env from the process
        // env (the child sees the filtered ctx env); a host building its
        // own env map could diverge whitelist and the child's $TMPDIR.
        let tmp = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
        // /var -> /private/var etc: canonicalize or the subpath rules
        // never match Seatbelt's resolved-path checks. Unresolvable path
        // (e.g. dangling TMPDIR) falls back to the literal unchanged.
        let canon = |p: String| {
            std::path::Path::new(&p)
                .canonicalize()
                .map(|c| c.display().to_string())
                .unwrap_or(p)
        };
        let profile = seatbelt_profile(
            &cwd_c.display().to_string(),
            &canon(tmp),
            &canon("/var/tmp".to_string()),
        );
        let std_cmd = cmd.as_std_mut();
        let program = std_cmd.get_program().to_os_string();
        let args: Vec<_> = std_cmd.get_args().map(std::ffi::OsString::from).collect();
        *cmd = tokio::process::Command::new("sandbox-exec");
        cmd.arg("-p").arg(&profile).arg(program).args(args);
        Ok(())
    }
    #[cfg(target_os = "linux")]
    {
        let _ = (cmd, cwd);
        Err("CONGA_SANDBOX=1 but this build has no Linux sandbox backend".to_string())
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (cmd, cwd);
        Err("sandbox unsupported on this platform".into())
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn profile_allows_read_execute_and_denies_write_by_default() {
        let p = seatbelt_profile("/tmp/cwd", "/tmp/dir", "/private/var/tmp");
        assert!(p.contains("(version 1)"), "{p}");
        assert!(p.contains("(allow default)"), "read/exec broadly: {p}");
        assert!(p.contains("(deny file-write*)"), "deny writes: {p}");
        assert!(
            p.contains("(allow file-write* (subpath \"/tmp/cwd\"))"),
            "{p}"
        );
        assert!(
            p.contains("(allow file-write* (subpath \"/tmp/dir\"))"),
            "{p}"
        );
    }

    /// The probe must agree with reality: run the same deny-rule profile
    /// directly and assert `seatbelt_usable()` reached the same verdict.
    /// This is the test that catches the platform regression — on a macOS
    /// where Seatbelt still works it asserts Ok; on one where `sandbox-exec`
    /// has been neutered it asserts Err. Self-consistent either way, so it
    /// never goes red purely because the host OS changed.
    #[test]
    fn seatbelt_usable_matches_direct_sandbox_exec() {
        let direct = std::process::Command::new("sandbox-exec")
            .arg("-p")
            .arg("(version 1)(allow default)(deny file-write*)")
            .arg("/usr/bin/true")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let expected_ok = matches!(direct, Ok(s) if s.success());
        assert_eq!(
            seatbelt_usable().is_ok(),
            expected_ok,
            "probe disagrees with a direct sandbox-exec invocation: {:?}",
            direct
        );
        if !expected_ok {
            // Documented degradation, not a code defect: surface it loudly.
            eprintln!(
                "NOTE: this macOS refuses restrictive Seatbelt profiles; \
                 CONGA_SANDBOX=1 is unavailable here."
            );
        }
    }

    #[test]
    fn profile_includes_var_tmp_as_passed() {
        let p = seatbelt_profile("/x", "/y", "/private/var/tmp");
        assert!(
            p.contains("(allow file-write* (subpath \"/private/var/tmp\"))"),
            "{p}"
        );
    }
}

// The env-flag logic is cross-platform, so it gets its own non-gated test
// module (the `tests` module above is macOS-only for the seatbelt profile).
#[cfg(test)]
mod flag_tests {
    use super::*;

    #[test]
    fn sandbox_enabled_only_on_exact_flag() {
        let mut env = std::collections::HashMap::new();
        assert!(!sandbox_enabled(&env));
        env.insert("CONGA_SANDBOX".to_string(), "0".to_string());
        assert!(!sandbox_enabled(&env));
        env.insert("CONGA_SANDBOX".to_string(), "1".to_string());
        assert!(sandbox_enabled(&env));
    }
}
