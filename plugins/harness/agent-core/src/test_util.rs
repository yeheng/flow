//! Test-only helpers shared across this crate's unit-test modules.

/// A fake environment lookup: maps var name -> value; missing vars report
/// [`std::env::VarError::NotPresent`]. Lets tests inject env config without
/// touching the process environment.
pub(crate) fn fake_env(
    pairs: &[(&str, &str)],
) -> impl Fn(&str) -> Result<String, std::env::VarError> {
    let map: std::collections::HashMap<String, String> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |k: &str| map.get(k).cloned().ok_or(std::env::VarError::NotPresent)
}
