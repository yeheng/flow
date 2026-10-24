//! Skills: a prompt-appended catalog of on-disk instruction files.
//!
//! Global skills live at `<config_dir>/skills/*.md`; project skills at
//! `<cwd>/.conga/skills/*.md` (same `name:` wins over the global one).
//! Only the catalog line + readable path is appended — the model reads a
//! skill's full content with the `read` tool on demand. Global entries carry
//! an absolute path (under `~/.conga`, which `read` accepts); project
//! entries carry a cwd-relative path (`read` resolves those within cwd).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Catalog descriptions are capped at this many chars: the catalog rides in
/// the system prompt (paid on every request), so a runaway frontmatter
/// description must not leak skill bodies into it.
const MAX_DESCRIPTION_CHARS: usize = 200;

struct SkillMeta {
    description: String,
    source: PathBuf,
}

/// Production entry: global root is conga's config dir (`~/.conga`).
pub fn append_skills(base: &str, cwd: &Path) -> String {
    append_skills_in(base, cwd, &conga::storage::config_dir())
}

/// Testable core: `global_root` is injected (production uses the config dir).
pub fn append_skills_in(base: &str, cwd: &Path, global_root: &Path) -> String {
    let catalog = scan_catalog(cwd, global_root);
    if catalog.is_empty() {
        return base.to_string();
    }
    let mut out = String::from(base);
    out.push_str(
        "\n\n## Skills\n\nThe entries below are skills. To follow one, load its \
         full instructions with the `read` tool using the path shown for it.\n\n",
    );
    for (name, meta) in &catalog {
        out.push_str(&format!(
            "- name: {name} — {} (source: {})\n",
            catalog_description(&meta.description),
            meta.source.display()
        ));
    }
    out
}

/// Shared scan: global skills first, then project skills (same `name:`
/// wins over the global one).
fn scan_catalog(cwd: &Path, global_root: &Path) -> BTreeMap<String, SkillMeta> {
    let mut catalog: BTreeMap<String, SkillMeta> = BTreeMap::new();
    scan_dir(&global_root.join("skills"), None, &mut catalog);
    scan_dir(&cwd.join(".conga").join("skills"), Some(cwd), &mut catalog);
    catalog
}

/// (name, description) pairs for consumers that need the catalog without
/// prompt formatting (evolve's extractor input). Same scan + override
/// semantics as `append_skills_in`.
pub(crate) fn catalog_entries(cwd: &Path, global_root: &Path) -> Vec<(String, String)> {
    scan_catalog(cwd, global_root)
        .into_iter()
        .map(|(name, meta)| (name, meta.description))
        .collect()
}

fn scan_dir(dir: &Path, relative_to: Option<&Path>, catalog: &mut BTreeMap<String, SkillMeta>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return, // no dir = no skills; not an error
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("md") {
            continue;
        }
        match std::fs::read_to_string(&path)
            .ok()
            .and_then(|c| parse_frontmatter(&c))
        {
            Some((name, description)) => {
                // Project skills are stored cwd-relative: `read` resolves
                // relative paths within cwd, while absolute paths are only
                // allowed under the conga config dir. Global skills keep
                // the absolute path.
                let source = relative_to
                    .and_then(|base| path.strip_prefix(base).ok())
                    .unwrap_or(&path)
                    .to_path_buf();
                catalog.insert(
                    name,
                    SkillMeta {
                        description,
                        source,
                    },
                );
            }
            None => {
                tracing::warn!(path = %path.display(), "skill missing frontmatter name/description; skipped")
            }
        }
    }
}

/// One-line, bounded description for the catalog line.
fn catalog_description(d: &str) -> String {
    if d.chars().count() <= MAX_DESCRIPTION_CHARS {
        return d.replace(['\n', '\r'], " ");
    }
    let mut t: String = d.chars().take(MAX_DESCRIPTION_CHARS).collect();
    t.push('…');
    t
}

/// Hand-parsed frontmatter: `---\n` … `---` block carrying both `name:` and
/// `description:` lines. `description` may use a block scalar (`|`/`>` with
/// optional `-`/`+` chomping) — the indented lines that follow are collapsed
/// to one line for the catalog. Anything else is None (caller warns + skips).
fn parse_frontmatter(content: &str) -> Option<(String, String)> {
    let mut name = None;
    let mut description = None;
    let mut lines = content.strip_prefix("---\n")?.lines().peekable();
    while let Some(line) = lines.next() {
        if line.trim_end() == "---" {
            return match (name, description) {
                (Some(n), Some(d)) => Some((n, d)),
                _ => None,
            };
        }
        if let Some(v) = line.strip_prefix("name:") {
            name = parse_value(v, &mut lines);
        } else if let Some(v) = line.strip_prefix("description:") {
            description = parse_value(v, &mut lines);
        }
    }
    None // unterminated frontmatter block
}

/// Value of a frontmatter key: a plain single-line scalar, or a `|`/`>`
/// block scalar (the indented lines that follow) collapsed to one line.
/// `None` for an empty value.
fn parse_value(v: &str, lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Option<String> {
    let v = v.trim();
    // `|`/`>` optionally followed by a chomping indicator (`-`/`+`) opens a
    // block scalar; anything else after the indicator (e.g. `|2`) is treated
    // as a plain scalar.
    let block_scalar = matches!(v.as_bytes().first(), Some(b'|') | Some(b'>'))
        && v[1..].bytes().all(|b| b == b'-' || b == b'+');
    if !block_scalar {
        let scalar = unquote(v);
        return (!scalar.is_empty()).then(|| scalar.to_string());
    }
    let mut parts: Vec<&str> = Vec::new();
    while let Some(l) = lines.peek() {
        if l.starts_with(' ') || l.starts_with('\t') {
            let trimmed = l.trim();
            if !trimmed.is_empty() {
                parts.push(trimmed);
            }
            lines.next();
        } else if l.trim().is_empty() {
            lines.next(); // blank line inside a block adds nothing once collapsed
        } else {
            break; // dedent: the block ends, this line is a normal entry
        }
    }
    (!parts.is_empty()).then(|| parts.join(" "))
}

/// Strip one balanced pair of surrounding quotes from a plain scalar
/// (`"my-skill"` / `'my-skill'`): editors and project templates routinely
/// emit quoted frontmatter, and the quotes would otherwise pollute the
/// catalog line (and the tool name the model matches against). Only a
/// matching pair at both ends is removed - a bare quote mid-value stays
/// part of the scalar.
fn unquote(v: &str) -> &str {
    let b = v.as_bytes();
    if b.len() >= 2
        && ((b[0] == b'"' && b[b.len() - 1] == b'"') || (b[0] == b'\'' && b[b.len() - 1] == b'\''))
    {
        &v[1..v.len() - 1]
    } else {
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    fn skill_file(dir: &Path, file: &str, body: &str) {
        fs::create_dir_all(dir.join("skills")).unwrap();
        fs::write(dir.join("skills").join(file), body).unwrap();
    }

    const OK: &str =
        "---\nname: code-review\ndescription: Review diffs for bugs\n---\nBody never injected.\n";

    #[test]
    fn appends_catalog_for_valid_skill() {
        let tmp = tempfile::tempdir().unwrap();
        skill_file(tmp.path(), "review.md", OK);
        let out = append_skills_in("BASE", Path::new("/nope"), tmp.path());
        assert!(out.starts_with("BASE"));
        assert!(out.contains("## Skills"));
        assert!(out.contains("- name: code-review — Review diffs for bugs"));
        assert!(out.contains(&format!(
            "(source: {})",
            tmp.path().join("skills/review.md").display()
        )));
        assert!(
            out.contains("`read`"),
            "must tell the model to use the read tool"
        );
        assert!(!out.contains("Body never injected"));
    }

    #[test]
    fn missing_description_and_no_frontmatter_are_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        skill_file(tmp.path(), "bad.md", "---\nname: x\n---\nbody\n");
        assert_eq!(
            append_skills_in("BASE", Path::new("/nope"), tmp.path()),
            "BASE"
        );
        skill_file(tmp.path(), "plain.md", "Just some markdown.\n");
        assert_eq!(
            append_skills_in("BASE", Path::new("/nope"), tmp.path()),
            "BASE"
        );
    }

    #[test]
    fn no_skills_returns_base_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(
            append_skills_in("BASE", Path::new("/nope"), tmp.path()),
            "BASE"
        );
    }

    #[test]
    fn project_overrides_global_on_name_clash() {
        let g = tempfile::tempdir().unwrap();
        let p = tempfile::tempdir().unwrap();
        skill_file(
            g.path(),
            "a.md",
            "---\nname: dup\ndescription: global one\n---\n",
        );
        skill_file(
            &p.path().join(".conga"),
            "b.md",
            "---\nname: dup\ndescription: project one\n---\n",
        );
        let out = append_skills_in("BASE", p.path(), g.path());
        assert!(out.contains("project one") && !out.contains("global one"));
        assert_eq!(
            out.matches("- name: dup").count(),
            1,
            "exactly one line per name"
        );
    }

    #[test]
    fn project_only_skill_is_included() {
        let g = tempfile::tempdir().unwrap();
        let p = tempfile::tempdir().unwrap();
        skill_file(
            &p.path().join(".conga"),
            "only.md",
            "---\nname: proj\ndescription: from project\n---\n",
        );
        assert!(
            append_skills_in("BASE", p.path(), g.path()).contains("- name: proj — from project")
        );
    }

    #[test]
    fn project_skill_source_is_cwd_relative() {
        let g = tempfile::tempdir().unwrap();
        let p = tempfile::tempdir().unwrap();
        skill_file(g.path(), "glob.md", OK);
        skill_file(
            &p.path().join(".conga"),
            "proj.md",
            "---\nname: proj\ndescription: from project\n---\n",
        );
        let out = append_skills_in("BASE", p.path(), g.path());
        // Project entries must be cwd-relative: `read` rejects absolute
        // paths outside `~/.conga`, so the catalog must hand the model a
        // path it can actually use.
        assert!(
            out.contains("(source: .conga/skills/proj.md)"),
            "project source should be cwd-relative, got: {out}"
        );
        assert!(
            out.contains(&format!(
                "(source: {})",
                g.path().join("skills/glob.md").display()
            )),
            "global source should stay absolute"
        );
    }

    #[test]
    fn quoted_scalars_are_unquoted() {
        let tmp = tempfile::tempdir().unwrap();
        skill_file(
            tmp.path(),
            "quoted.md",
            "---\nname: \"code-review\"\ndescription: 'Review diffs for bugs'\n---\nbody\n",
        );
        let out = append_skills_in("BASE", Path::new("/nope"), tmp.path());
        assert!(
            // (em dash separator - matches the catalog format above)
            out.contains("- name: code-review — Review diffs for bugs"),
            "quotes must be stripped for both keys, got: {out}"
        );
    }

    #[test]
    fn unbalanced_quotes_stay_part_of_scalar() {
        let tmp = tempfile::tempdir().unwrap();
        skill_file(
            tmp.path(),
            "odd.md",
            "---\nname: it's-fine\ndescription: say \"hi\" now\n---\n",
        );
        let out = append_skills_in("BASE", Path::new("/nope"), tmp.path());
        assert!(
            // (em dash separator - matches the catalog format above)
            out.contains("- name: it's-fine — say \"hi\" now"),
            "mid-value quotes must survive, got: {out}"
        );
    }

    #[test]
    fn literal_block_scalar_description_is_collapsed() {
        let tmp = tempfile::tempdir().unwrap();
        skill_file(
            tmp.path(),
            "block.md",
            "---\nname: blk\ndescription: |\n  First line of purpose.\n  Second line, more detail.\n---\nbody\n",
        );
        let out = append_skills_in("BASE", Path::new("/nope"), tmp.path());
        assert!(
            out.contains("- name: blk — First line of purpose. Second line, more detail."),
            "block scalar must collapse to one catalog line, got: {out}"
        );
    }

    #[test]
    fn folded_block_scalar_with_chomp_is_collapsed() {
        let tmp = tempfile::tempdir().unwrap();
        skill_file(
            tmp.path(),
            "fold.md",
            "---\nname: fold\ndescription: >-\n  folded one\n  folded two\n---\n",
        );
        let out = append_skills_in("BASE", Path::new("/nope"), tmp.path());
        assert!(
            out.contains("- name: fold — folded one folded two"),
            "folded scalar with chomping must collapse, got: {out}"
        );
    }

    #[test]
    fn long_description_is_truncated() {
        let tmp = tempfile::tempdir().unwrap();
        let long: String = "x".repeat(300);
        skill_file(
            tmp.path(),
            "long.md",
            &format!("---\nname: long\ndescription: {long}\n---\n"),
        );
        let out = append_skills_in("BASE", Path::new("/nope"), tmp.path());
        let expected: String = "x".repeat(MAX_DESCRIPTION_CHARS);
        assert!(
            out.contains(&format!("- name: long — {expected}…")),
            "description must be capped at {MAX_DESCRIPTION_CHARS} chars, got: {out}"
        );
        assert!(
            !out.contains(&"x".repeat(MAX_DESCRIPTION_CHARS + 1)),
            "no run longer than the cap may appear"
        );
    }
}
