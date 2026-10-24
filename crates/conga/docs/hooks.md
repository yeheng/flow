# Process Hooks (`hooks.json`)

A **process hook** is an external command conga runs before a tool call.
No Rust crate required — configure via `hooks.json`, write the hook in any
language (see [the `rtk` example](#a-worked-example-prefixing-bash-commands-with-rtk)).
The protocol is a compatible subset of Claude Code's, so many existing
hook scripts work unmodified.

Hooks sit in the host's hook stack **before** the permission policy: they
can block or rewrite a call early, but they can never bypass the policy
underneath. Sub-agents inherit the same composed stack — a hook blocking
`bash` blocks it in sub-agents too.

**Trust:** `hooks.json` is code. Anyone who can write it can run arbitrary
commands in your name — same trust level as `settings.json`. Hooks are
**not** sandboxed.

---

## Where hooks are discovered

| File | Scope |
|---|---|
| `<config_dir>/hooks.json` — usually `~/.conga/hooks.json` | global |
| `<project>/.conga/hooks.json` | project |

- `<config_dir>` follows `$HOME`; pre-rename installs that never created
  `~/.conga` keep using the legacy `~/.gasket` root (same rule as the rest
  of conga's config).
- Both files compose: **global entries load first, project entries are
  appended after** (ordering matters — see [Chains](#chains)).
- A missing file is silent (no hooks). A file that exists but is unreadable
  or unparseable is **skipped with a warning** — a broken `hooks.json`
  never prevents the host from starting.
- Files are read once per session assembly, not per tool call.

---

## The file format

```json
{
  "PreToolUse": [
    {
      "matcher": "bash",
      "hooks": [
        {
          "type": "command",
          "command": "sh .conga/hooks/rtk-prefix.sh",
          "timeout": 10
        }
      ]
    }
  ]
}
```

- Only `PreToolUse` is honored in v1. Unknown event keys (`PostToolUse`, …)
  are ignored, so a `hooks.json` written for Claude Code loads without
  errors — its other events just don't run yet.
- `matcher` — which tools the group applies to:

  | Matcher | Matches |
  |---|---|
  | `""`, `"*"`, `"all"` | every tool |
  | `"bash"` | only `bash` |
  | `"bash,write"` | `bash` or `write` |

  Comma-separated **exact** names. No regex; `*` only as "everything".
- `type` — only `"command"` is supported; the field may be omitted
  (absent = `command`). Any other type is skipped with a warning.
- `command` — run via `sh -c <command>` with the working directory pinned
  to the project dir, so relative paths like `.conga/hooks/check.sh`
  resolve no matter where conga was started.
- `timeout` — seconds, default `10`; an explicit `0` also means the
  default.

---

## The stdin payload

The hook process receives one JSON object on stdin:

```json
{
  "tool_name": "bash",
  "tool_input": { "command": "echo hi" },
  "tool_call_id": "tc-1",
  "risk": "low"
}
```

- `tool_input` is exactly the arguments the tool would receive.
- `risk` is the tool's risk level, lowercase: `"low"` / `"medium"` /
  `"high"`.
- `session_id` is **not** provided — a known divergence from Claude Code.

---

## The verdict protocol

The hook answers with its exit code, and — for exit 0 — a JSON decision
object on stdout:

| Hook result | Verdict |
|---|---|
| exit code `2` | **Block**. stderr becomes the reason shown to the model (empty stderr → `"blocked by process hook"`). |
| exit `0` + stdout `{"hookSpecificOutput": {"permissionDecision": "deny", "permissionDecisionReason": "…"}}` | **Block**. Reason = `permissionDecisionReason`, else stderr, else the fixed fallback. `deny` wins even if `updatedInput` is also present. |
| exit `0` + `updatedInput` object (decision `"allow"` or absent) | **Modify**. The tool's input is replaced by `updatedInput`. |
| exit `0` + `permissionDecision: "allow"`, no `updatedInput` | **Allow**. Remaining hooks are skipped. |
| exit `0`, no parsable decision object | No opinion — the next hook runs. |
| any other exit code, spawn failure, or timeout | **Warn + allow** (fail-open; see below). |

Within one hook's stdout the precedence is: **deny → rewrite → allow**.
A block reason is returned to the model as an error tool result; the tool
body never runs.

`updatedInput` replaces the tool input **wholesale** — include every key
the tool needs, not only the changed ones.

### Chains

With multiple matching hooks (global then project, groups in file order):

- The **first Block stops** everything.
- An explicit **allow stops** the chain (short-circuits remaining hooks).
- Otherwise hooks **thread the args**: hook *N+1* receives hook *N*'s
  rewrite, and the **last Modify wins**.

---

## Fail-open, on purpose

Every failure mode — nonzero exit (other than 2), spawn failure, timeout —
allows the call with a warning. Rationale: the permission policy still
gates underneath, so a broken or wedged hook degrades to "no extra gate"
instead of bricking the agent. The deny paths are the exception: `deny`
and exit 2 always block (fail-closed at the hook level).

---

## A worked example: prefixing `bash` commands with `rtk`

`rtk` is a read-only file-tools wrapper; routing `bash` commands through
it when it is installed keeps edits reviewable. Drop two files into the project:

**`.conga/hooks/rtk-prefix.sh`** — read the payload, rewrite `command`,
emit the decision:

```sh
#!/bin/sh
# Prefix bash commands with `rtk` when it is installed and not yet used.
input=$(cat)
command=$(printf '%s' "$input" | sed -n 's/.*"command"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
[ -n "$command" ] || exit 0                      # no command -> no opinion
command -v rtk >/dev/null 2>&1 || exit 0         # rtk missing -> no opinion
case $command in "rtk "*) exit 0 ;; esac         # already prefixed
escaped=$(printf '%s' "$command" | sed 's/\\/\\\\/g; s/"/\\"/g')
printf '{"hookSpecificOutput":{"updatedInput":{"command":"rtk %s"}}}\n' "$escaped"
```

**`.conga/hooks.json`**:

```json
{
  "PreToolUse": [
    {
      "matcher": "bash",
      "hooks": [
        { "type": "command", "command": "sh .conga/hooks/rtk-prefix.sh" }
      ]
    }
  ]
}
```

Now a `bash` call with input `{"command": "echo hi"}` reaches the tool as
`{"command": "rtk echo hi"}`; without `rtk` on `PATH` the call passes
through unchanged. The extraction is deliberately minimal (no embedded
double quotes) — swap in `jq` or python for full JSON handling.

Test the script standalone before wiring it in:

```sh
echo '{"tool_name":"bash","tool_input":{"command":"echo hi"},
       "tool_call_id":"tc-1","risk":"low"}' | sh .conga/hooks/rtk-prefix.sh
# {"hookSpecificOutput":{"updatedInput":{"command":"rtk echo hi"}}}
```

---

## v1 limitations

- `PreToolUse` only — `after_tool_call` is a passthrough; `PostToolUse`
  process hooks would need an async seam and are deferred until a consumer
  exists.
- No session or turn lifecycle events.
- Sub-agents inherit the stack (no per-agent hook scoping).

---

## Summary

- Two discovery points: global `<config_dir>/hooks.json`, then project
  `<project>/.conga/hooks.json`; both compose.
- stdin: `{tool_name, tool_input, tool_call_id, risk}` — no `session_id`.
- exit 2 → Block(stderr); `deny` → Block(reason); `updatedInput` → Modify
  (replaces the whole input); `allow` → Allow + short-circuit; anything
  else → warn + allow.
- Matcher: `*`/`all`/exact comma-separated names; no regex.
- `timeout` seconds, default 10; commands run `sh -c` in the project dir.
- `hooks.json` is trusted code, not sandboxed; failures fail open because
  the permission policy still gates underneath.
