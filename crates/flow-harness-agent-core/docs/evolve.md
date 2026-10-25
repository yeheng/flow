# Self-Evolution (`/evolve`)

A design for conga's self-evolution capability: distill reusable
**experience** and **skills** from session transcripts on demand, and
surface relevant experience in every turn's prompt.

Informed by: ReasoningBank (extract→consolidate→retrieve loop), Voyager
(verified admission into a skill library), Anthropic Agent Skills
(filesystem skill packs with frontmatter + progressive disclosure — the
read path below is exactly this pattern), Letta/MemGPT (self-editable
memory), EvoAgentX (evaluation-gated evolution), Darwin Gödel Machine /
AlphaEvolve (archive + rollback as the safety net — here distilled to:
human-readable files, provenance keys, and `git init ~/.conga` if you
want history).

One sentence: distill sessions into tagged markdown notes, list them in
a prompt catalog, let the model `read` the relevant ones on demand —
and gate every write behind a human.

---

## What evolves (and what does not)

Evolves:

1. **Experience memory** — "next time this happens, do X" insights
   extracted from full session trajectories.
2. **Skills** — repeatable procedures distilled into the existing
   skills directory format.

Does **not** evolve (explicit non-goals):

- `systemPrompt` / hooks config / `CODING_AGENT_PROMPT` (offline prompt
  optimization needs an evaluator conga does not have).
- conga's own source code (worst risk/benefit for a personal assistant).
- No embedding store, no FTS5 memory index, **no relevance scorer** —
  the catalog plus model-driven `read` (progressive disclosure) does
  the selection, exactly like skills.
- No per-turn online extraction, no timers, no file watchers, no
  per-turn bookkeeping (the read path writes zero bytes).

---

## Storage

### Experience: `~/.conga/memory/*.md` (single root)

Same shape as skills (frontmatter + body), so one scanner style serves
both. No project-level root: nobody asked for project-scoped
experience, and a second root costs a second scanner, override rules,
and double the tests. Add it the day someone needs it.

```markdown
---
title: cargo-workspace-cyclic-dependency
tags: [rust, cargo, build-error]
created: 2026-08-25T10:00:00Z
source_session: <session-id>
---
On "cyclic package dependency": check workspace members' path refs first,
then [patch] sections; root cause here was conga-ext -> conga-host.
```

Frontmatter is exactly: `title`, `tags`, `created`, `source_session`.
No counters, no usage tracking. Hard limits: **64 entries**, body
≤ 2KB per entry. Small enough to scan fully per turn — no index, no
vector store.

Audit and rollback are the files themselves: every entry carries its
`source_session`, everything is human-readable and human-editable,
deleting a file reverts it. No separate log file — one fact, one place.

### Skills: existing `skills/*.md`, plus provenance

Two extra frontmatter keys on evolved skills: `provenance: evolve` and
`source_session: <id>`. Skills **without** those keys are user-authored;
evolve never overwrites user-authored skills. Existing skill parsing
only reads `name:`/`description:` — extra keys are transparent.

---

## Write path: `/evolve` (explicit, expensive)

One entry point, `Host::evolve(session_id)`, reached two ways:

- CLI: `/evolve [--session <id>]` in `handle_slash` — direct call, does
  not spend a main-model turn.
- Tool: built-in `evolve` tool (risk = High) for gateway/desktop —
  the permission matrix already gates High risk (blocked in
  Suggest/Plan, approved in AutoEdit, free in FullAuto); no new
  interaction concepts.

Pipeline (4 steps, no consolidation pass):

1. **Load trajectory** — project the session's `events.jsonl` via
   `derive_messages` (which already restarts from the last
   `Cleared`/`Compacted` checkpoint, so dropped segments are skipped by
   construction) and render it to text. If overlong, truncate oldest
   messages and say so in the input.
2. **Extract** — one sub-agent via `HostSubagentSpawner` (inherits the
   fast-model routing). Input includes the current memory/skills
   catalog (titles + tags/descriptions) so it proposes deltas, not
   echoes. Structured output:
   ```json
   { "insights":  [{"title","tags","content"}],
     "skills":    [{"name","description","body"}],
     "retires":   ["existing titles made obsolete by the above"],
     "duplicates": ["catalog entries the above would duplicate"] }
   ```
3. **Admit** — each candidate (adds and retires alike) passes through
   the existing `Approver` closure (`approver("evolve_write",
   {title, content})`): CLI prints y/N, gateway does a WS round-trip,
   desktop does IPC. Rejected = dropped, no partial writes.
   The **cap is an admission check**, not a background job: with the
   library at 64, an add is admitted only if a same-run retire freed a
   slot.
4. **Persist** — insights to `~/.conga/memory/`, skills to the skills
   dir, approved retires delete their files. One pass, atomic per file.

The extraction output is **never executed**: insights and skills are
prompt text only (deliberately one notch more conservative than
Voyager's executable skill library).

### Content quality (top implementation priority)

The architectural risk here is near zero (prompt text only); the real
risk is **garbage insights**. An extractor left to its own devices
produces "when the build fails, read the error carefully" — and once
that pollutes the library, every turn's catalog carries it. Two
defenses, both mandatory:

- The extraction prompt must require each insight to carry
  **root cause + the fix actually applied + evidence from the
  trajectory**. No general advice, only what this session proved.
- The approver payload shows title, tags, and the full body — enough to
  judge specificity in one glance. If you cannot tell what situation
  an insight applies to, reject it.

---

## Read path: catalog + progressive disclosure (automatic, cheap, read-only)

The host keeps the system prompt **byte-stable across turns** — that is
the invariant that keeps the provider prompt-cache prefix warm (see the
`run_turn` comments: volatile content rides in the request tail, never
in the prompt). So memory must NOT be injected as per-turn-varying
selected content — that would bust the cache every turn.

Instead, memory rides exactly like skills:

- At the per-turn seam where `run_turn` already re-reads settings and
  rebuilds the base prompt, compose:
  `base_prompt -> settings systemPrompt -> append_skills -> append_memory`.
- `append_memory(base)` scans `~/.conga/memory/*.md` and appends a
  `## Memory` catalog — one line per entry (`title — first body line`,
  bounded) with the readable file path, mirroring `append_skills`.
- The **model** decides relevance from the catalog and pulls the full
  entry with the existing `read` tool (absolute paths under `~/.conga`
  are already allowed there).
- The catalog is deterministic (sorted by title): same files → same
  bytes → the cache prefix survives every turn. It changes only when
  the library changes (i.e. right after `/evolve` or a manual edit) —
  one legitimate cache miss per real change, and the new entry is
  visible the very next turn.
- Skills move to the same seam (out of one-shot `assemble_host`), so an
  evolved skill also enters the catalog the turn after it is written.

Degradation: missing/empty memory dir → silent no-op; an unparsable
entry → warn + skip (same semantics as `skills::scan_dir`).

---

## Security boundaries

- Evolved content is prompt text only; no new code-execution surface.
- Prompt-injection in trajectories (e.g. a poisoned web page) can at
  worst pollute a **candidate** — still behind the human Approver and
  the bounded catalog (64 one-line entries). Blast radius bounded.
- `evolve` tool is High risk: Suggest/Plan cannot trigger it at all.
- Multiple concurrent sessions share the library through plain files;
  the read path is stateless and read-only, and the write path runs
  only under human approval — last writer wins, no counters to race on.

---

## Verification

1. Unit (pure functions, `skills.rs` test style): frontmatter parse;
   catalog line bounding + unparsable-entry skip; cap admission check
   (add at cap rejected / freed slot admitted); `Compacted`/`Cleared`
   segment handling via `derive_messages`; byte-stability (two scans of
   the same dir produce identical output).
2. Integration (tempdir fake home): run `/evolve` with a stubbed
   extraction sub-agent returning fixed JSON → assert memory/skills on
   disk, retires deleted, no log sidecar; next turn asserts the
   catalog section in the composed prompt and that the read path wrote
   nothing.
3. Smoke (real CLI): create a "stumble → correct" trajectory, `/evolve`,
   observe the approval flow and the new catalog line in the next
   turn's prompt; confirm the produced insight names the root cause,
   not a platitude.

---

## Change surface

| File | Change |
|---|---|
| `conga-host/src/memory.rs` (new) | entry format, scanner, read-only catalog append |
| `conga-host/src/evolve.rs` (new) | `Host::evolve` pipeline, trajectory renderer, extraction prompt, admission (incl. cap check) |
| `conga-host/src/lib.rs` | per-turn seam: skills rescan + `append_memory`; spawner field for evolve |
| `conga-host/src/assembly.rs` | move one-shot skills append to the seam |
| `conga-host/src/tools/mod.rs` | register `evolve` tool (High risk) |
| `conga-cli/src/main.rs` | `/evolve [--session <id>]` |
| `docs/` | document commands, formats, limits |

---

## Usage

- CLI: `/evolve` (current session) or `/evolve --session <id>`.
- Any transport: ask the agent to "evolve this session" — the High-risk
  `evolve` tool routes to the same core.
- Every candidate (insight add, skill add/update, retire) is a separate
  approval; denying one drops only that candidate.
- Library: `~/.conga/memory/*.md` (cap 64) and the skills dirs. Delete a
  file to undo an entry; `git init ~/.conga` for history.
