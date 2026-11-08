# True-Mem V2 — Customization & Operations Handbook

Living doc for this fork of [rizal72/true-mem](https://github.com/rizal72/true-mem): a
native **OpenCode V2** port plus local customizations. Keep it updated as the code changes.

- **Local checkout:** `~/dev/true-mem-v2`
- **GitHub:** https://github.com/ilahomesick/opencode-true-mem-v2 (public)
- **npm name:** `opencode-true-mem-v2` *(prepared; not yet published — no npm auth configured)*
- **Runtime data:** `~/.true-mem/` (`memory.db`, `plugin-debug.log`, `config.jsonc`)
- **Query helper:** `memq <words>` (read-only, offline)

---

## 1. What this is

True-mem stores durable "memories" (preferences, constraints, decisions, learnings,
semantic facts) in SQLite and injects the relevant ones into the system prompt. This fork
adds:

1. a **V2 adapter** so it loads on OpenCode V2 (upstream is V1-only),
2. a **TUI companion** that toasts when a memory is saved,
3. a series of correctness/lifecycle fixes, and
4. **semantic embeddings / vector search** (current work in progress).

The cognitive core (classifier, patterns, decay, STM/LTM, reconsolidation) is upstream's,
largely untouched.

---

## 2. Install & load

The plugin runs from the built bundle in this repo; OpenCode loads it through a small
bridge directory (auto-discovered global plugin).

**Server plugin** — `~/.config/opencode/plugins/true-mem-v2/`
- `index.ts` → `export { default } from "/Users/ilariosalatino/dev/true-mem-v2/dist/entry.js";`
- `tui.ts` → `export { default } from "/Users/ilariosalatino/dev/true-mem-v2/dist/tui.js";`
- `package.json` → `{ "name": "true-mem-v2", "type": "module", "exports": { ".": "./index.ts", "./tui": "./tui.ts" } }`

**TUI companion registration** — `~/.config/opencode/cli.json`
```jsonc
{ "plugins": ["/Users/ilariosalatino/.config/opencode/plugins/true-mem-v2"] }
```

The server bridge is auto-discovered from the plugins dir; the TUI entry must be listed in
`cli.json`. (`tui.json` was migrated into `cli.json` by OpenCode.)

> Once published, users can instead do `{ "plugins": ["opencode-true-mem-v2"] }` in
> `opencode.jsonc` and `cli.json` — see README.

---

## 3. Repo layout (what matters)

| Path | Role |
|---|---|
| `src/v2/entry.ts` | **V2 plugin entry** — wraps the V1 core with V2 hooks/events |
| `src/tui.ts` | TUI toast companion (exported as `./tui`) |
| `src/adapters/opencode/index.ts` | V1 core integration: extraction, injection, hooks, DB orchestration |
| `src/storage/database.ts` | SQLite: sessions/events/memories, decay, reconsolidation, vector search |
| `src/memory/classifier.ts` | Classification + confidence + role awareness (multi-language) |
| `src/memory/patterns.ts` | Signal regexes, scope logic, memory-list command |
| `src/memory/embeddings.ts` | Jaccard + embedding helpers (embed/cosine/serialize) |
| `src/memory/embedding-worker.ts` | Isolated embedding worker (ONNX / transformers.js) |
| `src/utils/jsonc.ts` | Config parsing (comments + trailing commas) |
| `scripts/` | Embedding daemon / backfill / query / vector-search smoke test |
| `package.json` | Entry `dist/entry.js`, exports `.` and `./tui`; build script |

**Build outputs:** `dist/entry.js`, `dist/tui.js`, `dist/memory/embedding-worker.js`
(all bundled; only node builtins remain external).

---

## 4. How the V2 adapter works

Upstream's `createTrueMemoryPlugin(ctx)` returns a V1 `Hooks` object. `src/v2/entry.ts`
calls it with a **fabricated V1 client** backed by V2 APIs, then re-registers the hooks on
V2 domains:

| V1 hook | V2 registration |
|---|---|
| `chat.message` | `ctx.session.hook("prompt", …)` |
| `experimental.chat.system.transform` | `ctx.session.hook("context", …)` |
| `experimental.session.compacting` | `ctx.session.hook("compaction", …)` → `ctx.generate.text` → `event.result` |
| `tool.execute.before/after` | `ctx.tool.hook("execute.before"/"execute.after", …)` |
| `event` | `ctx.event.subscribe({ signal })` |
| `client.session.messages` | `ctx.session.context({ sessionID })` (+ V2→V1 message conversion) |

### V2 gotchas learned the hard way
- A V2 plugin **must** `export default { id, setup }` (or `Plugin.define`). A V1 function
  export fails with *"must export a default definition with an id and an effect or setup"*.
- **There is no public `session.idle`.** Turn completion arrives as
  `session.execution.succeeded` (and `session.status` with `status:"idle"`). Map those to
  the core's idle handler.
- `session.status` / lifecycle events carry `data.sessionID`.
- The host may run `setup()` more than once → **one active instance per location**
  (globalThis takeover) to avoid N× injection; dedupe events by `id`.
- V2 **prunes the transcript** on compaction; if `messages.length < storedWatermark`,
  **reset the watermark to 0** or extraction silently stops forever (see §6).
- Hook `event.system` is `SystemPart[]`, not a string; append a new text part for injections.
- `tool.execute.before` `event.input` is mutable; `tool.execute.after` exposes
  `status` + `result`/`error`.

---

## 5. Customizations ledger

### 5.1 V2 adapter + TUI companion (initial port)
- `src/v2/entry.ts`, `src/tui.ts`, `package.json` entry/exports.
- TUI polls `~/.true-mem/plugin-debug.log` and shows `context.ui.toast.show(...)` on each
  stored memory; bursts are coalesced into one toast; duplicate lines deduped;
  single active poller per process.

### 5.2 Review-driven correctness fixes
- Event-id dedupe; synthesized-idle coalescing (one per session per ~2s).
- Per-location single active instance (no N× injection).
- `INSERT OR IGNORE` for `sessions`; self-heal missing session rows (watermark black hole).
- Exact-dup content-hash lookup is **scope-aware** (`project_scope IS ?`).
- Drop duplicate `incrementFrequency` on reconsolidation `duplicate`.
- Guarded `ROLLBACK` (don't mask the real error after `COMMIT`).
- Reset regex `lastIndex` before `.test()` in loops (stateful `/g` bug).
- Normalize non-string tool output (prevents a throw that drops a turn's extraction).
- Truncate stored tool input/output to 2000 chars.
- Global/project split uses loose equality (`== null`) — `projectScope` is `undefined` for
  global rows, not `null`.

### 5.3 Lifecycle / hygiene
- Close the instance's SQLite connection on dispose (upstream `executeShutdown` is never
  called; each reload leaked a connection).
- `handleSessionEnd` skips O(all-memories) maintenance for **sub-agent** sessions.
- `tool.execute.after` must not let a sub-agent session hijack `currentSessionId`.
- Context cache sweeps expired entries (+ hard cap).
- Clear the `Promise.race` timeout in `shouldInjectResumedSession`.
- `markSessionCreated` early-returns for already-tracked sessions (no injected-flag reset).
- Removed dead `getDefaultConfig`/ESM `require` and `_inTransaction`.

### 5.4 Transcript-pruning fix (cherry-picked from @deathcl)
- After compaction the host prunes messages; `messages.length < watermark` would skip
  extraction forever. Reset watermark to 0 and rescan (per-pass dedupe collapses repeats).

### 5.5 Semantic embeddings / vector search *(current, uncommitted)*
- `embedText()`: 384-d `Xenova/all-MiniLM-L6-v2` (q8/cpu, mean pooling, normalized) via the
  isolated embedding worker; returns null and falls back to Jaccard.
- `vectorToBuffer` / `bufferToVector` / `cosineSimilarity` implemented (were stubs).
- `database.ts`: stored-embedding vector search path + `associations` backfill.
- `scripts/`: `embed-serve.mjs` (warm unix-socket daemon), `embed-query.mjs`,
  `backfill-embeddings.mjs`, `smoke-vector-search.mjs` (copies the DB, never touches live).
- Enable with `TRUE_MEM_EMBEDDINGS=1` / `embeddingsEnabled: 1`.

### 5.6 `question`-tool answer attribution
- Answers submitted through the question tool arrive as a **tool result**, but the words
  are the human's. `extractConversationText*` now emits them as `Human:` lines so
  extraction recognises them (both plain and role-aware paths).

---

## 6. Model lineup (OMO-slim) decisions

Config: `~/.config/opencode/oh-my-opencode-slim.jsonc`, preset `opencode-go`.

- **orchestrator:** `deepseek-v4.1-flash` (unchanged) — chosen for fastest TTFT (0.90s);
  the interactive loop is latency-sensitive.
- **fixer:** `[step-5-preview-free (high), deepseek-v4.1-flash (high)]` — see bake-off.
- **oracle / council:** `glm-5.3`; council seats remain diverse.
- **explorer / observer:** `step-5-preview-free` first, with fallbacks.

### 3-round bake-off (same spec, isolated worktrees)
| | deepseek-v4.1-flash | glm-5.3-flash | step-5-preview-free |
|---|---|---|---|
| R1 (7 bugs) | correct, minimal | **retry-cap bug** | correct + thorough |
| R2 (lifecycle/hygiene) | correct | correct | correct + most thorough |
| R3 (utility + tests) | correct | correct | correct; 13 tests (most) |
| neutral property test | 0 failures | 0 failures | 0 failures |

**Verdict:** `step-5-preview` won all three; promoted to fixer primary with a paid
fallback. Post-promo Step-5 ≈ $1/$2.70 per 1M (≈5× the flash tier) and ~2.5× slower — so
keep the fallback.

---

## 7. Operations

```bash
# build (also required after editing src)
cd ~/dev/true-mem-v2 && bun run build

# typecheck
bunx tsc --noEmit

# reload the running service (picks up dist via the watcher)
opencode reload

# inspect what the plugin is doing
tail -f ~/.true-mem/plugin-debug.log

# query memories (read-only)
memq true-mem
memq opencode model

# smoke: send a prompt in a fresh session, then check the DB
cd ~ && opencode run "Remember that I always ... in every project."
```

- The running server's watcher reloads `dist/entry.js` on rebuild; the TUI plugin also
  reloads via `opencode reload`.
- Deep-link: `~/.config/opencode/plugins/true-mem-v2/index.ts` is watched.

---

## 8. Known limitations

- **Compaction injection** is ported but only takes over when memories are actually
  injected (bails otherwise so the host compacts normally). With `injectionMode: 1` it is
  partly redundant.
- Extraction runs for **all top-level sessions**; the orchestrator session is included.
- Sub-agent sessions are skipped (child-session registry); top-level sessions are not.
- Embeddings are optional/experimental; Jaccard remains the default.
- Per-pass dedupe can drop a legitimately repeated instruction within one window.
- `memq` is read-only; there is no first-class memory-write tool (auto-extraction + direct
  DB writes only).

---

## 9. Publishing status

- Package prepared as **`opencode-true-mem-v2@0.1.0`** (`private` removed,
  `publishConfig.access: public`, ships `dist/` + README + LICENSE + PORT-NOTES).
- **Not published:** no npm auth (`~/.npmrc` absent). To publish: `bunx npm login` (or add
  an automation token to `~/.npmrc`), then `bun publish` in the repo.
- **Name collision:** `true-mem-v2` on npm is owned by **deathcl (danwad)**, who forked and
  republished this port (MIT-compliant; kept `author: rizal72`; no credit to us). Their
  fork added Spanish classifier support, JSONC trailing commas, and the transcript-pruning
  fix (the last was cherry-picked here). Audit of their npm tarball found no extra payload
  or install scripts.
- `opencode-true-mem-v2` remains available on npm.

---

## 10. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Plugin must export a default definition…` | V1-shaped export; V2 needs `{ id, setup }` |
| No memories ever saved | idle event not mapped; or watermark ≥ message count (pruning) |
| Injection duplicated | multiple plugin instances; check per-location takeover |
| Toasts spam / repeat | coalesce + dedupe key; single active poller |
| `UNIQUE constraint failed: sessions.id` | duplicate `session.created`; `INSERT OR IGNORE` + event dedupe |
| Extraction stops after a long session | transcript pruned → watermark reset fix |
| Plugin not loaded | bridge file / `cli.json` registration; check load logs |
| SQLite busy | WAL + `busy_timeout`; avoid long transactions |

---

*Last updated: 2026-11-08. Keep this file in sync with the code.*
