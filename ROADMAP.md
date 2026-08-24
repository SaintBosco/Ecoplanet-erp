# Carbon ERP — Roadmap & Handoff Notes

Written after a build audit of the packaged `Carbon ERP-win32-x64` output
(originally living at `OneDrive\Desktop\2026 BVA Report\...`, still there and
still live — **do not modify or delete anything in that folder**, it's the
running copy). This repo is a clean source import from that build, with
`node_modules/`, `backend/data/` (live + backup JSON data), and `.env`
(real DB credentials) deliberately excluded. See `.gitignore`.

Owner: Luca. Read this fully before changing anything — several items below
are open decisions, not settled designs.

## 1. What this actually is

- `desktop/` — a Nativefier wrapper (Electron shell, `nativefierVersion 52.0.0`,
  Electron `25.7.0`) that just loads `http://localhost:3001/dashboard.html` in
  a Chromium window. No custom Electron code of note (`inject/` was an empty
  placeholder, not carried into this repo).
- `backend/` — a single Express server (`server.js`, ~4,800 lines) serving the
  API and the dashboard, exposed on port 3001. Started as a sibling `node`
  process by `launcher.js` / `start-carbon-erp.ps1` / `start-carbon-erp.bat`,
  not embedded in the Electron process.
- Data layer today: `backend/db.js` (`JsonDB`) — one JSON file
  (`backend/data/app.db`, default path from `server.js`) rewritten in full on
  every single write. This is what's actually wired into `server.js` right now.
- Data layer in progress: `backend/prisma/schema.prisma` (Postgres, via
  `DATABASE_URL`) + `backend/scripts/migrate-to-pg.js`. The migration script
  **only covers 4 tables**: `users`, `accounts`, `customers`, `suppliers`. The
  `JsonDB` default schema in `db.js` defines ~90 tables total (stock, sales,
  purchasing, manufacturing, HR/payroll, DMS, BVA reporting, etc.). Nothing
  else has a migration path yet.
- `backend/src/{controllers,middleware,routes,services,workers}` exists but
  `server.js` doesn't `require` any of it — looks like an abandoned
  modularization pass. Confirm before deleting; don't delete unilaterally.

## 2. Decisions already made (by Luca, 2026-08-24)

- **DB direction: finish the Postgres/Prisma migration.** Not staying on
  `JsonDB` long-term.
- **Distribution target: other EcoPlanet staff, not just Luca's machine.**
  This has architectural consequences — see Open Questions below before
  writing any multi-user code.

## 3. Open questions — ask before building Phase 2/3

The two decisions above don't compose cleanly on their own; the next agent
should get answers before implementing, not assume:

1. **Topology for multi-user access.** Two very different shapes fit "other
   staff too":
   - (a) One central server (backend + Postgres + Redis) that every staff
     member's desktop shell points at over the network, or
   - (b) Each machine runs its own local backend + local Postgres, with data
     synced/consolidated some other way.
   These lead to different auth, deployment, and packaging work. Don't guess.
2. **Auth model.** `bcryptjs` + `jsonwebtoken` are already dependencies and
   `db.js` has a `users`/`sessions`/`login_history` shape, but if this becomes
   multi-user over a network, confirm whether existing auth is sufficient or
   needs session/token hardening (e.g. JWT secret currently comes from
   wherever `server.js` reads it — audit that before exposing this beyond
   localhost).
3. **Redis.** `bullmq` + `ioredis` are runtime dependencies of the backend.
   Confirm whether background jobs are actually in use today, and if so,
   where Redis will live under whichever topology is chosen in (1).
4. **`better-sqlite3`** is a dependency but `db.js` doesn't use it. Confirm
   it's dead before dropping it (grep `backend/src/` and `server.js` for any
   reference — it wasn't required in the first 50 lines checked, but the file
   is 4,800 lines).

## 4. Phased plan

### Phase 0 — Stop the bleeding (do first, independent of everything else)
- [ ] Confirm the live `.env` (real `DATABASE_URL`) and `backend/data/*.json*`
  are never included in any future build/zip/share of this app — they were
  found shipped inside the distributable in the OneDrive-synced folder.
- [ ] Rotate/replace any credential that was in that shipped `.env`, out of
  caution, once a real DB host/credentials exist (see Phase 2).
- [ ] Add `.gitignore` equivalent rules to whatever packaging script produces
  the `-win32-x64` build (this repo's `.gitignore` doesn't protect the build
  output folder).

### Phase 1 — Finish the Postgres/Prisma migration
- [ ] Extend `schema.prisma` to cover the full `JsonDB` schema (currently 4 of
  ~90 tables modeled).
- [ ] Extend `migrate-to-pg.js` to migrate the remaining tables, or write a
  generic migrator driven by the `JsonDB` schema list in `db.js`.
- [ ] Swap `server.js`'s `require('./db')` usage for the Prisma client,
  incrementally per route/table so it can be tested in slices rather than one
  big rewrite.
- [ ] Decide what happens to `JsonDB` once migration is complete (delete
  `db.js`, or keep as an offline/fallback mode — pick one, don't leave both
  paths live indefinitely).

### Phase 2 — Multi-user readiness (blocked on Open Question 1 above)
- [ ] Stand up the agreed topology (central server, or per-machine + sync).
- [ ] Point `DATABASE_URL` at the real target (not `localhost:5432` — that
  only works for a single machine).
- [ ] Resolve the Redis question (Open Question 3) for the same topology.
- [ ] Revisit auth (Open Question 2) once this is reachable over a network
  instead of just `localhost`.

### Phase 3 — Packaging & launch robustness
- [ ] Fix `start-carbon-erp.bat`'s cleanup: it kills the backend via
  `taskkill /IM node.exe /FI "WINDOWTITLE eq Carbon ERP Backend*"`, but the
  backend is started with `start /B` (no window), so the filter won't match —
  this likely orphans the backend process and leaves port 3001 bound. The
  `.ps1` version tracks the PID properly (`$backendProcess.Id`) and should be
  the one kept/extended; consider retiring the `.bat`.
- [ ] Replace the fixed `Start-Sleep -Seconds 3` / timer-based waits in both
  `launcher.js` and `start-carbon-erp.ps1` with a real readiness poll against
  `/health`, with retries, before launching the Electron shell.
- [ ] Decide whether end-user machines are expected to have Node.js on `PATH`
  (current requirement, via `spawn('node', ...)` in `launcher.js`) — if not,
  bundle a Node runtime or compile the backend to a standalone binary.
- [ ] `desktop/nativefier.json` has `"name": "MoniNvest ERP"` (leftover from
  the Money Invest ERP nativefier config) even though the exe is renamed
  `Carbon ERP.exe` — fix so window title / taskbar / About panel match.
- [ ] Bump Electron off `25.7.0` (EOL) once other changes are stable enough to
  retest against a newer version.
- [ ] Re-enable `asar` packaging for the shipped build once source stops
  needing to be hand-edited in place inside the packaged folder.
- [ ] Resolve `backend/src/` (Open Question 4 category) — wire it in or
  delete it, don't ship unused modularization.

## 5. Notes for whoever picks this up

- The `2026 BVA Report` OneDrive folder holds the **live running build** —
  treat it as production, not a workspace. All future edits happen in this
  repo; rebuild/repackage from here when ready to update that folder.
- `backend/seed-ecoplanet.js` (48KB) is the EcoPlanet-specific seed data
  script — legitimate source, kept in the repo.
- If in doubt about scope for any Phase 3 item, it's lower priority than
  anything in Phase 0/1 — don't let packaging polish block the DB migration.
