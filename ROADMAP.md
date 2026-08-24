# Carbon ERP — Roadmap & Handoff Notes

Written after a build audit of the packaged `Carbon ERP-win32-x64` output
(originally living at `OneDrive\Desktop\2026 BVA Report\...`, still there and
still live — **do not modify or delete anything in that folder**, it's the
running copy). This repo is a clean source import from that build, with
`node_modules/`, `backend/data/` (live + backup JSON data), and `.env`
(real DB credentials) deliberately excluded. See `.gitignore`.

Owner: Luca. Read this fully before changing anything.

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
- Auth: not JWT despite `jsonwebtoken` sitting in `package.json` — actual
  implementation is custom: `pbkdf2Sync` password hashing with a random
  per-user salt (`hashPassword`/`verifyPassword`, server.js ~L92) and a
  `crypto.randomBytes(32)` bearer token stored in a `sessions` table with a
  7-day expiry, checked via the `auth` middleware (server.js ~L216). The
  crypto choices here are sound. What's missing: no rate limiting on
  `/api/auth/login`, and CORS is wide open (`Access-Control-Allow-Origin: '*'`,
  server.js L72) rather than restricted to the real app origin.
- `backend/src/{controllers,middleware,routes,services,workers}` exists but
  `server.js` doesn't `require` any of it — looks like an abandoned
  modularization pass. Confirm before deleting; don't delete unilaterally.
- **Dependency audit**: grepped `server.js` for every require of the declared
  `backend/package.json` dependencies. `express`, `multer`, `ws`, `xlsx` are
  genuinely used. `bcryptjs`, `better-sqlite3`, `bullmq`, `cors`, `ioredis`,
  `jsonwebtoken` — **6 of 10 dependencies — have zero references anywhere in
  server.js** (the only file anything actually runs from). Treat these as
  dead until someone confirms otherwise via the `src/` folder or intentional
  future use; don't design infrastructure (e.g. Redis) around them.

## 2. Decisions already made (by Luca)

- **DB direction**: finish the Postgres/Prisma migration. Not staying on
  `JsonDB` long-term.
- **Distribution target**: other EcoPlanet staff, not just Luca's machine.
- **Topology**: one central server, not per-machine local backends.
- **Hosting**: a new cloud VPS (Linux).
- **Access**: public internet, TLS-terminated.

## 3. Open questions — remaining, resolved, and newly found

**Resolved by investigation:**
- ~~Redis needed?~~ No — `ioredis`/`bullmq` are unused dead dependencies (see
  §1). Deploy scaffolding (§5) has no Redis service. Revisit only if
  background jobs get built for real.
- ~~`better-sqlite3` in use?~~ No — confirmed dead the same way.

**Resolved -- 2026-08-24, this session:**
- **Rate limiting on `/api/auth/login`**: added via `express-rate-limit`
  (10 attempts / 15 min / IP, 429 with a clear error past that). Tested
  directly -- the 11th/12th rapid attempt returned 429.
- **Wildcard CORS**: replaced with an `ALLOWED_ORIGINS` env-driven allowlist
  (comma-separated, defaults to `http://localhost:3001` for local dev).
  Tested directly -- a disallowed `Origin` gets no
  `Access-Control-Allow-Origin` header back; an allowed one gets it
  reflected correctly, with `Vary: Origin` set. Wired into
  `deploy/docker-compose.yml` and both `.env.example` files. Still needs
  the real domain added once chosen (item 2 below) -- only localhost is
  allowlisted right now.
- Further auth hardening (account lockout, "log out everywhere" on password
  change) intentionally not done -- scope was the two findings above only.
  Revisit if Luca wants more before going live.

**Still open -- need an answer before Phase 2 finishes:**
1. **VPS provider and sizing.** "A new cloud VPS" was chosen but not which
   provider or spec. Does not block writing the deploy scaffolding (done,
   Section 5), but blocks actually standing it up.
2. **Domain name.** `deploy/Caddyfile` has a placeholder
   (`erp.REPLACE-ME.example.com`) -- needs a real registered domain pointed at
   the VPS's IP before Caddy can issue a TLS cert, and needs to be added to
   `ALLOWED_ORIGINS` (both `.env.example` files) once chosen.
3. **Dependency cleanup.** Drop the 6 dead dependencies from
   `backend/package.json`, or is any of them slated for real use soon (e.g.
   `jsonwebtoken` if auth gets redone as real JWT)? Do not remove silently --
   ask first, since it is a design choice, not just cleanup.

## 4. Phased plan

### Phase 0 — Stop the bleeding (do first, independent of everything else)
- [ ] Confirm the live `.env` (real `DATABASE_URL`) and `backend/data/*.json*`
  are never included in any future build/zip/share of this app — they were
  found shipped inside the distributable in the OneDrive-synced folder.
- [ ] Rotate/replace any credential that was in that shipped `.env` once the
  real VPS Postgres instance exists (§5).
- [ ] Add `.gitignore`-equivalent rules to whatever packaging script produces
  the `-win32-x64` build — this repo's `.gitignore` doesn't protect that
  build output folder, which still exists untouched.

### Phase 1 — Finish the Postgres/Prisma migration
- [ ] Extend `schema.prisma` to cover the full `JsonDB` schema (currently 4 of
  ~90 tables modeled).
- [ ] Extend `migrate-to-pg.js` to migrate the remaining tables, or write a
  generic migrator driven by the `JsonDB` schema list in `db.js`.
- [ ] Swap `server.js`'s `require('./db')` usage for the Prisma client,
  incrementally per route/table so it can be tested in slices rather than one
  big rewrite.
- [ ] Decide what happens to `JsonDB` once migration is complete (delete
  `db.js`, or keep as an offline/fallback mode — pick one).

### Phase 2 — Central server deployment (topology decided; provider/domain still open)
- [ ] `deploy/docker-compose.yml` + `deploy/Caddyfile` + `backend/Dockerfile`
  are scaffolded in this repo already — Postgres + backend + Caddy
  (auto-TLS), no Redis (see §3). Review before using.
- [ ] Pick VPS provider, provision, install Docker.
- [ ] Register/point a real domain, swap it into `deploy/Caddyfile`.
- [ ] Copy `deploy/.env.example` → `deploy/.env` on the VPS with a real
  `POSTGRES_PASSWORD`, `docker compose up -d`.
- [x] Security hardening for public exposure: rate-limit login (done — see §3),
  restrict CORS from `*` to `ALLOWED_ORIGINS` (done — see §3, still needs the
  real domain added once chosen).
- [ ] Repoint `desktop/nativefier.json`'s `targetUrl` from
  `http://localhost:3001/...` to the real `https://` domain, rebuild the
  Nativefier shell, and re-distribute to staff. Once staff hit the central
  server, `launcher.js`/`start-carbon-erp.*` (which spawn a local backend)
  are no longer needed on their machines — only on Luca's for local dev.

### Phase 3 — Packaging & launch robustness (local/dev use only, post Phase 2)
- [ ] Fix `start-carbon-erp.bat`'s cleanup: it kills the backend via
  `taskkill /IM node.exe /FI "WINDOWTITLE eq Carbon ERP Backend*"`, but the
  backend is started with `start /B` (no window), so the filter won't match —
  this likely orphans the backend process and leaves port 3001 bound. The
  `.ps1` version tracks the PID properly and should be the one kept/extended;
  consider retiring the `.bat`.
- [ ] Replace the fixed `Start-Sleep -Seconds 3` / timer-based waits in both
  `launcher.js` and `start-carbon-erp.ps1` with a real readiness poll against
  `/health`, with retries.
- [ ] `desktop/nativefier.json` has `"name": "MoniNvest ERP"` (leftover from
  the Money Invest ERP nativefier config) even though the exe is renamed
  `Carbon ERP.exe` — fix so window title / taskbar / About panel match.
- [ ] Bump Electron off `25.7.0` (EOL) once other changes are stable enough to
  retest against a newer version.
- [ ] Re-enable `asar` packaging for the shipped build.
- [ ] Resolve `backend/src/` (§3, item 4 territory) — wire it in or delete it.

## 5. Deploy scaffolding added in this repo

- `backend/Dockerfile` — plain Node 20 alpine image, `npm ci --omit=dev`,
  runs `server.js`. Has a commented-out `prisma generate` line for once
  Phase 1 lands.
- `deploy/docker-compose.yml` — `postgres` + `backend` + `caddy`. Postgres and
  backend are not published to the host, only Caddy exposes 80/443.
- `deploy/Caddyfile` — reverse proxy with automatic Let's Encrypt TLS once a
  real domain replaces the placeholder.
- `deploy/.env.example` — copy to `deploy/.env` on the VPS, fill in
  `POSTGRES_PASSWORD`, never commit the real one.

## 6. Notes for whoever picks this up

- The `2026 BVA Report` OneDrive folder holds the **live running build** —
  treat it as production, not a workspace. All future edits happen in this
  repo; rebuild/repackage from here when ready to update that folder.
- `backend/seed-ecoplanet.js` (48KB) is the EcoPlanet-specific seed data
  script — legitimate source, kept in the repo.
- If in doubt about scope for any Phase 3 item, it's lower priority than
  anything in Phase 0/1/2 — don't let packaging polish block the migration
  or the deployment.
