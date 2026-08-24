# Carbon ERP

Local-first ERP for the EcoPlanet Bamboo group (EPBSA, SA1, SA2, EcoPlanet Carbon).
Desktop shell is a Nativefier wrapper (`desktop/`) around a local Node/Express
backend (`backend/`) that currently persists to a flat JSON file
(`backend/db.js`), with an in-progress migration to PostgreSQL via Prisma
(`backend/prisma/schema.prisma`, `backend/scripts/migrate-to-pg.js`).

See ROADMAP.md before making changes — it has the current state, known issues,
and the phased plan.

## Run locally
1. `cd backend && npm install`
2. Copy `backend/.env.example` to `backend/.env` and point `DATABASE_URL`
   at a real Postgres instance (or leave unset to fall back to the JSON store
   for now — see ROADMAP.md, Phase 2).
3. From repo root: `.\start-carbon-erp.ps1` (preferred) starts the backend
   and the desktop shell together.
