# RMIT Clinical Data Repository (CDR) — MIMIC-IV on Databricks

A Clinical Data Repository built on the Databricks Data Intelligence Platform for the
RMIT × Databricks × AWS × Cevo capstone. MIMIC-IV is ingested into a Unity Catalog governed
lakehouse using the Medallion Architecture, then exposed to researchers through
analytics-ready Gold tables.

```
MIMIC-IV (AWS S3)
      │  Unity Catalog external volume
      ▼
   BRONZE ── source-aligned Delta tables (31 tables)
      │
      ▼
   SILVER ── typed, validated, flag-carrying clinical entities
      │
      ▼
   GOLD   ── dimensions + facts (star schema) for AI/BI and Genie
      │
      ▼
  Governance (tags, comments, RBAC, masks, row filters) around every layer
```

## Repository layout

| Path | Contents |
|---|---|
| `01_Bronze/Data_Extractor.py` | Single configurable ingestion entry point (`SOURCE_MODE` = `aws_volume` / `demo` / `upload`) |
| `02_Silver/` | One notebook per table (31), plus `00_Silver_Registry`, `00_Silver_Standards`, `99_Silver_Run_All`, `90_Silver_Cross_Table_Validation`, `91_Silver_Metadata_Documentation` |
| `03_Gold/` | `Gold_Build_All.py` (the build that produced the tables in the catalog), `00_Gold_Registry` (build SQL + grain + keys + checks), `01_Gold_Engine`, `99_Gold_Run_All`, `90_Gold_Validation`, `91_Gold_Governance_Snapshot`, `_archive/` (superseded builders) |
| `04_Governance/` | `jobs/` is the live control chain (tag → verify → group → mask → verify RBAC), and it masks Gold **and** Silver; `rbac/role_catalog.json` is the role source of truth; the loose top-level notebooks are reference. Full file map in [`04_Governance/README.md`](04_Governance/README.md) |
| `05_Demo_Genie_Dashboards/` | Deliverable 3: [`genie_space.md`](05_Demo_Genie_Dashboards/genie_space.md) (space settings, the curated 16-table list, and the instruction text that forms the semantic layer) and [`dashboard_spec.md`](05_Demo_Genie_Dashboards/dashboard_spec.md) (nine tiles, each tied to the benchmark query that produces its measure). The space and the dashboards themselves live in the workspace and are **not built yet** |
| `06_Genie_Vocabulary/` | `01_Gold_Dim_Medication_Route.py` (reference vocabulary, source embedded so the table rebuilds without an external dictionary) and `02_Genie_Validation.py` (benchmark runner, Q1–Q14) |
| `mimic_iv_cdr_bundle/` | Databricks Asset Bundle: `databricks.yml` plus one job YAML per layer and an orchestrator. References the notebooks by absolute workspace path — it does not contain them |
| `_archive/` | Superseded files kept for provenance — see [`_archive/README.md`](_archive/README.md) |
| `docs/` | [`gold_governance_checks.md`](docs/gold_governance_checks.md) (source readiness, pre- and post-replacement checks, key changes) and [`gold_build_cost.md`](docs/gold_build_cost.md) (the large-table rule) are the engineering notes the notebooks cite. [`architecture_blueprint.md`](docs/architecture_blueprint.md) is the blueprint: data-flow, infrastructure and Unity Catalog security-boundary diagrams, plus the mapping from the controls to the Australian Privacy Principles |
| `Utils/CleanUp.py` | Destructive cleanup of obsolete schemas/volumes — read the warnings first |

Silver has exactly one source of truth: `02_Silver/00_Silver_Registry.py`. Per-table notebooks hold
no duplicated build logic; the runner and the validation notebooks all read the registry.

Gold has **two build paths**: the builder that produced the catalog, and the harder-gated path it was
meant to grow into.

| Path | Entry point | State |
|---|---|---|
| **The builder** | `03_Gold/Gold_Build_All.py` | **This produced the 27 Gold tables in the catalog**, gated on its own `build_approved` widget. Its header states it replaces the build logic in `Dimensions`, `Facts`, `00_Gold_Registry` / `01_Gold_Engine` and `10_Gold_POE_and_Dimension_Fixes`. It also builds the three POE orders tables that sit outside the registry |
| Gated path (not yet usable) | `03_Gold/00_Gold_Registry` → `01_Gold_Engine` → `99_Gold_Run_All` | **Builds nothing today:** `CANDIDATE_VALIDATION_APPROVED = False` blocks every `scan="large"` table, and `approved_to_publish` / `silver_sources_confirmed` default to `False` for every table. There is deliberately no override parameter |

Four Gold tables sit outside `00_Gold_Registry` and are classified only by `jobs/01_apply_tags`: the
three POE orders tables (`dim_order_type`, `fact_order`, `fact_order_detail`, domain `orders`) and
`dim_medication_route` from `06_Genie_Vocabulary/`.

`poe` and `poe_detail` are **in scope**. They were excluded on 2026-10-19 over a source-provenance
question — `hosp/` holds both `poe.csv.gz` and a 5.1 GB `poe.csv`, and a differently-sized
`poe.csv.gz` sits in `cdr_mimiciv.default.raw` — and were reinstated on 2026-10-26, when the team
accepted the existing Bronze copies as the source of record rather than rebuilding them. The
provenance question itself is still recorded in
[`01_Bronze/Data_Extractor.py`](01_Bronze/Data_Extractor.py); nothing downstream filters or corrects
rows on the strength of that provenance.

## Access control at a glance

`04_Governance/rbac/role_catalog.json` is the single source of truth for roles; the notebooks
read it rather than hard-coding grants.

| Control | Notebook | Enforced by |
|---|---|---|
| Classification tags (`layer`, `type`, `domain`, `mimic_module`, `hipaa_identifiability`, `hl7_confidentiality`) | `jobs/01_apply_tags` | Unity Catalog governed tags |
| Group privileges (`USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`) | `jobs/03_create_groups` | `GRANT … ON CATALOG / SCHEMA / TABLE` |
| Column masks (gold and silver identifiers) | `jobs/01_apply_tags` classifies them, `jobs/05_fine_grained_controls` masks every schema in `masking.masked_schemas` | `ALTER TABLE … ALTER COLUMN … SET MASK` |
| Row filters (RLS levels 1–3) | `jobs/05_fine_grained_controls` | `ALTER TABLE … SET ROW FILTER` |
| Verification | `jobs/02_verify_tags`, `jobs/04_verify_rbac` | a job run goes red on any gap |

The role catalog, the masking postures and the known limits of each control are documented in
[`04_Governance/rbac/README.md`](04_Governance/rbac/README.md); the tag vocabulary is in
[`04_Governance/tags/README.md`](04_Governance/tags/README.md).

## Run order

1. **Bronze** — `01_Bronze/Data_Extractor.py` with `DRY_RUN = True` first, then `False`.
2. **Silver** — `02_Silver/99_Silver_Run_All.py` with `dry_run=true` (preview), then an explicit
   `tables` list with `dry_run=false`.
3. **Silver docs / validation** — `91_Silver_Metadata_Documentation.py`,
   `90_Silver_Cross_Table_Validation.py`, then `04_Governance` metadata notebooks.
4. **Gold** — `03_Gold/Gold_Build_All.py` with `build_approved=true` is the path that builds the
   tables. The registry-gated alternative (`99_Gold_Run_All.py`) prints a plan and refuses to write
   until per-table approvals and `CANDIDATE_VALIDATION_APPROVED` are set. Either way, follow with
   `90_Gold_Validation.py` and `91_Gold_Governance_Snapshot.py` (once before and once after any
   replacement).
5. **Governance** — `04_Governance/jobs/01_apply_tags` → `02_verify_tags` → `03_create_groups` →
   `05_fine_grained_controls` → `04_verify_rbac`. Masks are applied before they are verified, so that
   `04` reads the state `05` just wrote. There is no sixth step: `apply_masks_notebook` was retired on
   2026-10-26 and Silver masking is part of `01` and `05`.

Every notebook that writes takes `mode=dry_run` (print the statements) before `mode=apply`, and
the two verification notebooks take `fail_on_gap` so a run can be made red rather than advisory.

## Gate model

Nothing publishes by accident. Each layer has an explicit gate that a real run must pass:

| Layer | Gate |
|---|---|
| Bronze | `DRY_RUN = True`; create-only writes; `REBUILD` needs `REBUILD_CONFIRM`; `poe`/`poe_detail` excluded |
| Silver | Preview only for `tables=all`; a real run needs an explicit list and `approved_to_publish=True` per table in `00_Silver_Registry` |
| Gold | Explicit list, `approved_to_publish=True` **and** `silver_sources_confirmed=True`, Silver source not `NEEDS_PROFILING`/`DEFERRED`, and `confirm_large_scan=true` for `scan="large"` tables |

Approvals are deliberate, reviewed changes: set `approved_to_publish=True` together with
`approved_by` / `approved_on` in the registry, in a commit of its own.

## Current status

Tracked here so the team and the assessor see the same picture as the code.

| Item | Status |
|---|---|
| Bronze ingestion | Implemented for all 31 MIMIC-IV tables, gated by `DRY_RUN`; nothing is excluded |
| Silver | 31 per-table notebooks; 30 registered in `00_Silver_Registry` — of those, 5 are `NEEDS_PROFILING` (`services`, `labevents`, `microbiologyevents`, `omr`, `provider`) and `chartevents` is deferred because its Bronze table does not exist. `poe` / `poe_detail` were reinstated on 2026-10-26 |
| Gold | 24 tables in `00_Gold_Registry` (**13 dimensions, 11 facts**); `Gold_Build_All` builds 27 (adds the 3 POE orders tables); with `dim_medication_route` that is 28 in the catalog, all classified by `01_apply_tags`. `CANDIDATE_VALIDATION_APPROVED` still blocks the `scan="large"` builds on the registry path |
| Governance controls | Tags, groups, grants, column masks (gold **and silver**) and row filters implemented across the five notebooks; offline harness at **154/154** checks |
| Governance job | `jobs/governance_job.json` chains tagging (`01`) and verification (`02`); the access-control notebooks (`03`–`05`) are run individually today. The bundle's orchestrator chains neither: its only governance task calls `01_apply_tags` without parameters, so it runs at the `dry_run` default |
| Genie / AI-BI deliverable | Partly evidenced: `06_Genie_Vocabulary/02_Genie_Validation.py` runs the Q1–Q14 benchmark but its scoring sheet is blank, and the space and dashboards are specified in `05_Demo_Genie_Dashboards/` but not yet created in the workspace |
| `docs/` | Three pages, all written, every citation resolving. The two engineering notes fold in six earlier documents that were cited but never written; the blueprint adds the diagrams and the privacy mapping |
| Git history | Layer-order commits: Bronze → Silver → Gold → Governance → Genie, then the repo docs and bundle |
| Workspace sync | Was verified identical to this repository on 2026-10-26, then **three governance notebooks changed again** the same day when Silver masking moved into `jobs/01` and `jobs/05`: `jobs/01_apply_tags`, `jobs/04_verify_rbac`, `jobs/05_fine_grained_controls`, plus `rbac/role_catalog.json`. Those four need re-importing, and `04_Governance/apply_masks_notebook` has to be deleted from the workspace. See [`HANDOVER.md`](HANDOVER.md) §5 |

## Offline test suite

The governance notebooks only need a cluster at run time; the harness stubs `spark`, `dbutils`
and `display`, executes the notebook files exactly as Databricks would, and asserts on the SQL
they produce:

```bash
python3 04_Governance/test/test_governance_offline.py
```

149 checks, all passing. Nothing else in the repo is covered by an offline test.

## Conventions

- **No cleaning in Gold.** Unusual source values are carried through; Silver `dq_*` flags travel
  with them. Durations are never clamped.
- **Flag, don't fix.** Where a value is clinically questionable, add a `dq_*` boolean rather than
  correcting or deleting the row.
- **Surrogate keys** are `xxhash64` over one length-prefixed, NULL-safe encoded string, so field
  boundaries and NULLs stay distinguishable. Uniqueness is validated, never assumed.
- **Nothing is dropped.** Publication is `CREATE OR REPLACE` / create-only, with Delta time travel
  as the way back. `Utils/CleanUp.py` is the only destructive notebook.
- **Identifiers keep their Silver types** in Gold; governance functions that depend on a type are
  documented separately.

## Outstanding work

Both decisions that were open — whether the POE orders tables are in the trusted pipeline, and which
Gold build path is the real one — were settled on 2026-10-26 and are recorded in
[`HANDOVER.md`](HANDOVER.md). What remains:

1. **The Genie deliverable.** Create the space from
   [`05_Demo_Genie_Dashboards/genie_space.md`](05_Demo_Genie_Dashboards/genie_space.md), run
   `06_Genie_Vocabulary/02_Genie_Validation.py`, fill in its scoring sheet, then build the nine tiles
   in [`dashboard_spec.md`](05_Demo_Genie_Dashboards/dashboard_spec.md) as a researcher identity
   rather than as an admin.
2. **Apply the Silver masks.** They are implemented and tested — `jobs/01` tags the Silver identifier
   columns and `jobs/05` masks them — but they have not been applied to the live catalog yet, so Silver
   still returns raw identifiers to anyone holding `USE SCHEMA` + `SELECT`. Running the chain with
   `05` in `apply` mode closes that. Note the live grants that make it matter: five groups the catalog
   no longer provisions (`cdr_cmo_admin` among them) still hold Silver and Bronze `SELECT`.
3. **The bundle.** `mimic_iv_cdr_bundle/` records intended orchestration and is not deployed. Its
   layer tasks default to `dry_run` and it chains no governance verification.
4. **Commit the documentation round.** The changes listed in
   [`HANDOVER.md`](HANDOVER.md) §5 are written and the workspace is in sync, but these files are
   still uncommitted in git.
