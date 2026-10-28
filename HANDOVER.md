# Handover guide

Written 2026-10-26. This is the companion to [`README.md`](README.md): the README describes what the
system *is*, this file describes what is **true right now**, which decisions were taken and why, and
what a new owner has to do next.

Everything below was checked against the files on disk and the live catalog, not copied from other
documents. Where something is an estimate or an assumption, it says so.

---

## 1. Verified state

| Area | State | Evidence |
|---|---|---|
| Offline test suite | **149/149 pass** | `python3 04_Governance/test/test_governance_offline.py` |
| Python syntax | Every `.py` compiles (the one exception found has been fixed) | `python3 -m py_compile` over all files |
| Bundle YAML | All 5 files parse | `YAML.load_file` on `mimic_iv_cdr_bundle/**/*.yml` |
| Tags | Applied: **335 statements, 0 errors**, all three layers | `jobs/01_apply_tags` with `mode=apply` |
| Tag verification | 0 gaps — schema, table and column tags all present, overrides at their override values | `jobs/02_verify_tags` with `fail_on_gap=true` |
| Column masks | Live on the Gold identifiers; `gold.dim_patient.gender` and `anchor_age` are **exempt and unmasked**, as decided | `information_schema.column_masks`; `jobs/05` prints both as `EXEMPT` |
| Row filters | Three functions, `rls.id_anchored_filters` in the catalog: `rls_service` on `gold.dim_service(service)`, `rls_hadm_service_filter_int` / `_bigint` on the 11 Gold facts carrying `hadm_id`, `rls_subject_service_filter_int` on `gold.dim_patient(subject_id)` | `information_schema.row_filters` |
| Row-filter coverage | Every fact is now filtered, plus `dim_service` and `dim_patient`. `fact_service` is deliberately **excluded** — the admission filter reads it, and a filter on the table it reads recurses. The 14 remaining Gold tables are dimensions with no `hadm_id`/`subject_id`, reported as a note by `jobs/04` | `jobs/05` output |
| Subject filter snapshot | `rls_subject_service_filter` reads `rbac.subject_service_map`, a physical table, because a function that joins `dim_patient` recurses and a view would inherit the filter. **It is stale after any Gold rebuild** — re-run `jobs/05` | `jobs/05` output, `rbac` schema |
| Silver masks | Now implemented in the tag-driven chain (`jobs/01` tags the Silver identifier columns, `jobs/05` masks gold **and** silver) but **not yet applied** — Silver still returns raw identifiers to anyone holding `USE SCHEMA` + `SELECT` | `jobs/01`, `jobs/05`, harness `154/154`; run order step 4 |
| POE / orders | **In scope** as of 2026-10-26 (see §2) | `01_Bronze/Data_Extractor.py`, both registries |
| Workspace sync | The four Silver-masking files (`jobs/01`, `jobs/04`, `jobs/05`, `rbac/role_catalog.json`) were re-imported and diffed identical after the 2026-10-26 change. `rbac/role_catalog.json` changed **again on 2026-10-27** (the role set is now six, one per control — §2) and needs one more import; that is the only outstanding sync | `databricks workspace export-dir` plus a line-by-line diff — see §5 |
| Git | Six commits, in layer order; the 2026-10-26 documentation changes are written but uncommitted | `git log`, `git status` |
| `05_Demo_Genie_Dashboards/` | Specified, not built: the space settings, the instruction text and nine dashboard tiles are written. The folder exists in the workspace but is empty | `ls` |

`jobs/05` masks both Gold and Silver from the same tag taxonomy; what keeps the two layers'
treatment deliberate rather than identical is `masking.masked_schemas` (both), `deferred_columns`
(Gold defers the `hadm_id` / `stay_id` join path, Silver defers nothing) and the fact that only Gold
carries a cohort column, so only Gold has row filters.

---

## 2. Decisions taken on 2026-10-26

### The POE orders tables are in the trusted pipeline

They were excluded on 2026-10-19 because the source file could not be pinned down: `hosp/` holds both
`poe.csv.gz` and a 5.1 GB `poe.csv`, and a differently-sized `poe.csv.gz` exists in
`cdr_mimiciv.default.raw`. Meanwhile the Silver POE notebooks had been re-profiled on 2026-10-20 and
said the tables were "in scope", and `Gold_Build_All` already built the three orders tables from
them — so the code disagreed with itself.

**Decision:** the existing Bronze copies `cdr_mimiciv.bronze.poe` and `cdr_mimiciv.bronze.poe_detail`
are accepted as the source of record. **No rebuild**, so the ambiguity is accepted rather than
resolved, and the note recording it stays in [`01_Bronze/Data_Extractor.py`](01_Bronze/Data_Extractor.py).
Nothing downstream filters or corrects rows on the strength of that provenance.

What changed: `EXCLUDED_TABLES` in Bronze, `EXCLUDED` in the Silver registry and `EXCLUDED_SOURCES`
in the Gold registry are now empty sets (the mechanisms are kept, the sets are not). `poe` and
`poe_detail` are registered in `00_Silver_Registry` like any other table, and the README, both job
YAMLs and the runner's header say all 31 tables are in scope.

**Still to confirm:** the readiness axes on the two new registry entries are a first cut
(`readiness="NEEDS_REVIEW"`, `semantics_reviewed="partial"`, `profiled="yes"`,
`approved_to_publish=False`, and four `dq_*` flags for `poe`). Review them against the notebooks
before a real Silver run. Also note that `05_poe`'s header records a **publish blocker** as of
2026-10-20: the existing `cdr_mimiciv.silver.poe` (written 2026-10-13, every column STRING) failed to
read with `UC_DEPENDENCY_DOES_NOT_EXIST`. Confirm that is resolved before rebuilding the orders
tables.

### Silver identifier masking moves into the tag-driven control chain

The Silver masker (`apply_masks_notebook`) had never run, and could not run. It required a
`cdr_mimiciv.rbac.data_classification` table that does not exist in the catalog, and it derived its
mask names (`mask_ctrl_*` / `mask_cres_*`) from `proposed_handling` values (`Controlled`,
`Controlled Research`) that the platform does not use. The functions that do exist — `mask_direct_*`,
`mask_quasi_*`, `mask_hl7_vr_*` — come from the governed tag taxonomy, so the two halves of that
notebook were written against different worlds.

**Decision:** delete the second mechanism rather than repair it. `jobs/01_apply_tags` now tags the
Silver identifier columns by name — `subject_id`, `provider_id` and `caregiver_id` as direct
identifiers, `hadm_id` and `stay_id` as quasi identifiers, and `poe_id` as a direct identifier
(because MIMIC-IV builds it as `<subject_id>-<sequence>`, so it re-identifies the patient even when
`subject_id` is masked). `jobs/05_fine_grained_controls` masks every schema listed in the new
`masking.masked_schemas` key, and `masking.deferred_columns` became a per-schema map so Gold can keep
deferring the `hadm_id` / `stay_id` join path while Silver defers nothing. Row filters stay gold-only;
only gold has a cohort column. `apply_masks_notebook` is archived, not deleted, so the intent survives
as a record.

One consequence to hold on to: a mask resolves its posture **per calling identity**, so the
Silver-to-Gold build must run as `cdr_admins` — the only role with `MODIFY`. Run it as a masked role
and `subject_id` arrives as NULL, which would silently change hash-based surrogate keys instead of
failing loudly.

Two things this does *not* fix. Both are recorded rather than quietly accepted:

- **Dropped roles still hold Silver.** `cdr_cmo_admin`, `cdr_data_engineers` and three others are no
  longer in the role catalog, but the live catalog still grants them `SELECT` and `USE_SCHEMA` on
  **silver and bronze** as well as gold. Before this change that meant a role whose policy says
  `masked` could read raw `subject_id` one schema down. The masks now cover them; the grants are still
  drift, and revoking them is §3 item 2.
- **Silver `patients.dod` carries no tag**, so no mask, while Gold's `deathtime` is
  `V_Very_Restricted`. Death dates are treated differently in the two layers. Left alone here because
  deciding it is a policy call, not a bug fix.

### `Gold_Build_All.py` is the Gold builder

| Path | Entry point | Role |
|---|---|---|
| **Builder** | `03_Gold/Gold_Build_All.py` | Produced the 27 Gold tables in the catalog, including the 3 POE orders tables outside the registry. Gated on its own `build_approved` widget |
| Gated path | `00_Gold_Registry` → `01_Gold_Engine` → `99_Gold_Run_All` | The registry-driven path. Builds nothing today: `CANDIDATE_VALIDATION_APPROVED = False` blocks every `scan="large"` table, and per-table `approved_to_publish` / `silver_sources_confirmed` default to `False`. Making it usable needs the reviewed candidate-table validation described in [`docs/gold_build_cost.md`](docs/gold_build_cost.md), which is real work |

The README, the Gold registry header and the bundle's Gold job now say which is which, so a reader
cannot mistake the gated path for the one that built the data.

### The role catalog is trimmed to one role per demonstrated control

`roles.pdf` specifies eleven groups and the catalog provisions six, because the deliverable asks for
RBAC that can be *shown*, and ten near-identical roles show nothing:

| Group | Demonstrates |
|---|---|
| `cdr_admins` | schema level — all three layers, `SELECT` + `MODIFY`, unmasked |
| `cdr_standard_researcher` | column level — the whole `gold` schema, identifiers still arrive `NULL` |
| `cdr_researcher_psychiatry` | row level — identical grants to the line above, PSYCH rows appear |
| `cdr_pharmacy_analyst` | table level — no schema `SELECT`, only the tagged pharmacy + demographics tables |
| `cdr_external_researcher` | table level, narrow — diagnostics only, identifiers redacted rather than `NULL` |
| `cdr_mortality_investigator` | confidentiality policy — the only non-admin role with `V_Very_Restricted` unmasked |

`cdr_mortality_investigator` also moved from RLS level 1 to 3: at level 1 any mortality rate it
computed had a denominator with PSYCH admissions removed — a biased result produced by the access
policy rather than by the analysis.

**A consequence that needs action, not a footnote:** `03_create_groups` never revokes. The five
dropped PDF groups (`cdr_data_engineers`, `cdr_cmo_admin`, `cdr_researcher_peds`,
`cdr_hospital_researchers`, `cdr_bi_developer`) keep every grant they hold until someone revokes it,
so the catalog says six while the metastore can still say eleven. `04_verify_rbac` prints each one as
"group is not in the role catalog"; that list is the revoke checklist.

Two smaller consequences, accepted: pipelines now run as `cdr_admins` because there is no separate
engineering identity left, and there is no neonatal cohort, so `PEDS` / `NEONATAL` / `NICU` services
are standard-cohort rows visible at every level. Only psychiatry is restricted.

---

## 3. Remaining work, in order

The workspace re-import is **complete** (see §5), so nothing below waits on getting a notebook into
Databricks any more. The list starts at the governance control that is still missing.

1. **Re-import the role catalog, then run the chain.** The Silver-masking notebooks are already in the
   workspace and diffed identical; `rbac/role_catalog.json` changed **again on 2026-10-27** (now six
   roles, one per control — §2), so it needs one more import. Then run §4 with `05`
   in `apply` mode: `05` has to re-create the mask functions and `rls_service` for the new catalog to
   take effect, and `03` has to create the two groups. Until that run, Silver still hands out raw
   identifiers and both new roles exist only on paper.
2. **Revoke the five dropped groups.** `cdr_data_engineers`, `cdr_cmo_admin`, `cdr_researcher_peds`,
   `cdr_hospital_researchers` and `cdr_bi_developer` still hold live grants, and `04_verify_rbac`
   flags each as "not in the role catalog". Revoke their catalog, schema and table grants, delete the
   groups, and re-run `04` until the only notes left are the declared limitations. Delete the two
   misspelled groups too if they still exist.
3. **Chain `03`, `04` and `05` into `jobs/governance_job.json`** with verification tasks and
   `fail_on_gap=true`, so one run produces the whole evidence trail.
4. **The Genie deliverable.** Create the space from
   [`05_Demo_Genie_Dashboards/genie_space.md`](05_Demo_Genie_Dashboards/genie_space.md) — start with
   the curated 16 tables and paste the instruction block. Then run
   `06_Genie_Vocabulary/02_Genie_Validation.py` as an admin on Serverless and fill the Q1–Q14
   scoring sheet with the approved result, Genie's result, the verdict, the failure cause and the
   fix. Its SQL is syntax-checked but has never been run against the data, so expect to fix a column
   or table name — correct those in the benchmark *and* in the tile that cites it. Finally build the
   nine tiles in [`dashboard_spec.md`](05_Demo_Genie_Dashboards/dashboard_spec.md) as
   `cdr_standard_researcher`, not as an admin, and record the identity per tile.
5. **Decide the bundle.** `mimic_iv_cdr_bundle/` is not deployed and currently serves as a record of
   intended orchestration. If it is ever deployed: flip the two `dry_run: "true"` layer defaults,
   chain the governance verification, and repoint `alert_email` and prod `run_as` away from the single
   student address (flagged with a TODO in `databricks.yml`).
6. **Consider the `docs/` cut.** Eight files are cited by the notebooks; two are now written
   ([`docs/gold_governance_checks.md`](docs/gold_governance_checks.md),
   [`docs/gold_build_cost.md`](docs/gold_build_cost.md)) and the other six topics are folded into
   them. Any reference that still names a file that does not exist is a losing end.

---

## 4. Run order for the control chain

| Step | Notebook | Mode | Expected evidence |
|---|---|---|---|
| 1 | `jobs/01_apply_tags` | `apply` | `applied=335 errors=0` |
| 2 | `jobs/02_verify_tags` | `fail_on_gap=true` | `0 gaps` |
| 3 | `jobs/03_create_groups` | `apply` | every role granted; run **after** tags exist, because domain-scoped grants are discovered from the `domain` tag |
| 4 | `jobs/05_fine_grained_controls` | `rollback` then `apply` | `EXEMPT gold.dim_patient.gender`, `EXEMPT …anchor_age`, Silver identifiers masked, `errors=0` |
| 5 | `jobs/04_verify_rbac` | `fail_on_gap=false`, then `true` | `gaps=0`, with `notes=` for the declared limitations |
| 6 | `test/governance_test_notebook` | run as several identities | counts per role, before and after masking |

Notes that cost time if you learn them the hard way:

- **Governed tags validate their values.** `layer`, `type`, `domain`, `mimic_module`,
  `hipaa_identifiability` and `hl7_confidentiality` must be registered with their allowed values in
  Catalog → `cdr_mimiciv` → Governed tags. A value that is not registered fails the statement; the
  code cannot add one.
- **`jobs/04` needs the account groups to exist.** Row filters and masks use
  `is_account_group_member()`, so workspace-local groups of the same name fail closed.
- **`jobs/05` rollback does not clear exempt columns.** Rollback drops masks only for its current
  targets, so a mask on `gender` or `anchor_age` from an older run survives both rollback and apply.
  Check `information_schema.column_masks` for `dim_patient` before a first apply.
- `04_Governance/jobs/governance_job.json` uses `/Shared/RMIT_CDR_Capstone/…` while the notebooks'
  `catalog_path` widget uses `/Workspace/Shared/RMIT_CDR_Capstone/…`. **Confirmed on this workspace on
  2026-10-26:** both paths list the same object IDs, so the two forms are interchangeable and the job
  can be imported as written.

---

## 5. Workspace import — verified complete

Repository root in the workspace: `/Workspace/Shared/RMIT_CDR_Capstone/` (see §4 on the `/Shared`
short form).

The import was performed through the Databricks CLI and then **verified by re-exporting the whole
folder and diffing it line by line against this repository**. All 13 notebooks below came back
identical. If the repository changes again, re-run that check rather than trusting this table:

```bash
databricks workspace export-dir /Workspace/Shared/RMIT_CDR_Capstone <tmp-dir> --profile <profile>
```

### The 13 notebooks imported

**Four whose behaviour changed.** Until these were imported the workspace still treated `poe` and
`poe_detail` as excluded, and one notebook was not valid Python:

| Local file | Workspace path (under the repository root) | What changed |
|---|---|---|
| [`01_Bronze/Data_Extractor.py`](01_Bronze/Data_Extractor.py) | `01_Bronze/Data_Extractor` | `EXCLUDED_TABLES = {}` and `EXCLUSION_REASON` removed — 11 code lines |
| [`02_Silver/00_Silver_Registry.py`](02_Silver/00_Silver_Registry.py) | `02_Silver/00_Silver_Registry` | `EXCLUDED = {}`; `poe` and `poe_detail` registered — 9 code lines. **Review the axes before a real run** — see §2 |
| [`03_Gold/00_Gold_Registry.py`](03_Gold/00_Gold_Registry.py) | `03_Gold/00_Gold_Registry` | `EXCLUDED_SOURCES = {}` plus two doc-path corrections — 9 code lines |
| [`02_Silver/90_ICU_Silver_Quality_Review.py`](02_Silver/90_ICU_Silver_Quality_Review.py) | `02_Silver/90_ICU_Silver_Quality_Review` | **Was not valid Python:** every one of its 103 lines was prefixed `--` instead of `#`. Repaired, and it now compiles |

**Seven whose comments changed and whose code did not** — zero executable lines in any of them. Each
repoints a citation from `docs/silver_source_file_evidence.md` (cited but never written) to
`docs/gold_governance_checks.md`:

`02_Silver/99_Silver_Run_All` · `02_Silver/01_Identity_Encounter/01_Patients` ·
`02_Silver/01_Identity_Encounter/02_admissions` ·
`02_Silver/02_Diagnoses_Procedures/02_diagnoses_icd` ·
`02_Silver/02_Diagnoses_Procedures/04_procedures_icd` ·
`02_Silver/03_Labs_Observations/01_d_labitems` · `02_Silver/04_Medication_Orders/04_emar_details`

**Two created under corrected names:**

| Local file | Workspace path | Formerly called |
|---|---|---|
| [`06_Genie_Vocabulary/01_Gold_Dim_Medication_Route.py`](06_Genie_Vocabulary/01_Gold_Dim_Medication_Route.py) | `06_Genie_Vocabulary/01_Gold_Dim_Medication_Route` | `01_Gold_Dim_Medication_Route.py` |
| [`06_Genie_Vocabulary/02_Genie_Validation.py`](06_Genie_Vocabulary/02_Genie_Validation.py) | `06_Genie_Vocabulary/02_Genie_Validation` | `Genie Validation` |

Note the naming trap in the first of those: the old object's name **ended in `.py`**. The notebook
was literally called `01_Gold_Dim_Medication_Route.py`, so exporting the directory produced a file
named `.py.py`. The double extension was an artefact of the exporter, not the name of the object —
anyone auditing the workspace by filename should expect one `.py` there, not two.

### Deleted — confirmed gone

| Object | Why |
|---|---|
| `06_Genie_Vocabulary/01_Gold_Dim_Medication_Route.py` | Superseded by the correctly-named copy above |
| `06_Genie_Vocabulary/Genie Validation` | Superseded by `02_Genie_Validation` |
| `outputevents` (workspace root) | A one-table Bronze comment script, superseded by `04_Governance/02_Catalog_Metadata_and_Tags.py`. Archived as [`_archive/outputevents_bronze_comments.py`](_archive/outputevents_bronze_comments.py) |
| `04_Governance/.DS_Store` | Editor junk |

A re-export after these deletions found **no object in the workspace that is absent from this
repository**.

### The control chain was untouched

The five governance notebooks (`jobs/01`–`jobs/05`) and `apply_masks_notebook` came back
**byte-identical**, which confirms that the tag, RBAC, mask and row-filter runs recorded in §1 were
all made against the current code. Nothing needs re-importing for them.

### Left out of sync deliberately

- `04_Governance/test/test_governance_offline.py` differs from the repository copy, but it never runs
  in Databricks — it is the laptop harness described in the README.
- The **five `mimic_iv_cdr_bundle/` YAMLs** differ, and are left alone because the bundle is not
  deployed. Sync them if it is ever deployed.

### Changed after the verified import — re-import these four

The Silver-masking change (§2) landed after the import above was verified, so four files are now ahead
of the workspace:

| Local file | Workspace path | What changed |
|---|---|---|
| [`jobs/01_apply_tags.py`](04_Governance/jobs/01_apply_tags.py) | `04_Governance/jobs/01_apply_tags` | Adds the Silver identifier column pass |
| [`jobs/05_fine_grained_controls.py`](04_Governance/jobs/05_fine_grained_controls.py) | `04_Governance/jobs/05_fine_grained_controls` | Masks every schema in `masking.masked_schemas`, not just gold |
| [`jobs/04_verify_rbac.py`](04_Governance/jobs/04_verify_rbac.py) | `04_Governance/jobs/04_verify_rbac` | Verifies masks across both schemas |
| [`rbac/role_catalog.json`](04_Governance/rbac/role_catalog.json) | `04_Governance/rbac/role_catalog.json` | `masked_schemas` added; `deferred_columns` is now per schema; `exempt_columns` entries name their schema. Changed again 2026-10-27: trimmed to six roles, one per demonstrated control; mortality at RLS level 3 |

**And delete `04_Governance/apply_masks_notebook` from the workspace** — it is retired and now lives in
[`_archive/`](_archive/apply_masks_notebook.py). Re-import the role catalog *before* running the chain:
`05` and `04` read it at run time and will fail if it lacks the new keys.

### Documentation is repo-only

Twelve files exist in the repository but not in the workspace. Nothing breaks without them, but seven
of the notebooks above now cite `docs/gold_governance_checks.md` and that path does not exist in the
workspace, so the citation dangles for anyone reading the notebook in Databricks. Importing the three
`docs/` files is the cheapest fix.

| File | What it is |
|---|---|
| [`HANDOVER.md`](HANDOVER.md) | This file |
| [`docs/architecture_blueprint.md`](docs/architecture_blueprint.md) | The blueprint: data-flow, medallion-gate, infrastructure and Unity Catalog security-boundary diagrams, the control chain, the enforced-versus-declared table, and the mapping from the controls to the Australian Privacy Principles |
| [`05_Demo_Genie_Dashboards/genie_space.md`](05_Demo_Genie_Dashboards/genie_space.md) | How to recreate the Genie space: settings, the curated table list, and the instruction text that forms the semantic layer |
| [`05_Demo_Genie_Dashboards/dashboard_spec.md`](05_Demo_Genie_Dashboards/dashboard_spec.md) | The nine dashboard tiles, each tied to the benchmark query that produces its measure, and the captions each one must carry |
| [`docs/gold_governance_checks.md`](docs/gold_governance_checks.md) | Source readiness, the checks to run before and after any Gold replacement, and why `CREATE OR REPLACE` is the open question |
| [`docs/gold_build_cost.md`](docs/gold_build_cost.md) | What "large" costs: the `scan="large"` tables, why each is never staged, and what the dry run reports |
| [`04_Governance/README.md`](04_Governance/README.md) | File map: which notebooks are the live control chain, which are reference, and the runtime prerequisites |
| [`_archive/README.md`](_archive/README.md) | What is quarantined and what superseded it |
| [`README.md`](README.md) | Corrected counts, layout and status, and the two Gold build paths |
| [`04_Governance/tags/README.md`](04_Governance/tags/README.md) | The tag taxonomy and the governed-tag policy that validates tag values |
| [`05_Demo_Genie_Dashboards/README.md`](05_Demo_Genie_Dashboards/README.md) | Index for deliverable 3 |
| [`_archive/outputevents_bronze_comments.py`](_archive/outputevents_bronze_comments.py) | The superseded Bronze comment script, kept for provenance |

---

## 6. Evidence to collect for the report

Screenshots or saved outputs of: the `02_verify_tags` summary tables (tag counts by name and value),
the `04_verify_rbac` final `N gaps, M notes` block, `05`'s mask list, the identity test
(`test/governance_test_notebook`) before and after masking, the Q1–Q14 scoring sheet, and the two
`91_Gold_Governance_Snapshot` outputs (before and after a Gold replacement) with the comparison drawn
between them.

Two artefacts the brief asks for that no notebook in this repository produces, so they have to be
captured by hand:

- **Audit evidence.** The design relies on Databricks-native auditing rather than a custom audit
  platform, which is the right call, but it means nothing is captured by default. Run a query against
  `system.access.audit` filtered to `cdr_mimiciv` and keep the result — that is what demonstrates the
  brief's "safeguarded *and audited*". See [`docs/architecture_blueprint.md`](docs/architecture_blueprint.md) §8.
- **Dashboard exports.** One `.lvdash.json` or screenshot per tile in
  [`05_Demo_Genie_Dashboards/dashboard_spec.md`](05_Demo_Genie_Dashboards/dashboard_spec.md), plus the
  identity each was built and viewed as.
