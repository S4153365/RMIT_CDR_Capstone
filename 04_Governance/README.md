# `04_Governance/` — file map

Two different things live in this folder: the **live control chain** that creates and verifies the
catalog's access controls, and **reference material** that explains or reports on them. The numbering
collides on purpose-free numbers — `02` and `05` each appear twice — so read the folder:
**`jobs/` is always the live chain; everything else is reference or documentation.**

## The live control chain — `jobs/`

Run in this order. Every writing notebook has a `dry_run` mode; the two verification notebooks have
`fail_on_gap`, which raises instead of printing an advisory summary.

| Notebook | What it does | Writes |
|---|---|---|
| [`jobs/01_apply_tags.py`](jobs/01_apply_tags.py) | Schema / table / column tags across bronze, silver and gold: `layer`, `type`, `domain`, `mimic_module`, `hipaa_identifiability`, `hl7_confidentiality`. Default plus override at column level. Idempotent | `ALTER … SET TAGS` |
| [`jobs/02_verify_tags.py`](jobs/02_verify_tags.py) | Reads metadata only and checks that `01` left the catalog in the expected state | nothing |
| [`jobs/03_create_groups.py`](jobs/03_create_groups.py) | Creates the 11 catalog groups and grants catalog, schema and domain-scoped table privileges from `rbac/role_catalog.json` | `CREATE GROUP`, `GRANT` |
| [`jobs/04_verify_rbac.py`](jobs/04_verify_rbac.py) | Compares live grants, masks and row filters against `rbac/role_catalog.json` via the `information_schema` privilege and mask views | nothing |
| [`jobs/05_fine_grained_controls.py`](jobs/05_fine_grained_controls.py) | Turns classification into enforcement: one mask function per (posture, type), one row-filter function for the service column, and the two id-anchored row filters (`rls.id_anchored_filters`) that carry the same cohort rule to the facts via `hadm_id` and to `dim_patient` via `subject_id`. Then `SET MASK` on every tagged column in **gold and silver** and `SET ROW FILTER` on every table a filter is declared for | `CREATE FUNCTION`, `CREATE TABLE` (the subject snapshot), `ALTER … SET MASK`, `SET ROW FILTER` |
| [`jobs/governance_job.json`](jobs/governance_job.json) | Job definition for `01` → `02`, parameterised by `mode` (default `dry_run`) and `catalog`. **`03`, `04` and `05` are not chained in yet** — they are still run individually | — |

**Silver identifier masking is part of this chain, not a separate notebook.** `jobs/01` tags the
Silver identifier columns by name, and `jobs/05` masks every schema listed in
`masking.masked_schemas` — so there is one taxonomy, one set of mask functions, and one place a
reviewer has to look. Row filters stay gold-only, because only gold carries a cohort column.

The separate `apply_masks_notebook` that used to do this was **retired on 2026-10-26**. It could not
run: it required a `cdr_mimiciv.rbac.data_classification` table that does not exist, and it derived
its mask names (`mask_ctrl_*` / `mask_cres_*`) from a `proposed_handling` vocabulary that the platform
does not use. It is kept in [`_archive/`](../_archive/apply_masks_notebook.py) as a record of intent,
not as something to run.

## Single source of truth

[`rbac/role_catalog.json`](rbac/role_catalog.json) is read by `03`, `04` and `05`. Grants, masking
postures, row-filter levels, deferred columns and exempt columns are all defined there and nowhere
else. [`rbac/README.md`](rbac/README.md) documents the role model and its known limits;
[`tags/README.md`](tags/README.md) documents the tag taxonomy and the governed-tag policy that
validates tag values.

## Reference material — not part of the control chain

| File | What it is |
|---|---|
| [`01_Governance_Overview.py`](01_Governance_Overview.py) | A 22-section narrative of the governance model (architecture, medallion model, personas, RLS, lineage, auditing, future extensions). Contains no executable statements — it is documentation in notebook form |
| [`02_Catalog_Metadata_and_Tags.py`](02_Catalog_Metadata_and_Tags.py) | Applies the official MIMIC-IV table and column **comments** from [`comments.json`](comments.json) (31 tables) to bronze, then silver, then gold. Despite the filename it does **not** set governed tags — that is `jobs/01` |
| [`05_Data_Quality_Governance.py`](05_Data_Quality_Governance.py) | Read-only consolidation of the `dq_*` rules already implemented in Silver: discovery, rule inventory, results, coverage |
| [`tags/Gold_Column_level.py`](tags/Gold_Column_level.py) | The original standalone column-level tagging script, kept as a worked example of the default-plus-override walk. Superseded by `jobs/01`, which adds the identifier name rules and the Bronze/Silver levels |

The boundaries these controls build — identity to catalog to schema to table to row to column — and how
each one maps to the Australian Privacy Principles, are drawn in
[`../docs/architecture_blueprint.md`](../docs/architecture_blueprint.md).

## Tests

| File | What it is |
|---|---|
| [`test/test_governance_offline.py`](test/test_governance_offline.py) | Runs on a laptop with no cluster: stubs `spark`, `dbutils` and `display`, executes the notebooks exactly as Databricks would, and asserts on the SQL they produce. 149 checks. `python3 04_Governance/test/test_governance_offline.py` |
| [`test/governance_test_notebook.py`](test/governance_test_notebook.py) | Run **in** the workspace **as each identity** to evidence what that role actually sees. Prints counts only, never patient rows, so its output is safe to screenshot |

## Runtime prerequisites

- `jobs/03`, `04` and `05` need `cdr_admins` membership; `03` and `05` stop with an explicit error
  otherwise.
- The tag values `layer`, `type`, `domain`, `mimic_module`, `hipaa_identifiability` and
  `hl7_confidentiality` must be registered as **governed tags** with their allowed values in
  Catalog → `cdr_mimiciv` → Governed tags. Unity Catalog validates tag values against that list, so
  `jobs/01` cannot add a value that is not registered — a missing value fails the statement, it does
  not fall back.
- `jobs/05` creates the mask functions it attaches, so there is no separate "create the functions
  first" step. It does need the governed tag values registered (above) and `cdr_admins` membership.
- Because a mask resolves the posture **per calling identity**, the Silver-to-Gold build must run as
  `cdr_admins`, which is also why that role is the only one with `MODIFY`. Run it as a masked role and
  `subject_id` arrives as NULL, which
  would change hash-based surrogate keys instead of failing loudly.
- Row filters and masks use `is_account_group_member()`, which is the documented function for these
  policies. The groups must therefore be **account groups** — a workspace-local group of the same
  name fails closed, restricting everyone.
