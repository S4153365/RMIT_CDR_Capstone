# CDR Role Catalog (Unity Catalog access control)

`role_catalog.json` is the **single source of truth** for who can see what in
`cdr_mimiciv`. The notebooks read it; nothing about roles is hard-coded any more.

It is derived from `roles.pdf` (*Role Catalog Schema Tables*) and trimmed to one role per
demonstrated control. The corrections and the cuts are listed at the bottom of this page.

| File | Role |
|---|---|
| `role_catalog.json` | groups, schema privileges, gold domains, column posture, RLS level |
| `../jobs/03_create_groups.py` | creates groups, applies `USE CATALOG` / `USE SCHEMA` / `SELECT` / `MODIFY` |
| `../jobs/04_verify_rbac.py` | diffs live grants, masks and row filters against this catalog |
| `../jobs/05_fine_grained_controls.py` | row filters (RLS levels) and column masks (HIPAA / HL7) |
| `../../_archive/apply_masks_notebook.py` | retired: Silver identifiers are masked by `05` from the same tags. Kept as a record of intent |

## The catalog

Seven roles — the final provisioned set (2026-10-28 team decision). Each one demonstrates a
**different Unity Catalog control**, so a reader can see the mechanism rather than infer it:

| Group | Demonstrates | Schemas | Gold domains | Direct identifier | PHI quasi | De-identified | HL7 V_Restricted | RLS |
|---|---|---|---|---|---|---|---|---|
| `cdr_admins` | full administration | bronze, silver, gold | ALL | Unmasked | Unmasked | Unmasked | Unmasked | Level 3 (Global) |
| `cdr_data_engineers` | **platform / pipeline engineering** | bronze, silver, gold (read + write) | ALL | Unmasked | Unmasked | Unmasked | Unmasked | Level 3 (Global) |
| `cdr_data_scientist` | **layer-level separation** | silver | — (no gold access) | Masked | Masked | Unmasked | Masked | Level 1 |
| `cdr_standard_researcher` | **column level** | gold | ALL | Masked | Masked | Unmasked | Masked | Level 1 (Standard) |
| `cdr_researcher_psychiatry` | **row level** | gold | ALL | Masked | Masked | Unmasked | Masked | Level 2 (Standard + Psychiatry) |
| `cdr_pharmacy_analyst` | **table level** | gold | pharmacy | Masked | Masked | Unmasked | Masked | Level 1 (Standard) |
| `cdr_external_researcher` | **table level, narrow** | gold | diagnostics | Masked | Masked | Unmasked | Masked | Level 1 (Standard) |

How each one proves its point:

- **Schema level.** `cdr_admins` holds `SELECT` + `MODIFY` on all three layers; every other
  role starts with `gold` only, so a `SELECT` against `silver.patients` is denied by the
  *schema* grant before any mask is involved.
- **Column level.** `cdr_standard_researcher` is granted the entire `gold` schema and can
  still not read an identifier: `subject_id` and `provider_id` arrive as `NULL`. The only
  thing between the role and the raw value is the mask, which is what makes it a column
  control rather than a grant.
- **Row level.** `cdr_standard_researcher` and `cdr_researcher_psychiatry` hold **identical**
  grants and identical column posture. The only reason their query results differ is the row
  filter letting PSYCH rows through for one and not the other. Run the same
  `SELECT COUNT(*) FROM gold.fact_service` as both — that pair is the demonstration.
- **Table level.** `cdr_pharmacy_analyst` has no schema `SELECT` at all; it holds `SELECT` on
  the individual tables carrying `domain=pharmacy`, resolved from the tag.
  `cdr_external_researcher` is the narrow version: `diagnostics` only, so an
  attempted read of `gold.fact_admission` is denied by the *table* grant.
- **Layer-level separation.** `cdr_data_scientist` is granted `silver` only: the ML / data
  science team gets the full approved Silver population with identifiers masked, and a query
  against `gold` is stopped by the *schema* grant before any mask is involved.
- **Platform engineering.** `cdr_data_engineers` holds read/write/build on all three layers and
  is unmasked like `cdr_admins`, but deliberately **without** catalog administration: ownership,
  grants and governance changes stay with `cdr_admins`. Engineering access without security
  administration.
- **Confidentiality note.** `cdr_mortality_investigator` was removed in the 2026-10-28
  finalisation. Consequence, accepted: HL7 `V_Very_Restricted` data (`fact_admission.deathtime`)
  is readable only by the platform roles (`cdr_admins`, `cdr_data_engineers`); every research
  role receives `NULL`. `N_Normal` and `R_Restricted` columns are unmasked for every role.

### What was cut, and the one thing you must do about it

The catalog provisions seven roles (2026-10-28 finalisation). Compared with the eleven groups
in the `roles.pdf` draft, `cdr_data_engineers` and `cdr_data_scientist` are now provisioned for
real: engineering is no longer folded into `cdr_admins`, and the ML / data science team gets
silver-only access. The PDF roles that remain cut (`cdr_cmo_admin`, `cdr_researcher_peds`,
`cdr_hospital_researchers`, `cdr_bi_developer`) each duplicated a control one of the seven
already shows. Two more were tried during handover and cut in the same pass —
`cdr_data_stewards` (policy ownership) and `cdr_auditor` (audit review). Neither was ever
created in the account, so nothing needs revoking for them, but their absence is why the
report states plainly that no role can audit without table access.

**`03_create_groups` never revokes.** Deleting a role here does not remove its live grants,
so after this trim the catalog says six and the metastore can still say eleven. `04`
reports every `cdr_*` group that holds grants but is no longer in the catalog as a note —
use that list as the revoke checklist, and drop the groups in the account console.

Two consequences, accepted rather than hidden: build pipelines run as `cdr_admins`, and
there is no neonatal cohort, so `PEDS` / `NEONATAL` / `NICU` services are standard-cohort rows
visible to every level. Only psychiatry is a restricted cohort.

`SELECT`/`MODIFY` are granted at schema level for `ALL`-domain roles; domain-scoped
roles get table-level `SELECT` resolved from the `domain` tag that
`jobs/01_apply_tags` writes. That is why the tag vocabulary and the domains in this
catalog must match — `03_create_groups` warns and `04_verify_rbac` fails when they do not.

## How the controls work

**Column masks** (Gold, `jobs/05_fine_grained_controls`). One function per posture and
type, e.g. `cdr_mimiciv.rbac.mask_direct_int`, attached to every column tagged
`hipaa_identifiability = phi_direct_identifier`. The function itself resolves the role:

```sql
CASE
  WHEN is_account_group_member('cdr_admins')
       OR is_account_group_member('cdr_data_engineers') THEN v            -- unmasked
  ELSE NULL                                                              -- masked
END
```

**Row filters** (Gold, same notebook). `cdr_mimiciv.rbac.rls_service` encodes all three
levels and is attached to every Gold base table that carries the cohort column
(`curr_service`, `service` or `service_name`):

```sql
CASE
  WHEN is_account_group_member(<level 3 groups>) THEN TRUE
  WHEN service IS NULL THEN TRUE
  WHEN upper(trim(service)) IN ('PSYCH','PSYCHIATRY') THEN is_account_group_member('cdr_researcher_psychiatry')
  ELSE TRUE
END
```

A row whose cohort column is NULL is treated as a standard row (visible to every level).
That is deliberate: an unknown service is not evidence of a restricted cohort, and
failing closed there would silently drop most of the fact table. If the team would rather
fail closed, change `WHEN service IS NULL THEN TRUE` to `THEN is_account_group_member(...)`
for the level 3 groups only.

**Id-anchored row filters** (Gold, same notebook). `rls_service` needs the service column
inside the table it protects, and only `dim_service` and `fact_service` have one. The other Gold
tables are protected through their keys instead, declared in `rls.id_anchored_filters`:

| Filter | Anchor | Attached to | Resolves through |
|---|---|---|---|
| `rls_hadm_service_filter_<type>` | `hadm_id` | the 11 Gold facts carrying `hadm_id` | `fact_service` -> `dim_service` |
| `rls_subject_service_filter_<type>` | `subject_id` | `dim_patient` | `rbac.subject_service_map` |

Both are permissive in the same way as `rls_service`: a caller outside the cohort group gets every
row, so the cohort is the only behaviour they change. Three rules make them safe to attach:

- **Never attach a filter to a table it reads.** `fact_service` is therefore excluded from the hadm
  filter's targets — Unity Catalog would recurse. The `reads` list in the catalog encodes this and
  `jobs/05` refuses a target that appears in it.
- **One filter per table.** Unity Catalog allows a single row filter, so `jobs/05` skips any table
  that already carries `rls_service`.
- **The parameter type must match the column.** `hadm_id` is INT on nine Gold facts and BIGINT on
  three, so the function name carries the type — the same reason the mask functions do.

The subject filter reads a physical snapshot rather than joining: a function that joins
`dim_patient` recurses, and a view would inherit `dim_patient`'s own filter. `rbac.subject_service_map`
is therefore stale after any Gold rebuild and has to be refreshed by re-running `jobs/05`.

Filtering `dim_patient` has an analytical cost worth stating: a cohort role can no longer see the
non-cohort population, so any rate it computes has an unknowable denominator. The facts are the
things that should be filtered; the patient dimension is filtered here to demonstrate the control.

Dimensions carrying neither key cannot be row-filtered; `04_verify_rbac` lists them so the gap is
visible rather than assumed closed.

## Known limits

- Groups must be **account groups**. `is_account_group_member()` (the documented function
  inside row filters and masks) does not resolve workspace-local groups, so a
  workspace-local group fails closed: members see the masked/restricted view.
- `hadm_id` and `stay_id` are tagged as quasi identifiers but masking is **deferred**
  (`masking.deferred_columns`): they are the join path between Gold facts for researchers.
  That is an open team decision; `04_verify_rbac` reports them as a note, not a gap.
- Masks on a base table are inherited by its views; views themselves are not tagged or masked.
- The same identifier column is masked consistently in every Gold table, so joins on
  `subject_id` return nothing for masked roles (NULL never matches NULL). Gold is modelled
  with surrogate keys (`patient_key`, `provider_key`) precisely so that masked roles keep
  working; do not reintroduce raw ids as join keys.
- `03_create_groups` never revokes. To remove access, revoke by hand and re-run `04_verify_rbac`.

## Run order

```
01_apply_tags            mode=dry_run, then apply
02_verify_tags           fail_on_gap=true
03_create_groups         mode=dry_run, then apply
05_fine_grained_controls mode=rollback, then apply   (05 before 04: it creates what 04 checks)
04_verify_rbac           fail_on_gap=false, then true
```

Roll back Gold controls with `05_fine_grained_controls` `mode=rollback`.

## Corrections made to `roles.pdf`

- Group names corrected: `cdr_researcher_pyschiatry` -> `cdr_researcher_psychiatry`,
  `cdr_reasearcher_peds` -> `cdr_researcher_peds`. Typos must not become real principals.
- `role_clinical_auditor` and `role_neonatal_researcher` existed only in the old notebook
  code (one had a grant matrix but no group, the other a group but no matrix). Neither is
  in the PDF and neither is provisioned: there is no separate neonatal cohort, and no role
  can read audit logs without table access. Both are stated as limitations.
- `cdr_hospital_researchers` was in the PDF and the test notebook but had no provisioning
  at all. It is not provisioned here either — it was the standard researcher with a
  different job title.
- Six groups named in this page's history are not provisioned: the four PDF groups listed
  under "What was cut" plus the two governance roles tried and dropped at handover.
- Domain values in the old grant matrix (`drug`, `person`, `measurement`) were OMOP-ish
  names that no table carried; they are now the real tag values (`pharmacy`,
  `demographics`, `diagnostics`).
