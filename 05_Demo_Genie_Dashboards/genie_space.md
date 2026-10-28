# Genie space — recreation artifact

The Genie space is configured in the workspace, not in this repository, so this file is what a new
owner needs to rebuild it: the settings, the table list, and the instruction text to paste. Written
2026-10-26.

Companion files: [`dashboard_spec.md`](dashboard_spec.md) for the dashboards, and
[`../06_Genie_Vocabulary/02_Genie_Validation.py`](../06_Genie_Vocabulary/02_Genie_Validation.py) for
the Q1–Q14 benchmark that scores whether the space answers correctly.

## Space settings

| Setting | Value | Why |
|---|---|---|
| Name | `RMIT CDR — Clinical Data Repository (MIMIC-IV)` | Matches the catalog, so the space is recognisable in a list |
| Description | `Governed natural-language access to the cdr_mimiciv Gold layer. De-identified MIMIC-IV data for research and teaching. Ask about admissions, ICU stays, diagnoses, labs, medications, orders and microbiology.` | Genie reads the description; it sets the expected domain |
| SQL warehouse | Serverless | The space runs only aggregate queries; a serverless warehouse starts on demand |
| Tables | The 16 in the table below | Curated from the Gold layer |
| Instructions | The block in [§Instructions](#instructions) | Encodes the traps the benchmark tests |

**Start with the curated set, not all 28 tables.** The 16 below are exactly the tables the Q1–Q14
benchmark reads, so every table in the space has at least one reviewed example behind it. Tables
with no benchmark coverage add surface area without adding evidence, which is how a space starts
answering confidently from a table nobody checked. Add the remainder once the benchmark scores well.

| Table | Grain | What it holds |
|---|---|---|
| `fact_admission` | one row per hospital admission | `admittime`, `dischtime`, `deathtime`, `hospital_expire_flag`, demographics attributes, `admit_date_key` |
| `dim_patient` | one row per patient | `subject_id`, `gender`, `anchor_age`, `anchor_year`, `anchor_year_group` |
| `dim_date` | one row per date key | `year` and the date parts the Gold layer uses |
| `fact_diagnosis` | one row per diagnosis on an admission | `diagnosis_key`, `seq_num`, `hadm_id` |
| `dim_diagnosis` | one row per coded diagnosis | `icd_version`, `icd_code`, `diagnosis_title` |
| `dim_procedure` | one row per coded procedure | procedure vocabulary |
| `fact_icu_stay` | one row per ICU stay | `stay_id`, `hadm_id`, `intime`, `outtime` |
| `fact_lab` | one row per lab result, **admission level** | `lab_item_key`, `charttime`, `valuenum`, `valueuom` |
| `dim_lab_item` | one row per lab item | `lab_test_name`, `fluid` |
| `fact_microbiology` | one row per microbiology record | `microbiology_test_key`, `charttime`, `spec_type_desc` |
| `dim_microbiology_test` | one row per microbiology test | `spec_type_desc` |
| `fact_medication_administration` | one row per administration event | `medication_key`, `charttime`, `hadm_id` |
| `fact_prescription` | one row per prescription | `hadm_id`, medication and dose fields |
| `dim_medication` | one row per medication | `drug` and the medication attributes |
| `dim_medication_route` | one row per route code | `route_code`, `route_name`, `functional_group`, `umls_cui`, `synonyms_text` — join on `UPPER(TRIM(route))` to `route_code_normalized` |
| `fact_order` | one row per order | `poe_id`, order type and order metadata |

`dim_provider` is deliberately absent: it carries `provider_id`, which is a direct identifier, and no
benchmark question needs it. Add it only with a stated purpose.

## Instructions

Paste this block into the space's **Instructions** field. Every paragraph traces to a rule that is
already true in the Gold layer or to a benchmark question that tests it; nothing here is a guess
about what Genie should do.

```text
This space answers questions about de-identified MIMIC-IV clinical data in the cdr_mimiciv catalog,
Gold layer. Always query gold tables.

DEFINITIONS YOU MUST STATE
- State the definition you used for any term that could be read more than one way — in particular
  "readmission", "mortality", "died" and "ICU stay". If the question does not pin the definition
  down, say which one you applied and answer under it.
- Readmission means: the index admission ended with the patient alive (hospital_expire_flag = 0) and
  the patient's next admission began within 30 days of the discharge time. Partition by patient and
  order by admittime to find the next admission.
- Primary diagnosis means fact_diagnosis.seq_num = 1. A diagnosis is not otherwise ranked.
- In-hospital death only: fact_admission.deathtime and hospital_expire_flag cover death inside the
  hospital. There is no post-discharge date of death in this data, so questions about 30-day or
  90-day mortality after discharge can only be answered as in-hospital mortality. Say so.

DATA SHAPE RULES
- Dates are shifted per patient in MIMIC-IV: every patient keeps their internal time intervals but
  the calendar dates move. The year must be taken from dim_date via fact_admission.admit_date_key.
  If asked for a calendar year, answer with the years that actually appear and explain the shift.
- fact_lab is at admission level and has no stay_id. To relate labs to an ICU stay, join on hadm_id
  and compare charttime against the ICU intime, and say the unit is the admission.
- Age is not a column. Compute it as dim_patient.anchor_age + (year(admittime) - dim_patient.anchor_year).
- Never join two fact tables to the same admission in one query to count both. Count in separate
  subqueries then join the counts, or the row multiplication will inflate every average.
- There is no infusion-rate or vital-sign table in this catalog. inputevents, chartevents,
  procedureevents and the ICU observations are not exposed here. Drug-dose thresholds, infusion
  rates and continuous vital-sign questions cannot be answered; say that rather than approximating.
- For medication routes, join dim_medication_route on UPPER(TRIM(route)) = route_code_normalized and
  report functional_group rather than the raw code where the raw code is an abbreviation.

HONESTY RULES
- Use only the tables available to you. If a question needs a table that is not in this space, say
  which table is missing instead of substituting something similar.
- Some identifier columns are masked for your role and will return a masked or null value. If a
  question needs identifiers, report that they are masked for your role rather than guessing,
  enumerating or reconstructing them.
- This is de-identified research data with no cleaning applied. Values that look clinically unusual
  are carried through deliberately and flagged, not corrected. Do not filter them out silently.
- If a result looks surprising, show the query you ran and name the assumption that produced it.

REFUSALS
- Decline questions asking for clinical recommendations — which treatment a patient should receive,
  which drug to use, which patients should be treated differently. Offer a descriptive question about
  the data instead.
- Decline questions that rank hospitals, wards or providers by quality or performance. This data
  cannot support that inference, and the identifiers are masked. Offer a descriptive alternative.
```

## Trusted queries

Genie improves fastest when the space holds reviewed SQL for the questions it gets wrong. The
benchmark file already contains the approved SQL for Q1–Q14 and the checks C1–C4; paste the ones it
fails into the space's example SQL after confirming each one runs. Do not paste SQL that has not been
run — a wrong example teaches the space the wrong grain.

The three highest-value examples to add first, because they encode the traps:

- **Q6** — 30-day readmission by primary diagnosis: shows the `LEAD()` window and the surviving-to-discharge filter.
- **Q8** — average diagnoses and prescriptions per admission: shows counting in separate subqueries to avoid row multiplication.
- **Q7** — 28-day in-hospital mortality by age group: shows the computed age and the `deathtime` bound.

## Verifying the space

1. Run [`02_Genie_Validation.py`](../06_Genie_Vocabulary/02_Genie_Validation.py) as an admin on
   Serverless. It prints the approved result for each question.
2. Ask the space the same Q1–Q14 in a fresh chat each time, so earlier answers cannot steer later ones.
3. Fill the scoring sheet at the bottom of that notebook: approved result, Genie's result, verdict,
   failure cause, fix.
4. Record which identity you asked as. A space queried as an admin returns unmasked columns that a
   researcher role never sees, so answers can differ between identities for the same question.

The space is evidenced when the scoring sheet is filled in. Until then the benchmark is a runner
without a result.
