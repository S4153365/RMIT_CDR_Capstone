# AI/BI dashboard spec

What to build on top of the governed Gold layer, and what each tile proves. Written 2026-10-26.

The project brief asks for a demonstration where a researcher gets "accurate data summaries and visual
dashboards without writing SQL". The dashboard is the second half of that sentence, so this file
specifies the tiles rather than inventing numbers to put in them. Every measure below comes from SQL
that is already written and reviewed in
[`../06_Genie_Vocabulary/02_Genie_Validation.py`](../06_Genie_Vocabulary/02_Genie_Validation.py) —
the benchmark's approved queries. **None of that SQL has been run against the data yet**, so step 0 is
to run it there, fix any wrong table or column name, and only then build the tiles.

See [`genie_space.md`](genie_space.md) for the Genie space these dashboards are demonstrated alongside.

## Step 0 — before building anything

1. Run `02_Genie_Validation.py` as an admin on Serverless. Every query in the table below is
   identified by its benchmark id; fix any that fail and note the correction.
2. Build and verify the dashboards as **`cdr_standard_researcher`**, not as an admin. This is the
   persona the brief names. A dashboard built as an admin renders columns that a researcher sees
   masked, so an admin-built dashboard is not evidence that a researcher can read it.
3. Record the identity for every tile in the table below. If a tile was built as one role and viewed
   as another, say so — that difference is itself a governance finding worth showing.

## Tiles

Each tile names the benchmark query that produces its measure, so the SQL has one home and the tile
cannot silently disagree with it.

| # | Tile | Benchmark query | Measure | Dimension | Visual |
|---|---|---|---|---|---|
| 1 | 30-day readmission rate, ten most common primary diagnoses | Q6 | `readmit_pct` | `diagnosis_title` | Horizontal bar, sorted descending |
| 2 | 28-day in-hospital mortality after ICU admission, by age group | Q7 | `mortality_pct` | `age_group` | Column chart, age bands in order `<40`, `40-64`, `65-79`, `80+` |
| 3 | ICU admissions with acute kidney injury | Q4 | `admissions_with_aki` | none | Counter |
| 4 | ICU stays with lactate above 2 mmol/L within 6 hours of admission | Q5 | `icu_stays` | none | Counter |
| 5 | ICU admissions by year | Q9 | `admissions` | `year` | Column chart |
| 6 | Average diagnoses and prescriptions per admission | Q8 | `avg_diagnoses`, `avg_prescriptions` | none | Two counters side by side |
| 7 | Median hours from hospital admission to first ICU stay | Q14 | `median_hours` | none | Counter |
| 8 | Vasopressor administrations by drug | C1 | `administrations` | `drug` | Column chart |
| 9 | Median hours from first blood culture to first broad-spectrum antibiotic | Q13 | `median_hours` | none | Counter |

### Layout

Row 1 is the three counters that answer a question in one number — AKI (3), lactate (4), and
admission-to-ICU time (7). Row 2 is the two rate charts researchers actually read together, readmission
(1) and mortality (2). Row 3 is the supporting distributions, years (5) and vasopressors (8). Row 4
holds the averages (6) and the antibiotic timing (9).

## Captions that must appear on the dashboard

These are not decoration. Each one prevents a reader from drawing a conclusion the data does not
support, and each traces to a documented limitation:

- **Tile 2 must say "in-hospital death only".** There is no post-discharge date of death in this
  data, so 28-day mortality cannot be measured after discharge. Without the caption the tile reads as
  all-cause 28-day mortality, which it is not.
- **Tile 1 must state the readmission rule**: index admission survived to discharge, next admission
  within 30 days of discharge.
- **Tile 5 must explain the date shift.** MIMIC-IV shifts dates per patient, so "2019" is not a
  calendar year in the usual sense. Run Q9 first and report the years that actually appear.
- **Tile 9 is provisional.** The broad-spectrum antibiotic list in Q13 is an example, not a defined
  standard. The list must be stated on the tile and approved before the number is quoted.

## One tile that is deliberately not specified

Grouping medication administrations by `dim_medication_route.functional_group` would be a good
addition — the route vocabulary exists precisely to make route abbreviations readable, and it is the
clearest demonstration of the semantic layer. It is left out because the route column on the Gold
medication fact is **not yet confirmed**: the vocabulary notebook's own join example carries the
placeholder `<medication_fact>` and says to confirm which Gold table holds a route column before
using it. Confirm that first, then add the tile.

## Evidence to keep

A tile is only evidence if a reviewer can reproduce it:

- The dashboard export (`.lvdash.json`) or a screenshot, per tile.
- The dataset SQL behind each tile, which is the benchmark query named above.
- The identity each tile was built and viewed as, and whether any column appeared masked.
- The filled Q1–Q14 scoring sheet, which is where the natural-language half of the demonstration is
  evidenced. The dashboards show the governed data; the scoring sheet shows Genie's answers compared
  against the approved result.

Record the same artefacts in [`../HANDOVER.md`](../HANDOVER.md) §6 so a new owner knows what exists
and where.
