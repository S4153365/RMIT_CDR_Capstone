# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Dependency-Ordered Runner
# MAGIC
# MAGIC Runs the per-table Silver notebooks in dependency order. Each notebook publishes its table with
# MAGIC `CREATE OR REPLACE TABLE` semantics (`writeTo(...).createOrReplace()`), so **running with
# MAGIC `dry_run=false` replaces the target Silver tables**.
# MAGIC
# MAGIC ## Defaults
# MAGIC `source_schema = cdr_mimiciv.bronze` → `target_schema = cdr_mimiciv.silver`.
# MAGIC The default target is the schema that existing Gold tables read. Review
# MAGIC `docs/gold_governance_checks.md` (Source readiness: impact on existing Gold) before running against it.
# MAGIC
# MAGIC ## Parameters
# MAGIC | Parameter | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | source_schema | cdr_mimiciv.bronze | Bronze `catalog.schema` |
# MAGIC | target_schema | cdr_mimiciv.silver | Silver `catalog.schema` |
# MAGIC | tables | all | `all` (preview only), comma-separated names, or a JSON list. A real run needs an explicit list |
# MAGIC | dry_run | true | Print the ordered plan; write nothing |
# MAGIC | enforce_profiled_counts | true | Assert the row/flag counts established by profiling the full MIMIC-IV dataset. Use `false` for other datasets (demo, Cevo). Row preservation is always asserted |
# MAGIC | stop_on_failure | true | Stop at the first failed table |
# MAGIC | timeout_seconds | 7200 | Per-notebook timeout |
# MAGIC | run_cross_validation | true | Run `90_Silver_Cross_Table_Validation` for the tables that succeeded |
# MAGIC
# MAGIC ## Preview-only by default; real runs are gated
# MAGIC `dry_run=true` prints the ordered plan with each table's four readiness axes and writes nothing.
# MAGIC A real run (`dry_run=false`) is refused unless (1) `tables` is an explicit list (`all` is preview-only)
# MAGIC and (2) every selected table has `approved_to_publish=True` in `00_Silver_Registry` (a reviewed
# MAGIC change, with `approved_by`/`approved_on`). Tables marked `NEEDS_PROFILING` cannot be run here at all:
# MAGIC run their own notebook after the profiling has been reviewed. There is no override parameter.
# MAGIC Before any real run, complete the readiness checklist in `docs/gold_governance_checks.md`.
# MAGIC
# MAGIC ## Never run by this notebook
# MAGIC `chartevents` only (deferred: the Bronze table does not exist). `poe` and `poe_detail` were
# MAGIC reinstated on 2026-10-26 and are registered like every other table.
# MAGIC
# MAGIC ## After a real run
# MAGIC Whether `CREATE OR REPLACE TABLE` keeps table/column comments, tags, masks and grants is not
# MAGIC established (see `docs/gold_governance_checks.md` §3); verify them and rerun
# MAGIC `91_Silver_Metadata_Documentation` / metadata injection / mask application as needed.

# COMMAND ----------

# MAGIC %run ./00_Silver_Registry

# COMMAND ----------

import json
import re

dbutils.widgets.text("source_schema", "cdr_mimiciv.bronze")
dbutils.widgets.text("target_schema", "cdr_mimiciv.silver")
dbutils.widgets.text("tables", "all")
dbutils.widgets.text("dry_run", "true")
dbutils.widgets.text("enforce_profiled_counts", "true")
dbutils.widgets.text("stop_on_failure", "true")
dbutils.widgets.text("timeout_seconds", "7200")
dbutils.widgets.text("run_cross_validation", "true")


def flag(name):
    value = dbutils.widgets.get(name).strip().lower()
    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")
    return value == "true"


BRONZE = dbutils.widgets.get("source_schema").strip()
SILVER = dbutils.widgets.get("target_schema").strip()
RAW_TABLES = dbutils.widgets.get("tables").strip()
DRY_RUN = flag("dry_run")
ENFORCE_PROFILED_COUNTS = flag("enforce_profiled_counts")
STOP_ON_FAILURE = flag("stop_on_failure")
RUN_CROSS_VALIDATION = flag("run_cross_validation")
TIMEOUT_SECONDS = int(dbutils.widgets.get("timeout_seconds"))

SCHEMA_PATTERN = r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*"
for _schema in (BRONZE, SILVER):
    if not re.fullmatch(SCHEMA_PATTERN, _schema):
        raise ValueError(f"Schema must be catalog.schema: {_schema}")
if BRONZE == SILVER:
    raise ValueError("source_schema and target_schema must differ")
if TIMEOUT_SECONDS < 60:
    raise ValueError("timeout_seconds must be at least 60")

if RAW_TABLES == "all":
    REQUESTED = "all"
elif RAW_TABLES.startswith("["):
    REQUESTED = json.loads(RAW_TABLES)
else:
    REQUESTED = [t.strip() for t in RAW_TABLES.split(",") if t.strip()]

# COMMAND ----------

plan = build_plan(REQUESTED)

print(f"Source : {BRONZE}")
print(f"Target : {SILVER}   (existing tables are REPLACED when dry_run=false)")
print(f"dry_run={DRY_RUN} | enforce_profiled_counts={ENFORCE_PROFILED_COUNTS}")
print(f"Never run: excluded={sorted(EXCLUDED)} deferred={sorted(DEFERRED)}\n")

for step in plan["steps"]:
    needs = [d for d in step["depends_on"] if d not in plan["selected"]]
    axes = (f"code={'Y' if step['code_prepared'] else 'N'} semantics={step['semantics_reviewed']:<7} "
            f"profiled={step['profiled']:<7} approved={'Y' if step['approved_to_publish'] else 'N'}")
    print(f"{step['position']:>2}. {step['table']:<18} {axes}  {step['notebook']}"
          + (f"   (must already exist in target: {needs})" if needs else ""))
for table, reason in plan["skipped"].items():
    print(f"--  {table:<18} NOT RUN: {reason}")

# plan-time metadata checks only (no data is read)
missing_bronze = [s["table"] for s in plan["steps"] if not spark.catalog.tableExists(f"{BRONZE}.{s['table']}")]
missing_deps = sorted({d for s in plan["steps"] for d in s["depends_on"]
                       if d not in plan["selected"] and not spark.catalog.tableExists(f"{SILVER}.{d}")})
print("\nMissing Bronze sources:", missing_bronze or "none")
print("Dependencies outside the selection that are missing in the target:", missing_deps or "none")
if missing_bronze or missing_deps:
    raise RuntimeError("Resolve missing Bronze sources / Silver dependencies before running")

# COMMAND ----------

if DRY_RUN:
    print("DRY RUN - no notebook was run and nothing was written.")
    try:
        require_executable(plan)
        print("The selection would be accepted for a real run.")
    except ValueError as exc:
        print(f"A real run of this selection would be refused: {exc}")
else:
    require_executable(plan)          # explicit selection + recorded approvals, or stop here
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
    results = {}
    arguments = {
        "source_schema": BRONZE,
        "target_schema": SILVER,
        "enforce_profiled_counts": str(ENFORCE_PROFILED_COUNTS).lower(),
    }
    for step in plan["steps"]:
        print(f"Running {step['table']} ...")
        try:
            output = dbutils.notebook.run(step["notebook"], TIMEOUT_SECONDS, arguments)
            results[step["table"]] = ("SUCCESS", output)
        except Exception as exc:
            results[step["table"]] = ("FAILED", " ".join(str(exc).split())[:300])
            print(f"FAILED {step['table']}: {results[step['table']][1]}")
            if STOP_ON_FAILURE:
                break

    print("\nResults")
    for table, (status, detail) in results.items():
        print(f"  {table:<18} {status:<8} {detail}")

    succeeded = [t for t, (status, _) in results.items() if status == "SUCCESS"]
    if RUN_CROSS_VALIDATION and succeeded:
        print("\nCross-table validation")
        print(dbutils.notebook.run("./90_Silver_Cross_Table_Validation", TIMEOUT_SECONDS,
                                   {**arguments, "tables": ",".join(succeeded)}))

    failed = [t for t, (status, _) in results.items() if status != "SUCCESS"]
    if failed:
        raise RuntimeError(f"Silver run finished with failures: {failed}")
