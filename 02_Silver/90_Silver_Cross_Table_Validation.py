# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Cross-Table Validation
# MAGIC
# MAGIC Read-only checks over published Silver tables, driven by `00_Silver_Registry`.
# MAGIC
# MAGIC **Asserted (fails the run):** table exists; row preservation versus Bronze for row-preserving
# MAGIC tables; no NULL grain keys; unique grain key where profiling established uniqueness; every
# MAGIC registered DQ flag column exists, is boolean and has no NULLs.
# MAGIC
# MAGIC **Observational (printed, never asserted, never a row flag):** referential relationships to
# MAGIC `patients` and `admissions`, and the ICU/transfer care-unit comparison. Orphan or mismatch counts
# MAGIC are cross-table findings to investigate; they do not justify removing or flagging clinical rows.
# MAGIC
# MAGIC Observational checks join large tables, so they are off by default (`relationship_checks=false`).
# MAGIC
# MAGIC The profiling evidence recorded for `transfers` (full MIMIC-IV): every `icustays.stay_id` exists as a
# MAGIC `transfer_id` with agreeing `hadm_id`, `subject_id` and `intime`; `icustays.first_careunit` differs from
# MAGIC the transfer `careunit` for 7,956 of 94,458 stays. The ICU check below reprints these counts for
# MAGIC comparison and does not assert them.

# COMMAND ----------

# MAGIC %run ./00_Silver_Registry

# COMMAND ----------

import re
from pyspark.sql import functions as F

dbutils.widgets.text("source_schema", "cdr_mimiciv.bronze")
dbutils.widgets.text("target_schema", "cdr_mimiciv.silver")
dbutils.widgets.text("tables", "all")
dbutils.widgets.text("enforce_profiled_counts", "true")
dbutils.widgets.text("relationship_checks", "false")

BRONZE = dbutils.widgets.get("source_schema").strip()
SILVER = dbutils.widgets.get("target_schema").strip()
RAW_TABLES = dbutils.widgets.get("tables").strip()
RELATIONSHIP_CHECKS = dbutils.widgets.get("relationship_checks").strip().lower() == "true"

for _schema in (BRONZE, SILVER):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", _schema):
        raise ValueError(f"Schema must be catalog.schema: {_schema}")

tables = [t for t in REGISTRY if t not in EXCLUDED and t not in DEFERRED] if RAW_TABLES == "all" \
    else [t.strip() for t in RAW_TABLES.split(",") if t.strip()]
unknown = [t for t in tables if t not in REGISTRY]
if unknown:
    raise ValueError(f"Not in the Silver registry: {unknown}")

# COMMAND ----------

failures, report = [], []

for table in tables:
    spec = REGISTRY[table]
    target = f"{SILVER}.{table}"
    if not spark.catalog.tableExists(target):
        failures.append(f"{table}: Silver table missing")
        continue

    silver = spark.table(target)
    aggregates = [F.count(F.lit(1)).alias("rows")]
    for key in spec["keys"]:
        aggregates.append(F.sum(F.when(F.col(key).isNull(), 1).otherwise(0)).alias(f"null_{key}"))
    if spec["unique_key"] and len(spec["keys"]) == 1:
        aggregates.append(F.countDistinct(spec["keys"][0]).alias("distinct_key"))
    for flag_column in spec["flags"]:
        aggregates.append(F.sum(F.when(F.col(flag_column).isNull(), 1).otherwise(0)).alias(f"null_{flag_column}"))
    # one pass over the Silver table
    stats = silver.agg(*aggregates).first().asDict()

    if spec["row_preserving"]:
        bronze_rows = spark.table(f"{BRONZE}.{table}").count()   # Delta statistics
        if stats["rows"] != bronze_rows:
            failures.append(f"{table}: Silver rows {stats['rows']:,} != Bronze rows {bronze_rows:,}")
    for key in spec["keys"]:
        if stats[f"null_{key}"]:
            failures.append(f"{table}: {stats[f'null_{key}']:,} NULL {key}")
    if "distinct_key" in stats and stats["distinct_key"] != stats["rows"]:
        failures.append(f"{table}: {spec['keys'][0]} is not unique ({stats['distinct_key']:,} of {stats['rows']:,})")

    column_types = {f.name: f.dataType.simpleString() for f in silver.schema.fields}
    for flag_column in spec["flags"]:
        if column_types.get(flag_column) != "boolean":
            failures.append(f"{table}: {flag_column} is {column_types.get(flag_column)}, expected boolean")
        elif stats[f"null_{flag_column}"]:
            failures.append(f"{table}: {flag_column} contains NULLs")

    report.append((table, stats["rows"], {c: int(stats[f"null_{c}"]) for c in spec["keys"]}))

for table, rows, null_keys in report:
    print(f"{table:<18} rows={rows:>14,}  key NULLs={null_keys}")

# COMMAND ----------

# Observational relationship checks (opt-in; never asserted, never row-level flags)
if RELATIONSHIP_CHECKS:
    patients_ref = spark.table(f"{SILVER}.patients").select("subject_id").distinct()
    admissions_ref = spark.table(f"{SILVER}.admissions").select("hadm_id", "subject_id").distinct()
    for table in tables:
        columns = spark.table(f"{SILVER}.{table}").columns
        silver = spark.table(f"{SILVER}.{table}")
        if table not in ("patients", "admissions") and "subject_id" in columns:
            orphans = silver.select("subject_id").distinct().join(patients_ref, "subject_id", "left_anti").count()
            print(f"{table}: distinct subject_id not in patients = {orphans:,}")
        if table != "admissions" and "hadm_id" in columns:
            orphans = (silver.filter(F.col("hadm_id").isNotNull()).select("hadm_id").distinct()
                       .join(admissions_ref.select("hadm_id"), "hadm_id", "left_anti").count())
            print(f"{table}: distinct non-null hadm_id not in admissions = {orphans:,}")

    if all(spark.catalog.tableExists(f"{SILVER}.{t}") for t in ("icustays", "transfers")):
        link = (spark.table(f"{SILVER}.icustays").alias("i")
                .join(spark.table(f"{SILVER}.transfers").alias("t"), F.col("i.stay_id") == F.col("t.transfer_id"), "left"))
        display(link.select(
            F.count("*").alias("icustays_rows"),
            F.sum(F.when(F.col("t.transfer_id").isNotNull(), 1).otherwise(0)).alias("stay_id_found_as_transfer_id"),
            F.sum(F.when(F.col("t.transfer_id").isNotNull() & ~F.col("t.careunit").eqNullSafe(F.col("i.first_careunit")), 1).otherwise(0)).alias("first_careunit_differs"),
        ))
else:
    print("Relationship checks skipped (relationship_checks=false).")

# COMMAND ----------

if failures:
    print("\nCross-table validation FAILURES:")
    for failure in failures:
        print(" -", failure)
    raise AssertionError(f"{len(failures)} Silver cross-table validation failure(s)")

print("\nCross-table validation passed for:", tables)
dbutils.notebook.exit(f"cross_validation:OK:{len(tables)}")
