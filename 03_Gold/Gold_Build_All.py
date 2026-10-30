# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold — build all dimensions and facts (one script)
# MAGIC
# MAGIC Builds every table in `cdr_mimiciv.gold` from `cdr_mimiciv.silver` in one place:
# MAGIC 14 dimensions and 13 facts. It replaces the build logic spread over `Dimensions`, `Facts`,
# MAGIC `00_Gold_Registry` / `01_Gold_Engine` and `10_Gold_POE_and_Dimension_Fixes`. Those notebooks are
# MAGIC left in place; nothing here deletes them.
# MAGIC
# MAGIC Nothing runs until the `build_approved` widget is `true`. The `tables` widget limits a run to a
# MAGIC comma-separated list (default: all, in dependency order).
# MAGIC
# MAGIC ## Conventions
# MAGIC - **Keys.** Surrogate keys are `xxhash64` over the business-key columns, the encoding the current
# MAGIC   Gold tables use (`patient_key = xxhash64(CAST(subject_id AS BIGINT))`, `provider_key = xxhash64(provider_id)`,
# MAGIC   `xxhash64(col1, col2)` for two-part keys). Facts get foreign keys by `LEFT JOIN` to the dimension on its
# MAGIC   business key, so dimensions and facts always agree. An unmatched row keeps a NULL key.
# MAGIC   A 64-bit hash can collide, so the checks at the end verify uniqueness instead of assuming it.
# MAGIC - **Occurrence numbers.** Where Silver has no unique row id and rows tie on every identifying column
# MAGIC   (prescriptions, eMAR detail, chartevents), `ROW_NUMBER()` makes the key unique **within one build**.
# MAGIC   Do not store or join to those keys from outside a build.
# MAGIC - **No cleaning in Gold.** No rows are filtered, deduplicated or corrected. Durations are not clamped.
# MAGIC - **Types follow Silver.** Identifiers keep their Silver types (earlier builds stored some as STRING).
# MAGIC - **Dates are shifted per patient** (MIMIC-IV docs). `dim_date` is the shifted calendar, 2100-01-01 to 2250-12-31.
# MAGIC - **No Silver objects are created.** Earlier notebooks created `silver.v_*` helper views; this script uses CTEs.
# MAGIC
# MAGIC ## Changes from the tables built before 2026-10-20
# MAGIC | Table | Change |
# MAGIC |---|---|
# MAGIC | `fact_medication_administration` | One row per eMAR detail record. The earlier build also joined `prescriptions` on `pharmacy_id`, which repeats rows when a pharmacy order has several components (167,072,237 rows from 87,371,064 detail records). The medication key is now set only where the `pharmacy_id` maps to exactly one medication. |
# MAGIC | `fact_icu_observation` | Keeps every Silver chartevents row (432,997,491). The earlier table held 380,550,497, one per (stay, item, charttime). |
# MAGIC | `fact_icu_stay`, `fact_prescription` | Identifiers in Silver types instead of STRING. |
# MAGIC | `fact_admission`, `fact_transfer` | `los_hours`, `los_days`, `transfer_duration_hours` derived from the timestamps (Silver no longer carries them). |
# MAGIC | `dim_careunit`, `dim_date`, `dim_patient`, `dim_provider`, `dim_order_type`, `fact_order`, `fact_transfer` | As built on 2026-10-20 (`10_Gold_POE_and_Dimension_Fixes`). |
# MAGIC | `fact_order_detail` | New, one row per (`poe_id`, `field_name`) from `silver.poe_detail`. |

# COMMAND ----------

dbutils.widgets.text("build_approved", "true")
dbutils.widgets.text("tables", "all")

BUILD_APPROVED = dbutils.widgets.get("build_approved").strip().lower() == "true"
TABLES_ARG = dbutils.widgets.get("tables").strip()

S = "cdr_mimiciv.silver"
G = "cdr_mimiciv.gold"


def date_key(col):
    return f"CAST(date_format({col}, 'yyyyMMdd') AS INT)"


def hours_between(start, end):
    return (f"CASE WHEN {start} IS NOT NULL AND {end} IS NOT NULL "
            f"THEN CAST(unix_micros({end}) - unix_micros({start}) AS DOUBLE) / 3600000000.0 END")


BUILDS = {}   # table -> (comment, SELECT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

BUILDS["dim_date"] = (
    "Calendar dimension over the SHIFTED MIMIC-IV date range (dates are shifted per patient; no real-calendar meaning). One row per day, 2100-01-01 to 2250-12-31.",
    f"""
SELECT
    {date_key('d')} AS date_key,
    d AS full_date,
    year(d) AS year,
    quarter(d) AS quarter,
    month(d) AS month,
    date_format(d, 'MMMM') AS month_name,
    day(d) AS day_of_month,
    weekofyear(d) AS week_of_year,
    dayofweek(d) AS day_of_week,
    date_format(d, 'EEEE') AS day_name
FROM (SELECT explode(sequence(DATE '2100-01-01', DATE '2250-12-31', INTERVAL 1 DAY)) AS d) t""")

BUILDS["dim_patient"] = (
    "One row per patient (subject_id) with demographics. Date of death is deliberately not carried. age_deidentified_flag: anchor_age above 89 is shown as 91 (MIMIC-IV docs).",
    f"""
SELECT
    xxhash64(CAST(subject_id AS BIGINT)) AS patient_key,
    subject_id,
    gender,
    anchor_age,
    anchor_year,
    anchor_year_group,
    age_deidentified_90_plus AS age_deidentified_flag
FROM {S}.patients
WHERE subject_id IS NOT NULL""")

BUILDS["dim_provider"] = (
    "Provider identifiers (de-identified). No other provider attributes exist in MIMIC-IV.",
    f"""
SELECT xxhash64(provider_id) AS provider_key, provider_id
FROM {S}.provider
WHERE provider_id IS NOT NULL""")

BUILDS["dim_careunit"] = (
    "Distinct care unit names seen in ICU stays (first/last) and transfers. Name variants (UNKNOWN/Unknown) are kept separate.",
    f"""
SELECT xxhash64(careunit_name) AS careunit_key, careunit_name
FROM (
    SELECT first_careunit AS careunit_name FROM {S}.icustays
    UNION SELECT last_careunit FROM {S}.icustays
    UNION SELECT careunit FROM {S}.transfers
) u
WHERE careunit_name IS NOT NULL""")

BUILDS["dim_admission_type"] = (
    "Distinct admission types (9 documented values).",
    f"""
SELECT xxhash64(admission_type) AS admission_type_key, admission_type
FROM (SELECT DISTINCT admission_type FROM {S}.admissions WHERE admission_type IS NOT NULL) t""")

BUILDS["dim_location"] = (
    "Admission and discharge locations, typed by role (ADMISSION / DISCHARGE).",
    f"""
SELECT xxhash64(location_type, location_name) AS location_key, location_type, location_name
FROM (
    SELECT 'ADMISSION' AS location_type, admission_location AS location_name
    FROM {S}.admissions WHERE admission_location IS NOT NULL
    UNION
    SELECT 'DISCHARGE' AS location_type, discharge_location AS location_name
    FROM {S}.admissions WHERE discharge_location IS NOT NULL
) l""")

BUILDS["dim_diagnosis"] = (
    "ICD-9/ICD-10 diagnosis dictionary; always keyed on (icd_code, icd_version).",
    f"""
SELECT xxhash64(icd_code, icd_version) AS diagnosis_key, icd_code, icd_version, long_title AS diagnosis_title
FROM {S}.d_icd_diagnoses
WHERE icd_code IS NOT NULL AND icd_version IS NOT NULL""")

BUILDS["dim_procedure"] = (
    "ICD-9/ICD-10 procedure dictionary; always keyed on (icd_code, icd_version).",
    f"""
SELECT xxhash64(icd_code, icd_version) AS procedure_key, icd_code, icd_version, long_title AS procedure_title
FROM {S}.d_icd_procedures
WHERE icd_code IS NOT NULL AND icd_version IS NOT NULL""")

BUILDS["dim_lab_item"] = (
    "Laboratory item dictionary (itemid, label, fluid, category).",
    f"""
SELECT xxhash64(itemid) AS lab_item_key, itemid, label AS lab_test_name, fluid, category
FROM {S}.d_labitems
WHERE itemid IS NOT NULL""")

BUILDS["dim_icu_item"] = (
    "ICU item dictionary (itemid, label, abbreviation, linksto, category, unit, param_type).",
    f"""
SELECT xxhash64(itemid) AS icu_item_key, itemid, label AS item_name, abbreviation, linksto,
       category, unitname, param_type
FROM {S}.d_items
WHERE itemid IS NOT NULL""")

BUILDS["dim_medication"] = (
    "Distinct prescription drug definitions (drug_type, drug, formulary code, gsn, ndc) with a drug name. Not a clinical drug vocabulary.",
    f"""
SELECT xxhash64(drug_type, drug, formulary_drug_cd, gsn, ndc) AS medication_key,
       drug_type, drug, formulary_drug_cd, gsn, ndc
FROM (
    SELECT DISTINCT drug_type, drug, formulary_drug_cd, gsn, ndc
    FROM {S}.prescriptions
    WHERE drug IS NOT NULL
) m""")

BUILDS["dim_service"] = (
    "Distinct hospital service codes seen as previous or current service.",
    f"""
SELECT xxhash64(service) AS service_key, service
FROM (
    SELECT curr_service AS service FROM {S}.services WHERE curr_service IS NOT NULL
    UNION
    SELECT prev_service AS service FROM {S}.services WHERE prev_service IS NOT NULL
) s""")

BUILDS["dim_microbiology_test"] = (
    "Microbiology specimen/test pairs, one row per (spec_itemid, test_itemid).",
    f"""
SELECT xxhash64(spec_itemid, test_itemid) AS microbiology_test_key,
       spec_itemid, MIN(spec_type_desc) AS spec_type_desc,
       test_itemid, MIN(test_name) AS test_name
FROM {S}.microbiologyevents
WHERE test_itemid IS NOT NULL
GROUP BY spec_itemid, test_itemid""")

BUILDS["dim_order_type"] = (
    "One row per distinct (order_type, order_subtype) in Silver poe. order_type has 16 documented values; order_subtype is best read with order_type.",
    f"""
SELECT xxhash64(order_type, order_subtype) AS order_type_key, order_type, order_subtype
FROM (SELECT DISTINCT order_type, order_subtype FROM {S}.poe) t""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts

# COMMAND ----------

BUILDS["fact_admission"] = (
    "One row per hospital admission (hadm_id). los_hours/los_days derived from admittime and dischtime, never clamped. "
    "age_at_admission is anchor_age plus the years between admittime and anchor_year; 91 means 90 or over (MIMIC-IV de-identification).",
    f"""
SELECT
    a.hadm_id,
    p.patient_key,
    at.admission_type_key,
    al.location_key AS admission_location_key,
    dl.location_key AS discharge_location_key,
    {date_key('a.admittime')} AS admit_date_key,
    {date_key('a.dischtime')} AS discharge_date_key,
    a.admittime,
    a.dischtime,
    a.deathtime,
    a.hospital_expire_flag,
    a.insurance,
    a.language,
    a.marital_status,
    a.race,
    p.anchor_age + (year(a.admittime) - p.anchor_year) AS age_at_admission,
    {hours_between('a.admittime', 'a.dischtime')} AS los_hours,
    {hours_between('a.admittime', 'a.dischtime')} / 24.0 AS los_days
FROM {S}.admissions a
LEFT JOIN {G}.dim_patient p ON a.subject_id = p.subject_id
LEFT JOIN {G}.dim_admission_type at ON a.admission_type = at.admission_type
LEFT JOIN {G}.dim_location al ON al.location_type = 'ADMISSION' AND a.admission_location = al.location_name
LEFT JOIN {G}.dim_location dl ON dl.location_type = 'DISCHARGE' AND a.discharge_location = dl.location_name
WHERE a.hadm_id IS NOT NULL""")

BUILDS["fact_icu_stay"] = (
    "One row per ICU stay (stay_id); los_days is the MIMIC-documented fractional-day length of stay.",
    f"""
SELECT
    i.stay_id AS icu_stay_fact_key,
    i.stay_id,
    i.hadm_id,
    p.patient_key,
    fc.careunit_key AS first_careunit_key,
    lc.careunit_key AS last_careunit_key,
    {date_key('i.intime')} AS in_date_key,
    {date_key('i.outtime')} AS out_date_key,
    i.intime,
    i.outtime,
    i.los AS los_days
FROM {S}.icustays i
LEFT JOIN {G}.dim_patient p ON i.subject_id = p.subject_id
LEFT JOIN {G}.dim_careunit fc ON i.first_careunit = fc.careunit_name
LEFT JOIN {G}.dim_careunit lc ON i.last_careunit = lc.careunit_name
WHERE i.stay_id IS NOT NULL""")

BUILDS["fact_transfer"] = (
    "One row per patient transfer/location stay (transfer_id) with derived duration and the Silver transfer-time flag.",
    f"""
SELECT
    t.transfer_id,
    p.patient_key,
    t.hadm_id,
    cu.careunit_key,
    {date_key('t.intime')} AS in_date_key,
    {date_key('t.outtime')} AS out_date_key,
    t.eventtype,
    t.intime,
    t.outtime,
    {hours_between('t.intime', 't.outtime')} AS transfer_duration_hours,
    t.dq_invalid_transfer_time
FROM {S}.transfers t
LEFT JOIN {G}.dim_patient p ON t.subject_id = p.subject_id
LEFT JOIN {G}.dim_careunit cu ON t.careunit = cu.careunit_name
WHERE t.transfer_id IS NOT NULL""")

BUILDS["fact_service"] = (
    "One row per hospital service transfer event (Silver services row). prev_service is NULL for a patient's first service.",
    f"""
SELECT
    s.hadm_id,
    p.patient_key,
    ps.service_key AS previous_service_key,
    cs.service_key AS current_service_key,
    {date_key('s.transfertime')} AS transfer_date_key,
    s.transfertime
FROM {S}.services s
LEFT JOIN {G}.dim_patient p ON s.subject_id = p.subject_id
LEFT JOIN {G}.dim_service ps ON s.prev_service = ps.service
LEFT JOIN {G}.dim_service cs ON s.curr_service = cs.service
WHERE s.hadm_id IS NOT NULL""")

BUILDS["fact_diagnosis"] = (
    "One row per coded hospital diagnosis (hadm_id, seq_num, icd_code, icd_version). seq_num is an approximate priority (MIMIC-IV docs).",
    f"""
SELECT
    xxhash64(d.hadm_id, d.seq_num, d.icd_code, d.icd_version) AS diagnosis_fact_key,
    d.hadm_id,
    p.patient_key,
    dx.diagnosis_key,
    d.seq_num,
    d.seq_num = 1 AS primary_diagnosis_flag,
    {date_key('a.admittime')} AS admit_date_key,
    {date_key('a.dischtime')} AS discharge_date_key
FROM {S}.diagnoses_icd d
LEFT JOIN {G}.dim_patient p ON d.subject_id = p.subject_id
LEFT JOIN {G}.dim_diagnosis dx ON d.icd_code = dx.icd_code AND d.icd_version = dx.icd_version
LEFT JOIN {S}.admissions a ON d.hadm_id = a.hadm_id
WHERE d.hadm_id IS NOT NULL AND d.icd_code IS NOT NULL AND d.icd_version IS NOT NULL""")

BUILDS["fact_procedure"] = (
    "One row per coded hospital procedure (hadm_id, seq_num, icd_code, icd_version).",
    f"""
SELECT
    xxhash64(x.hadm_id, x.seq_num, x.icd_code, x.icd_version) AS procedure_fact_key,
    x.hadm_id,
    dp.patient_key,
    pr.procedure_key,
    x.seq_num,
    {date_key('x.chartdate')} AS procedure_date_key,
    x.chartdate
FROM {S}.procedures_icd x
LEFT JOIN {G}.dim_patient dp ON x.subject_id = dp.subject_id
LEFT JOIN {G}.dim_procedure pr ON x.icd_code = pr.icd_code AND x.icd_version = pr.icd_version
WHERE x.hadm_id IS NOT NULL AND x.icd_code IS NOT NULL AND x.icd_version IS NOT NULL""")

BUILDS["fact_lab"] = (
    "One row per laboratory measurement (labevent_id). hadm_id is NULL for specimens taken outside a hospital stay (MIMIC-IV docs).",
    f"""
SELECT
    l.labevent_id AS lab_fact_key,
    l.hadm_id,
    p.patient_key,
    li.lab_item_key,
    {date_key('l.charttime')} AS lab_date_key,
    l.charttime,
    l.storetime,
    l.valuenum,
    l.value,
    l.valueuom,
    l.ref_range_lower,
    l.ref_range_upper,
    l.flag,
    l.priority
FROM {S}.labevents l
LEFT JOIN {G}.dim_patient p ON l.subject_id = p.subject_id
LEFT JOIN {G}.dim_lab_item li ON l.itemid = li.itemid
WHERE l.labevent_id IS NOT NULL""")

BUILDS["fact_microbiology"] = (
    "One row per microbiology result record (microevent_id): test, organism and isolate level.",
    f"""
SELECT
    m.microevent_id,
    m.micro_specimen_id,
    p.patient_key,
    m.hadm_id,
    mt.microbiology_test_key,
    pr.provider_key,
    {date_key('m.chartdate')} AS chart_date_key,
    {date_key('m.storedate')} AS store_date_key,
    m.charttime,
    m.storetime,
    m.test_seq,
    m.org_itemid,
    m.org_name,
    m.isolate_num,
    m.quantity
FROM {S}.microbiologyevents m
LEFT JOIN {G}.dim_patient p ON m.subject_id = p.subject_id
LEFT JOIN {G}.dim_provider pr ON m.order_provider_id = pr.provider_id
LEFT JOIN {G}.dim_microbiology_test mt ON m.spec_itemid <=> mt.spec_itemid AND m.test_itemid = mt.test_itemid
WHERE m.microevent_id IS NOT NULL""")

BUILDS["fact_prescription"] = (
    "One row per prescription record (a drug component of a pharmacy order; pharmacy_id is not unique). Only prescriptions with an hadm_id.",
    f"""
SELECT
    pr.pharmacy_id,
    pr.poe_id,
    pr.poe_seq,
    p.patient_key,
    pr.hadm_id,
    m.medication_key,
    prov.provider_key,
    {date_key('pr.starttime')} AS start_date_key,
    {date_key('pr.stoptime')} AS stop_date_key,
    pr.starttime,
    pr.stoptime,
    pr.prod_strength,
    pr.form_rx,
    pr.dose_val_rx,
    pr.dose_unit_rx,
    pr.form_val_disp
FROM {S}.prescriptions pr
LEFT JOIN {G}.dim_patient p ON pr.subject_id = p.subject_id
LEFT JOIN {G}.dim_provider prov ON pr.order_provider_id = prov.provider_id
LEFT JOIN {G}.dim_medication m
    ON pr.drug_type <=> m.drug_type AND pr.drug <=> m.drug AND pr.formulary_drug_cd <=> m.formulary_drug_cd
   AND pr.gsn <=> m.gsn AND pr.ndc <=> m.ndc
WHERE pr.hadm_id IS NOT NULL""")

BUILDS["fact_medication_administration"] = (
    "One row per eMAR detail record (parent 'dose due' row has NULL parent_field_ordinal; child rows are doses given - do not sum both). medication_key only where the pharmacy_id maps to exactly one medication. med_admin_fact_key is unique within one build only.",
    f"""
WITH rx_med AS (
    SELECT pr.pharmacy_id, MIN(m.medication_key) AS medication_key
    FROM {S}.prescriptions pr
    JOIN {G}.dim_medication m
      ON pr.drug_type <=> m.drug_type AND pr.drug <=> m.drug AND pr.formulary_drug_cd <=> m.formulary_drug_cd
     AND pr.gsn <=> m.gsn AND pr.ndc <=> m.ndc
    GROUP BY pr.pharmacy_id
    HAVING COUNT(DISTINCT m.medication_key) = 1
),
ed AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY emar_id, parent_field_ordinal
                              ORDER BY administration_type, dose_due, dose_given, product_code, route, site) AS detail_occurrence
    FROM {S}.emar_detail
)
SELECT
    xxhash64(e.emar_id, ed.parent_field_ordinal, ed.detail_occurrence) AS med_admin_fact_key,
    e.hadm_id,
    p.patient_key,
    rm.medication_key,
    {date_key('e.charttime')} AS admin_date_key,
    e.charttime,
    e.scheduletime,
    e.event_txt AS administration_status,
    ed.administration_type,
    ed.barcode_type,
    ed.dose_due,
    ed.dose_due_unit,
    ed.dose_given,
    ed.dose_given_unit,
    ed.route,
    ed.site,
    ed.complete_dose_not_given
FROM {S}.emar e
LEFT JOIN ed ON e.emar_id = ed.emar_id
LEFT JOIN {G}.dim_patient p ON e.subject_id = p.subject_id
LEFT JOIN rx_med rm ON e.pharmacy_id = rm.pharmacy_id
WHERE e.emar_id IS NOT NULL""")

BUILDS["fact_icu_observation"] = (
    "One row per charted ICU observation (every Silver chartevents row; no documented unique id). observation_fact_key is unique within one build only.",
    f"""
WITH c AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY stay_id, itemid, charttime, storetime, caregiver_id, value
                              ORDER BY valuenum, valueuom, warning) AS obs_occurrence
    FROM {S}.chartevents
)
SELECT
    xxhash64(c.stay_id, c.itemid, c.charttime, c.storetime, c.caregiver_id, c.value, c.obs_occurrence) AS observation_fact_key,
    c.stay_id,
    c.hadm_id,
    p.patient_key,
    di.icu_item_key,
    {date_key('c.charttime')} AS chart_date_key,
    c.charttime,
    c.storetime,
    c.valuenum,
    c.value,
    c.valueuom,
    c.warning
FROM c
LEFT JOIN {G}.dim_patient p ON c.subject_id = p.subject_id
LEFT JOIN {G}.dim_icu_item di ON c.itemid = di.itemid
WHERE c.stay_id IS NOT NULL AND c.itemid IS NOT NULL""")

BUILDS["fact_order"] = (
    "One row per provider order (poe_id, unique; MIMIC-IV hosp poe). No rows filtered; Silver dq_* flags carried. is_discontinued = discontinued_by_poe_id IS NOT NULL (our rule).",
    f"""
SELECT
    o.poe_id,
    o.poe_seq,
    p.patient_key,
    o.hadm_id,
    ot.order_type_key,
    pr.provider_key AS order_provider_key,
    {date_key('o.ordertime')} AS order_date_key,
    o.ordertime,
    o.transaction_type,
    o.order_status,
    o.discontinue_of_poe_id,
    o.discontinued_by_poe_id,
    o.discontinued_by_poe_id IS NOT NULL AS is_discontinued,
    o.dq_hadm_not_in_admissions,
    o.dq_discontinue_of_poe_id_not_in_poe,
    o.dq_discontinued_by_poe_id_not_in_poe,
    o.dq_ordertime_before_previous_seq
FROM {S}.poe o
LEFT JOIN {G}.dim_patient p ON o.subject_id = p.subject_id
LEFT JOIN {G}.dim_order_type ot ON o.order_type <=> ot.order_type AND o.order_subtype <=> ot.order_subtype
LEFT JOIN {G}.dim_provider pr ON o.order_provider_id = pr.provider_id""")

BUILDS["fact_order_detail"] = (
    "One row per order detail (poe_id, field_name; MIMIC-IV hosp poe_detail). Holds the extra attributes of some orders (e.g. Admit to, Code status, Tubes & Drains type). Join to fact_order on poe_id.",
    f"""
SELECT
    d.poe_id,
    d.poe_seq,
    p.patient_key,
    d.field_name,
    d.field_value
FROM {S}.poe_detail d
LEFT JOIN {G}.dim_patient p ON d.subject_id = p.subject_id""")

# dependency order: every dimension before any fact
ORDER = [
    "dim_date", "dim_patient", "dim_provider", "dim_careunit", "dim_admission_type", "dim_location",
    "dim_diagnosis", "dim_procedure", "dim_lab_item", "dim_icu_item", "dim_medication", "dim_service",
    "dim_microbiology_test", "dim_order_type",
    "fact_admission", "fact_icu_stay", "fact_transfer", "fact_service", "fact_diagnosis", "fact_procedure",
    "fact_microbiology", "fact_order", "fact_order_detail", "fact_prescription", "fact_medication_administration",
    "fact_lab", "fact_icu_observation",
]
assert sorted(ORDER) == sorted(BUILDS), "ORDER and BUILDS disagree"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build

# COMMAND ----------

selected = ORDER if TABLES_ARG.lower() == "all" else [t.strip() for t in TABLES_ARG.split(",") if t.strip()]
unknown = [t for t in selected if t not in BUILDS]
assert not unknown, f"Unknown tables: {unknown}"
selected = [t for t in ORDER if t in selected]
print("Tables to build, in order:", selected)

if not BUILD_APPROVED:
    raise RuntimeError("Set widget build_approved=true to run the builds.")

for name in selected:
    comment, select_sql = BUILDS[name]
    spark.sql(f"CREATE OR REPLACE TABLE {G}.{name} COMMENT '{comment.replace(chr(39), chr(39) * 2)}' AS {select_sql}")
    print("Built:", f"{G}.{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks: grain, row reconciliation, foreign keys

# COMMAND ----------

GRAIN = {   # table -> (key columns, Silver row-count expression or None)
    "dim_patient": (["patient_key"], f"SELECT COUNT(*) FROM {S}.patients WHERE subject_id IS NOT NULL"),
    "dim_provider": (["provider_key"], f"SELECT COUNT(*) FROM {S}.provider WHERE provider_id IS NOT NULL"),
    "dim_careunit": (["careunit_key"], None),
    "dim_admission_type": (["admission_type_key"], None),
    "dim_location": (["location_key"], None),
    "dim_diagnosis": (["diagnosis_key"], None),
    "dim_procedure": (["procedure_key"], None),
    "dim_lab_item": (["lab_item_key"], None),
    "dim_icu_item": (["icu_item_key"], None),
    "dim_medication": (["medication_key"], None),
    "dim_service": (["service_key"], None),
    "dim_microbiology_test": (["microbiology_test_key"], None),
    "dim_order_type": (["order_type_key"], None),
    "dim_date": (["date_key"], None),
    "fact_admission": (["hadm_id"], f"SELECT COUNT(*) FROM {S}.admissions WHERE hadm_id IS NOT NULL"),
    "fact_icu_stay": (["stay_id"], f"SELECT COUNT(*) FROM {S}.icustays WHERE stay_id IS NOT NULL"),
    "fact_transfer": (["transfer_id"], f"SELECT COUNT(*) FROM {S}.transfers WHERE transfer_id IS NOT NULL"),
    "fact_service": (None, f"SELECT COUNT(*) FROM {S}.services WHERE hadm_id IS NOT NULL"),
    "fact_diagnosis": (["diagnosis_fact_key"], f"SELECT COUNT(*) FROM {S}.diagnoses_icd WHERE hadm_id IS NOT NULL AND icd_code IS NOT NULL AND icd_version IS NOT NULL"),
    "fact_procedure": (["procedure_fact_key"], f"SELECT COUNT(*) FROM {S}.procedures_icd WHERE hadm_id IS NOT NULL AND icd_code IS NOT NULL AND icd_version IS NOT NULL"),
    "fact_lab": (["lab_fact_key"], f"SELECT COUNT(*) FROM {S}.labevents WHERE labevent_id IS NOT NULL"),
    "fact_microbiology": (["microevent_id"], f"SELECT COUNT(*) FROM {S}.microbiologyevents WHERE microevent_id IS NOT NULL"),
    "fact_prescription": (None, f"SELECT COUNT(*) FROM {S}.prescriptions WHERE hadm_id IS NOT NULL"),
    "fact_medication_administration": (["med_admin_fact_key"], f"SELECT COUNT(*) FROM {S}.emar e LEFT JOIN {S}.emar_detail d ON e.emar_id = d.emar_id"),
    "fact_icu_observation": (["observation_fact_key"], f"SELECT COUNT(*) FROM {S}.chartevents WHERE stay_id IS NOT NULL AND itemid IS NOT NULL"),
    "fact_order": (["poe_id"], f"SELECT COUNT(*) FROM {S}.poe"),
    "fact_order_detail": (["poe_id", "field_name"], f"SELECT COUNT(*) FROM {S}.poe_detail"),
}

for name in selected:
    keys, expected_sql = GRAIN[name]
    rows = spark.table(f"{G}.{name}").count()
    msg = f"{name}: {rows:,} rows"
    if keys:
        distinct = spark.sql(f"SELECT COUNT(DISTINCT {', '.join(keys)}) FROM {G}.{name}").first()[0]
        msg += f", {distinct:,} distinct {'/'.join(keys)}"
        assert distinct == rows, f"{name}: key not unique"
    if expected_sql:
        expected = spark.sql(expected_sql).first()[0]
        msg += f", Silver {expected:,}"
        assert rows == expected, f"{name}: row count differs from Silver"
    print(msg)

# COMMAND ----------

FKS = [   # (fact, column, dimension, dimension key)
    ("fact_admission", "patient_key", "dim_patient", "patient_key"),
    ("fact_admission", "admission_type_key", "dim_admission_type", "admission_type_key"),
    ("fact_admission", "admission_location_key", "dim_location", "location_key"),
    ("fact_admission", "discharge_location_key", "dim_location", "location_key"),
    ("fact_admission", "admit_date_key", "dim_date", "date_key"),
    ("fact_admission", "discharge_date_key", "dim_date", "date_key"),
    ("fact_diagnosis", "patient_key", "dim_patient", "patient_key"),
    ("fact_diagnosis", "diagnosis_key", "dim_diagnosis", "diagnosis_key"),
    ("fact_diagnosis", "admit_date_key", "dim_date", "date_key"),
    ("fact_diagnosis", "discharge_date_key", "dim_date", "date_key"),
    ("fact_icu_stay", "patient_key", "dim_patient", "patient_key"),
    ("fact_icu_stay", "first_careunit_key", "dim_careunit", "careunit_key"),
    ("fact_icu_stay", "last_careunit_key", "dim_careunit", "careunit_key"),
    ("fact_icu_stay", "in_date_key", "dim_date", "date_key"),
    ("fact_icu_stay", "out_date_key", "dim_date", "date_key"),
    ("fact_transfer", "patient_key", "dim_patient", "patient_key"),
    ("fact_transfer", "careunit_key", "dim_careunit", "careunit_key"),
    ("fact_transfer", "in_date_key", "dim_date", "date_key"),
    ("fact_transfer", "out_date_key", "dim_date", "date_key"),
    ("fact_service", "patient_key", "dim_patient", "patient_key"),
    ("fact_service", "previous_service_key", "dim_service", "service_key"),
    ("fact_service", "current_service_key", "dim_service", "service_key"),
    ("fact_service", "transfer_date_key", "dim_date", "date_key"),
    ("fact_procedure", "patient_key", "dim_patient", "patient_key"),
    ("fact_procedure", "procedure_key", "dim_procedure", "procedure_key"),
    ("fact_procedure", "procedure_date_key", "dim_date", "date_key"),
    ("fact_lab", "patient_key", "dim_patient", "patient_key"),
    ("fact_lab", "lab_item_key", "dim_lab_item", "lab_item_key"),
    ("fact_lab", "lab_date_key", "dim_date", "date_key"),
    ("fact_microbiology", "patient_key", "dim_patient", "patient_key"),
    ("fact_microbiology", "microbiology_test_key", "dim_microbiology_test", "microbiology_test_key"),
    ("fact_microbiology", "provider_key", "dim_provider", "provider_key"),
    ("fact_microbiology", "chart_date_key", "dim_date", "date_key"),
    ("fact_microbiology", "store_date_key", "dim_date", "date_key"),
    ("fact_prescription", "patient_key", "dim_patient", "patient_key"),
    ("fact_prescription", "medication_key", "dim_medication", "medication_key"),
    ("fact_prescription", "provider_key", "dim_provider", "provider_key"),
    ("fact_prescription", "start_date_key", "dim_date", "date_key"),
    ("fact_prescription", "stop_date_key", "dim_date", "date_key"),
    ("fact_medication_administration", "patient_key", "dim_patient", "patient_key"),
    ("fact_medication_administration", "medication_key", "dim_medication", "medication_key"),
    ("fact_medication_administration", "admin_date_key", "dim_date", "date_key"),
    ("fact_icu_observation", "patient_key", "dim_patient", "patient_key"),
    ("fact_icu_observation", "icu_item_key", "dim_icu_item", "icu_item_key"),
    ("fact_icu_observation", "chart_date_key", "dim_date", "date_key"),
    ("fact_order", "patient_key", "dim_patient", "patient_key"),
    ("fact_order", "order_type_key", "dim_order_type", "order_type_key"),
    ("fact_order", "order_provider_key", "dim_provider", "provider_key"),
    ("fact_order", "order_date_key", "dim_date", "date_key"),
    ("fact_order_detail", "patient_key", "dim_patient", "patient_key"),
]

failures = []
for f, c, d, k in FKS:
    if f not in selected and d not in selected:
        continue
    r = spark.sql(f"""
        SELECT SUM(CASE WHEN x.{c} IS NULL THEN 1 ELSE 0 END) AS null_keys,
               SUM(CASE WHEN x.{c} IS NOT NULL AND y.{k} IS NULL THEN 1 ELSE 0 END) AS orphan_keys
        FROM {G}.{f} x
        LEFT JOIN (SELECT DISTINCT {k} FROM {G}.{d}) y ON x.{c} = y.{k}""").first()
    print(f"{f}.{c} -> {d}: {r.orphan_keys} orphan, {r.null_keys} NULL (NULL = no source value or no match)")
    if r.orphan_keys:
        failures.append(f"{f}.{c}")

assert not failures, f"Foreign keys with no matching dimension row: {failures}"
dbutils.notebook.exit("gold_build_all:OK")