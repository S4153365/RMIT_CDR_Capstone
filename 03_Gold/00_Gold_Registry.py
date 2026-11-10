# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Registry and Build Definitions
# MAGIC
# MAGIC Single source of truth for the 24 Gold tables: the `SELECT` that builds each table, its grain, keys,
# MAGIC Silver sources, expected Silver column contract, validation checks and readiness axes. The runner
# MAGIC (`99_Gold_Run_All`), `Dimensions`, `Facts` and `90_Gold_Validation` all read this file, so the build logic
# MAGIC exists in exactly one place. Plain Python only (no Spark), so it can be tested locally.
# MAGIC
# MAGIC `cdr_mimiciv.bronze` -> `cdr_mimiciv.silver` -> `cdr_mimiciv.gold`. Source and target schemas are parameters;
# MAGIC nothing here hard-codes a schema. `poe` and `poe_detail` are in scope as of 2026-10-26; the three POE
# MAGIC orders tables are built outside this registry by `Gold_Build_All`, not by `99_Gold_Run_All`.
# MAGIC
# MAGIC **Which build produced the tables in the catalog.** The 27 Gold tables were built by
# MAGIC `03_Gold/Gold_Build_All.py` (standalone, gated on its own `build_approved` widget). This registry and
# MAGIC `99_Gold_Run_All` remain the harder-gated path: several tables need approvals and a reviewed
# MAGIC candidate-table validation before a real run, so nothing here builds today. See README.md,
# MAGIC "Two Gold build paths".
# MAGIC
# MAGIC ## Conventions (this file is the specification; `docs/gold_governance_checks.md` §4 records the
# MAGIC key changes between Gold builds)
# MAGIC - **Surrogate keys** are `xxhash64` over ONE string built by a self-delimiting encoding of the key parts: each part is
# MAGIC   cast to STRING, then NULL becomes `N;` and any value `v` becomes `V<length>:<v>`. NULL is therefore distinguishable
# MAGIC   from every real value (including the empty string and the text `N;`), and field boundaries are preserved
# MAGIC   (`('ab','c')` and `('a','bc')` encode differently). Hashing one STRING also removes the INT/BIGINT sensitivity the
# MAGIC   Databricks documentation describes for `xxhash64`. Timestamps enter keys as `unix_micros` (time-zone independent).
# MAGIC   A 64-bit hash can still collide, so uniqueness and a natural-grain count are validated, never assumed.
# MAGIC - **Occurrence numbers** (`rx_occurrence`, `detail_occurrence`, `service_occurrence`) are `ROW_NUMBER()` over rows that tie on
# MAGIC   the ordering columns. They make keys unique *within one build*; they are NOT a stable source-record identity (see
# MAGIC   `docs/gold_governance_checks.md` §4). Do not join to them from outside a single build.
# MAGIC - **Natural keys** are used where MIMIC documents a unique id (`labevent_id`, `microevent_id`, `transfer_id`, `stay_id`, `hadm_id`).
# MAGIC - **Foreign keys** come from `LEFT JOIN` to the dimension on its business key. An unmatched row keeps a NULL key
# MAGIC   (no "unknown" member is invented). Dimensions are validated unique on that business key first, so a join cannot
# MAGIC   multiply fact rows.
# MAGIC - **Identifier columns** (`subject_id`, `hadm_id`, `stay_id`, `itemid`, ...) keep the type of the reviewed Silver source; Gold does
# MAGIC   not narrow them. Types are therefore not uniform across facts (for example `hadm_id` is bigint in `diagnoses_icd`,
# MAGIC   int in `transfers`, unverified in `admissions`). Governance functions that depend on a type (the `dim_patient.subject_id`
# MAGIC   mask is declared for INT) are handled and documented separately in `docs/gold_governance_checks.md`.
# MAGIC - **ANSI mode** is switched on by the engine so an invalid cast (for example in join-only expressions) fails instead of becoming NULL.
# MAGIC - **No cleaning in Gold.** Unusual source values are carried through. Silver `dq_*` flags are carried as columns.
# MAGIC - **Durations** (`los_hours`, `los_days`, `transfer_duration_hours`) are derived here from the preserved timestamps and
# MAGIC   are never clamped, so a reversed interval gives a negative duration next to its Silver flag.

# COMMAND ----------

import re

DEFAULT_SOURCE_SCHEMA = "cdr_mimiciv.silver"
DEFAULT_TARGET_SCHEMA = "cdr_mimiciv.gold"
SCHEMA_PATTERN = r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*"
DATE_PATTERN = r"\d{4}-\d{2}-\d{2}"
LARGE_TABLE_ROWS = 2_500_000          # cost rule: no repeated scans or window/exceptAll work above this without confirmation

# Date dimension defaults. MIMIC-IV shifts dates into the future (documentation: between 2100 and 2200 for the
# anchor; observed source dates run past 2210, to 2214). Events can precede the anchor year, so the range starts
# earlier. The range is a parameter and `90_Gold_Validation` reports any fact date key that falls outside it.
DEFAULT_DATE_START = "2090-01-01"
DEFAULT_DATE_END = "2250-12-31"

# Large builds are not publishable yet. They are not staged (staging would re-evaluate a huge SELECT several times), so today
# their results could only be validated AFTER the existing Gold table was replaced. Until a reviewed candidate-table approach exists
# (build to a candidate table, run the same checks on it, only then promote; see docs/gold_build_cost.md) every scan="large"
# table is refused by `require_executable`, even with approvals and `confirm_large_scan`. Previews and read-only validation of
# tables that already exist are unaffected.
CANDIDATE_VALIDATION_APPROVED = False
LARGE_BUILD_BLOCK_REASON = ("large build blocked: its results cannot yet be validated BEFORE existing Gold is replaced "
                            "(a reviewed candidate-table approach is required first; docs/gold_build_cost.md)")

# Silver sources a Gold build must never read. The mechanism stays; the set is deliberately EMPTY.
# `poe` and `poe_detail` were excluded on 2026-10-19 and reinstated on 2026-10-26; the Gold orders tables
# (`dim_order_type`, `fact_order`, `fact_order_detail`) are built from them by `Gold_Build_All.py`.
EXCLUDED_SOURCES = {}


# COMMAND ----------

# SQL building blocks

def _txt(col):
    """STRING form of a column for key hashing (identifiers, numbers, dates)."""
    return f"CAST({col} AS STRING)"


def _ts(col):
    """Time-zone independent text of a timestamp for key hashing."""
    return f"CAST(unix_micros({col}) AS STRING)"


def _encode_part(expr):
    """Length-prefixed, self-delimiting encoding of one key part (expr is a STRING expression).

    NULL         -> 'N;'
    any string v -> 'V' || length(v) || ':' || v      (so '' -> 'V0:', and the text 'N;' -> 'V2:N;')
    The two forms can never be confused, and because the length precedes the value, concatenated parts cannot shift
    characters across a field boundary (('ab','c') and ('a','bc') encode differently).
    """
    return f"COALESCE(concat('V', CAST(length({expr}) AS STRING), ':', {expr}), 'N;')"


def key_hash(*string_parts):
    """xxhash64 of ONE unambiguous string built from the encoded parts. A 64-bit hash can still collide in principle,
    so uniqueness and the natural-grain count are checked by validation, never assumed."""
    return "xxhash64(concat(" + ", ".join(_encode_part(p) for p in string_parts) + "))"


def date_key(col):
    """yyyymmdd INT from a DATE or TIMESTAMP column (NULL stays NULL). Uses the session time zone for timestamps."""
    return f"CAST(date_format({col}, 'yyyyMMdd') AS INT)"


def duration_hours(start, end):
    """Hours between two timestamps; NULL when either is NULL; negative when reversed (never clamped)."""
    return (f"CASE WHEN {start} IS NOT NULL AND {end} IS NOT NULL "
            f"THEN CAST(unix_micros({end}) - unix_micros({start}) AS DOUBLE) / 3600000000.0 END")


MEDICATION_COLUMNS = ["drug_type", "drug", "formulary_drug_cd", "gsn", "ndc"]


def medication_key(alias=""):
    """The ONE definition of dim_medication.medication_key, used by the dimension and by both consuming facts."""
    prefix = f"{alias}." if alias else ""
    return key_hash(*[_txt(prefix + c) for c in MEDICATION_COLUMNS])


EMAR_DETAIL_COLUMNS = [
    "subject_id", "emar_id", "emar_seq", "parent_field_ordinal", "administration_type", "pharmacy_id", "barcode_type",
    "reason_for_no_barcode", "complete_dose_not_given", "dose_due", "dose_due_unit", "dose_given", "dose_given_unit",
    "will_remainder_of_dose_be_given", "product_amount_given", "product_unit", "product_code", "product_description",
    "product_description_other", "prior_infusion_rate", "infusion_rate", "infusion_rate_adjustment",
    "infusion_rate_adjustment_amount", "infusion_rate_unit", "route", "infusion_complete", "completion_interval",
    "new_iv_bag_hung", "continued_infusion_in_other_location", "restart_interval", "side", "site",
    "non_formulary_visual_verification",
]
ADMISSION_DQ_FLAGS = ["dq_invalid_admission_time", "dq_invalid_ed_time", "dq_death_before_admission",
                      "dq_death_after_discharge", "dq_expire_deathtime_mismatch"]
TRANSFER_DQ_FLAGS = ["dq_invalid_transfer_time", "dq_missing_outtime", "dq_hadm_subject_mismatch"]


# COMMAND ----------

# Dimension SELECTs. Each builder returns one SELECT (optionally starting with WITH); the engine wraps it in
# CREATE OR REPLACE TABLE ... USING DELTA COMMENT ... AS. `S` = source schema, `G` = target schema, `p` = parameters.

def select_dim_date(S, G, p):
    return f"""
WITH date_range AS (
    SELECT sequence(DATE '{p['date_start']}', DATE '{p['date_end']}', INTERVAL 1 DAY) AS dates
)
SELECT
    CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
    d AS full_date,
    year(d) AS year,
    quarter(d) AS quarter,
    month(d) AS month,
    date_format(d, 'MMMM') AS month_name,
    day(d) AS day_of_month,
    weekofyear(d) AS week_of_year,
    dayofweek(d) AS day_of_week,
    date_format(d, 'EEEE') AS day_name
FROM date_range
LATERAL VIEW explode(dates) t AS d"""


def select_dim_patient(S, G, p):
    return f"""
SELECT
    {key_hash(_txt('subject_id'))} AS patient_key,
    subject_id,
    gender,
    anchor_age,
    anchor_year,
    anchor_year_group,
    age_deidentified_90_plus AS age_deidentified_flag
FROM {S}.patients
WHERE subject_id IS NOT NULL"""


def select_dim_careunit(S, G, p):
    return f"""
WITH careunits AS (
    SELECT first_careunit AS careunit_name FROM {S}.icustays WHERE first_careunit IS NOT NULL
    UNION
    SELECT last_careunit AS careunit_name FROM {S}.icustays WHERE last_careunit IS NOT NULL
    UNION
    SELECT careunit AS careunit_name FROM {S}.transfers WHERE careunit IS NOT NULL
)
SELECT
    {key_hash(_txt('careunit_name'))} AS careunit_key,
    careunit_name
FROM careunits"""


def select_dim_admission_type(S, G, p):
    return f"""
SELECT
    {key_hash(_txt('admission_type'))} AS admission_type_key,
    admission_type
FROM (SELECT DISTINCT admission_type FROM {S}.admissions WHERE admission_type IS NOT NULL)"""


def select_dim_location(S, G, p):
    return f"""
WITH locations AS (
    SELECT 'ADMISSION' AS location_type, admission_location AS location_name
    FROM {S}.admissions WHERE admission_location IS NOT NULL
    UNION
    SELECT 'DISCHARGE' AS location_type, discharge_location AS location_name
    FROM {S}.admissions WHERE discharge_location IS NOT NULL
)
SELECT
    {key_hash(_txt('location_type'), _txt('location_name'))} AS location_key,
    location_type,
    location_name
FROM locations"""


def select_dim_diagnosis(S, G, p):
    return f"""
SELECT
    {key_hash(_txt('icd_code'), _txt('icd_version'))} AS diagnosis_key,
    icd_code,
    icd_version,
    long_title AS diagnosis_title
FROM {S}.d_icd_diagnoses
WHERE icd_code IS NOT NULL AND icd_version IS NOT NULL"""


def select_dim_procedure(S, G, p):
    return f"""
SELECT
    {key_hash(_txt('icd_code'), _txt('icd_version'))} AS procedure_key,
    icd_code,
    icd_version,
    long_title AS procedure_title
FROM {S}.d_icd_procedures
WHERE icd_code IS NOT NULL AND icd_version IS NOT NULL"""


def select_dim_lab_item(S, G, p):
    return f"""
SELECT
    {key_hash(_txt('itemid'))} AS lab_item_key,
    itemid,
    label AS lab_test_name,
    fluid,
    category
FROM {S}.d_labitems
WHERE itemid IS NOT NULL"""


def select_dim_icu_item(S, G, p):
    return f"""
SELECT
    {key_hash(_txt('itemid'))} AS icu_item_key,
    itemid,
    label AS item_name,
    abbreviation,
    linksto,
    category,
    unitname,
    param_type
FROM {S}.d_items
WHERE itemid IS NOT NULL"""


def select_dim_medication(S, G, p):
    # Grain: one row per distinct (drug_type, drug, formulary_drug_cd, gsn, ndc) with a drug name. This is the notebook
    # definition the existing facts join on; the captured dim_medication.sql (key on drug, ndc only; no gsn) is superseded.
    cols = ", ".join(MEDICATION_COLUMNS)
    return f"""
SELECT
    {medication_key()} AS medication_key,
    {cols}
FROM (
    SELECT DISTINCT {cols}
    FROM {S}.prescriptions
    WHERE drug IS NOT NULL
)"""


def select_dim_provider(S, G, p):
    return f"""
SELECT
    {key_hash(_txt('provider_id'))} AS provider_key,
    provider_id
FROM {S}.provider
WHERE provider_id IS NOT NULL"""


def select_dim_service(S, G, p):
    return f"""
WITH services AS (
    SELECT curr_service AS service FROM {S}.services WHERE curr_service IS NOT NULL
    UNION
    SELECT prev_service AS service FROM {S}.services WHERE prev_service IS NOT NULL
)
SELECT
    {key_hash(_txt('service'))} AS service_key,
    service
FROM services"""


def select_dim_microbiology_test(S, G, p):
    # Grain: one row per (spec_itemid, test_itemid). The names are functionally dependent on the ids in MIMIC; MIN() only
    # collapses identical values, and the pre-check blocks the build if any pair has more than one name.
    return f"""
SELECT
    {key_hash(_txt('spec_itemid'), _txt('test_itemid'))} AS microbiology_test_key,
    spec_itemid,
    MIN(spec_type_desc) AS spec_type_desc,
    test_itemid,
    MIN(test_name) AS test_name
FROM {S}.microbiologyevents
WHERE test_itemid IS NOT NULL
GROUP BY spec_itemid, test_itemid"""


# COMMAND ----------

# Fact SELECTs

def select_fact_admission(S, G, p):
    flags = ",\n    ".join(f"a.{f}" for f in ADMISSION_DQ_FLAGS)
    return f"""
WITH adm AS (
    SELECT a.*, {duration_hours('a.admittime', 'a.dischtime')} AS los_hours
    FROM {S}.admissions a
    WHERE a.hadm_id IS NOT NULL
)
SELECT
    a.hadm_id,
    p.patient_key,
    t.admission_type_key,
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
    a.los_hours,
    a.los_hours / 24.0 AS los_days,
    {flags}
FROM adm a
LEFT JOIN {G}.dim_patient p ON a.subject_id = p.subject_id
LEFT JOIN {G}.dim_admission_type t ON a.admission_type = t.admission_type
LEFT JOIN {G}.dim_location al ON al.location_type = 'ADMISSION' AND a.admission_location = al.location_name
LEFT JOIN {G}.dim_location dl ON dl.location_type = 'DISCHARGE' AND a.discharge_location = dl.location_name"""


def select_fact_icu_stay(S, G, p):
    return f"""
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
WHERE i.stay_id IS NOT NULL"""


def select_fact_transfer(S, G, p):
    flags = ",\n    ".join(f"t.{f}" for f in TRANSFER_DQ_FLAGS)
    return f"""
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
    {duration_hours('t.intime', 't.outtime')} AS transfer_duration_hours,
    {flags}
FROM {S}.transfers t
LEFT JOIN {G}.dim_patient p ON t.subject_id = p.subject_id
LEFT JOIN {G}.dim_careunit cu ON t.careunit = cu.careunit_name
WHERE t.transfer_id IS NOT NULL"""


def select_fact_diagnosis(S, G, p):
    # Admission dates come from silver.admissions (unique on hadm_id, pre-checked), not from fact_admission,
    # so the facts do not depend on each other and share one date definition.
    key = key_hash(_txt('d.subject_id'), _txt('d.hadm_id'), _txt('d.seq_num'), _txt('d.icd_code'), _txt('d.icd_version'))
    return f"""
SELECT
    {key} AS diagnosis_fact_key,
    d.hadm_id,
    p.patient_key,
    dx.diagnosis_key,
    d.seq_num,
    CASE WHEN d.seq_num = 1 THEN TRUE ELSE FALSE END AS primary_diagnosis_flag,
    {date_key('a.admittime')} AS admit_date_key,
    {date_key('a.dischtime')} AS discharge_date_key
FROM {S}.diagnoses_icd d
LEFT JOIN {G}.dim_patient p ON d.subject_id = p.subject_id
LEFT JOIN {G}.dim_diagnosis dx ON d.icd_code = dx.icd_code AND d.icd_version = dx.icd_version
LEFT JOIN {S}.admissions a ON d.hadm_id = a.hadm_id
WHERE d.hadm_id IS NOT NULL AND d.icd_code IS NOT NULL AND d.icd_version IS NOT NULL"""


def select_fact_procedure(S, G, p):
    key = key_hash(_txt('x.subject_id'), _txt('x.hadm_id'), _txt('x.seq_num'), _txt('x.chartdate'),
                   _txt('x.icd_code'), _txt('x.icd_version'))
    return f"""
SELECT
    {key} AS procedure_fact_key,
    x.hadm_id,
    dp.patient_key,
    pr.procedure_key,
    x.seq_num,
    {date_key('x.chartdate')} AS procedure_date_key,
    x.chartdate
FROM {S}.procedures_icd x
LEFT JOIN {G}.dim_patient dp ON x.subject_id = dp.subject_id
LEFT JOIN {G}.dim_procedure pr ON x.icd_code = pr.icd_code AND x.icd_version = pr.icd_version
WHERE x.hadm_id IS NOT NULL AND x.icd_code IS NOT NULL AND x.icd_version IS NOT NULL"""


def select_fact_lab(S, G, p):
    return f"""
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
WHERE l.labevent_id IS NOT NULL"""


def select_fact_icu_observation(S, G, p):
    # DEFERRED: silver.chartevents is not prepared for publication. chartevents has no documented unique row id; the key
    # hashes every identifying column and uniqueness is checked as an observation (a row-number tiebreak over ~433M rows
    # is deliberately not built).
    key = key_hash(_txt('c.stay_id'), _txt('c.itemid'), _ts('c.charttime'), _ts('c.storetime'),
                   _txt('c.value'), _txt('c.valuenum'), _txt('c.valueuom'), _txt('c.warning'), _txt('c.caregiver_id'))
    return f"""
SELECT
    {key} AS observation_fact_key,
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
    c.warning,
    c._dq_missing_observation AS dq_missing_observation,
    c._dq_early_storetime AS dq_early_storetime
FROM {S}.chartevents c
LEFT JOIN {G}.dim_patient p ON c.subject_id = p.subject_id
LEFT JOIN {G}.dim_icu_item di ON c.itemid = di.itemid
WHERE c.stay_id IS NOT NULL AND c.itemid IS NOT NULL"""


PRESCRIPTION_COLUMNS = ["subject_id", "hadm_id", "pharmacy_id", "poe_id", "poe_seq", "order_provider_id", "starttime",
                        "stoptime", "drug_type", "drug", "formulary_drug_cd", "gsn", "ndc", "prod_strength", "form_rx",
                        "dose_val_rx", "dose_unit_rx", "form_val_disp"]
PRESCRIPTION_IDENTITY = ["subject_id", "hadm_id", "pharmacy_id", "drug_type", "drug", "formulary_drug_cd", "gsn", "ndc",
                         "starttime", "stoptime"]
PRESCRIPTION_TIEBREAK = ["prod_strength", "form_rx", "dose_val_rx", "dose_unit_rx", "form_val_disp", "order_provider_id",
                         "poe_id", "poe_seq"]


def select_fact_prescription(S, G, p):
    # Grain: one prescription row (a drug component of an order). pharmacy_id is NOT assumed unique (MAIN/BASE/ADDITIVE rows).
    # `rx_occurrence` numbers rows that are identical on the identity columns so the key stays unique without dropping rows.
    cols = ", ".join(PRESCRIPTION_COLUMNS)
    partition = ", ".join(PRESCRIPTION_IDENTITY)
    order = ", ".join(PRESCRIPTION_TIEBREAK)
    key = key_hash(_txt('pr.subject_id'), _txt('pr.hadm_id'), _txt('pr.pharmacy_id'), _txt('pr.drug_type'),
                   _txt('pr.drug'), _txt('pr.formulary_drug_cd'), _txt('pr.gsn'), _txt('pr.ndc'),
                   _ts('pr.starttime'), _ts('pr.stoptime'), _txt('pr.rx_occurrence'))
    return f"""
WITH rx AS (
    SELECT {cols},
           ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order}) AS rx_occurrence
    FROM {S}.prescriptions
    WHERE hadm_id IS NOT NULL
)
SELECT
    {key} AS prescription_fact_key,
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
FROM rx pr
LEFT JOIN {G}.dim_patient p ON pr.subject_id = p.subject_id
LEFT JOIN {G}.dim_provider prov ON pr.order_provider_id = prov.provider_id
LEFT JOIN {G}.dim_medication m
    ON m.medication_key = CASE WHEN pr.drug IS NOT NULL THEN {medication_key('pr')} END"""


def select_fact_medication_administration(S, G, p):
    # Grain: one row per eMAR record, built with a FULL OUTER JOIN so that no record is discarded:
    #   MATCHED     an emar_detail record whose emar_id exists in emar        (one row per detail record)
    #   HEADER_ONLY an emar event with no emar_detail record                  (one row for the event)
    #   DETAIL_ONLY an emar_detail record whose emar_id is absent from emar   (one row per detail record, orphan)
    # `record_match_status` carries the class so consumers can count them separately.
    # Medication link: prescriptions can hold several rows per pharmacy_id, so a row-for-row join would multiply rows.
    # pharmacy_id is resolved to a medication_key ONLY when every prescription row for it names the same medication;
    # otherwise the key is NULL and `medication_link_status` explains why. No prescription is ever chosen arbitrarily.
    detail_hash = key_hash(*[_txt("ed." + c) for c in EMAR_DETAIL_COLUMNS if c not in ("emar_id", "parent_field_ordinal")])
    carried = [c for c in EMAR_DETAIL_COLUMNS if c not in ("subject_id", "emar_id", "emar_seq", "pharmacy_id")]
    detail_cols = ", ".join(f"ed.{c}" for c in carried)
    key = key_hash(_txt("COALESCE(e.emar_id, d.emar_id)"), _txt("d.parent_field_ordinal"), _txt("d.detail_occurrence"))
    return f"""
WITH detail AS (
    SELECT ed.emar_id, ed.subject_id AS detail_subject_id, ed.pharmacy_id AS detail_pharmacy_id, {detail_cols},
           ROW_NUMBER() OVER (PARTITION BY ed.emar_id, ed.parent_field_ordinal ORDER BY {detail_hash}) AS detail_occurrence
    FROM {S}.emar_detail ed
),
rx_medication AS (
    SELECT
        CAST(pharmacy_id AS BIGINT) AS pharmacy_id,
        COUNT(DISTINCT medkey) AS n_medication_keys,
        CASE WHEN COUNT(DISTINCT medkey) = 1 THEN MIN(medkey) END AS medication_key
    FROM (
        SELECT pharmacy_id,
               CASE WHEN drug IS NOT NULL THEN {medication_key()} END AS medkey
        FROM {S}.prescriptions
        WHERE pharmacy_id IS NOT NULL
    )
    GROUP BY CAST(pharmacy_id AS BIGINT)
)
SELECT
    {key} AS med_admin_fact_key,
    COALESCE(e.emar_id, d.emar_id) AS emar_id,
    CASE
        WHEN e.emar_id IS NULL THEN 'DETAIL_ONLY'
        WHEN d.emar_id IS NULL THEN 'HEADER_ONLY'
        ELSE 'MATCHED'
    END AS record_match_status,
    e.hadm_id,
    pt.patient_key,
    rx.medication_key,
    CASE
        WHEN COALESCE(e.pharmacy_id, d.detail_pharmacy_id) IS NULL THEN 'NO_PHARMACY_ID'
        WHEN rx.pharmacy_id IS NULL THEN 'NO_PRESCRIPTION'
        WHEN rx.n_medication_keys = 0 THEN 'NO_DRUG_NAME'
        WHEN rx.n_medication_keys = 1 THEN 'UNIQUE'
        ELSE 'AMBIGUOUS'
    END AS medication_link_status,
    {date_key('e.charttime')} AS admin_date_key,
    e.charttime,
    e.scheduletime,
    e.medication AS emar_medication,
    e.event_txt AS administration_status,
    d.parent_field_ordinal,
    d.administration_type,
    d.barcode_type,
    d.dose_due,
    d.dose_due_unit,
    d.dose_given,
    d.dose_given_unit,
    d.route,
    d.site,
    d.complete_dose_not_given
FROM {S}.emar e
FULL OUTER JOIN detail d ON e.emar_id = d.emar_id
LEFT JOIN {G}.dim_patient pt ON COALESCE(e.subject_id, d.detail_subject_id) = pt.subject_id
LEFT JOIN rx_medication rx ON CAST(COALESCE(e.pharmacy_id, d.detail_pharmacy_id) AS BIGINT) = rx.pharmacy_id"""


def select_fact_microbiology(S, G, p):
    return f"""
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
WHERE m.microevent_id IS NOT NULL"""


def select_fact_service(S, G, p):
    key = key_hash(_txt('s.hadm_id'), _ts('s.transfertime'), _txt('s.prev_service'), _txt('s.curr_service'),
                   _txt('s.service_occurrence'))
    return f"""
WITH svc AS (
    SELECT hadm_id, subject_id, transfertime, prev_service, curr_service,
           ROW_NUMBER() OVER (PARTITION BY hadm_id, transfertime, prev_service, curr_service
                              ORDER BY subject_id) AS service_occurrence
    FROM {S}.services
    WHERE hadm_id IS NOT NULL
)
SELECT
    {key} AS service_fact_key,
    s.hadm_id,
    p.patient_key,
    ps.service_key AS previous_service_key,
    cs.service_key AS current_service_key,
    {date_key('s.transfertime')} AS transfer_date_key,
    s.transfertime
FROM svc s
LEFT JOIN {G}.dim_patient p ON s.subject_id = p.subject_id
LEFT JOIN {G}.dim_service ps ON s.prev_service = ps.service
LEFT JOIN {G}.dim_service cs ON s.curr_service = cs.service"""


# COMMAND ----------

# Silver column contracts: expected columns of each Silver table, with types where the REVIEWED Silver notebook casts
# explicitly. `None` = the column must exist but its type is a Bronze pass-through (unverified). A tuple lists accepted
# Spark simple types. Derived from the prepared Silver code (docs/gold_governance_checks.md, Source readiness), not from published tables.

def _types(**columns):
    return dict(columns)


SILVER_CONTRACT = {
    "patients": _types(subject_id=("int",), gender=("string",), anchor_age=("int",), anchor_year=("int",),
                       anchor_year_group=("string",), age_deidentified_90_plus=("boolean",)),
    "admissions": _types(subject_id=None, hadm_id=None, admittime=("timestamp",), dischtime=("timestamp",),
                         deathtime=("timestamp",), admission_type=None, admission_location=None, discharge_location=None,
                         insurance=None, language=None, marital_status=None, race=None, hospital_expire_flag=None,
                         **{f: ("boolean",) for f in ADMISSION_DQ_FLAGS}),
    "icustays": _types(subject_id=("int",), hadm_id=("int",), stay_id=("int",), first_careunit=("string",),
                       last_careunit=("string",), intime=("timestamp",), outtime=("timestamp",), los=("double",)),
    "transfers": _types(subject_id=("int",), hadm_id=("int",), transfer_id=("int",), eventtype=("string",),
                        careunit=("string",), intime=("timestamp",), outtime=("timestamp",),
                        **{f: ("boolean",) for f in TRANSFER_DQ_FLAGS}),
    "d_icd_diagnoses": _types(icd_code=("string",), icd_version=("int",), long_title=("string",)),
    "d_icd_procedures": _types(icd_code=("string",), icd_version=("int",), long_title=("string",)),
    "diagnoses_icd": _types(subject_id=("bigint",), hadm_id=("bigint",), seq_num=("int",), icd_code=("string",),
                            icd_version=("int",)),
    "procedures_icd": _types(subject_id=("bigint",), hadm_id=("bigint",), seq_num=("int",), chartdate=("date",),
                             icd_code=("string",), icd_version=("int",)),
    "d_labitems": _types(itemid=("int",), label=("string",), fluid=("string",), category=("string",)),
    "d_items": _types(itemid=("int",), label=("string",), abbreviation=("string",), linksto=("string",),
                      category=("string",), unitname=("string",), param_type=("string",)),
    "prescriptions": _types(subject_id=("bigint",), hadm_id=("bigint",), pharmacy_id=("bigint",),
                            starttime=("timestamp",), stoptime=("timestamp",), poe_id=None, poe_seq=None,
                            order_provider_id=None, drug_type=None, drug=None, formulary_drug_cd=None, gsn=None, ndc=None,
                            prod_strength=None, form_rx=None, dose_val_rx=None, dose_unit_rx=None, form_val_disp=None),
    "emar": _types(subject_id=None, hadm_id=None, emar_id=None, pharmacy_id=None, charttime=("timestamp",),
                   scheduletime=("timestamp",), medication=None, event_txt=None),
    "emar_detail": {c: None for c in EMAR_DETAIL_COLUMNS},
    # Stub notebooks in the local Silver code (NEEDS_PROFILING): Bronze types pass through, so only existence and time types are stated.
    "labevents": _types(labevent_id=None, subject_id=None, hadm_id=None, itemid=None, charttime=("timestamp",),
                        storetime=("timestamp",), valuenum=None, value=None, valueuom=None, ref_range_lower=None,
                        ref_range_upper=None, flag=None, priority=None),
    "microbiologyevents": _types(microevent_id=None, subject_id=None, hadm_id=None, micro_specimen_id=None,
                                 order_provider_id=None, chartdate=("date", "timestamp"), storedate=("date", "timestamp"),
                                 charttime=("timestamp",), storetime=("timestamp",), spec_itemid=None, spec_type_desc=None,
                                 test_seq=None, test_itemid=None, test_name=None, org_itemid=None, org_name=None,
                                 isolate_num=None, quantity=None),
    "services": _types(subject_id=("int",), hadm_id=("int",), transfertime=("timestamp",), prev_service=("string",),
                       curr_service=("string",)),
    "provider": _types(provider_id=("string",)),
    # Deferred (Bronze table missing; Silver transformation not reviewed).
    "chartevents": _types(subject_id=None, hadm_id=None, stay_id=None, caregiver_id=None, charttime=("timestamp",),
                          storetime=("timestamp",), itemid=None, value=None, valuenum=None, valueuom=None, warning=None,
                          _dq_missing_observation=("boolean",), _dq_early_storetime=("boolean",)),
}


# COMMAND ----------

# Check helpers. A check is (name, mode, sql_fn(S, G)); the SQL returns ONE number.
# mode "zero": the number must be 0 (blocks / fails validation). mode "observe": reported, never fails.

def _dup_groups(table, cols):
    """SQL: number of key values that occur more than once (NULL-safe grouping)."""
    c = ", ".join(cols)
    return f"SELECT COUNT(*) FROM (SELECT {c} FROM {table} GROUP BY {c} HAVING COUNT(*) > 1)"


def _count(table, where="TRUE"):
    return f"SELECT COUNT(*) FROM {table} WHERE {where}"


# COMMAND ----------

GOLD = {
    # ------------------------------------------------------------------ dimensions
    "dim_date": dict(
        group="dimension", select=select_dim_date, silver=[], gold_deps=[],
        comment="Calendar dimension over the shifted MIMIC-IV date range; one row per calendar day.",
        grain="one row per calendar date", business_key=["full_date"], surrogate_key="date_key", grain_key=["date_key"],
        key_unique="yes", fks=[], date_keys=[], flags=[], scan="small", domain="dimension",
        expected_rows=lambda S, G, p: f"SELECT datediff(DATE '{p['date_end']}', DATE '{p['date_start']}') + 1",
        pre_checks=[], post_checks=[],
        semantics_reviewed="partial", source_status="NO_SILVER_SOURCE",
        blockers=["Range is a parameter; coverage of fact dates is only proven by 90_Gold_Validation.",
                  "Weekday/month attributes describe the shifted calendar, not real-world dates."]),
    "dim_patient": dict(
        group="dimension", select=select_dim_patient, silver=["patients"], gold_deps=[],
        comment="One row per patient (subject_id) with demographics. Date of death is deliberately not carried.",
        grain="one row per subject_id", business_key=["subject_id"], surrogate_key="patient_key", grain_key=["patient_key"],
        key_unique="yes", fks=[], date_keys=[], flags=["age_deidentified_flag"], scan="small",
        expected_rows=lambda S, G, p: _count(f"{S}.patients", "subject_id IS NOT NULL"),
        pre_checks=[], post_checks=[],
        semantics_reviewed="partial", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["silver.patients is semantics-partial, unprofiled and unapproved (docs/gold_governance_checks.md, Source readiness).",
                  "Existing column mask on gold.dim_patient.subject_id must be re-verified/re-applied after replacement."]),
    "dim_careunit": dict(
        group="dimension", select=select_dim_careunit, silver=["icustays", "transfers"], gold_deps=[],
        comment="Distinct care unit names seen in ICU stays and transfers. Name variants (UNKNOWN/Unknown) are kept separate.",
        grain="one row per distinct care unit name", business_key=["careunit_name"], surrogate_key="careunit_key",
        grain_key=["careunit_key"], key_unique="yes", fks=[], date_keys=[], flags=[], scan="small",
        expected_rows=lambda S, G, p: ("SELECT COUNT(*) FROM (SELECT first_careunit AS n FROM {S}.icustays WHERE first_careunit IS NOT NULL "
                                       "UNION SELECT last_careunit FROM {S}.icustays WHERE last_careunit IS NOT NULL "
                                       "UNION SELECT careunit FROM {S}.transfers WHERE careunit IS NOT NULL)").format(S=S),
        pre_checks=[], post_checks=[],
        semantics_reviewed="partial", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["Needs silver.icustays and silver.transfers published; case variants are preserved, not merged."]),
    "dim_admission_type": dict(
        group="dimension", select=select_dim_admission_type, silver=["admissions"], gold_deps=[],
        comment="Distinct admission types.",
        grain="one row per distinct admission_type", business_key=["admission_type"], surrogate_key="admission_type_key",
        grain_key=["admission_type_key"], key_unique="yes", fks=[], date_keys=[], flags=[], scan="small",
        expected_rows=lambda S, G, p: f"SELECT COUNT(DISTINCT admission_type) FROM {S}.admissions",
        pre_checks=[], post_checks=[],
        semantics_reviewed="partial", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["silver.admissions unprofiled/unapproved; admission_type text is a Bronze pass-through (no trim)."]),
    "dim_location": dict(
        group="dimension", select=select_dim_location, silver=["admissions"], gold_deps=[],
        comment="Admission and discharge locations, typed by role (ADMISSION / DISCHARGE).",
        grain="one row per (location_type, location_name)", business_key=["location_type", "location_name"],
        surrogate_key="location_key", grain_key=["location_key"], key_unique="yes", fks=[], date_keys=[], flags=[],
        scan="small",
        expected_rows=lambda S, G, p: (f"SELECT (SELECT COUNT(DISTINCT admission_location) FROM {S}.admissions) + "
                                       f"(SELECT COUNT(DISTINCT discharge_location) FROM {S}.admissions)"),
        pre_checks=[], post_checks=[],
        semantics_reviewed="partial", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["silver.admissions unprofiled/unapproved."]),
    "dim_diagnosis": dict(
        group="dimension", select=select_dim_diagnosis, silver=["d_icd_diagnoses"], gold_deps=[],
        comment="ICD-9/ICD-10 diagnosis dictionary.",
        grain="one row per (icd_code, icd_version)", business_key=["icd_code", "icd_version"], surrogate_key="diagnosis_key",
        grain_key=["diagnosis_key"], key_unique="yes", fks=[], date_keys=[], flags=[], scan="small",
        expected_rows=lambda S, G, p: ("SELECT COUNT(*) FROM (SELECT DISTINCT icd_code, icd_version FROM "
                                       f"{S}.d_icd_diagnoses WHERE icd_code IS NOT NULL AND icd_version IS NOT NULL)"),
        pre_checks=[], post_checks=[],
        semantics_reviewed="yes", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["Rebuild together with fact_diagnosis: Silver now trims icd_code on both sides."]),
    "dim_procedure": dict(
        group="dimension", select=select_dim_procedure, silver=["d_icd_procedures"], gold_deps=[],
        comment="ICD-9/ICD-10 procedure dictionary.",
        grain="one row per (icd_code, icd_version)", business_key=["icd_code", "icd_version"], surrogate_key="procedure_key",
        grain_key=["procedure_key"], key_unique="yes", fks=[], date_keys=[], flags=[], scan="small",
        expected_rows=lambda S, G, p: ("SELECT COUNT(*) FROM (SELECT DISTINCT icd_code, icd_version FROM "
                                       f"{S}.d_icd_procedures WHERE icd_code IS NOT NULL AND icd_version IS NOT NULL)"),
        pre_checks=[], post_checks=[],
        semantics_reviewed="yes", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["Rebuild together with fact_procedure."]),
    "dim_lab_item": dict(
        group="dimension", select=select_dim_lab_item, silver=["d_labitems"], gold_deps=[],
        comment="Laboratory item dictionary (itemid, label, fluid, category).",
        grain="one row per lab itemid", business_key=["itemid"], surrogate_key="lab_item_key", grain_key=["lab_item_key"],
        key_unique="yes", fks=[], date_keys=[], flags=[], scan="small",
        expected_rows=lambda S, G, p: f"SELECT COUNT(DISTINCT itemid) FROM {S}.d_labitems WHERE itemid IS NOT NULL",
        pre_checks=[], post_checks=[
            ("lab_test_name_null", "observe", lambda S, G: _count(f"{G}.dim_lab_item", "lab_test_name IS NULL")),
        ],
        semantics_reviewed="partial", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["Silver turns blank labels into NULL, so lab_test_name can be NULL (4 known cases in the local file)."]),
    "dim_icu_item": dict(
        group="dimension", select=select_dim_icu_item, silver=["d_items"], gold_deps=[],
        comment="ICU item dictionary (itemid, label, abbreviation, linksto, category, unit, param_type).",
        grain="one row per ICU itemid", business_key=["itemid"], surrogate_key="icu_item_key", grain_key=["icu_item_key"],
        key_unique="yes", fks=[], date_keys=[], flags=[], scan="small",
        expected_rows=lambda S, G, p: f"SELECT COUNT(DISTINCT itemid) FROM {S}.d_items WHERE itemid IS NOT NULL",
        pre_checks=[], post_checks=[],
        semantics_reviewed="yes", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["Silver d_items is profiled but not approved."]),
    "dim_medication": dict(
        group="dimension", select=select_dim_medication, silver=["prescriptions"], gold_deps=[],
        comment="Distinct prescription drug definitions (drug_type, drug, formulary code, gsn, ndc). Not a clinical drug vocabulary.",
        grain="one row per distinct (drug_type, drug, formulary_drug_cd, gsn, ndc) with a drug name",
        business_key=list(MEDICATION_COLUMNS), surrogate_key="medication_key", grain_key=["medication_key"],
        key_unique="yes", fks=[], date_keys=[], flags=[], scan="large",
        expected_rows=lambda S, G, p: ("SELECT COUNT(*) FROM (SELECT DISTINCT " + ", ".join(MEDICATION_COLUMNS) +
                                       f" FROM {S}.prescriptions WHERE drug IS NOT NULL)"),
        pre_checks=[("prescriptions_without_drug", "observe",
                     lambda S, G: _count(f"{S}.prescriptions", "drug IS NULL"))],
        post_checks=[],
        semantics_reviewed="partial", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["Resolves the dim_medication.sql vs notebook conflict in favour of the notebook definition.",
                  "gsn/ndc types are Bronze pass-through (unverified); different text forms create separate rows.",
                  "Scans ~20M prescription rows (DISTINCT): requires confirm_large_scan."]),
    "dim_provider": dict(
        group="dimension", select=select_dim_provider, silver=["provider"], gold_deps=[],
        comment="Provider identifiers (de-identified). No other provider attributes exist in MIMIC-IV.",
        grain="one row per provider_id", business_key=["provider_id"], surrogate_key="provider_key",
        grain_key=["provider_key"], key_unique="yes", fks=[], date_keys=[], flags=[], scan="small",
        expected_rows=lambda S, G, p: f"SELECT COUNT(DISTINCT provider_id) FROM {S}.provider WHERE provider_id IS NOT NULL",
        pre_checks=[], post_checks=[],
        semantics_reviewed="partial", source_status="NEEDS_PROFILING",
        blockers=["silver.provider is a NEEDS_PROFILING stub in the local Silver code (live table state not confirmed from local evidence); Gold cannot be run until it is profiled and approved."]),
    "dim_service": dict(
        group="dimension", select=select_dim_service, silver=["services"], gold_deps=[],
        comment="Distinct hospital service codes seen as previous or current service.",
        grain="one row per distinct service code", business_key=["service"], surrogate_key="service_key",
        grain_key=["service_key"], key_unique="yes", fks=[], date_keys=[], flags=[], scan="small",
        expected_rows=lambda S, G, p: ("SELECT COUNT(*) FROM (SELECT curr_service AS s FROM " + f"{S}.services WHERE curr_service IS NOT NULL "
                                       f"UNION SELECT prev_service FROM {S}.services WHERE prev_service IS NOT NULL)"),
        pre_checks=[], post_checks=[],
        semantics_reviewed="partial", source_status="NEEDS_PROFILING",
        blockers=["silver.services is a NEEDS_PROFILING stub in the local Silver code (live table state not confirmed from local evidence)."]),
    "dim_microbiology_test": dict(
        group="dimension", select=select_dim_microbiology_test, silver=["microbiologyevents"], gold_deps=[],
        comment="Microbiology specimen/test pairs.",
        grain="one row per (spec_itemid, test_itemid)", business_key=["spec_itemid", "test_itemid"],
        surrogate_key="microbiology_test_key", grain_key=["microbiology_test_key"], key_unique="yes", fks=[],
        date_keys=[], flags=[], scan="large",
        expected_rows=lambda S, G, p: (f"SELECT COUNT(*) FROM (SELECT DISTINCT spec_itemid, test_itemid FROM "
                                       f"{S}.microbiologyevents WHERE test_itemid IS NOT NULL)"),
        pre_checks=[("pair_has_multiple_names", "zero",
                     lambda S, G: (f"SELECT COUNT(*) FROM (SELECT spec_itemid, test_itemid FROM {S}.microbiologyevents "
                                   "WHERE test_itemid IS NOT NULL GROUP BY spec_itemid, test_itemid "
                                   "HAVING COUNT(DISTINCT spec_type_desc) > 1 OR COUNT(DISTINCT test_name) > 1)"))],
        post_checks=[],
        semantics_reviewed="no", source_status="NEEDS_PROFILING",
        blockers=["silver.microbiologyevents is a NEEDS_PROFILING stub in the local Silver code (live table state not confirmed from local evidence).",
                  "The old dimension kept spec_type_desc/test_name in the DISTINCT, which could duplicate a (spec, test) pair; "
                  "this grain assumes ids determine names and blocks the build if they do not."]),
    # ------------------------------------------------------------------ facts
    "fact_admission": dict(
        group="fact", select=select_fact_admission, silver=["admissions"],
        gold_deps=["dim_patient", "dim_admission_type", "dim_location"],
        comment="One row per hospital admission with derived length of stay, age at admission and Silver data-quality flags.",
        grain="one row per hadm_id", business_key=["hadm_id"], surrogate_key=None, grain_key=["hadm_id"], key_unique="yes",
        fks=[("patient_key", "dim_patient", "patient_key", True),
             ("admission_type_key", "dim_admission_type", "admission_type_key", False),
             ("admission_location_key", "dim_location", "location_key", False),
             ("discharge_location_key", "dim_location", "location_key", False)],
        date_keys=["admit_date_key", "discharge_date_key"], flags=list(ADMISSION_DQ_FLAGS), scan="small",
        expected_rows=lambda S, G, p: _count(f"{S}.admissions", "hadm_id IS NOT NULL"),
        source_rows=lambda S, G, p: f"SELECT COUNT(*) FROM {S}.admissions",
        pre_checks=[("admissions_hadm_id_duplicates", "zero", lambda S, G: _dup_groups(f"{S}.admissions", ["hadm_id"]))],
        post_checks=[
            ("los_not_derived_when_times_present", "zero",
             lambda S, G: _count(f"{G}.fact_admission", "admittime IS NOT NULL AND dischtime IS NOT NULL AND (los_hours IS NULL OR los_days IS NULL)")),
            ("los_days_not_hours_over_24", "zero",
             lambda S, G: _count(f"{G}.fact_admission", "los_hours IS NOT NULL AND ABS(los_days * 24.0 - los_hours) > 0.000001")),
            ("negative_los_differs_from_dq_flag", "zero",
             lambda S, G: (f"SELECT ABS(SUM(CASE WHEN los_hours < 0 THEN 1 ELSE 0 END) - "
                           f"SUM(CASE WHEN dq_invalid_admission_time THEN 1 ELSE 0 END)) FROM {G}.fact_admission")),
            ("negative_los_rows", "observe", lambda S, G: _count(f"{G}.fact_admission", "los_hours < 0")),
            ("deathtime_present_rows", "observe", lambda S, G: _count(f"{G}.fact_admission", "deathtime IS NOT NULL")),
        ],
        semantics_reviewed="partial", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["Derives los_hours/los_days (absent from Silver by design). Negative values (admittime after dischtime) are preserved and flagged.",
                  "marital_status stays NULL where Silver is NULL."]),
    "fact_icu_stay": dict(
        group="fact", select=select_fact_icu_stay, silver=["icustays"], gold_deps=["dim_patient", "dim_careunit"],
        comment="One row per ICU stay; los_days is the MIMIC-documented fractional-day length of stay.",
        grain="one row per stay_id", business_key=["stay_id"], surrogate_key="icu_stay_fact_key",
        grain_key=["stay_id"], key_unique="yes",
        fks=[("patient_key", "dim_patient", "patient_key", True),
             ("first_careunit_key", "dim_careunit", "careunit_key", False),
             ("last_careunit_key", "dim_careunit", "careunit_key", False)],
        date_keys=["in_date_key", "out_date_key"], flags=[], scan="small",
        expected_rows=lambda S, G, p: _count(f"{S}.icustays", "stay_id IS NOT NULL"),
        source_rows=lambda S, G, p: f"SELECT COUNT(*) FROM {S}.icustays",
        pre_checks=[], post_checks=[
            ("key_differs_from_stay_id", "zero", lambda S, G: _count(f"{G}.fact_icu_stay", "icu_stay_fact_key <> stay_id")),
        ],
        semantics_reviewed="yes", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["Silver icustays is profiled but not approved; ICU/transfer careunit mismatch (7,956 of 94,458) is an observation, not a Gold rule."]),
    "fact_transfer": dict(
        group="fact", select=select_fact_transfer, silver=["transfers"], gold_deps=["dim_patient", "dim_careunit"],
        comment="One row per patient transfer/location stay with derived duration and Silver data-quality flags.",
        grain="one row per transfer_id", business_key=["transfer_id"], surrogate_key=None, grain_key=["transfer_id"],
        key_unique="yes",
        fks=[("patient_key", "dim_patient", "patient_key", True),
             ("careunit_key", "dim_careunit", "careunit_key", False)],
        date_keys=["in_date_key", "out_date_key"], flags=list(TRANSFER_DQ_FLAGS), scan="small",
        expected_rows=lambda S, G, p: _count(f"{S}.transfers", "transfer_id IS NOT NULL"),
        source_rows=lambda S, G, p: f"SELECT COUNT(*) FROM {S}.transfers",
        pre_checks=[], post_checks=[
            ("negative_duration_differs_from_dq_flag", "zero",
             lambda S, G: (f"SELECT ABS(SUM(CASE WHEN transfer_duration_hours < 0 THEN 1 ELSE 0 END) - "
                           f"SUM(CASE WHEN dq_invalid_transfer_time THEN 1 ELSE 0 END)) FROM {G}.fact_transfer")),
            ("negative_duration_rows", "observe", lambda S, G: _count(f"{G}.fact_transfer", "transfer_duration_hours < 0")),
            ("null_duration_rows", "observe", lambda S, G: _count(f"{G}.fact_transfer", "transfer_duration_hours IS NULL")),
        ],
        semantics_reviewed="yes", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["Derives transfer_duration_hours (absent from Silver by design). NULL outtime (discharge rows) gives NULL duration; reversed times give negative values."]),
    "fact_diagnosis": dict(
        group="fact", select=select_fact_diagnosis, silver=["diagnoses_icd", "admissions"],
        gold_deps=["dim_patient", "dim_diagnosis"],
        comment="One row per coded hospital diagnosis (admission x sequence x ICD code).",
        grain="one row per (subject_id, hadm_id, seq_num, icd_code, icd_version)", business_key=["hadm_id", "seq_num"],
        surrogate_key="diagnosis_fact_key", grain_key=["diagnosis_fact_key"], key_unique="yes",
        fks=[("patient_key", "dim_patient", "patient_key", True),
             ("diagnosis_key", "dim_diagnosis", "diagnosis_key", False)],
        date_keys=["admit_date_key", "discharge_date_key"], flags=[], scan="large",
        expected_rows=lambda S, G, p: _count(f"{S}.diagnoses_icd", "hadm_id IS NOT NULL AND icd_code IS NOT NULL AND icd_version IS NOT NULL"),
        source_rows=lambda S, G, p: f"SELECT COUNT(*) FROM {S}.diagnoses_icd",
        natural_grain_distinct=lambda S, G, p: ("SELECT COUNT(*) FROM (SELECT DISTINCT subject_id, hadm_id, seq_num, icd_code, icd_version "
                                                f"FROM {S}.diagnoses_icd WHERE hadm_id IS NOT NULL AND icd_code IS NOT NULL AND icd_version IS NOT NULL)"),
        pre_checks=[("admissions_hadm_id_duplicates", "zero", lambda S, G: _dup_groups(f"{S}.admissions", ["hadm_id"]))],
        post_checks=[
            ("admit_date_key_missing", "observe", lambda S, G: _count(f"{G}.fact_diagnosis", "admit_date_key IS NULL")),
        ],
        semantics_reviewed="partial", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["The documented (subject_id, hadm_id, seq_num) key is NOT unique in the local file (116 rows share it); the key here uses the full 5-column record.",
                  "primary_diagnosis_flag = (seq_num = 1) is carried from the existing design; no clinical validation."]),
    "fact_procedure": dict(
        group="fact", select=select_fact_procedure, silver=["procedures_icd"], gold_deps=["dim_patient", "dim_procedure"],
        comment="One row per coded hospital procedure (admission x sequence x ICD code x date).",
        grain="one row per (subject_id, hadm_id, seq_num, chartdate, icd_code, icd_version)", business_key=["hadm_id", "seq_num"],
        surrogate_key="procedure_fact_key", grain_key=["procedure_fact_key"], key_unique="yes",
        fks=[("patient_key", "dim_patient", "patient_key", True),
             ("procedure_key", "dim_procedure", "procedure_key", False)],
        date_keys=["procedure_date_key"], flags=[], scan="small",
        expected_rows=lambda S, G, p: _count(f"{S}.procedures_icd", "hadm_id IS NOT NULL AND icd_code IS NOT NULL AND icd_version IS NOT NULL"),
        source_rows=lambda S, G, p: f"SELECT COUNT(*) FROM {S}.procedures_icd",
        natural_grain_distinct=lambda S, G, p: ("SELECT COUNT(*) FROM (SELECT DISTINCT subject_id, hadm_id, seq_num, chartdate, icd_code, icd_version "
                                                f"FROM {S}.procedures_icd WHERE hadm_id IS NOT NULL AND icd_code IS NOT NULL AND icd_version IS NOT NULL)"),
        pre_checks=[], post_checks=[],
        semantics_reviewed="partial", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["The documented 3-column key is not unique locally (5 rows share it); the old 4-column hash omitted chartdate and could collide."]),
    "fact_lab": dict(
        group="fact", select=select_fact_lab, silver=["labevents"], gold_deps=["dim_patient", "dim_lab_item"],
        comment="One row per laboratory measurement (labevent_id).",
        grain="one row per labevent_id", business_key=["lab_fact_key"], surrogate_key="lab_fact_key",
        grain_key=["lab_fact_key"], key_unique="yes",
        fks=[("patient_key", "dim_patient", "patient_key", True),
             ("lab_item_key", "dim_lab_item", "lab_item_key", False)],
        date_keys=["lab_date_key"], flags=[], scan="large",
        expected_rows=lambda S, G, p: _count(f"{S}.labevents", "labevent_id IS NOT NULL"),
        source_rows=lambda S, G, p: f"SELECT COUNT(*) FROM {S}.labevents",
        pre_checks=[], post_checks=[
            ("storetime_before_charttime", "observe", lambda S, G: _count(f"{G}.fact_lab", "storetime < charttime")),
        ],
        semantics_reviewed="no", source_status="NEEDS_PROFILING",
        blockers=["silver.labevents is a NEEDS_PROFILING stub in the local Silver code (live table state not confirmed from local evidence); ~158M rows.",
                  "Source-file evidence (docs/gold_governance_checks.md, Source readiness) supports labevent_id uniqueness, but not the Bronze profile."]),
    "fact_icu_observation": dict(
        group="fact", select=select_fact_icu_observation, silver=["chartevents"], gold_deps=["dim_patient", "dim_icu_item"],
        comment="One row per charted ICU observation (chartevents). DEFERRED until Bronze/Silver chartevents exist.",
        grain="one row per chartevents record (no documented unique id)", business_key=["stay_id", "itemid"],
        surrogate_key="observation_fact_key", grain_key=["observation_fact_key"], key_unique="unprofiled",
        fks=[("patient_key", "dim_patient", "patient_key", True),
             ("icu_item_key", "dim_icu_item", "icu_item_key", False)],
        date_keys=["chart_date_key"], flags=["dq_missing_observation", "dq_early_storetime"], scan="large",
        expected_rows=lambda S, G, p: _count(f"{S}.chartevents", "stay_id IS NOT NULL AND itemid IS NOT NULL"),
        source_rows=lambda S, G, p: f"SELECT COUNT(*) FROM {S}.chartevents",
        pre_checks=[], post_checks=[],
        semantics_reviewed="no", source_status="DEFERRED",
        blockers=["Bronze chartevents was never loaded (paused for cost); Silver chartevents is deferred and unreviewed.",
                  "Key uniqueness unprofiled; duplicates (if any) are reported, not removed. ~433M rows."]),
    "fact_prescription": dict(
        group="fact", select=select_fact_prescription, silver=["prescriptions"],
        gold_deps=["dim_patient", "dim_medication", "dim_provider"],
        comment="One row per prescription drug component (a pharmacy order can have several). prescription_fact_key is unique only within one build: do not reference or store it.",
        grain="one row per prescriptions record (pharmacy_id is not unique)", business_key=["pharmacy_id"],
        surrogate_key="prescription_fact_key", grain_key=["prescription_fact_key"], key_unique="yes",
        fks=[("patient_key", "dim_patient", "patient_key", True),
             ("medication_key", "dim_medication", "medication_key", False),
             ("provider_key", "dim_provider", "provider_key", False)],
        date_keys=["start_date_key", "stop_date_key"], flags=[], scan="large",
        expected_rows=lambda S, G, p: _count(f"{S}.prescriptions", "hadm_id IS NOT NULL"),
        source_rows=lambda S, G, p: f"SELECT COUNT(*) FROM {S}.prescriptions",
        pre_checks=[], post_checks=[
            ("stoptime_before_starttime", "observe", lambda S, G: _count(f"{G}.fact_prescription", "stoptime < starttime")),
        ],
        semantics_reviewed="partial", source_status="NEEDS_PROFILING",
        blockers=["Depends on dim_provider, whose Silver source (provider) is NEEDS_PROFILING.",
                  "Silver prescriptions is semantics-partial/profiled-partial.",
                  "poe_id/poe_seq are carried as plain attributes only (POE tables are excluded and not joinable)."]),
    "fact_medication_administration": dict(
        group="fact", select=select_fact_medication_administration, silver=["emar", "emar_detail", "prescriptions"],
        gold_deps=["dim_patient", "dim_medication"],
        comment="One row per eMAR record: matched detail records, header-only events and orphan detail records, with the medication linked only where unambiguous. med_admin_fact_key is unique only within one build: do not reference or store it.",
        grain="one row per emar_detail record (MATCHED, or DETAIL_ONLY when its emar_id is absent from emar) plus one row per emar event with no detail record (HEADER_ONLY)",
        business_key=["emar_id"],
        surrogate_key="med_admin_fact_key", grain_key=["med_admin_fact_key"], key_unique="yes",
        fks=[("patient_key", "dim_patient", "patient_key", True),
             ("medication_key", "dim_medication", "medication_key", False)],
        date_keys=["admin_date_key"], flags=[], scan="large",
        expected_rows=lambda S, G, p: (f"SELECT (SELECT COUNT(*) FROM {S}.emar_detail) + "
                                       f"(SELECT COUNT(*) FROM {S}.emar e LEFT ANTI JOIN {S}.emar_detail d ON e.emar_id = d.emar_id)"),
        pre_checks=[
            # The FULL OUTER JOIN multiplies rows if emar_id repeats in emar, and NULL ids never match. Documented NOT NULL / unique;
            # not assumed: a violation blocks the build.
            ("emar_event_id_duplicates", "zero", lambda S, G: _dup_groups(f"{S}.emar", ["emar_id"])),
            ("emar_or_emar_detail_emar_id_null", "zero",
             lambda S, G: (f"SELECT (SELECT COUNT(*) FROM {S}.emar WHERE emar_id IS NULL) + "
                           f"(SELECT COUNT(*) FROM {S}.emar_detail WHERE emar_id IS NULL)")),
        ],
        post_checks=[
            # Per-class row reconciliation against Silver. With emar_id unique in emar (pre-check), each detail record joins to at most
            # one header, so MATCHED rows = detail records with a header, DETAIL_ONLY = detail records without one, HEADER_ONLY =
            # headers without detail. Equality of all three (plus the total in rows_equal_expected_from_silver) shows no record was
            # multiplied, merged or dropped; checking that each emar_id merely appears would not.
            ("reconcile_matched_rows", "zero",
             lambda S, G: (f"SELECT ABS((SELECT COUNT(*) FROM {G}.fact_medication_administration WHERE record_match_status = 'MATCHED') - "
                           f"(SELECT COUNT(*) FROM {S}.emar_detail d LEFT SEMI JOIN {S}.emar e ON d.emar_id = e.emar_id))")),
            ("reconcile_header_only_rows", "zero",
             lambda S, G: (f"SELECT ABS((SELECT COUNT(*) FROM {G}.fact_medication_administration WHERE record_match_status = 'HEADER_ONLY') - "
                           f"(SELECT COUNT(*) FROM {S}.emar e LEFT ANTI JOIN {S}.emar_detail d ON e.emar_id = d.emar_id))")),
            ("reconcile_detail_only_rows", "zero",
             lambda S, G: (f"SELECT ABS((SELECT COUNT(*) FROM {G}.fact_medication_administration WHERE record_match_status = 'DETAIL_ONLY') - "
                           f"(SELECT COUNT(*) FROM {S}.emar_detail d LEFT ANTI JOIN {S}.emar e ON d.emar_id = e.emar_id))")),
            ("rows_matched", "observe", lambda S, G: _count(f"{G}.fact_medication_administration", "record_match_status = 'MATCHED'")),
            ("rows_header_only", "observe", lambda S, G: _count(f"{G}.fact_medication_administration", "record_match_status = 'HEADER_ONLY'")),
            ("rows_detail_only", "observe", lambda S, G: _count(f"{G}.fact_medication_administration", "record_match_status = 'DETAIL_ONLY'")),
            ("medication_link_unique", "observe", lambda S, G: _count(f"{G}.fact_medication_administration", "medication_link_status = 'UNIQUE'")),
            ("medication_link_ambiguous", "observe", lambda S, G: _count(f"{G}.fact_medication_administration", "medication_link_status = 'AMBIGUOUS'")),
            ("medication_link_no_prescription", "observe", lambda S, G: _count(f"{G}.fact_medication_administration", "medication_link_status = 'NO_PRESCRIPTION'")),
            ("medication_link_no_drug_name", "observe", lambda S, G: _count(f"{G}.fact_medication_administration", "medication_link_status = 'NO_DRUG_NAME'")),
            ("medication_link_no_pharmacy_id", "observe", lambda S, G: _count(f"{G}.fact_medication_administration", "medication_link_status = 'NO_PHARMACY_ID'")),
        ],
        semantics_reviewed="partial", source_status="PREPARED_NOT_PUBLISHED",
        blockers=["Medication link is deliberately partial: AMBIGUOUS, unmatched and nameless links carry a NULL medication_key and a status; no prescription is chosen arbitrarily.",
                  "emar_detail keeps 4,861 profiled exact-duplicate groups; they stay separate rows. detail_occurrence makes their keys unique within one build only (not a stable source-record identity).",
                  "FULL OUTER JOIN of ~42.8M eMAR rows with ~87M detail rows plus a window over the detail rows and a ~20M-row prescription aggregate: requires confirm_large_scan.",
                  "emar/emar_detail Silver types are Bronze pass-through (unverified); orphan-detail and header-only counts are unknown until validation is run."]),
    "fact_microbiology": dict(
        group="fact", select=select_fact_microbiology, silver=["microbiologyevents"],
        gold_deps=["dim_patient", "dim_microbiology_test", "dim_provider"],
        comment="One row per microbiology result record (microevent_id): test, organism and isolate level.",
        grain="one row per microevent_id", business_key=["microevent_id"], surrogate_key=None, grain_key=["microevent_id"],
        key_unique="yes",
        fks=[("patient_key", "dim_patient", "patient_key", True),
             ("microbiology_test_key", "dim_microbiology_test", "microbiology_test_key", False),
             ("provider_key", "dim_provider", "provider_key", False)],
        date_keys=["chart_date_key", "store_date_key"], flags=[], scan="large",
        expected_rows=lambda S, G, p: _count(f"{S}.microbiologyevents", "microevent_id IS NOT NULL"),
        source_rows=lambda S, G, p: f"SELECT COUNT(*) FROM {S}.microbiologyevents",
        pre_checks=[], post_checks=[],
        semantics_reviewed="no", source_status="NEEDS_PROFILING",
        blockers=["silver.microbiologyevents is a NEEDS_PROFILING stub in the local Silver code (live table state not confirmed from local evidence).",
                  "Depends on dim_provider (provider is NEEDS_PROFILING) and dim_microbiology_test.",
                  "Antibiotic/sensitivity columns (ab_*, dilution_*, interpretation) are not in the existing fact and not added."]),
    "fact_service": dict(
        group="fact", select=select_fact_service, silver=["services"], gold_deps=["dim_patient", "dim_service"],
        comment="One row per hospital service transfer event. service_fact_key is unique only within one build: do not reference or store it.",
        grain="one row per services record (no documented unique key)", business_key=["hadm_id"],
        surrogate_key="service_fact_key", grain_key=["service_fact_key"], key_unique="yes",
        fks=[("patient_key", "dim_patient", "patient_key", True),
             ("previous_service_key", "dim_service", "service_key", False),
             ("current_service_key", "dim_service", "service_key", False)],
        date_keys=["transfer_date_key"], flags=[], scan="small",
        expected_rows=lambda S, G, p: _count(f"{S}.services", "hadm_id IS NOT NULL"),
        source_rows=lambda S, G, p: f"SELECT COUNT(*) FROM {S}.services",
        pre_checks=[], post_checks=[],
        semantics_reviewed="no", source_status="PREPARED_NOT_PUBLISHED",
        silver_sources_confirmed=True, approved_to_publish=True,
        approved_by="s4135469@student.rmit.edu.au", approved_on="2026-10-20",
        blockers=["silver.services was NEEDS_PROFILING but is confirmed populated (593,071 rows); approved for fact_service schema fix."]),
}

# fact_icu_observation: flags carried from the deferred Silver stub are named _dq_* there; Gold exposes dq_*.


# COMMAND ----------

# Readiness axes shared by all tables. Approvals are a deliberate, reviewed change per table.

OCCURRENCE_KEYED_FACTS = ("fact_prescription", "fact_medication_administration", "fact_service")

# ESTIMATED passes over data per large build, counted by reading the SQL and the checks in this file with run_observe_checks=false.
# They are not measured cost: no run has happened, row counts are unconfirmed, and Delta/Spark may read fewer or more bytes.
# Each OBSERVE check adds one more pass (large tables skip them unless run_observe_checks=true). Large tables are never
# staged (staging would evaluate the SELECT several extra times); a failed check is therefore found AFTER publication.
COST_NOTES = {
    "dim_medication": "Silver prescriptions (~20M rows) is read 2x: the build (DISTINCT shuffle) and the expected-count query. The NULL-drug pre-check is an OBSERVE and runs only with run_observe_checks. Gold-side checks read the small dimension.",
    "dim_microbiology_test": "Silver microbiologyevents is read 3x: the blocking name-conflict pre-check (group-by), the build (group-by) and the expected-count (distinct).",
    "fact_diagnosis": "Silver diagnoses_icd (~6.4M) is read 4x: build, expected-count, source count, natural-grain distinct. silver.admissions (~546k) is read 2x (join, duplicate pre-check). The Gold fact is read 3x: profile pass (with dim_date joins), key group-by, distinct-key count.",
    "fact_lab": "Silver labevents (~158M) is read 3x: build, expected-count, source count (a plain COUNT(*) may be answered from Delta metadata). The Gold fact is read 2x: profile pass and a key group-by over ~158M keys (large shuffle).",
    "fact_prescription": "Silver prescriptions (~20M) is read 3x: build (window over a 10-column partition, sorted by 8 tie-break columns), expected-count, source count. The Gold fact is read 2x: profile pass and key group-by.",
    "fact_medication_administration": "Estimated: Silver emar_detail (~87M) 7x, emar (~42.8M) 7x, prescriptions (~20M) 1x. Blocking pre-checks (emar_id duplicate group-by; NULL emar_id counts) read emar 2x and emar_detail 1x; the build is a FULL OUTER JOIN shuffle plus a window sorted by a 31-column hash plus a prescription aggregate; the expected-count reads emar_detail 2x and emar 1x; three per-class reconciliations each re-read both Silver tables. The Gold fact is read 5x (profile pass, key group-by, three class counts).",
    "fact_microbiology": "Silver microbiologyevents is read 3x: build, expected-count, source count. The Gold fact is read 2x: profile pass and key group-by.",
    "fact_icu_observation": "DEFERRED and not runnable. If it were: ~433M source rows read 3x and the Gold fact 2x.",
}

for _name, _spec in GOLD.items():
    _spec.setdefault("code_prepared", True)
    _spec.setdefault("silver_sources_confirmed", False)   # someone confirms the Silver tables are published and match SILVER_CONTRACT
    _spec.setdefault("validation_evidence", "none")        # "none" until a 90_Gold_Validation result is reviewed
    _spec.setdefault("approved_to_publish", False)
    _spec.setdefault("approved_by", None)
    _spec.setdefault("approved_on", None)
    _spec.setdefault("natural_grain_distinct", None)
    if _name in OCCURRENCE_KEYED_FACTS:
        _spec.setdefault("key_stability", "UNIQUE_WITHIN_BUILD_ONLY")      # never a foreign-key target; not stable across rebuilds
    elif _spec["group"] == "dimension":
        _spec.setdefault("key_stability", "DETERMINISTIC_FROM_BUSINESS_KEY")
    elif _spec["surrogate_key"]:
        _spec.setdefault("key_stability", "DETERMINISTIC_FROM_RECORD_COLUMNS")
    else:
        _spec.setdefault("key_stability", "NATURAL_SOURCE_ID")
    _spec.setdefault("silver_availability", "NOT_APPLICABLE" if not _spec["silver"] else "NOT_CONFIRMED_FROM_LOCAL_EVIDENCE")
    _spec.setdefault("cost_note", COST_NOTES.get(_name, ""))
    _spec["silver_contract"] = {t: SILVER_CONTRACT[t] for t in _spec["silver"]}
    _spec.setdefault("source_rows", None)

BLOCKING_SOURCE_STATUS = {"NEEDS_PROFILING", "DEFERRED"}


# COMMAND ----------

def render_select(table, S, G, p=None, registry=None):
    registry = GOLD if registry is None else registry
    params = {"date_start": DEFAULT_DATE_START, "date_end": DEFAULT_DATE_END}
    params.update(p or {})
    return registry[table]["select"](S, G, params).strip()


def render_ctas(table, S, G, p=None, registry=None):
    registry = GOLD if registry is None else registry
    comment = registry[table]["comment"].replace("'", "")
    return (f"CREATE OR REPLACE TABLE {G}.{table}\nUSING DELTA\nCOMMENT '{comment}'\nAS\n"
            + render_select(table, S, G, p, registry))


def dependency_order(selected, registry=None):
    """Stable topological order over Gold dependencies (dependencies outside the selection are ignored)."""
    registry = GOLD if registry is None else registry
    ordered, done, visiting = [], set(), set()

    def visit(table):
        if table in done:
            return
        if table in visiting:
            raise ValueError(f"Dependency cycle involving {table}")
        visiting.add(table)
        for dependency in registry[table]["gold_deps"]:
            if dependency in selected:
                visit(dependency)
        visiting.discard(table)
        done.add(table)
        ordered.append(table)

    for table in registry:
        if table in selected:
            visit(table)
    return ordered


def build_plan(requested="all", group=None, registry=None):
    """Planning only. `requested="all"` (optionally limited to a group) is accepted for previews only."""
    registry = GOLD if registry is None else registry
    if requested == "all":
        names = [t for t, s in registry.items() if group in (None, s["group"])]
    else:
        names = list(requested)
    if not names:
        raise ValueError("Select at least one table")
    if len(names) != len(set(names)):
        raise ValueError("Duplicate table names requested")
    for table in names:
        if table in EXCLUDED_SOURCES:
            raise ValueError(f"{table} is excluded: {EXCLUDED_SOURCES[table]}")
        if table not in registry:
            raise ValueError(f"Not in the Gold registry: {table}")
        if group is not None and registry[table]["group"] != group:
            raise ValueError(f"{table} is a {registry[table]['group']}, not a {group}")
    selected = set(names)
    steps = []
    for i, table in enumerate(dependency_order(selected, registry)):
        spec = registry[table]
        steps.append(dict(position=i + 1, table=table, group=spec["group"], silver=list(spec["silver"]),
                          gold_deps=list(spec["gold_deps"]), code_prepared=spec["code_prepared"],
                          semantics_reviewed=spec["semantics_reviewed"], source_status=spec["source_status"],
                          silver_sources_confirmed=spec["silver_sources_confirmed"],
                          validation_evidence=spec["validation_evidence"], approved_to_publish=spec["approved_to_publish"],
                          scan=spec["scan"], silver_availability=spec["silver_availability"], cost_note=spec["cost_note"]))
    return dict(steps=steps, selected=selected, requested=requested)


def require_executable(plan):
    """Gate for a real run: explicit selection, recorded approval, confirmed Silver sources, no blocking source status."""
    if plan["requested"] == "all":
        raise ValueError("A real run needs an explicit table selection; 'all' is preview-only")
    blocked = {}
    for step in plan["steps"]:
        reasons = []
        if step["source_status"] in BLOCKING_SOURCE_STATUS:
            reasons.append(f"Silver source status is {step['source_status']}")
        if not step["silver_sources_confirmed"]:
            reasons.append("silver_sources_confirmed is False in 00_Gold_Registry")
        if step["scan"] == "large" and not CANDIDATE_VALIDATION_APPROVED:
            reasons.append(LARGE_BUILD_BLOCK_REASON)
        if not step["approved_to_publish"]:
            reasons.append("approved_to_publish is False in 00_Gold_Registry")
        if reasons:
            blocked[step["table"]] = "; ".join(reasons)
    if blocked:
        raise ValueError(f"Not approved for publication: {blocked}")
    return True