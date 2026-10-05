# Databricks notebook source
# MAGIC %run ./read_sap_metadata_ingestion

# COMMAND ----------

# MAGIC %run ./pipeline_register

# COMMAND ----------


from collections import defaultdict
import json
import logging
import re
import uuid
from datetime import datetime

logger = logging.getLogger("sap_ingestion_orchestrator")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_h)

BASE_VOLUME = "/Volumes/rgaplxdatabricks/uct_demo/landing"

# The Delta table that replaced the Excel template. Written by the
# config-entry notebook (MERGE INTO ... pipeline_table_config). Kept as a
# widget so dev/qa/prod can point at different config tables without edits.
DEFAULT_CONFIG_TABLE = "rgaplxdatabricks.uct_demo.pipeline_table_config"
CONFIG_DIR  = f"{BASE_VOLUME}/configs"

# FIX (ported from the Excel orchestrator): don't let widget registration
# itself take the whole notebook down if it's being run somewhere widgets
# behave unexpectedly (e.g. re-run in an interactive cell). Fall back to
# safe defaults rather than raising before we've even started.
try:
    dbutils.widgets.text("environment", "dev", "Environment")
    dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
    dbutils.widgets.text("config_table", DEFAULT_CONFIG_TABLE, "Config Table (catalog.schema.table)")
    ENVIRONMENT   = (dbutils.widgets.get("environment") or "dev").strip().lower()
    PIPELINE_NAME = (dbutils.widgets.get("pipeline_name") or "").strip()
    CONFIG_TABLE  = (dbutils.widgets.get("config_table") or DEFAULT_CONFIG_TABLE).strip()
except Exception:
    ENVIRONMENT   = "dev"
    PIPELINE_NAME = ""
    CONFIG_TABLE  = DEFAULT_CONFIG_TABLE

logger.info(f"Widgets resolved -> environment={ENVIRONMENT!r}  "
            f"pipeline_name={PIPELINE_NAME!r}  config_table={CONFIG_TABLE!r}")

# NOTE: the `%run ./pipeline_registry` / `%run ./read_sap_metadata` lines that
# used to sit here were removed — %run must be the only thing in its cell,
# and inside a Python cell it's a syntax error. The %run cells at the top of
# this notebook are the ones that actually load those dependencies.

# FIX (ported from the Excel orchestrator): this notebook is glued together
# by the %run cells above, which inject CTRL / PipelineRegistry /
# read_sap_metadata / VALID_SOURCE_LOAD_TYPES into the global namespace.
# That's fine as a notebook, but it fails silently and unhelpfully if the
# cells run out of order or this file ever gets imported as a plain module.
# Fail loud, immediately, with a message that names the actual cause.
for _dep in ("CTRL", "PipelineRegistry", "read_sap_metadata", "VALID_SOURCE_LOAD_TYPES"):
    if _dep not in globals():
        raise RuntimeError(
            f"'{_dep}' is not defined. This file depends on `%run ./read_sap_metadata_ingestion` "
            f"and `%run ./pipeline_register` executing first, as notebook "
            f"cells above. If you're running this outside a notebook, add explicit "
            f"imports instead of relying on %run."
        )

if not PIPELINE_NAME:
    raise ValueError(
        "Widget 'pipeline_name' is required — set it to a Pipeline_name "
        "value present in pipeline_table_config."
    )

if ENVIRONMENT not in ("dev", "qa", "prod"):
    raise ValueError(f"environment widget must be dev|qa|prod — got '{ENVIRONMENT}'")

if len(CONFIG_TABLE.split(".")) != 3:
    raise ValueError(
        f"config_table widget must be fully qualified (catalog.schema.table) — got '{CONFIG_TABLE}'"
    )

# Landing zone for SAP DSP exports. Metadata_path is not a template column —
# it is always derived from this base + Table_name. Source_path values in
# the template are relative to this same base (one or more, comma-separated);
# each is prefixed with LANDING_BASE_PATH before use.




# LANDING_BASE_PATH = "/Volumes/ingestion_test/landing/landing_volume/DEMO/"
LANDING_BASE_PATH = "/Volumes/ingestion_test/landing/landing_volume/"






# Config rows now come from the Delta table, not an Excel file in a Volume.
# (CTRL is still used for sap_metadata_landing — that table did not move.)
PIPELINE_TABLE_CONFIG = CONFIG_TABLE

# FIX (ported from the Excel orchestrator): the exact bug class this whole
# pipeline has been chasing — a human typing a bare flag value ("D") into a
# cell that's supposed to hold a full SQL condition. Require an operator
# before we'll trust an override; otherwise fall back to the
# SAP-metadata-derived condition instead of silently forwarding something
# that will break at pipeline-run time, several steps removed from where
# the mistake was made.
_CONDITION_OPERATOR_RE = re.compile(r"(=|!=|<>|\bLIKE\b|\bIS\b)", re.IGNORECASE)


def _looks_like_sql_condition(val: str) -> bool:
    return bool(_CONDITION_OPERATOR_RE.search(val))


def _resolve_landing_paths(row: dict) -> dict:
    """
    Derive Metadata_path and expand Source_path against LANDING_BASE_PATH.

    Metadata_path (always derived, not a template column):
      {LANDING_BASE_PATH}/{Table_name}/.sap.partfile.metadata

    Source_path (template column, relative to LANDING_BASE_PATH):
      One path, or multiple comma-separated paths — each is prefixed with
      LANDING_BASE_PATH and the row's Source_path is rewritten to the
      comma-joined absolute paths.

      e.g. Table_name=Z_MARA_CDS_NEW, Source_path=Z_MARA_CDS_NEW/initial/
        -> Metadata_path = /Volumes/ingestion_cat/landing/landing_volume/DEMO/Z_MARA_CDS_NEW/.sap.partfile.metadata
        -> Source_path   = /Volumes/ingestion_cat/landing/landing_volume/DEMO/Z_MARA_CDS_NEW/initial/
    """
    base = LANDING_BASE_PATH.rstrip("/")

    table_name = str(row.get("Table_name", "")).strip()
    if table_name:
        row["Metadata_path"] = f"{base}/{table_name}/.sap.partfile.metadata"

    raw_source = str(row.get("Source_path", "")).strip()
    if raw_source:
        parts = [p.strip().lstrip("/") for p in raw_source.split(",") if p.strip()]
        row["Source_path"] = ",".join(f"{base}/{p}" for p in parts)

    return row


def _truthy(val, default=True):
    if val is None:
        return default
    v = str(val).strip().upper()
    if v in ("Y", "YES", "TRUE", "1"):
        return True
    if v in ("N", "NO", "FALSE", "0"):
        return False
    return default


def _sql_quote(val) -> str:
    """SQL-safe quoting for simple scalar values (mirrors PipelineRegistry._q)."""
    if val is None or val == "":
        return "null"
    return f"'{str(val).replace(chr(39), chr(39) * 2)}'"


# ── Step 0: land raw SAP metadata BEFORE any pipeline registration ─────────
# This is intentionally decoupled from add_source()/register(). If SAP
# metadata was successfully read, we capture it here regardless of whether
# pipeline registration later succeeds or fails — so a metadata scan is
# never wasted, unmapped ("rescued") columns stay visible for review, and
# failed rows can be re-driven from this table without re-touching the SAP
# volume at all.

def _land_sap_metadata(spark, pipeline_name: str, source_row: dict, meta: dict):
    """
    Upsert into sap_metadata_landing, keyed on (pipeline_name, table_name).
    A re-run against the same table (re-scanning after a metadata change,
    or re-driving a failed row) UPDATEs the existing landing row in place
    rather than accumulating duplicate scan history. landing_id/created_at
    are preserved on update; only the scan payload + updated_at change.
    is_processed/processed_at/registration_error are reset to reflect
    that this is a fresh, not-yet-(re)registered scan.
    """
    table_name    = str(source_row.get("Table_name", "")).strip()
    entity_name   = table_name.lower().replace(".", "_")
    metadata_path = str(source_row.get("Metadata_path", "")).strip()
    source_path   = str(source_row.get("Source_path", "")).strip()
    load_type_req = str(source_row.get("Load_type", "full")).strip().lower()

    all_cols  = meta.get("columns", [])
    known     = meta.get("schema_hints", {})
    known_names = {c.rstrip("!") for c in known.keys()}
    rescued   = [
        {"name": c.get("name", ""), "dataType": c.get("dataType", "")}
        for c in all_cols
        if c.get("name", "") not in known_names
    ]

    try:
        file_size = len(dbutils.fs.head(metadata_path, 1024 * 1024).encode("utf-8"))
    except Exception:
        file_size = None

    q = _sql_quote
    try:
        spark.sql(f"""
            MERGE INTO {CTRL}.sap_metadata_landing AS tgt
            USING (
                SELECT
                    {q(pipeline_name)}                          AS pipeline_name,
                    {q(table_name)}                              AS table_name,
                    {q(entity_name)}                              AS entity_name,
                    {q(metadata_path)}                             AS metadata_path,
                    {q(source_path)}                                AS source_path,
                    {q(meta.get("format"))}                          AS format,
                    {q(load_type_req)}                                AS load_type_requested,
                    {q(",".join(meta.get("primary_keys", [])))}        AS primary_keys,
                    {q(meta.get("key_str"))}                             AS key_str,
                    {q(meta.get("watermark"))}                            AS watermark_col,
                    {q(meta.get("operation_col"))}                         AS operation_col,
                    {q(meta.get("sequence_col"))}                           AS sequence_col,
                    {q(meta.get("soft_delete_condition"))}                   AS soft_delete_condition,
                    {q(json.dumps(all_cols))}                                 AS raw_columns_json,
                    {q(json.dumps(known))}                                     AS schema_hints_json,
                    {q(json.dumps(rescued))}                                    AS rescued_columns_json,
                    {file_size if file_size is not None else "null"}            AS metadata_file_size_bytes
            ) AS src
            ON  tgt.pipeline_name = src.pipeline_name
            AND tgt.table_name    = src.table_name
            WHEN MATCHED THEN UPDATE SET
                tgt.entity_name               = src.entity_name,
                tgt.metadata_path             = src.metadata_path,
                tgt.source_path               = src.source_path,
                tgt.format                    = src.format,
                tgt.load_type_requested       = src.load_type_requested,
                tgt.primary_keys              = src.primary_keys,
                tgt.key_str                   = src.key_str,
                tgt.watermark_col             = src.watermark_col,
                tgt.operation_col             = src.operation_col,
                tgt.sequence_col              = src.sequence_col,
                tgt.soft_delete_condition     = src.soft_delete_condition,
                tgt.raw_columns_json          = src.raw_columns_json,
                tgt.schema_hints_json         = src.schema_hints_json,
                tgt.rescued_columns_json      = src.rescued_columns_json,
                tgt.metadata_file_size_bytes  = src.metadata_file_size_bytes,
                tgt.scanned_at                = current_timestamp(),
                tgt.is_processed              = false,
                tgt.processed_at              = null,
                tgt.registration_error        = null,
                tgt.updated_at                = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                landing_id, pipeline_name, table_name, entity_name, metadata_path,
                source_path, format, load_type_requested, primary_keys, key_str,
                watermark_col, operation_col, sequence_col, soft_delete_condition,
                raw_columns_json, schema_hints_json, rescued_columns_json,
                metadata_file_size_bytes, scanned_at, is_processed,
                created_at, updated_at
            ) VALUES (
                {q(str(uuid.uuid4()))}, src.pipeline_name, src.table_name,
                src.entity_name, src.metadata_path, src.source_path,
                src.format, src.load_type_requested, src.primary_keys, src.key_str,
                src.watermark_col, src.operation_col, src.sequence_col,
                src.soft_delete_condition, src.raw_columns_json,
                src.schema_hints_json, src.rescued_columns_json,
                src.metadata_file_size_bytes, current_timestamp(), false,
                current_timestamp(), current_timestamp()
            )
        """)
        if rescued:
            logger.info(f"  Landed  : {table_name}  ({len(rescued)} rescued col(s) — see sap_metadata_landing)")
        else:
            logger.info(f"  Landed  : {table_name}")
    except Exception as exc:
        logger.warning(f"  Could not land metadata for '{table_name}': {exc}")


def _mark_landing_processed(spark, table_name: str, pipeline_name: str, error: str = None):
    q = _sql_quote
    try:
        spark.sql(f"""
            UPDATE {CTRL}.sap_metadata_landing
            SET    is_processed       = true,
                   processed_at       = current_timestamp(),
                   registration_error = {q(error)},
                   updated_at         = current_timestamp()
            WHERE  table_name    = {q(table_name)}
            AND    pipeline_name = {q(pipeline_name)}
            AND    is_processed  = false
        """)
    except Exception as exc:
        logger.warning(f"  Could not update landing status for '{table_name}': {exc}")


def _collapse_initial_delta_duplicates(pipeline_name: str, group_rows: list) -> list:
    """
    SAP DSP CDC exports commonly land as two sibling folders per table:

        {table}/initial/   -> full snapshot (first extraction)
        {table}/delta/      -> incremental change files (ongoing CDC)

    A template with one row per folder produces two rows sharing the same
    Table_name — which normalize to the same entity_name and collide in
    add_source() ("Source 'x' already added"). They aren't two sources: a
    streaming/Auto Loader read against the *parent* folder recursively picks
    up both initial/ and delta/ subfolders in file order, so this is really
    one source. When exactly this initial+delta pattern is detected, collapse
    it into a single row pointing at the shared parent folder. Any other kind
    of duplicate Table_name (paths that don't fit this pattern) is a genuine
    template ambiguity and raises a clear error instead of guessing.
    """
    by_table = defaultdict(list)
    for row in group_rows:
        by_table[str(row.get("Table_name", "")).strip()].append(row)

    collapsed = []
    for table_name, rows in by_table.items():
        if len(rows) == 1:
            collapsed.append(rows[0])
            continue

        paths     = [str(r.get("Source_path", "")).rstrip("/") for r in rows]
        suffixes  = {p.rsplit("/", 1)[-1] for p in paths}
        parents   = {p.rsplit("/", 1)[0] for p in paths}

        if suffixes <= {"initial", "delta"} and len(parents) == 1:
            parent = next(iter(parents))
            merged = dict(rows[0])
            merged["Source_path"] = parent + "/"
            logger.info(
                f"  Collapsed {len(rows)} row(s) for '{table_name}' "
                f"(initial/delta split) -> single source at {merged['Source_path']}"
            )
            collapsed.append(merged)
        else:
            raise ValueError(
                f"Pipeline '{pipeline_name}': Table_name '{table_name}' appears "
                f"{len(rows)} times with source paths that don't match the "
                f"standard initial/delta split: {paths}. Either make Table_name "
                f"unique per row, or point Source_path at each table's shared "
                f"parent folder (not the initial/ or delta/ subfolder itself) "
                f"so it's a single row."
            )
    return collapsed


def load_sap_pipeline_from_table(environment: str, pipeline_name: str):
    """
    Read rows for exactly one pipeline from the Delta config table
    (PIPELINE_TABLE_CONFIG, default rgaplxdatabricks.uct_demo.pipeline_table_config)
    and register that single pipeline. The Excel template is no longer read
    anywhere; the config-entry notebook MERGEs widget values into this table,
    one row per (pipeline_name, table_name).

    Column names are matched case-insensitively (the table uses lower_case,
    the code below uses Title_Case). Required columns: pipeline_name,
    table_name, source_path, load_type. Optional columns (merge_keys,
    watermark, format, join_silvers) may be absent from the table — they
    then read as empty and the SAP-metadata-derived defaults apply.

    Columns (same meaning as the old Excel template):
      Pipeline_name           -> filtered on directly in the SQL query below;
                                  every row returned belongs to this one pipeline
      Table_name               -> source entity name
      Source_path               -> path(s) relative to LANDING_BASE_PATH;
                                    one path, or multiple comma-separated
                                    paths. Each is prefixed with
                                    LANDING_BASE_PATH to build the absolute
                                    autoloader path(s). e.g.
                                    "Z_MARA/initial/,Z_MARA/delta/"
                                    NOTE: Metadata_path is NOT a template
                                    column — it is always derived as
                                    {LANDING_BASE_PATH}/{Table_name}/.sap.partfile.metadata
                                    (see _resolve_landing_paths)
      Bronze_schema                -> bronze schema (per pipeline, from first row)
      Silver_schema                 -> silver schema (per pipeline, from first row)
      Gold_schema                    -> gold schema (per pipeline, from first row)
      Silver_table                    -> silver table name
      Load_type                        -> full / scd1 / scd2 / delta / delta_merge
                                           full   -> bronze + DLT silver, "snapshot" dlt_mode
                                                      (undeduped mirror, every event as-is)
                                           scd1   -> bronze + DLT silver, "upsert" dlt_mode
                                                      (create_auto_cdc_flow, one row per key)
                                           scd2   -> bronze + DLT silver, "history" dlt_mode
                                                      (create_auto_cdc_flow, full history kept)
                                           delta  -> bronze + DLT silver, "delta" dlt_mode
                                                      (plain incremental append, sequenced by
                                                       watermark, no keys/merge)
                                           delta_merge -> bronze + DLT silver too, registered
                                                      with silver type "scd1" — delta_merge has
                                                      no dlt_mode of its own, it's just scd1
                                                      (real upsert on keys) under a different
                                                      source-level name
                                           All five still get a materialized bronze layer via
                                           Auto Loader regardless of type — only the silver
                                           dlt_mode differs.
      Merge_keys                        -> comma list, REQUIRED when Load_type=delta_merge
                                            (used for both bronze source registration and,
                                             unless Gold_merge_keys overrides it, gold)
      Watermark                          -> auto-detected from .sap.partfile.metadata
                                             (semanticType=_change_mode + TIMESTAMP -> __timestamp)
                                             table column overrides if provided
      Soft_delete_condition               -> apply_as_deletes expr (scd2 only). Must contain
                                              a real SQL operator (=, !=, <>, LIKE, IS) — a
                                              bare value like "D" is rejected with a warning
                                              and the SAP-metadata-derived condition is used
                                              instead (see _looks_like_sql_condition).
      Is_active                            -> True/False, skip row if False
      Format                                -> optional override (else from metadata file)

      Gold_view                              -> name of the gold VIEW for this row's table.
                                                 Gold is VIEW-ONLY — there is no materialized
                                                 gold table option (this may change — the
                                                 table-vs-view question for DLT-native
                                                 Materialized Views / Streaming Tables is still
                                                 open). A view has no physical storage: it's a
                                                 plain `CREATE OR REPLACE VIEW` re-pointed at
                                                 current silver on every run, so there's no
                                                 load_type/merge_keys/watermark to configure
                                                 for it — it's always a full re-read.
      Gold_sql_file                          -> optional. Filename of a .sql file under
                                                 {BASE_VOLUME}/sql_files/<Gold_sql_file>
                                                 holding the view's SELECT body. If blank (and
                                                 Gold_view is set), the view defaults to a
                                                 straight passthrough of this row's own silver:
                                                 "SELECT * FROM {silver_table}".
                                                 If given, the SQL file's text is used as-is,
                                                 with exactly one auto-substitution: the literal
                                                 placeholder "{silver_table}" is replaced with
                                                 THIS row's own silver table, fully qualified
                                                 (rgaplxdatabricks.{Silver_schema}.
                                                 {Pipeline_name}_{Silver_table}_silver). If the
                                                 placeholder isn't found in the file, a warning
                                                 is logged rather than silently shipping a gold
                                                 object with no primary silver reference.
      Join_Silvers                           -> optional comma list of OTHER silver names
                                                 (from other rows in this same pipeline) that
                                                 Gold_sql_file's SQL joins against. Only
                                                 meaningful alongside Gold_sql_file — a plain
                                                 passthrough view has nothing to join. Each
                                                 name gets a short alias (underscores stripped,
                                                 truncated to 12 chars — e.g. "customer_dim"
                                                 -> "customerdim") that YOU must use verbatim
                                                 in the .sql file's JOIN clause; nothing here
                                                 checks that your SQL text and the generated
                                                 alias actually agree, and a collision between
                                                 two names' generated aliases is logged as a
                                                 warning, not blocked. This only registers which
                                                 tables exist under which aliases — it does NOT
                                                 write the JOIN/ON clause or any WHERE filter
                                                 for you.
      Depends_on_silvers                     -> optional comma list of silver names this SQL
                                                 file references, in order (first is normally
                                                 this row's own silver, matching {silver_table}).
                                                 Informational only — used for the lineage
                                                 trail in pipeline_definitions.depends_on. It
                                                 does NOT drive any substitution, aliasing, or
                                                 ordering — that's what Join_Silvers is for.
                                                 gold_pipeline.py always runs after the ENTIRE
                                                 bronze/silver Lakeflow pipeline has completed,
                                                 so every silver a gold SQL file might reference
                                                 already exists by the time any gold view is
                                                 (re)created — there's nothing to wait on. If
                                                 omitted, defaults to just this row's own
                                                 Silver_table.
    """

    pipeline_name = (pipeline_name or "").strip()
    if not pipeline_name:
        raise ValueError("load_sap_pipeline_from_table: pipeline_name is required")

    q = _sql_quote
    if not spark.catalog.tableExists(PIPELINE_TABLE_CONFIG):
        raise ValueError(
            f"Config table {PIPELINE_TABLE_CONFIG} does not exist or is not "
            f"accessible — check the config_table widget and your grants."
        )

    # Record which Delta version of the config was used, so a registered
    # pipeline config can always be traced back to the exact config rows
    # (SELECT * FROM <table> VERSION AS OF <n>).
    try:
        config_version = spark.sql(
            f"DESCRIBE HISTORY {PIPELINE_TABLE_CONFIG} LIMIT 1"
        ).collect()[0]["version"]
        logger.info(f"Reading {PIPELINE_TABLE_CONFIG} at Delta version {config_version}")
    except Exception:
        config_version = None

    df = spark.sql(
        f"SELECT * FROM {PIPELINE_TABLE_CONFIG} WHERE pipeline_name = {q(pipeline_name)}"
    )

    # Normalize column names to the canonical Title_Case the rest of this
    # file expects, matched CASE-INSENSITIVELY against whatever the Delta
    # table's actual columns are. This matters because the WHERE clause
    # above resolves 'Pipeline_name' case-insensitively (Spark's default),
    # so the query succeeds and returns rows even if the table's real
    # column is e.g. 'pipeline_name' or 'PIPELINE_NAME' — but row.asDict()
    # preserves whatever the ACTUAL stored case is, and plain Python dict
    # lookups like row.get("Pipeline_name") ARE case-sensitive. Without
    # this normalization, every row.get("Pipeline_name")/.get("Table_name")
    # below silently returns None even though the query above found rows,
    # which is exactly what "Pipeline_name missing for table 'None'"
    # (Table_name also None) means: the query matched, but every field
    # lookup afterward missed due to a casing mismatch.
    CANONICAL_COLUMNS = [
        "Pipeline_name", "Table_name", "Source_path",
        "Bronze_schema", "Silver_schema", "Gold_schema",
        "Silver_table", "Load_type", "Merge_keys", "Watermark",
        "Soft_delete_condition", "Is_active", "Format",
        "Gold_sql_file", "Gold_view", "Join_Silvers", "Depends_on_silvers",
    ]
    REQUIRED_COLUMNS = {"Pipeline_name", "Table_name", "Source_path", "Load_type"}
    actual_by_lower = {c.lower(): c for c in df.columns}
    unmatched = [c for c in CANONICAL_COLUMNS if c.lower() not in actual_by_lower]
    missing_required = [c for c in unmatched if c in REQUIRED_COLUMNS]
    if missing_required:
        raise ValueError(
            f"{PIPELINE_TABLE_CONFIG} is missing required column(s) "
            f"{missing_required}. Actual columns: {df.columns}"
        )
    missing_optional = [c for c in unmatched if c not in REQUIRED_COLUMNS]
    if missing_optional:
        logger.info(
            f"  {PIPELINE_TABLE_CONFIG} has no column(s) {missing_optional} — "
            f"they read as empty for every row (SAP-metadata defaults apply)."
        )

    rows = []
    for r in df.collect():
        raw = r.asDict()
        out = {}
        for canon in CANONICAL_COLUMNS:
            actual_name = actual_by_lower.get(canon.lower())
            v = raw.get(actual_name) if actual_name else None
            out[canon] = "" if v is None else str(v)
        rows.append(out)

    logger.info(f"Read {len(rows)} row(s) for pipeline_name='{pipeline_name}' "
                f"from {PIPELINE_TABLE_CONFIG}")

    if not rows:
        raise ValueError(
            f"No rows found in {PIPELINE_TABLE_CONFIG} for "
            f"Pipeline_name='{pipeline_name}'. Check the widget value "
            f"against what was loaded into the table."
        )

    # ── Derive Metadata_path + expand Source_path against the landing base
    # Metadata_path is no longer read from the template; Source_path is
    # resolved from relative (possibly comma-separated) to absolute here,
    # before any filtering/grouping/collapsing below.
    for row in rows:
        if _truthy(row.get("Is_active"), default=True) and not row["Source_path"].strip():
            raise ValueError(
                f"'{row['Table_name']}': source_path is empty in {PIPELINE_TABLE_CONFIG}. "
                f"Set it via the config-entry notebook."
            )

    rows = [_resolve_landing_paths(row) for row in rows]

    # ── Filter active rows ──────────────────────────────────
    active_rows = []
    for row in rows:
        table_name = str(row.get("Table_name", "")).strip()
        is_active  = _truthy(row.get("Is_active"), default=True)
        if not is_active:
            logger.info(f"  SKIPPED (Is_active=False) : {table_name}")
            continue
        load_type = (str(row.get("Load_type") or "").strip() or "full").lower()
        row["Load_type"] = load_type
        if load_type not in VALID_SOURCE_LOAD_TYPES:
            raise ValueError(
                f"'{table_name}': Load_type must be one of "
                f"{sorted(VALID_SOURCE_LOAD_TYPES)} — got '{load_type}'")
        if load_type == "delta_merge" and not str(row.get("Merge_keys", "")).strip():
            raise ValueError(f"'{table_name}': Load_type=delta_merge requires Merge_keys")
        active_rows.append(row)

    if not active_rows:
        logger.info("No active rows found — nothing to register")
        return {}

    # ── Group by Pipeline_name ──────────────────────────────
    # Kept even though the SQL query already filtered to one pipeline_name —
    # cheap defensive check against stray/duplicate data entry, and reuses
    # the same code path as before with no other changes.
    groups = defaultdict(list)
    for row in active_rows:
        pname = str(row.get("Pipeline_name", "")).strip()
        if not pname:
            raise ValueError(
                f"Pipeline_name missing for table '{row.get('Table_name')}'"
            )
        groups[pname].append(row)

    if list(groups.keys()) != [pipeline_name]:
        raise ValueError(
            f"Expected only rows for Pipeline_name='{pipeline_name}' but "
            f"found {list(groups.keys())} — check {PIPELINE_TABLE_CONFIG} "
            f"for a data-entry issue."
        )

    # Collapse the standard SAP DSP initial/+delta/ export split into single
    # rows BEFORE metadata is landed or anything is registered, so a table
    # split across two folders is only ever read and registered once.
    for pname in list(groups.keys()):
        groups[pname] = _collapse_initial_delta_duplicates(pname, groups[pname])

    logger.info(f"\nFound {len(groups)} pipeline group(s): {list(groups.keys())}")

    # ── Step 0: land ALL active rows' SAP metadata first ─────
    # Runs before any PipelineRegistry is touched — a metadata read is
    # captured even if registration for that pipeline later fails.
    logger.info("\n" + "=" * 60)
    logger.info("STEP 0: Landing SAP metadata")
    logger.info("=" * 60)
    landed_meta = {}   # (pipeline_name, table_name) -> meta dict, reused below
    for pipeline_name, group_rows in groups.items():
        for source in group_rows:
            table_name    = str(source["Table_name"]).strip()
            metadata_path = str(source["Metadata_path"]).strip()
            meta = read_sap_metadata(metadata_path)
            landed_meta[(pipeline_name, table_name)] = meta
            _land_sap_metadata(spark, pipeline_name, source, meta)

    results = {}

    # ── Process each pipeline group ─────────────────────────
    for pipeline_name, group_rows in groups.items():

        logger.info("\n" + "=" * 60)
        logger.info(f"PIPELINE: {pipeline_name}  ({len(group_rows)} source(s))")
        logger.info("=" * 60)

        # Schemas — taken from first row of the group
        first = group_rows[0]
        # `or` rather than .get(key, default): every canonical key is always
        # present (as "" when the column is NULL), so .get's default never fired.
        bronze_schema = str(first.get("Bronze_schema") or "").strip() or "sap_demo_bronze"
        silver_schema = str(first.get("Silver_schema") or "").strip() or "sap_demo_silver"
        gold_schema   = str(first.get("Gold_schema")   or "").strip() or "sap_demo_gold"

        # Schemas are pipeline-level but stored per row — warn if rows disagree
        # instead of silently using the first row's values.
        for col, chosen in (("Bronze_schema", bronze_schema),
                            ("Silver_schema", silver_schema),
                            ("Gold_schema",   gold_schema)):
            others = {str(r.get(col) or "").strip() for r in group_rows} - {"", chosen}
            if others:
                logger.warning(
                    f"  {col} differs across rows of '{pipeline_name}': using "
                    f"'{chosen}', ignoring {sorted(others)}"
                )

        r = PipelineRegistry(
            spark, dbutils,
            pipeline_id    = pipeline_name,
            pipeline_name  = f"SAP Pipeline — {pipeline_name}",
            environment    = environment,
            target_catalog = "rgaplxdatabricks",
            bronze_schema  = bronze_schema,
            silver_schema  = silver_schema,
            gold_schema    = gold_schema,
            config_dir     = CONFIG_DIR,
            alert_email    = "venkatesh.chandani@applexus.com",
            notes          = (f"SAP config-table-driven onboarding (multi-source) — "
                              f"{PIPELINE_TABLE_CONFIG}"
                              + (f" @ v{config_version}" if config_version is not None else ""))
        )

        for source in group_rows:
            # FIX (ported from the Excel orchestrator): bind table_name
            # before anything risky happens, so the except block below
            # always names the right table even if a later field access
            # in this iteration raises.
            table_name    = str(source["Table_name"]).strip()
            source_entity = table_name.lower().replace(".", "_")
            source_path   = str(source["Source_path"]).strip()
            metadata_path = str(source["Metadata_path"]).strip()
            silver_name   = str(source["Silver_table"]).strip()
            load_type     = str(source.get("Load_type") or "full").strip().lower()
            merge_keys    = str(source.get("Merge_keys", "")).strip() or None

            logger.info("─" * 55)
            logger.info(f"  Table       : {table_name}")
            logger.info(f"  Path        : {source_path}")
            logger.info(f"  Metadata    : {metadata_path}")
            logger.info(f"  Load type   : {load_type}")
            if silver_name:
                logger.info(f"  Silver      : {silver_name}")

            try:
                # Reuse metadata read during Step 0 — no second file read.
                meta = landed_meta.get((pipeline_name, table_name)) \
                       or read_sap_metadata(metadata_path)

                # Format: template override if provided, else from metadata
                csv_format  = str(source.get("Format", "")).strip().lower()
                file_format = csv_format if csv_format else meta["format"]

                # Watermark — auto-detected from .sap.partfile.metadata
                # (semanticType="_change_mode" + TIMESTAMP -> __timestamp)
                # template Watermark column overrides if provided
                watermark_raw = str(source.get("Watermark", "")).strip()
                watermark     = watermark_raw if watermark_raw \
                                else meta.get("watermark", "__timestamp")

                # Soft delete — auto-detected; only meaningful for scd2.
                # FIX (ported from the Excel orchestrator): validate the
                # template override before trusting it. A bare value like
                # "D" isn't a SQL condition — using it as one breaks the
                # DLT apply_as_deletes expression at run time, far removed
                # from where the typo was actually made.
                soft_delete_raw = str(source.get("Soft_delete_condition", "")).strip()
                if soft_delete_raw and not _looks_like_sql_condition(soft_delete_raw):
                    logger.warning(
                        f"  Soft_delete_condition for '{table_name}' looks like a bare "
                        f"value, not a SQL condition: {soft_delete_raw!r}. Ignoring the "
                        f"override and using the SAP-metadata-derived condition instead."
                    )
                    soft_delete_raw = ""
                soft_delete = (soft_delete_raw if soft_delete_raw
                               else meta.get("soft_delete_condition")) \
                              if load_type == "scd2" else None

                logger.info(f"  Format      : {file_format}"
                      + (" (override)" if csv_format else " (from metadata)"))
                logger.info(f"  Watermark   : {watermark}")
                if load_type == "scd2" and soft_delete:
                    logger.info(f"  Soft delete : {soft_delete}")
                logger.info(f"  Cols        : {len(meta['schema_hints'])}")

                # Register bronze — load_type carried through so downstream
                # config generation knows how bronze should refresh, not just
                # that it's a DLT source.
                r.add_source(
                    source_entity,
                    path                   = source_path,
                    keys                   = merge_keys or meta["key_str"],
                    watermark              = watermark,
                    format                 = file_format,
                    schema                 = meta["schema_hints"],
                    load_type              = load_type,
                    operation_col          = meta.get("operation_col"),
                    sequence_col           = meta.get("sequence_col"),
                    soft_delete_condition  = meta.get("soft_delete_condition"),
                )

                # Register silver — for all five load types now. delta_merge
                # has no dlt_mode of its own: it's just scd1 (real upsert on
                # keys) under a different source-level name, so it's
                # registered with silver type "scd1" directly rather than
                # adding a redundant parallel code path. "delta" gets its
                # own silver type -> "delta" dlt_mode (plain incremental
                # append sequenced by watermark, see register_silver_delta
                # in lakeflow_pipeline.py).
                silver_type = "scd1" if load_type == "delta_merge" else load_type
                if load_type in ("full", "scd1", "scd2", "delta", "delta_merge") and silver_name:
                    silver_kwargs = dict(
                        name        = silver_name,
                        type        = silver_type,
                        from_entity = source_entity,
                        keys        = merge_keys or meta["key_str"],
                        watermark   = watermark,
                    )
                    if load_type == "scd2" and soft_delete:
                        silver_kwargs["soft_delete"] = soft_delete
                    r.add_silver(**silver_kwargs)

                # Register gold — a MATERIALIZED VIEW, one per source table.
                # Gold is a 1:1 copy of this row's silver with light
                # transforms (casts, renames) from Gold_sql_file, or a plain
                # SELECT * passthrough. Every table reaches gold: if Gold_view
                # is blank, the table name is used, so the gold object is
                # {pipeline_name}_{gold_view or table}_gold in Gold_schema.
                # An MV is refreshed from its query, so there's no
                # load_type/merge_keys/watermark to configure for it.
                gold_sql_file = str(source.get("Gold_sql_file", "")).strip()
                gold_view     = str(source.get("Gold_view", "")).strip()
                join_silvers_raw = str(source.get("Join_Silvers", "")).strip()
                gold_object   = gold_view or source_entity

                has_dlt_silver = bool(silver_name) and load_type in (
                    "full", "scd1", "scd2", "delta", "delta_merge")
                silver_full = None
                if has_dlt_silver:
                    silver_layer = f"{pipeline_name}_{silver_name}_silver"
                    silver_full  = f"rgaplxdatabricks.{silver_schema}.{silver_layer}"

                # A gold view needs either an explicit SQL file, or can
                # default to a straight silver passthrough.
                if gold_object and not (gold_sql_file or has_dlt_silver):
                    logger.info(
                        f"  Gold SKIPPED: '{gold_object}' has no Gold_sql_file and "
                        f"no DLT silver to default a passthrough view from "
                        f"(silver_name={silver_name!r}, load_type={load_type!r})"
                    )
                    gold_object = None

                if gold_object:
                    try:
                        join_sources = []

                        if gold_sql_file:
                            sql_path = f"{BASE_VOLUME}/sql_files/{gold_sql_file}"
                            gold_sql = dbutils.fs.head(sql_path, 1_000_000)

                            # FIX (ported from the Excel orchestrator): check
                            # the placeholder actually exists before
                            # replacing. If it's missing (typo, wrong case,
                            # extra space in the .sql file), the gold object
                            # would otherwise ship with no primary-silver
                            # reference at all — a query that "works" but
                            # reads from nowhere.
                            if silver_full and "{silver_table}" not in gold_sql:
                                logger.warning(
                                    f"  '{{silver_table}}' placeholder not found in "
                                    f"{gold_sql_file} — gold object '{gold_object}' may be "
                                    f"missing its primary silver reference."
                                )
                            if silver_full:
                                gold_sql = gold_sql.replace("{silver_table}", silver_full)
                            sql_source_desc = gold_sql_file

                            # FIX (ported from the Excel orchestrator): resolve
                            # Join_Silvers into an actual join_sources list with
                            # collision-checked aliases, instead of leaving
                            # multi-silver joins with no registration path at
                            # all. Only meaningful alongside a real SQL file —
                            # a plain passthrough view has nothing to join.
                            seen_aliases = set()
                            if join_silvers_raw:
                                for s in [x.strip() for x in join_silvers_raw.split(",") if x.strip()]:
                                    alias = s.replace("_", "")[:12]
                                    if alias in seen_aliases:
                                        logger.warning(
                                            f"  Join alias collision for gold '{gold_object}': "
                                            f"'{alias}' (from '{s}') duplicates an existing "
                                            f"alias — the SQL will reference the wrong table."
                                        )
                                    seen_aliases.add(alias)
                                    join_sources.append({"entity": s, "type": "silver", "alias": alias})
                        else:
                            # Gold_view with no Gold_sql_file: default silver->gold
                            # passthrough view. Nothing to join.
                            gold_sql = f"SELECT * FROM {silver_full}"
                            sql_source_desc = "default passthrough (SELECT * FROM silver_table)"

                        # Informational only (feeds pipeline_definitions.depends_on
                        # for the lineage trail) — gold_pipeline.py always runs
                        # after the entire bronze/silver Lakeflow pipeline has
                        # completed, so nothing here needs to gate execution order.
                        depends_raw = str(source.get("Depends_on_silvers", "")).strip()
                        depends_on_list = (
                            [s.strip() for s in depends_raw.split(",") if s.strip()]
                            or ([silver_name] if silver_name else [])
                        )

                        gold_kwargs = dict(
                            name            = gold_object,
                            sql             = gold_sql,
                            load_type       = "full",
                            materialization = "view",
                            join_sources    = join_sources if join_sources else None,
                        )
                        if silver_name and has_dlt_silver:
                            gold_kwargs["from_silver"] = silver_name
                        else:
                            # No DLT silver for this row — gold view reads bronze directly.
                            gold_kwargs["from_bronze"] = source_entity

                        r.add_gold(**gold_kwargs)
                        logger.info(f"  Gold        : {gold_object} (materialized view)")
                        if join_sources:
                            logger.info(f"  Joins       : {[j['entity'] for j in join_sources]}")
                        if len(depends_on_list) > 1:
                            logger.info(f"  Depends on  : {depends_on_list}")
                        logger.info(f"  SQL source  : {sql_source_desc}")
                    except Exception as e:
                        logger.info(f"  Gold SKIPPED: {e}")

                _mark_landing_processed(spark, table_name, pipeline_name)

            except Exception as exc:
                _mark_landing_processed(spark, table_name, pipeline_name, error=str(exc))
                raise

        logger.info("─" * 55)
        config_path = r.register(overwrite=True)
        results[pipeline_name] = config_path
        logger.info(f"  Config    : {config_path}")

    logger.info("\n" + "=" * 60)
    logger.info(f"DONE — {len(results)} pipeline(s) registered")
    for pname, path in results.items():
        logger.info(f"  {pname} -> {path}")
    logger.info("=" * 60)

    return results

# ── Run ──────────────────────────────────────────────────────
# Driven entirely by the environment/pipeline_name widgets — generates a
# config for exactly this one pipeline, reading its rows from
# the Delta config table (the Excel template is no longer used anywhere).

load_sap_pipeline_from_table(ENVIRONMENT, PIPELINE_NAME)

# ── Archive rotation — NOT automatic, must be scheduled separately ────────
# CORRECTED from a previous version of this comment, which claimed cleanup
# happened inline on every register() call. It doesn't. pipeline_registry.py
# writes a new, uniquely-timestamped config file under
# {CONFIG_DIR}/{pipeline_id}/ on every register(overwrite=True) call above,
# and never deletes or caps anything itself — nothing here keeps "only the
# latest + N archived." Old config files accumulate in that folder forever
# unless purge_old_configs() (also in pipeline_registry.py) is run on its
# own schedule, e.g. as a separate daily Lakeflow Job:
#
#   purge_old_configs(spark, dbutils, CONFIG_DIR, retention_days=7)
#
# It moves config files older than retention_days into a dated archive/
# subfolder per pipeline, skipping whichever file is currently marked
# 'current' for that pipeline/environment. If that job isn't scheduled,
# nothing else in this codebase will do this cleanup for you.