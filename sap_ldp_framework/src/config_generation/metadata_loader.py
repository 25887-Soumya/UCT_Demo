# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "openpyxl",
# ]
# ///
# MAGIC %pip install openpyxl

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

from collections import defaultdict
import json
import logging
import uuid
from datetime import datetime

logger = logging.getLogger("sap_ingestion_orchestrator")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_h)

# Set from the framework_root parameter in the "Environment parameters"
# cell (framework_root/sql_files and framework_root/configs).
BASE_VOLUME = None
CONFIG_DIR  = None

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
# OPTIONAL: path to the onboarding Excel (sap_onboard_template.xlsx). If set, its
# rows for this pipeline_name are upserted into pipeline_table_config before the
# config is generated. If EMPTY, nothing changes — rows are read from
# pipeline_table_config exactly as before.
dbutils.widgets.text("template_path", "", "Template Path (Excel .xlsx or .csv, optional)")

ENVIRONMENT   = dbutils.widgets.get("environment").strip().lower()
PIPELINE_NAME = dbutils.widgets.get("pipeline_name").strip()
TEMPLATE_PATH = dbutils.widgets.get("template_path").strip()

logger.info(f"Widgets resolved -> environment={ENVIRONMENT!r}  pipeline_name={PIPELINE_NAME!r}  "
            f"template_path={TEMPLATE_PATH!r}")

# COMMAND ----------

# MAGIC %run ./pipeline_registry

# COMMAND ----------

# MAGIC %run ./read_sap_metadata

# COMMAND ----------

# MAGIC %run ./read_sql_server_metadata

# COMMAND ----------

if not PIPELINE_NAME:
    raise ValueError(
        "Widget 'pipeline_name' is required — set it to a Pipeline_name "
        "value present in pipeline_table_config."
    )

# Landing zone for SAP DSP exports. Metadata_path is no longer a template
# column — it is always derived from this base + Table_name. Source_path
# values in the template are relative to this same base (one or more,
# comma-separated); each is prefixed with LANDING_BASE_PATH before use.
LANDING_BASE_PATH = None   # set from the landing_base_path parameter ("Environment parameters" cell)

# CTRL and VALID_*_LOAD_TYPES come from pipeline_registry.py via
# `%run ./pipeline_registry` (see COMMAND cell at top of this notebook).
# VALID_SOURCE_LOAD_TYPES = {"full", "scd1", "scd2", "delta", "delta_merge"}

PIPELINE_TABLE_CONFIG = f"{CTRL}.pipeline_table_config"



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
        -> Metadata_path = {LANDING_BASE_PATH}Z_MARA_CDS_NEW/.sap.partfile.metadata
        -> Source_path   = {LANDING_BASE_PATH}Z_MARA_CDS_NEW/initial/
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


def _read_metadata_for_source(source: dict) -> dict:
    """
    Dispatches to the right metadata reader based on this row's
    Connection_name:
      ""  / "SAP"         -> read_sap_metadata(Metadata_path)   (default,
                              unchanged — every existing row with no
                              Connection_name set keeps working exactly
                              as before)
      "SQL_SERVER"        -> read_sql_server_metadata(Table_name, Connection_name)
      anything else       -> raises; not yet a recognized source type
    Returns the same dict shape either way — format, load_type, columns,
    primary_keys, key_str, watermark, operation_col, sequence_col,
    soft_delete_condition, schema_hints — so every caller below is
    unaffected by which reader actually ran.
    """
    table_name      = str(source.get("Table_name", "")).strip()
    connection_name = str(source.get("Connection_name", "")).strip()
    conn_upper      = connection_name.upper()

    if conn_upper in ("", "SAP"):
        metadata_path = str(source["Metadata_path"]).strip()
        return read_sap_metadata(metadata_path)
    elif conn_upper == "SQL_SERVER":
        return read_sql_server_metadata(table_name, connection_name)
    else:
        raise ValueError(
            f"'{table_name}': Connection_name='{connection_name}' is not a "
            f"recognized source type — expected blank/'SAP' or 'SQL_SERVER'."
        )


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


# ── Excel template -> pipeline_table_config ─────────────────────────────────
# Replacement for the load_pipeline_table_config widget notebook: same columns,
# same MERGE semantics (key = pipeline_name + table_name), but values come from
# an Excel sheet instead of widgets. Only runs when the template_path widget is
# non-empty.

_TEMPLATE_COLUMNS = [
    "pipeline_name", "table_name", "source_path", "bronze_schema",
    "silver_schema", "gold_schema", "silver_table", "gold_view",
    "load_type", "soft_delete_condition", "is_active",
    "gold_sql_file", "depends_on_silvers",
    "skip_source_to_landing", "connection_name",
    "watermark_col", "operation_col", "sequence_col",
]
_TEMPLATE_REQUIRED  = ["pipeline_name", "table_name", "source_path",
                       "bronze_schema", "silver_schema", "gold_schema"]
_TEMPLATE_BOOLEANS  = {"is_active": True, "skip_source_to_landing": False}   # col -> default
_TEMPLATE_NULLABLE  = ["soft_delete_condition", "connection_name",
                       "gold_sql_file", "depends_on_silvers",
                       "watermark_col", "operation_col", "sequence_col"]     # blank -> NULL


def _cell_to_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    # Users often type the literal word null/none in a cell meaning "blank".
    return "" if s.lower() in ("null", "none", "nan") else s


def _read_csv_rows(path: str) -> list:
    """All rows of a CSV template as tuples. Handles the UTF-8 byte-order mark
    Excel adds on "CSV UTF-8" export, and comma, semicolon or tab separators
    (Excel uses ";" in many European locales)."""
    import csv
    with open(path, newline="", encoding="utf-8-sig") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel                       # plain comma-separated
        return [tuple(r) for r in csv.reader(fh, dialect)]


def _read_template_rows(template_path: str, pipeline_name: str) -> list:
    """Read the template - first sheet of an Excel workbook (.xlsx/.xlsm) or
    a CSV file (.csv) with the same columns; return cleaned row dicts
    (lower-case column names) for exactly this pipeline_name."""
    import os

    path = template_path[5:] if template_path.startswith("dbfs:/") else template_path
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"template_path widget is set but the file was not found: {template_path!r}")

    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        all_rows = _read_csv_rows(path)
        ws = type("CsvSheet", (), {"title": "csv"})()   # for the messages below
    elif ext in (".xlsx", ".xlsm"):
        import openpyxl
        wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
        ws = wb.worksheets[0]
        all_rows = list(ws.iter_rows(values_only=True))
        wb.close()
    else:
        raise ValueError(
            f"Template '{template_path}': unsupported file type '{ext or '(none)'}'. "
            f"Use .xlsx, .xlsm or .csv (save an old .xls as .xlsx first).")
    if not all_rows:
        raise ValueError(f"Template '{template_path}' is empty.")

    header = [_cell_to_str(h).lower() for h in all_rows[0]]
    missing = [c for c in _TEMPLATE_REQUIRED if c not in header]
    if missing:
        raise ValueError(
            f"Template '{template_path}' (sheet '{ws.title}') is missing required "
            f"column header(s): {missing}. Found: {[h for h in header if h]}")
    unknown = [h for h in header if h and h not in _TEMPLATE_COLUMNS]
    if unknown:
        logger.warning(f"  Template: ignoring unrecognized column(s): {unknown}")

    parsed, seen_pipelines = [], set()
    for excel_row_no, values in enumerate(all_rows[1:], start=2):
        rec = {h: _cell_to_str(v) for h, v in zip(header, values) if h in _TEMPLATE_COLUMNS}
        if not any(rec.values()):
            continue                                   # fully blank row
        seen_pipelines.add(rec.get("pipeline_name", ""))
        if rec.get("pipeline_name", "") != pipeline_name:
            continue                                   # another pipeline's row
        rec["_row"] = excel_row_no
        parsed.append(rec)

    if not parsed:
        raise ValueError(
            f"Template '{template_path}' has no rows for pipeline_name="
            f"'{pipeline_name}'. pipeline_name values found: "
            f"{sorted(p for p in seen_pipelines if p)}")
    return parsed


def load_template_into_pipeline_table_config(spark, template_path: str, pipeline_name: str) -> int:
    """
    Upsert this pipeline's Excel rows into {PIPELINE_TABLE_CONFIG}
    (MERGE on pipeline_name + table_name — identical to
    load_pipeline_table_config.sql). Returns the number of rows merged.
    Rows already in the table but absent from the Excel are left untouched
    (use is_active=false in the Excel to switch a table off).
    """
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType

    rows = _read_template_rows(template_path, pipeline_name)

    clean, seen_tables = [], {}
    for rec in rows:
        rn = rec["_row"]
        blanks = [c for c in _TEMPLATE_REQUIRED if not rec.get(c)]
        if blanks:
            raise ValueError(f"Template row {rn}: required column(s) empty: {blanks}")

        table_name = rec["table_name"]
        if table_name in seen_tables:
            raise ValueError(
                f"Template rows {seen_tables[table_name]} and {rn}: duplicate "
                f"table_name '{table_name}' for pipeline '{pipeline_name}'. "
                f"Use one row per table (point source_path at the shared parent "
                f"folder, or comma-separate multiple paths).")
        seen_tables[table_name] = rn

        load_type = (rec.get("load_type") or "full").lower()
        if load_type not in VALID_SOURCE_LOAD_TYPES:
            raise ValueError(
                f"Template row {rn} ('{table_name}'): load_type must be one of "
                f"{sorted(VALID_SOURCE_LOAD_TYPES)} — got '{load_type}'")

        out = {c: rec.get(c, "") for c in _TEMPLATE_COLUMNS}
        out["load_type"] = load_type
        for c in _TEMPLATE_NULLABLE:
            out[c] = out[c] or None
        for c, default in _TEMPLATE_BOOLEANS.items():
            raw = rec.get(c, "")
            if raw == "":
                out[c] = default
            else:
                v = raw.strip().lower()
                if v in ("true", "yes", "y", "1"):
                    out[c] = True
                elif v in ("false", "no", "n", "0"):
                    out[c] = False
                else:
                    raise ValueError(
                        f"Template row {rn} ('{table_name}'): {c} must be true/false — got '{raw}'")
        # blank (not NULL) for optional name columns, same as the widget SQL
        out["silver_table"] = out["silver_table"] or ""
        out["gold_view"]    = out["gold_view"] or ""
        clean.append(out)

    schema = StructType([
        StructField(c, BooleanType() if c in _TEMPLATE_BOOLEANS else StringType(), True)
        for c in _TEMPLATE_COLUMNS
    ])
    spark.createDataFrame([tuple(r[c] for c in _TEMPLATE_COLUMNS) for r in clean], schema) \
         .createOrReplaceTempView("_template_pipeline_rows")

    # Only merge columns that actually exist in the target table (matched
    # case-insensitively), so an older table without e.g. gold_sql_file still works.
    tgt_by_lower = {c.lower(): c for c in spark.table(PIPELINE_TABLE_CONFIG).columns}
    skipped = [c for c in _TEMPLATE_COLUMNS if c not in tgt_by_lower]
    if skipped:
        logger.warning(f"  {PIPELINE_TABLE_CONFIG} has no column(s) {skipped} — "
                       f"those Excel values are NOT loaded.")
    use_cols = [c for c in _TEMPLATE_COLUMNS if c in tgt_by_lower]
    non_key = [c for c in use_cols if c not in ("pipeline_name", "table_name")]
    set_clause = ",\n                ".join(f"target.{tgt_by_lower[c]} = source.{c}" for c in non_key)
    cols_csv   = ", ".join(tgt_by_lower[c] for c in use_cols)
    vals_csv   = ", ".join(f"source.{c}" for c in use_cols)

    spark.sql(f"""
        MERGE INTO {PIPELINE_TABLE_CONFIG} AS target
        USING _template_pipeline_rows AS source
        ON  target.pipeline_name = source.pipeline_name
        AND target.table_name    = source.table_name
        WHEN MATCHED THEN UPDATE SET
                {set_clause}
        WHEN NOT MATCHED THEN INSERT ({cols_csv}) VALUES ({vals_csv})
    """)

    # Heads-up for rows left behind in the table that the Excel no longer lists.
    try:
        in_table = {r["table_name"] for r in spark.sql(
            f"SELECT table_name FROM {PIPELINE_TABLE_CONFIG} "
            f"WHERE pipeline_name = {_sql_quote(pipeline_name)}").collect()}
        stale = sorted(in_table - set(seen_tables))
        if stale:
            logger.warning(
                f"  {len(stale)} table(s) exist in {PIPELINE_TABLE_CONFIG} for "
                f"'{pipeline_name}' but not in the Excel: {stale} — they will "
                f"STILL be generated. Set is_active=false in the Excel to skip them.")
    except Exception as exc:
        logger.warning(f"  Could not check for stale rows: {exc}")

    logger.info(f"Template: merged {len(clean)} row(s) for pipeline_name='{pipeline_name}' "
                f"from {template_path} into {PIPELINE_TABLE_CONFIG}")
    return len(clean)


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
    Read rows for exactly one pipeline from
    {PIPELINE_TABLE_CONFIG} (same column
    structure the old Excel template had — the Excel file is no longer
    read anywhere; a separate job loads user-provided widget values
    straight into this Delta table) and register that single pipeline.

    Columns (identical meaning to the old Excel template):
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
      Soft_delete_condition               -> apply_as_deletes expr (scd2 only)
      Is_active                            -> True/False, skip row if False
      Format                                -> optional override (else from metadata file)

      Gold_view                              -> name of the gold VIEW for this row's table.
                                                 Gold is VIEW-ONLY — there is no materialized
                                                 gold table option. A view has no physical
                                                 storage: it's a plain `CREATE OR REPLACE VIEW`
                                                 re-pointed at current silver on every run, so
                                                 there's no load_type/merge_keys/watermark to
                                                 configure for it — it's always a full re-read.
      Gold_sql_file                          -> optional. Filename of a .sql file under
                                                 {BASE_VOLUME}/sql_files/<Gold_sql_file>
                                                 holding the view's SELECT body. If blank (and
                                                 Gold_view is set), the view defaults to a
                                                 straight passthrough of this row's own silver:
                                                 "SELECT * FROM {silver_table}".
                                                 If given, the SQL file's text is used as-is,
                                                 with exactly one substitution: the literal
                                                 placeholder "{silver_table}" is replaced with
                                                 THIS row's own silver table, fully qualified
                                                 ({TARGET_CATALOG}.{Silver_schema}.
                                                 {Pipeline_name}_{Silver_table}_silver).
                                                 Any OTHER silver tables the SQL joins against
                                                 are NOT auto-substituted or aliased — write
                                                 their fully-qualified names directly
                                                 (hardcoded) in the SQL file, e.g.
                                                 "<catalog>.sap_demo_silver.
                                                 sap_demo_uct_marc_silver_silver". This keeps
                                                 the SQL file self-contained and readable by
                                                 anyone without needing to know the alias
                                                 mapping.
      Depends_on_silvers                     -> optional comma list of silver names this SQL
                                                 file references, in order (first is normally
                                                 this row's own silver, matching {silver_table}).
                                                 Informational only — used for the lineage
                                                 trail in pipeline_definitions.depends_on. It
                                                 does NOT drive any substitution or ordering:
                                                 gold_pipeline.py always runs after the
                                                 ENTIRE bronze/silver Lakeflow pipeline has
                                                 completed, so every silver a gold SQL file
                                                 might reference already exists by the time any
                                                 gold view is (re)created — there's nothing to
                                                 wait on. If omitted, defaults to just this
                                                 row's own Silver_table.
    """

    pipeline_name = (pipeline_name or "").strip()
    if not pipeline_name:
        raise ValueError("load_sap_pipeline_from_table: pipeline_name is required")

    q = _sql_quote
    df = spark.sql(
        f"SELECT * FROM {PIPELINE_TABLE_CONFIG} WHERE Pipeline_name = {q(pipeline_name)}"
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
        "Gold_sql_file", "Gold_view", "Depends_on_silvers",
        "Connection_name",   # NEW — "" or "SAP" = .sap.partfile.metadata (default,
                              # unchanged behavior); "SQL_SERVER" = JDBC/INFORMATION_SCHEMA
                              # via read_sql_server_metadata() — see _read_metadata_for_source()
    ]
    actual_by_lower = {c.lower(): c for c in df.columns}
    unmatched = [c for c in CANONICAL_COLUMNS if c.lower() not in actual_by_lower]
    if unmatched:
        logger.warning(
            f"  {PIPELINE_TABLE_CONFIG} is missing expected column(s) "
            f"(case-insensitive match): {unmatched} — those fields will "
            f"read as empty/default for every row."
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
    rows = [_resolve_landing_paths(row) for row in rows]

    # ── Filter active rows ──────────────────────────────────
    active_rows = []
    for row in rows:
        table_name = str(row.get("Table_name", "")).strip()
        is_active  = _truthy(row.get("Is_active"), default=True)
        if not is_active:
            logger.info(f"  SKIPPED (Is_active=False) : {table_name}")
            continue
        load_type = str(row.get("Load_type", "full")).strip().lower()
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
            table_name = str(source["Table_name"]).strip()
            meta = _read_metadata_for_source(source)
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
        bronze_schema = str(first.get("Bronze_schema", "sap_demo_bronze")).strip()
        silver_schema = str(first.get("Silver_schema", "sap_demo_silver")).strip()
        gold_schema   = str(first.get("Gold_schema",   "sap_demo_gold")).strip()

        r = PipelineRegistry(
            spark, dbutils,
            pipeline_id    = pipeline_name,
            pipeline_name  = f"SAP Pipeline — {pipeline_name}",
            environment    = environment,
            target_catalog = TARGET_CATALOG,
            bronze_schema  = bronze_schema,
            silver_schema  = silver_schema,
            gold_schema    = gold_schema,
            config_dir     = CONFIG_DIR,
            alert_email    = ALERT_EMAIL,
            notes          = "SAP template-driven onboarding (multi-source)"
        )

        for source in group_rows:
            table_name    = str(source["Table_name"]).strip()
            source_entity = table_name.lower().replace(".", "_")
            source_path   = str(source["Source_path"]).strip()
            metadata_path = str(source["Metadata_path"]).strip()
            silver_name   = str(source["Silver_table"]).strip()
            load_type     = str(source.get("Load_type", "full")).strip().lower()
            merge_keys    = str(source.get("Merge_keys", "")).strip() or None

            logger.info("─" * 55)
            logger.info(f"  Table       : {table_name}")
            logger.info(f"  Path        : {source_path}")
            if str(source.get("Connection_name", "")).strip().upper() == "SQL_SERVER":
                logger.info(f"  Metadata    : INFORMATION_SCHEMA.COLUMNS via JDBC "
                            f"({source.get('Connection_name')})")
            else:
                logger.info(f"  Metadata    : {metadata_path}")
            logger.info(f"  Load type   : {load_type}")
            if silver_name:
                logger.info(f"  Silver      : {silver_name}")

            try:
                # Reuse metadata read during Step 0 — no second read/JDBC call.
                meta = landed_meta.get((pipeline_name, table_name)) \
                       or _read_metadata_for_source(source)

                # Format: Excel override if provided, else from metadata
                csv_format  = str(source.get("Format", "")).strip().lower()
                file_format = csv_format if csv_format else meta["format"]

                # Watermark — auto-detected from .sap.partfile.metadata
                # (semanticType="_change_mode" + TIMESTAMP -> __timestamp)
                # Excel Watermark column overrides if provided
                # SQL_SERVER: INFORMATION_SCHEMA can't supply a watermark, so it
                # stays EMPTY unless explicitly provided (future Excel template /
                # pipeline_table_config.Watermark). That is valid for
                # load_type=full; incremental types are rejected by add_source().
                is_sql_server = str(source.get("Connection_name", "")).strip().upper() == "SQL_SERVER"
                watermark_raw = str(source.get("Watermark", "")).strip()
                if watermark_raw:
                    watermark = watermark_raw                           # explicit override wins
                elif is_sql_server:
                    watermark = ""                                      # not derivable
                else:
                    watermark = meta.get("watermark") or "__timestamp"  # SAP: unchanged

                # Soft delete — auto-detected; only meaningful for scd2
                soft_delete_raw = str(source.get("Soft_delete_condition", "")).strip()
                soft_delete = (soft_delete_raw if soft_delete_raw
                               else meta.get("soft_delete_condition")) \
                              if load_type == "scd2" else None

                logger.info(f"  Format      : {file_format}"
                      + (" (override)" if csv_format else " (from metadata)"))
                logger.info(f"  Watermark   : {watermark or '(none)'}")
                if load_type == "scd2" and soft_delete:
                    logger.info(f"  Soft delete : {soft_delete}")
                logger.info(f"  Cols        : {len(meta['schema_hints'])}")

                # Register bronze — load_type carried through so downstream
                # config generation knows how bronze should refresh, not just
                # that it's a DLT source.
                r.add_source(
                    source_entity,
                    path                   = source_path,
                    keys                   = meta["key_str"],
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
                        keys        = meta["key_str"] or (merge_keys or ""),
                        watermark   = watermark,
                    )
                    if load_type == "scd2" and soft_delete:
                        silver_kwargs["soft_delete"] = soft_delete
                    r.add_silver(**silver_kwargs)

                # Register gold — a plain VIEW only. No materialized-table
                # option: every gold object is `CREATE OR REPLACE VIEW`,
                # re-pointed at current silver on every run, so there's no
                # load_type/merge_keys/watermark to configure for it.
                gold_sql_file = str(source.get("Gold_sql_file", "")).strip()
                gold_view     = str(source.get("Gold_view", "")).strip()
                gold_object   = gold_view

                has_dlt_silver = bool(silver_name) and load_type in (
                    "full", "scd1", "scd2", "delta", "delta_merge")
                silver_full = None
                if has_dlt_silver:
                    silver_layer = f"{pipeline_name}_{silver_name}_silver"
                    silver_full  = f"{TARGET_CATALOG}.{silver_schema}.{silver_layer}"

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
                        if gold_sql_file:
                            sql_path = f"{BASE_VOLUME}/sql_files/{gold_sql_file}"
                            gold_sql = dbutils.fs.head(sql_path, 1_000_000)
                            # Only THIS row's own silver gets substituted —
                            # any other silver the SQL joins against must be
                            # written out fully-qualified, hardcoded, by
                            # whoever authored the .sql file. Depends_on_silvers
                            # below is lineage-only; it does not drive any
                            # substitution.
                            if silver_full:
                                gold_sql = gold_sql.replace("{silver_table}", silver_full)
                            sql_source_desc = gold_sql_file
                        else:
                            # Gold_view with no Gold_sql_file: default silver->gold
                            # passthrough view.
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
                        )
                        if silver_name and has_dlt_silver:
                            gold_kwargs["from_silver"] = silver_name
                        else:
                            # No DLT silver for this row — gold view reads bronze directly.
                            gold_kwargs["from_bronze"] = source_entity

                        r.add_gold(**gold_kwargs)
                        logger.info(f"  Gold        : {gold_object} (view, load_type=full)")
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


# COMMAND ----------

# DBTITLE 1,Environment parameters
# Every environment-specific value comes from a job parameter
# (resources/control_jobs.yml), so the same notebook runs in dev, qa and prod:
#
#   catalog + control_schema -> CTRL, PIPELINE_TABLE_CONFIG (control tables)
#                               TARGET_CATALOG (bronze/silver/gold tables)
#   landing_base_path        -> LANDING_BASE_PATH (Source_path / Metadata_path base;
#                               also where jdbc_extract_to_landing.py writes)
#   framework_root           -> BASE_VOLUME (sql_files/) and CONFIG_DIR (configs/)
#                               default /Volumes/<catalog>/<control_schema>/landing
#   alert_email              -> ALERT_EMAIL (empty = the registry's default list)
#
# These are module-level names read when the functions run, so setting
# them here (after the %run cells, before the Run cell) is enough.

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("control_schema", "", "Control schema")
dbutils.widgets.text("framework_root", "", "Framework root (empty = /Volumes/<catalog>/<control_schema>/landing)")
dbutils.widgets.text("landing_base_path", "", "Landing base path (source_path is relative to it)")
dbutils.widgets.text("alert_email", "", "Alert e-mail (empty = registry default)")

_p_catalog        = dbutils.widgets.get("catalog").strip()
_p_control_schema = dbutils.widgets.get("control_schema").strip()
_p_framework_root = dbutils.widgets.get("framework_root").strip()
_p_landing_base   = dbutils.widgets.get("landing_base_path").strip()
ALERT_EMAIL       = dbutils.widgets.get("alert_email").strip()

_missing = [n for n, v in (("catalog", _p_catalog), ("control_schema", _p_control_schema),
                           ("landing_base_path", _p_landing_base)) if not v]
if _missing:
    raise ValueError(f"Required widget(s) empty: {_missing}. They are job parameters of "
                     f"Config_file_generator (resources/control_jobs.yml); when running by "
                     f"hand, fill them in at the top of the notebook.")

TARGET_CATALOG        = _p_catalog
CTRL                  = f"{_p_catalog}.{_p_control_schema}"
PIPELINE_TABLE_CONFIG = f"{CTRL}.pipeline_table_config"
LANDING_BASE_PATH     = _p_landing_base.rstrip("/") + "/"
BASE_VOLUME           = (_p_framework_root or f"/Volumes/{_p_catalog}/{_p_control_schema}/landing").rstrip("/")
CONFIG_DIR            = f"{BASE_VOLUME}/configs"

logger.info(f"Parameters -> target catalog : {TARGET_CATALOG}")
logger.info(f"Parameters -> control tables : {PIPELINE_TABLE_CONFIG}")
logger.info(f"Parameters -> landing base   : {LANDING_BASE_PATH}")
logger.info(f"Parameters -> config dir     : {CONFIG_DIR}")
logger.info(f"Parameters -> sql_files dir  : {BASE_VOLUME}/sql_files")
logger.info(f"Parameters -> alert e-mail   : {ALERT_EMAIL or '(registry default)'}")

# COMMAND ----------

# ── Run ──────────────────────────────────────────────────────
# Driven entirely by the environment/pipeline_name widgets — generates a
# config for exactly this one pipeline, reading its rows from
# pipeline_table_config (the Excel template is no longer used anywhere).

# If template_path is set, first load the Excel rows for this pipeline into
# pipeline_table_config; if empty, skip and use the table as-is (existing flow).
if TEMPLATE_PATH:
    load_template_into_pipeline_table_config(spark, TEMPLATE_PATH, PIPELINE_NAME)
else:
    logger.info("template_path is empty — using existing rows in pipeline_table_config")

load_sap_pipeline_from_table(ENVIRONMENT, PIPELINE_NAME)

# COMMAND ----------

# ── Archive rotation (now automatic, per pipeline, on every register()) ──
# _rotate_config_files() in pipeline_registry.py already keeps only the
# latest config in {CONFIG_DIR}/{pipeline_id}/ and caps
# {CONFIG_DIR}/{pipeline_id}/archive/ at 10 files, immediately after each
# register() call above — no separate scheduled job needed for that.
#
# purge_old_configs() below is still available as an OPTIONAL extra
# safety net (e.g. if you want a time-based cap in addition to the
# count-based one) but is not required for the "only latest + 10
# archived" behavior — that's now handled inline, per pipeline.

# purge_old_configs(spark, dbutils, CONFIG_DIR, retention_days=7)