# Databricks notebook source
# DBTITLE 1,Cell 3
import re
import json
from datetime import datetime

from pyspark import pipelines as dp                # ← was: import dlt
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType, TimestampNTZType,
    DateType, DoubleType, IntegerType, BooleanType, BinaryType
)
from pyspark.sql.functions import current_timestamp, col, expr, coalesce


# ══════════════════════════════════════════════════════════════════
# SILVER PIPELINE — bronze → silver only.
#
# Split out of the former combined lakeflow_pipeline_1.py. Bronze
# (landing → bronze, Auto Loader) now lives in its own separate
# Lakeflow pipeline, bronze_pipeline_1.py. This pipeline reads bronze
# tables via spark.readStream.table(...) exactly as the combined file
# did — the only thing that's changed is WHICH pipeline writes them.
#
# OPERATIONAL CONSEQUENCE OF THE SPLIT: this pipeline's tables only
# see whatever bronze_pipeline_1.py has already committed by the time
# THIS pipeline's own update runs. An orchestrating Databricks Job
# must sequence Bronze → (optionally a DQ check) → Silver explicitly;
# Lakeflow no longer runs them together as one DAG the way the
# combined file did.
# ══════════════════════════════════════════════════════════════════

# ── 0. File-based logging setup ─────────────────────────────
#
# Every stage below already prints to stdout (visible in the pipeline's
# driver/event logs). This adds a SECOND sink: every print is mirrored,
# via log(), into a per-run file under LOG_DIR. Volumes paths are
# FUSE-mounted under /Volumes, so plain Python file I/O works here —
# deliberately NOT spark.sql()/dbutils.fs, since DDL/DML is rejected
# inside a Lakeflow/SDP pipeline source file (see the note in section 9,
# below), and file I/O sidesteps that restriction entirely.
#
# SHARED FILE WITH gold_pipeline.py: the log filename is timestamped
# with the RESOLVED CONFIG's own timestamp (config_ts), not
# datetime.now() — both this file and gold_pipeline.py independently
# resolve the same "latest config file" for the same pipeline_id/
# environment, so as long as no new config gets registered between the
# two runs (true within one job execution), they land on the identical
# filename without any explicit hand-off between the two tasks. The
# filename construction is therefore deferred until AFTER config
# resolution (section 1) rather than done up front.
import os

LOG_DIR        = None  # set from framework_root once the pipeline configuration is read
COMPONENT      = "LAKEFLOW_SILVER"
_LOG_FILE_PATH  = None  # populated after config resolution — see _finalize_log_file below

_LOG_LINES = []  # buffered in memory — see note below on why we don't append

def log(stage: str, message: str, level: str = "INFO"):
    """print() as before, PLUS rewrite a structured, timestamped log file
    on Volumes with every call (best-effort — a logging failure never
    fails the pipeline itself).

    NOTE: this rewrites the WHOLE file each call rather than opening in
    append ("a") mode. Unity Catalog Volumes are FUSE-mounted, and that
    FUSE layer does not support seek — open(path, "a") triggers a seek
    to end-of-file internally and fails with "[Errno 29] Illegal seek".
    Plain "w" mode (truncate + write from a fresh handle) needs no seek,
    so it works on Volumes; the tradeoff is O(n) rewrite cost per log
    line, which is fine at this pipeline's log volume.
    """
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} [{level}] [{COMPONENT}] [{stage}] {message}"
    print(line)
    _LOG_LINES.append(line)
    if _LOG_FILE_PATH:
        try:
            with open(_LOG_FILE_PATH, "w") as f:
                f.write("\n".join(_LOG_LINES) + "\n")
        except Exception as ex:
            print(f"  WARNING: could not write to log file {_LOG_FILE_PATH}: {ex}")


def _finalize_log_file(pipeline_id: str, environment: str, run_ts: datetime):
    """
    Called once config_ts is known (section 1, below). Builds the SHARED
    filename from run_ts (== config_ts, the resolved config file's own
    timestamp) and — critically — reads any content ALREADY at that path
    first and prepends it to the in-memory buffer before this process's
    own lines. That's what makes this genuinely shared with
    gold_pipeline.py rather than one process clobbering the other's log:
    whichever of the two tasks runs second sees the first task's lines
    already sitting in the file and keeps them, instead of truncating
    them away on its first write (plain "w" mode with a fresh empty
    buffer would silently erase the other task's half of the run).
    """
    global _LOG_FILE_PATH
    run_ts_str = run_ts.strftime("%Y%m%d_%H%M%S")
    path = f"{LOG_DIR}/{pipeline_id}_{environment}_{run_ts_str}.log"
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        try:
            with open(path, "r") as f:
                existing = f.read()
            if existing.strip():
                _LOG_LINES[:0] = existing.rstrip("\n").split("\n")
        except FileNotFoundError:
            pass
        _LOG_FILE_PATH = path
        with open(_LOG_FILE_PATH, "w") as f:
            f.write("\n".join(_LOG_LINES) + "\n")
    except Exception as ex:
        print(f"  WARNING: could not finalize log file {path}: {ex}")
    return _LOG_FILE_PATH

# ── 1. Resolve latest config by filename timestamp ─────────
#
# pipeline_registry.py now writes configs one folder per pipeline, and
# keeps only the single latest file in that folder (older ones move to
# an archive/ subfolder there, capped at 10):
#
#   {config_out_dir}/{pipeline_id}/{pipeline_id}_{environment}_{YYYYMMDD_HHMMSS}_config.json
#   {config_out_dir}/{pipeline_id}/archive/...                (ignored here)
#
# Normally there's exactly one matching file in that folder. This still
# does timestamp-based "pick the newest" rather than assuming there's
# only one, so it degrades safely if that invariant is ever violated
# (e.g. a partial/failed rotation leaves two files behind briefly).

pipeline_id    = spark.conf.get("pipeline_id",    "")
environment    = spark.conf.get("environment",    "")
catalog        = spark.conf.get("catalog",        "")
control_schema = spark.conf.get("control_schema", "")

# One folder root per environment for everything the framework writes or reads:
#   {framework_root}/configs/<pipeline_id>/   generated config JSON
#   {framework_root}/sql_files/               gold SQL files
#   {framework_root}/logs/                    run logs (shared by bronze, silver, gold)
#   {framework_root}/schema/<layer>/          Auto Loader schema locations
# Default: /Volumes/<catalog>/<control_schema>/landing
framework_root = spark.conf.get("framework_root", "") or (
    f"/Volumes/{catalog}/{control_schema}/landing" if catalog and control_schema else "")

if not pipeline_id.strip():
    raise ValueError(
        "Pipeline Configuration is missing required key 'pipeline_id'. "
        "Set it under Pipeline settings → Advanced → Configuration "
        "(or the job task's pipeline configuration override) — "
        "this pipeline will not run against a default/guessed value."
    )
if not environment.strip():
    raise ValueError(
        "Pipeline Configuration is missing required key 'environment'. "
        "Set it under Pipeline settings → Advanced → Configuration "
        "(or the job task's pipeline configuration override) — "
        "this pipeline will not run against a default/guessed value."
    )

for _key, _val in (("catalog", catalog), ("control_schema", control_schema),
                   ("framework_root", framework_root)):
    if not _val.strip():
        raise ValueError(
            f"Pipeline Configuration is missing required key '{_key}'. "
            "It is set by the bundle (resources/pipelines.yml) - redeploy, or set it "
            "under Pipeline settings → Advanced → Configuration.")

framework_root = framework_root.rstrip("/")
CTRL           = f"{catalog}.{control_schema}"      # control tables
config_out_dir = f"{framework_root}/configs"
base_volume    = framework_root                       # Auto Loader schemas: {framework_root}/schema/<layer>
LOG_DIR        = f"{framework_root}/logs"

try:
    os.makedirs(LOG_DIR, exist_ok=True)
except Exception as _log_init_ex:
    print(f"  WARNING: could not initialize log directory {LOG_DIR}: {_log_init_ex}")

log("INIT", f"Lakeflow pipeline starting. pipeline_id={pipeline_id!r} environment={environment!r}")
log("INIT", f"Control schema: {CTRL} | framework root: {framework_root}")
log("INIT", "Log file path pending — finalized once config_ts is resolved (section 1)")

framework_config = {"base_volume": base_volume}

pipeline_config_dir = f"{config_out_dir.rstrip('/')}/{pipeline_id}"

_FNAME_RE = re.compile(
    rf"^{re.escape(pipeline_id)}_{re.escape(environment)}_(\d{{8}}_\d{{6}})_config\.json$"
)

def _resolve_latest_config(spark, dbutils, config_dir: str, fname_re: re.Pattern):
    """
    Scan config_dir (the per-pipeline folder — NOT its archive/ subfolder),
    match files against fname_re, parse the embedded YYYYMMDD_HHMMSS
    timestamp out of each matching filename, and return (path,
    parsed_datetime) for the newest one. Returns (None, None) if no file
    matches.
    """
    try:
        entries = dbutils.fs.ls(config_dir)
    except Exception as ex:
        raise FileNotFoundError(f"Could not list config dir '{config_dir}': {ex}")

    candidates = []
    for f in entries:
        if f.name == "archive" or f.name.endswith("/"):
            continue   # skip the archive subfolder
        m = fname_re.match(f.name)
        if not m:
            continue
        try:
            ts = datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        candidates.append((ts, f.path))

    if not candidates:
        return None, None

    candidates.sort(key=lambda t: t[0])   # oldest → newest
    latest_ts, latest_path = candidates[-1]
    return latest_path, latest_ts


config_path, config_ts = _resolve_latest_config(spark, dbutils, pipeline_config_dir, _FNAME_RE)

if not config_path:
    raise FileNotFoundError(
        f"No config found matching '{pipeline_id}_{environment}_<timestamp>_config.json' "
        f"in {pipeline_config_dir}."
    )

_finalize_log_file(pipeline_id, environment, config_ts)
log("INIT", f"Log file: {_LOG_FILE_PATH}  (shared with gold_pipeline.py — same "
            f"pipeline_id/environment/config_ts resolves to the identical path)")

config_str = "\n".join(
    r.value for r in spark.read.text(config_path).collect()
)
config = json.loads(config_str)

# Best-effort sanity check only — does NOT block the run. The registry's
# own recommended lookup is the control table (version_status='current');
# folder+timestamp scanning (above) can disagree with it if, e.g., a
# stale/failed config file was left behind with a newer timestamp than
# the row that's actually marked 'current'. Surface that as a warning
# rather than silently trusting the filename.
log("CONFIG_VALIDATE", "Cross-checking folder-latest config against pipeline_config_versions control table")
try:
    _ctrl_row = spark.sql(f"""
        SELECT config_path FROM {CTRL}.pipeline_config_versions
        WHERE pipeline_id = '{pipeline_id}' AND environment = '{environment}'
        AND   version_status = 'current'
    """).collect()
    if _ctrl_row and _ctrl_row[0]["config_path"] != config_path:
        log("CONFIG_VALIDATE",
            f"folder-latest config ({config_path}) differs from "
            f"pipeline_config_versions 'current' ({_ctrl_row[0]['config_path']}). "
            f"Using folder-latest per configuration.", level="WARNING")
    else:
        log("CONFIG_VALIDATE", "folder-latest config matches control table 'current' version")
except Exception as _ex:
    log("CONFIG_VALIDATE", f"Could not cross-check pipeline_config_versions: {_ex}", level="WARNING")

# SILVER-ONLY: plain bronze layers (dlt_mode=="bronze") are registered
# by bronze_pipeline_1.py instead — this pipeline never sees them.
# bronze_join stays in this list (unchanged from the combined file) so
# it's still logged and skipped, not silently dropped.
_SILVER_MODES = {"snapshot", "upsert", "history", "delta", "bronze_join", "full"}
dlt_layers = [
    l for l in config["layers"]
    if l.get("layer_type") == "dlt" and l.get("dlt_mode") in _SILVER_MODES
]

log("PIPELINE_START", "=" * 70)
log("PIPELINE_START", "LAKEFLOW SILVER PIPELINE START")
log("PIPELINE_START", "=" * 70)
log("PIPELINE_START", f"Pipeline   : {pipeline_id}")
log("PIPELINE_START", f"Environment: {environment}")
log("PIPELINE_START", f"Config     : {config_path}  (timestamp={config_ts})")
log("PIPELINE_START", f"Version    : v{config.get('version_number','?')}")
log("PIPELINE_START", f"DLT layers : {len(dlt_layers)}")
for _l in dlt_layers:
    log("PIPELINE_START", f"  {_l['name']}  ({_l.get('dlt_mode','?')})")
log("PIPELINE_START", "=" * 70)


# ── 2. Helpers (shared subset only) ─────────────────────────
#
# Bronze-only helpers from the combined file — TYPE_MAP, DDL_TYPE_MAP,
# schema_to_hints(), build_hard_schema(), _apply_expect_decorators(),
# _common_parent_if_direct_children(), _derive_archive_dir() — are not
# needed here and live only in bronze_pipeline_1.py. What silver still
# needs: SYSTEM_COL_CASTS/_normalize_system_columns() (the CDC
# pre-transform view also normalizes system-column casing, same as
# bronze does), get_dlt_table_name(), and apply_clean_rules().

# SAP DSP source files have been observed disagreeing on the exact case
# of these system columns across folders/batches for the SAME table
# (e.g. "__timestamp" in initial/, "__Timestamp" in delta/). Bronze
# tables are created with delta.columnMapping.mode="name", which tracks
# columns by exact case for schema-merge purposes — Spark SQL querying
# is case-insensitive, but schema evolution is not, so two different
# casings of the same logical column become two distinct physical
# columns the moment they land in the same table, and once that's
# happened Delta refuses to auto-merge them (schema merge failure,
# usually paired with a type conflict too, since only the canonically-
# cased column matches cloudFiles.schemaHints — a differently-cased one
# skips the hint and gets inferred straight from the raw physical type,
# e.g. LongType instead of the intended TIMESTAMP_NTZ).
# Normalizing name AND type here, right after every Auto Loader read,
# guarantees every batch of every file — regardless of source casing —
# collapses onto the exact same physical column before it ever reaches
# a table with column mapping enabled.
SYSTEM_COL_CASTS = {
    "__timestamp":       "timestamp_ntz",
    "__operation_type":  "string",
    "__sequence_number": "decimal(20,0)",
}

def _normalize_system_columns(df):
    """
    Collapses every case-variant of each known system column onto its
    single canonical lowercase name, casting to the canonical type.

    Handles two distinct scenarios, not just one:
      1. Only one casing present in this batch (the common case) ->
         plain rename + cast.
      2. MULTIPLE casings present simultaneously in the SAME batch
         (e.g. both "__timestamp" and "__Timestamp" as two separate
         columns at once) -> a plain withColumnRenamed would try to
         rename onto a name that already exists in this same
         DataFrame, colliding rather than merging. This is a real,
         not theoretical, case: Auto Loader unions schemas across
         every file included in a micro-batch, so if different SAP
         source files disagree on casing, both variants can legally
         land in the exact same batch together. Handled here by
         coalescing all variants into the canonical column (first
         non-null wins, in field order) and dropping the rest —
         never a rename in this branch, so no collision is possible.
    """
    for canonical, cast_type in SYSTEM_COL_CASTS.items():
        variants = [f.name for f in df.schema.fields if f.name.lower() == canonical]
        if not variants:
            continue

        if variants == [canonical]:
            # Already canonically named — still worth casting, since a
            # single file's own physical type can drift independently
            # of naming (e.g. one file's __timestamp column stored as
            # a raw long rather than a proper timestamp).
            df = df.withColumn(canonical, col(canonical).cast(cast_type))
            continue

        log("SCHEMA_NORMALIZE",
            f"Collapsing {len(variants)} casing variant(s) of '{canonical}' "
            f"present in the same batch: {variants}")
        casted = [col(v).cast(cast_type) for v in variants]
        combined = casted[0]
        for c in casted[1:]:
            combined = coalesce(combined, c)
        # Build the combined column BEFORE dropping the variants - a column
        # cannot be read after it has been dropped (that raised
        # UNRESOLVED_COLUMN, including in the single-variant case such as
        # a file that only has "__Timestamp").
        tmp = f"__tmp_{canonical.strip('_')}"
        df = (df.withColumn(tmp, combined)
                .drop(*variants)
                .withColumnRenamed(tmp, canonical))
    return df



def get_dlt_table_name(full_name: str) -> str:
    parts = full_name.split(".")
    if len(parts) == 3:
        return f"{parts[1]}.{parts[2]}"
    return full_name


def apply_clean_rules(df, clean_rules: list):
    for rule in (clean_rules or []):
        df = df.withColumn(
            rule["target_column"],
            expr(rule["transform_rule"])
        )
    return df



# ── 6. Silver SCD1 ─────────────────────────────────────────

def register_silver_snapshot(layer_config: dict):
    src = layer_config.get("source", {})
    if "table_name" not in src:
        raise KeyError(
            f"Layer '{layer_config.get('name')}': source block has no 'table_name' "
            f"(got {src!r}). This config was generated before the pipeline_registry.py "
            f"dlt_mode-mapping fix — re-run register() to regenerate the config, then "
            f"re-run this pipeline so it picks up the corrected version."
        )
    source_table = src["table_name"]
    target_table = get_dlt_table_name(layer_config["target"]["table_name"])
    # Column-level bronze->silver transforms (CONCAT/CAST/etc.) — from
    # transformation_rules (transform_type='column'), via add_silver's
    # transform_rules param. Empty list if none were registered.
    mappings = layer_config.get("transformations", {}).get("mappings", [])

    log("SILVER_SNAPSHOT", f"Registering silver snapshot layer '{layer_config['name']}': "
        f"{target_table} (full load from {source_table})")
    if mappings:
        log("SILVER_SNAPSHOT", f"Transforms : {[m['target_column'] for m in mappings]}")

    @dp.table(
        name=target_table,
        comment=f"Silver full load — {layer_config['name']}",
        table_properties={"delta.feature.timestampNtz": "supported"}
    )
    def _silver():
        df = spark.readStream.table(source_table)
        df = apply_clean_rules(df, mappings)
        return df

    log("SILVER_SNAPSHOT", f"Silver table '{target_table}' registered")

# ── 7. Silver CDC (SCD1 upsert / SCD2 history) ──────────────
#
# Shared by both real upsert (scd1: stored_as_scd_type=1, one row per
# key, latest values, no history) and history (scd2:
# stored_as_scd_type=2, every version kept via __START_AT/__END_AT) —
# same create_auto_cdc_flow mechanism, differing only in scd_type.

def _register_silver_cdc(layer_config: dict, scd_type: int):
    src = layer_config.get("source", {})
    if "table_name" not in src:
        raise KeyError(
            f"Layer '{layer_config.get('name')}': source block has no 'table_name' "
            f"(got {src!r}). This config was generated before the pipeline_registry.py "
            f"dlt_mode-mapping fix — re-run register() to regenerate the config, then "
            f"re-run this pipeline so it picks up the corrected version."
        )
    # Fully-qualified, same as register_silver_snapshot/register_silver_delta
    # use successfully — NOT .split(".")[-1]. A bare table name here resolves
    # against the session's current catalog/schema (which defaults to
    # "default"), not the schema this table actually lives in.
    source_table = src["table_name"]
    target_table = get_dlt_table_name(layer_config["target"]["table_name"])
    delete_cond  = layer_config.get("delete_condition")
    mappings     = layer_config.get("transformations", {}).get("mappings", [])
    label        = "SCD1 upsert" if scd_type == 1 else "SCD2 history"

    if "keys" not in layer_config:
        raise KeyError(
            f"Layer '{layer_config.get('name')}': no 'keys' in config — "
            f"create_auto_cdc_flow requires primary keys for {label}. This "
            f"table's SAP metadata almost certainly has no column flagged "
            f"primaryKey=true (check sap_metadata_landing.primary_keys for "
            f"it, or set Merge_keys explicitly in the onboarding template). "
            f"pipeline_registry.py's add_silver() now rejects this at "
            f"registration time — re-run register() to confirm, or if this "
            f"is an old config generated before that fix, regenerate it."
        )
    if "sequence_by" not in layer_config:
        raise KeyError(
            f"Layer '{layer_config.get('name')}': no 'sequence_by' in "
            f"config — create_auto_cdc_flow requires a watermark column for "
            f"{label}. Re-run register() to regenerate this config; "
            f"add_silver() now rejects an empty watermark at registration "
            f"time so this shouldn't recur."
        )

    log("SILVER_CDC", f"Registering silver {label} layer '{layer_config['name']}': "
        f"{target_table} (from {source_table}, "
        f"keys={layer_config['keys']}, sequence_by={layer_config['sequence_by']})")

    # create_auto_cdc_flow(source=...) takes a table/view NAME, not a
    # DataFrame — there's no inline .withColumn() step available the way
    # there is for the snapshot case above. Always route through an
    # intermediate view (not just when transforms are configured) so
    # _normalize_system_columns() runs here too — previously, a layer
    # with no `mappings` (like marb) pointed create_auto_cdc_flow
    # straight at the raw bronze table with zero defensive normalization
    # on this side. Bronze normalizes before IT writes, but this closes
    # the same gap from the read side, in case bronze's own guarantee is
    # ever weakened by a future change.
    #
    # IMPORTANT: sequence_by is ALSO renamed here to a fresh column,
    # never the raw config value directly — confirmed (raw source
    # parquet, bronze, and full/snapshot-mode silver all independently
    # verified as a single, correctly-typed __timestamp column, no
    # duplicate, anywhere) that the '__timestamp'/'__Timestamp'
    # TimestampNTZType/LongType conflict is NOT a data problem at all:
    # it happens only when running scd1/scd2 (create_auto_cdc_flow), and
    # the failing stack trace both times points at
    # computeAndValidateChangeFlowSchema — a function internal to
    # create_auto_cdc_flow's OWN schema computation, not the generic
    # table-write path bronze and snapshot-mode silver both use safely.
    # That's the one thing that differs between the working path and the
    # failing one: passing a column literally named "__timestamp" as
    # sequence_by into create_auto_cdc_flow. Deriving a differently-named
    # copy and passing THAT as sequence_by instead avoids whatever
    # internal collision the CDC engine has with that specific name,
    # without needing to know Databricks' exact internal mechanism.
    #
    # dp.view(name=...) requires a SIMPLE, unqualified name — views
    # registered inside a Lakeflow pipeline live in the pipeline's own
    # local namespace and can't be given a schema-qualified multipart
    # name (this was already broken this way in the original mappings-
    # only branch; it just never ran until now, since no layer had
    # mappings configured before).
    _src_tbl = get_dlt_table_name(source_table).split(".", 1)[1]
    transformed_view = f"{_src_tbl}_transformed"
    raw_sequence_col = layer_config["sequence_by"]
    cdc_sequence_col = "_cdc_sequence_ts"
    if mappings:
        log("SILVER_CDC", f"Transforms : {[m['target_column'] for m in mappings]} (via {transformed_view})")
    log("SILVER_CDC", f"sequence_by '{raw_sequence_col}' renamed to '{cdc_sequence_col}' "
        f"for create_auto_cdc_flow — avoids a confirmed schema-merge collision "
        f"internal to create_auto_cdc_flow when sequence_by is literally '__timestamp'")

    @dp.view(name=transformed_view, comment=f"Pre-CDC transforms — {layer_config['name']}")
    def _transformed():
        df = spark.readStream.table(source_table)
        df = _normalize_system_columns(df)
        if mappings:
            df = apply_clean_rules(df, mappings)
        # Derive the sequencing column, THEN remove the original entirely —
        # not just repurpose it. A prior fix renamed __timestamp's ROLE
        # (passed cdc_sequence_col as sequence_by instead) but left the
        # __timestamp COLUMN itself in the DataFrame, still reaching
        # create_auto_cdc_flow's source schema. Confirmed by a full DROP
        # TABLE + fresh full refresh on both bronze and silver still
        # hitting the identical error: there is no stored schema left to
        # blame, so whatever computeAndValidateChangeFlowSchema collides
        # on has to be the column's mere PRESENCE in the source schema,
        # not its use as sequence_by specifically. Renaming it away
        # entirely (not dropping it — the value is kept, just under a
        # name that can't collide) is the next, more complete test of
        # that same hypothesis.
        df = df.withColumn(cdc_sequence_col, col(raw_sequence_col).cast("timestamp_ntz"))
        df = df.withColumnRenamed(raw_sequence_col, "_source_timestamp")
        return df

    cdc_source = transformed_view

    dp.create_streaming_table(
        name=target_table,
        comment=f"Silver {label} — {layer_config['name']}",
        # Matches bronze's hardening exactly (see the SYSTEM_COL_CASTS
        # comment above _normalize_system_columns): without
        # delta.columnMapping.mode="name" explicitly set, this target's
        # schema-merge comparator (exercised on EVERY batch here, since
        # create_auto_cdc_flow evolves the target via a real MERGE, unlike
        # bronze's plain append) falls back to Delta's default case
        # handling instead of the same hardened comparison bronze gets.
        table_properties={
            "delta.feature.timestampNtz": "supported",
            "delta.columnMapping.mode":   "name",
            "delta.minReaderVersion":     "2",
            "delta.minWriterVersion":     "5",
        }
    )
    kwargs = dict(
        target             = target_table,
        source             = cdc_source,
        keys               = layer_config["keys"],
        sequence_by        = cdc_sequence_col,
        stored_as_scd_type = scd_type,
    )
    if delete_cond:
        kwargs["apply_as_deletes"] = expr(delete_cond)
    dp.create_auto_cdc_flow(**kwargs)   # was: dlt.apply_changes(**kwargs) — same signature
    log("SILVER_CDC", f"Silver {label} table '{target_table}' registered "
        f"(scd_type={scd_type}, keys={layer_config['keys']}, sequence_by={cdc_sequence_col})")


def register_silver_upsert(layer_config: dict):
    """SCD1: real upsert — one row per key, latest values, in place."""
    _register_silver_cdc(layer_config, scd_type=1)


def register_silver_history(layer_config: dict):
    """SCD2: full history retained via __START_AT/__END_AT."""
    _register_silver_cdc(layer_config, scd_type=2)

# ── 7b. Silver delta (plain incremental append) ─────────────
#
# "delta": no keys, no CDC, no merge — just land bronze rows into silver,
# sequenced by watermark_col. Unlike register_silver_snapshot ("full"),
# which is a deliberate undeduped mirror of every row/event bronze ever
# saw, "delta" is for sources that are ALREADY an incremental feed (each
# new bronze micro-batch is genuinely new rows only), so a plain
# streaming append is all that's needed here.
#
# Implementation-wise this is close to register_silver_snapshot — both
# are a `@dp.table` over `spark.readStream.table(source_table)`, and
# Lakeflow's own incremental engine (backed by Auto Loader's file
# tracking upstream in bronze) already guarantees each new row is
# processed exactly once, so no manual "> MAX(watermark)" filter is
# needed the way gold_pipeline.py's batch "delta" load_type needs one
# (that one re-reads the full table in a plain notebook every run and
# has to filter by MAX(watermark) itself). Kept as a separate function
# (rather than reusing register_silver_snapshot) so the two stay free to
# diverge later — e.g. if "delta" should filter out NULL watermark rows
# as a data-quality guard, which "full" deliberately should not.

def register_silver_delta(layer_config: dict):
    src = layer_config.get("source", {})
    if "table_name" not in src:
        raise KeyError(
            f"Layer '{layer_config.get('name')}': source block has no 'table_name' "
            f"(got {src!r}). This config was generated before the pipeline_registry.py "
            f"dlt_mode-mapping fix — re-run register() to regenerate the config, then "
            f"re-run this pipeline so it picks up the corrected version."
        )
    source_table  = src["table_name"]
    target_table  = get_dlt_table_name(layer_config["target"]["table_name"])
    watermark_col = layer_config.get("sequence_by")
    mappings      = layer_config.get("transformations", {}).get("mappings", [])

    log("SILVER_DELTA", f"Registering silver delta layer '{layer_config['name']}': "
        f"{target_table} (delta append from {source_table}, sequence_by={watermark_col})")
    if mappings:
        log("SILVER_DELTA", f"Transforms : {[m['target_column'] for m in mappings]}")

    @dp.table(
        name=target_table,
        comment=f"Silver delta append — {layer_config['name']}",
        table_properties={"delta.feature.timestampNtz": "supported"}
    )
    def _silver_delta():
        df = spark.readStream.table(source_table)
        if watermark_col:
            df = df.filter(f"{watermark_col} IS NOT NULL")
        df = apply_clean_rules(df, mappings)
        return df

    log("SILVER_DELTA", f"Silver delta table '{target_table}' registered")

# COMMAND ----------
# ── 6. DLT router (silver-only) ─────────────────────────────
#
# dlt_layers above is already pre-filtered to snapshot/upsert/history/
# delta/bronze_join/full only. "full" is kept as a backward-compat
# alias for "snapshot" (unchanged from the combined file) — configs
# generated before pipeline_registry.py's dlt_mode-mapping fix may
# still have "full" baked in. bronze_join is a gold-time join marker;
# it's genuinely a no-op here, kept only so it's still logged rather
# than silently vanishing from this pipeline's registration summary.
# The else/raise is defensive: a dlt_mode=="bronze" layer reaching
# here means the filter above has a bug.

def register_dlt_layer(layer_config: dict, framework_config: dict):
    dlt_mode = layer_config.get("dlt_mode")
    if dlt_mode == "full":
        dlt_mode = "snapshot"
    log("DLT_ROUTER", f"Registering: {layer_config['name']} ({dlt_mode})")
    if dlt_mode == "snapshot":
        register_silver_snapshot(layer_config)
    elif dlt_mode == "upsert":
        register_silver_upsert(layer_config)
    elif dlt_mode == "history":
        register_silver_history(layer_config)
    elif dlt_mode == "delta":
        register_silver_delta(layer_config)
    elif dlt_mode == "bronze_join":
        log("DLT_ROUTER", f"SKIP bronze_join — join at gold. (layer={layer_config['name']})")
    else:
        raise ValueError(
            f"register_dlt_layer (silver_pipeline_1.py) received dlt_mode="
            f"'{dlt_mode}' for layer '{layer_config.get('name')}' — this "
            f"pipeline only registers silver modes. dlt_mode=='bronze' "
            f"belongs in bronze_pipeline_1.py; check the dlt_layers filter "
            f"above for a bug if this fires."
        )

# COMMAND ----------
# ── 7. Register all silver layers ───────────────────────────
#
# See the equivalent note in bronze_pipeline_1.py: spark.sql() DDL/DML
# is rejected inside a Lakeflow/SDP pipeline source file
# (UNSUPPORTED_SPARK_SQL_COMMAND), so this file cannot write to
# control tables itself — that still happens entirely in
# gold_pipeline.py, reading this pipeline's event log after the run
# completes.

failed_layers = []
failure_details = []
log("LAYER_REGISTRATION", f"Starting registration of {len(dlt_layers)} silver DLT layer(s)")
for layer in dlt_layers:
    layer_name = layer.get("name", "?")
    try:
        register_dlt_layer(layer, framework_config)
        log("LAYER_REGISTRATION", f"Layer '{layer_name}' registered successfully")
    except Exception as exc:
        failed_layers.append(layer_name)
        reason = str(exc).replace("\n", " ").strip()
        if len(reason) > 200:
            reason = reason[:200] + "…"
        failure_details.append(f"{layer_name}: {reason}")
        log("LAYER_REGISTRATION", f"FAILED to register '{layer_name}': {exc}", level="ERROR")
        # Deliberately continue to the next layer rather than raising here —
        # one bad table definition shouldn't block every other table in the
        # same pipeline from being defined.

log("LAYER_REGISTRATION", f"Registered {len(dlt_layers) - len(failed_layers)}/{len(dlt_layers)} silver DLT layers successfully")

if failed_layers:
    # IMPORTANT: keep this on ONE line — see the note in the original
    # combined file; the Databricks run banner truncates at the first
    # newline.
    detail_str = "  |  ".join(failure_details)
    log("LAYER_REGISTRATION", f"{len(failed_layers)} layer(s) failed to register — {detail_str}", level="ERROR")
    raise RuntimeError(
        f"{len(failed_layers)} silver layer(s) failed to register — {detail_str}"
    )
else:
    log("PIPELINE_END", f"Silver pipeline definition completed successfully. Log file: {_LOG_FILE_PATH}")
