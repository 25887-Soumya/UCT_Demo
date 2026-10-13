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
# BRONZE PIPELINE — landing → bronze only.
#
# Split out of the former combined lakeflow_pipeline_1.py. Silver
# (snapshot/upsert/history/delta) now lives in its own separate
# Lakeflow pipeline, silver_pipeline_1.py, which reads these bronze
# tables via spark.readStream.table(...) exactly as before.
#
# OPERATIONAL CONSEQUENCE OF THE SPLIT: bronze and silver are no
# longer materialized together as one DAG in one pipeline update.
# Silver's tables only see whatever THIS pipeline has already
# committed to Bronze by the time Silver's update runs — so an
# orchestrating Databricks Job must now sequence them explicitly
# (Bronze pipeline task → optionally a DQ check task → Silver
# pipeline task) rather than relying on Lakeflow to run them
# together. This is also what now makes it possible to insert a real
# gate (e.g. bronze_dq_check.py) between the two, which a single
# combined pipeline could not do.
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
COMPONENT      = "LAKEFLOW_BRONZE"
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

# BRONZE-ONLY: silver/CDC/delta layers (dlt_mode in snapshot, upsert,
# history, delta) and the gold-only bronze_join marker are registered
# by silver_pipeline_1.py instead — this pipeline never sees them.
dlt_layers = [
    l for l in config["layers"]
    if l.get("layer_type") == "dlt" and l.get("dlt_mode") == "bronze"
]

log("PIPELINE_START", "=" * 70)
log("PIPELINE_START", "LAKEFLOW BRONZE PIPELINE START")
log("PIPELINE_START", "=" * 70)
log("PIPELINE_START", f"Pipeline   : {pipeline_id}")
log("PIPELINE_START", f"Environment: {environment}")
log("PIPELINE_START", f"Config     : {config_path}  (timestamp={config_ts})")
log("PIPELINE_START", f"Version    : v{config.get('version_number','?')}")
log("PIPELINE_START", f"DLT layers : {len(dlt_layers)}")
for _l in dlt_layers:
    log("PIPELINE_START", f"  {_l['name']}  ({_l.get('dlt_mode','?')})")
log("PIPELINE_START", "=" * 70)


# ── 2. Helpers ─────────────────────────────────────────────

TYPE_MAP = {
    "string":        StringType(),
    "long":          LongType(),
    "bigint":        LongType(),        # SAP BIGINT → LongType
    "timestamp":     TimestampType(),
    "timestamp_ntz": TimestampNTZType(),
    "date":          DateType(),
    "double":        DoubleType(),
    "integer":       IntegerType(),
    "int":           IntegerType(),
    "boolean":       BooleanType(),
    "binary":        BinaryType(),
}


DDL_TYPE_MAP = {
    "long":          "BIGINT",
    "bigint":        "BIGINT",          # SAP BIGINT
    "integer":       "INT",
    "int":           "INT",
    "string":        "STRING",
    "double":        "DOUBLE",
    "float":         "FLOAT",
    "boolean":       "BOOLEAN",
    "timestamp":     "TIMESTAMP",
    "timestamp_ntz": "TIMESTAMP_NTZ",
    "date":          "DATE",
    "binary":        "BINARY",
}

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


def schema_to_hints(schema_dict: dict) -> str:
    """Convert config schema dict → AutoLoader schemaHints string.
    - Unknown types excluded → parquet inference handles them
    - / columns backtick-quoted → valid SQL DDL
    TIME columns already excluded at read_sap_metadata level.
    """
    if not schema_dict:
        return ""
    parts = []
    for f in schema_dict.get("fields", []):
        name = f['name']
        typ  = DDL_TYPE_MAP.get(f['type'].lower())
        # DECIMAL(p,s) (SQL Server decimal/numeric/money) passes straight through
        if typ is None and re.fullmatch(r"decimal\(\s*\d+\s*,\s*\d+\s*\)", f['type'].strip().lower()):
            typ = f['type'].strip().upper().replace(" ", "")
        if typ is None:
            log("SCHEMA_HINTS", f"SKIP: {name} ({f['type']}) unsupported", level="WARNING")
            continue

        if '/' in name:
            name = f"`{name}`"
        parts.append(f"{name} {typ}")
    return ", ".join(parts)


def build_hard_schema(schema_dict: dict) -> StructType:
    """
    Build StructType from config schema fields.
    Only includes types supported by Spark/Delta.
    Unsupported types (TIME etc) excluded → go to _rescued_data.
    """
    if not schema_dict:
        return None
    fields = []
    for f in schema_dict.get("fields", []):
        spark_type = TYPE_MAP.get(f["type"].lower())
        if spark_type is None:
            log("SCHEMA_BUILD", f"EXCLUDED from schema: {f['name']} ({f['type']}) → _rescued_data", level="WARNING")
            continue
        fields.append(StructField(
            f["name"],
            spark_type,
            f.get("nullable", True)
        ))
    return StructType(fields) if fields else None


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


def _apply_expectations(fn, expect_rules: list):
    """Wrap fn with dp.expect_all / _or_drop / _or_fail per rule action
    and return the WRAPPED function.

    Was previously `_apply_expect_decorators`, called AFTER fn had
    already been registered by @dp.table/@dp.append_flow — it built the
    wrapped function but never returned it, so the wrapped version was
    silently discarded and the original, un-decorated function stayed
    registered. Expectations have to be applied to the RAW function
    BEFORE whatever registers it (@dp.table or @dp.append_flow) runs, so
    every call site below now does: build raw fn -> wrap with this ->
    register the wrapped result, in that order.
    """
    warn = {r["name"]: r["constraint"] for r in expect_rules
            if r.get("action", "warn") == "warn"}
    drop = {r["name"]: r["constraint"] for r in expect_rules
            if r.get("action") == "drop"}
    fail = {r["name"]: r["constraint"] for r in expect_rules
            if r.get("action") == "fail"}
    if warn: fn = dp.expect_all(warn)(fn)
    if drop: fn = dp.expect_all_or_drop(drop)(fn)
    if fail: fn = dp.expect_all_or_fail(fail)(fn)
    return fn


def _common_parent_if_direct_children(paths: list) -> str | None:
    """
    Auto Loader's streaming reader only ever accepts ONE path per flow —
    unlike the batch DataFrameReader, .load() on a streaming cloudFiles
    reader rejects a list outright (VALUE_NOT_NON_EMPTY_STR).

    If every path in `paths` is a direct subfolder of the same parent
    (e.g. ".../TABLE/initial/" and ".../TABLE/delta/" both under
    ".../TABLE/"), we can safely read them all with a single Auto Loader
    stream pointed at that shared parent with recursiveFileLookup=true.
    Returns the parent path (with trailing slash) in that case, else None.
    """
    norm = [p.rstrip("/") for p in paths if p.strip()]
    if len(norm) < 2:
        return None
    parents = {p.rsplit("/", 1)[0] for p in norm if "/" in p}
    if len(parents) == 1:
        return next(iter(parents)) + "/"
    return None

def _derive_archive_dir(path: str) -> str:
    """
    Archive destination for cloudFiles.cleanSource, derived from the
    actual source path being read — NOT nested under {table_name}/.

    A destination like {table_name}/delta/archive/ would sit INSIDE the
    tree recursiveFileLookup scans when initial/+delta/ get collapsed
    onto their common parent (see _common_parent_if_direct_children).
    Since Auto Loader tracks "already seen" purely by path, a file moved
    to a path still under that recursively-scanned tree looks brand new
    the moment it lands there — causing it to be re-ingested in a loop.
    Rooting the archive under a sibling _archive/ folder next to (not
    inside) {table_name}/ avoids that unconditionally, regardless of
    whether this particular table uses the recursive collapse or not.

    Handles both path shapes this function is ever called with:
      .../DEMO/TABLE/delta/     (single path, no collapse)
      .../DEMO/TABLE/           (collapsed common parent, recursive)
    """
    parts = path.rstrip("/").split("/")
    if parts[-1] in ("initial", "delta"):
        table_seg = parts[-2]
        base = "/".join(parts[:-2])
    else:
        table_seg = parts[-1]
        base = "/".join(parts[:-1])
    return f"{base}/_archive/{table_seg}/delta/"

# ── 3. Bronze (plain parquet reader) ───────────────────────

def register_bronze_file(layer_config: dict, framework_config: dict):

    source       = layer_config["source"]
    layer_name   = layer_config["name"]
    table_name   = get_dlt_table_name(layer_config["target"]["table_name"])
    schema_dict  = layer_config.get("schema")
    clean_rules  = layer_config.get("clean_rules", [])
    expect_rules = layer_config.get("expect_rules", [])

    hard_schema  = build_hard_schema(schema_dict) if schema_dict else None
    schema_hints = schema_to_hints(schema_dict) if schema_dict else ""

    # Must match pipeline_registry.py's _clean_schema_locations() exactly:
    #   schema_path = f"{base_volume}/schema/{layer_name}"
    # That function selectively wipes this path when a source's schema
    # hints change between registrations (its "TC-3 fix" comment) — but
    # only if Auto Loader is actually tracking schema state THERE. Without
    # this option, cloudFiles falls back to a Lakeflow-managed internal
    # location the registry never touches, silently making that whole
    # cleanup step a no-op. Setting this explicitly is what makes the
    # "Schema locations: N cleared" step in your registration logs
    # actually affect the running pipeline.
    schema_location = f"{framework_config['base_volume']}/schema/{layer_name}"

    raw_paths = [p.strip() for p in source["path"].split(",") if p.strip()]
    if not raw_paths:
        raise ValueError(f"Bronze layer '{layer_name}' has no source path")

    common_parent = _common_parent_if_direct_children(raw_paths)
    use_recursive = bool(common_parent)
    load_paths    = [common_parent] if common_parent else raw_paths

    log("BRONZE_FILE", f"Registering bronze file layer '{layer_name}' -> {table_name}")
    log("BRONZE_FILE", f"Hints  : {schema_hints[:120]}...")
    log("BRONZE_FILE", f"Schema : {schema_location}")
    if common_parent:
        log("BRONZE_FILE", f"Path   : {len(raw_paths)} sibling folder(s) -> single stream on common "
            f"parent (recursiveFileLookup=true): {common_parent}")
    elif len(raw_paths) > 1:
        log("BRONZE_FILE", f"Path   : {len(raw_paths)} disjoint path(s) -> one append_flow per path "
            f"into '{table_name}'")
    else:
        log("BRONZE_FILE", f"Path   : single source path -> {raw_paths[0]}")

    from pyspark.sql import functions as F  # noqa: F401 (kept for parity with SUPPORTED-type filtering below)
    SUPPORTED = {
        "StringType", "LongType", "IntegerType", "ShortType",
        "ByteType", "DoubleType", "FloatType", "DecimalType",
        "BooleanType", "DateType", "TimestampType",
        "TimestampNTZType", "BinaryType", "NullType"
    }

    def _read_and_shape(path: str):
        archive_dir = _derive_archive_dir(path)
        reader = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", source.get("format", "parquet"))
            .option("cloudFiles.schemaLocation", schema_location)
            # Plain directory listing instead of managed file events — avoids
            # CF_BUCKET_MISMATCH when the volume's physical container differs
            # from the one the file-event/checkpoint was registered against.
            .option("cloudFiles.useManagedFileEvents", "false")
            .option("cloudFiles.useNotifications", "false")
            #.option("cloudFiles.ignoreMissingFiles", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            # Move (not delete) processed files into a sibling _archive/
            # folder — see _derive_archive_dir for why it must NOT be
            # nested under {table_name}/. retentionDuration here is only
            # the wait-before-archiving buffer (time to notice a bad
            # batch before its source files disappear from the active
            # landing path) — it does NOT control how long files then
            # live in the archive. That's a second, separate retention
            # stage: purge_old_landing_archives() in pipeline_registry.py,
            # run as its own scheduled job (e.g. daily) against the
            # LANDING_BASE_PATH/_archive root, deleting anything older
            # than its own retention_days (30, per current requirement).
            .option("cloudFiles.cleanSource", "MOVE")
            .option("cloudFiles.cleanSource.moveDestination", archive_dir)
            .option("cloudFiles.cleanSource.retentionDuration", "7 days")
            #.option("cloudFiles.maxFilesAge", "1h") #Ignore files more than 1 hour as considered as old files"
        )
        if use_recursive:
            reader = reader.option("recursiveFileLookup", "true")

        # if hard_schema:
        #     reader = reader.schema(hard_schema)

        if schema_hints:
            reader = reader.option("cloudFiles.schemaHints", schema_hints)

        df = reader.load(path)   # ← always a single string, never a list
        df = _normalize_system_columns(df)

        cols_to_keep = [
            f.name for f in df.schema.fields
            if type(f.dataType).__name__ in SUPPORTED
        ]
        df = df.select(cols_to_keep)

        df = apply_clean_rules(df, clean_rules)
        df = df.withColumn("_ingestion_time", current_timestamp())
        return df

    if len(load_paths) == 1:
        def _bronze():
            return _read_and_shape(load_paths[0])

        if expect_rules:
            _bronze = _apply_expectations(_bronze, expect_rules)

        dp.table(
            name=table_name,
            comment=f"Bronze — {layer_name}",
            table_properties={
                "delta.feature.timestampNtz": "supported",
                "delta.columnMapping.mode":   "name",
                "delta.minReaderVersion":     "2",
                "delta.minWriterVersion":     "5",
            }
        )(_bronze)

        log("BRONZE_FILE", f"Bronze table '{table_name}' registered (single Auto Loader stream)"
            + (f" with {len(expect_rules)} expectation rule(s)" if expect_rules else ""))

    else:
        # Genuinely disjoint paths (no shared immediate parent) — fan
        # multiple Auto Loader flows into one target streaming table.
        # Each flow must close over its own path value explicitly
        # (default-arg trick) or every flow ends up reading the last path.
        dp.create_streaming_table(
            name=table_name,
            comment=f"Bronze — {layer_name}",
            table_properties={
                "delta.feature.timestampNtz": "supported",
                "delta.columnMapping.mode":   "name",
                "delta.minReaderVersion":     "2",
                "delta.minWriterVersion":     "5",
            }
        )
        for i, path in enumerate(load_paths):
            def _flow(path=path):
                return _read_and_shape(path)

            if expect_rules:
                _flow = _apply_expectations(_flow, expect_rules)

            dp.append_flow(target=table_name, name=f"{table_name}_flow_{i}")(_flow)

        log("BRONZE_FILE", f"Bronze table '{table_name}' registered ({len(load_paths)} append_flow(s))"
            + (f" with {len(expect_rules)} expectation rule(s) each" if expect_rules else ""))

# ── 4. Bronze DB (JDBC) ────────────────────────────────────

def register_bronze_db(layer_config: dict, framework_config: dict):
    source      = layer_config["source"]
    connection  = source["connection"]
    strategy    = source.get("incremental_strategy", {})
    table_name  = get_dlt_table_name(layer_config["target"]["table_name"])
    clean_rules = layer_config.get("clean_rules", [])

    log("BRONZE_DB", f"Registering bronze DB layer '{layer_config['name']}' -> {table_name} "
        f"(dbtable={source.get('dbtable')})")

    @dp.table(name=table_name, comment=f"Bronze DB — {layer_config['name']}")
    def _table():
        password = dbutils.secrets.get(
            scope=connection["secret_scope"],
            key=connection["secret_key"]
        )
        df = (
            spark.read.format("jdbc")
            .option("url",               connection["url"])
            .option("dbtable",           source["dbtable"])
            .option("user",              connection.get("user", ""))
            .option("password",          password)
            .option("pushDownPredicate", "true")
            .load()
        )
        watermark_col = strategy.get("watermark_column")
        lookback_days = int(strategy.get("lookback_window", 0))
        target_table  = layer_config["target"]["table_name"]
        if watermark_col:
            try:
                last_val = spark.sql(
                    f"SELECT MAX({watermark_col}) AS m FROM {target_table}"
                ).collect()[0]["m"]
                if last_val is None:
                    last_val = "1900-01-01"
            except Exception:
                last_val = "1900-01-01"
            df = df.filter(
                f"{watermark_col} IS NOT NULL AND "
                f"{watermark_col} > timestamp('{last_val}') "
                f"- INTERVAL {lookback_days} DAYS"
            )
        df = apply_clean_rules(df, clean_rules)
        return df.withColumn("_ingestion_time", current_timestamp())

# ── 5. Bronze router ───────────────────────────────────────

def register_bronze_layer(layer_config: dict, framework_config: dict):
    src_type = layer_config.get("source_type", "file")
    log("BRONZE_ROUTER", f"Layer '{layer_config.get('name')}': source_type={src_type!r}")
    if src_type == "file":
        register_bronze_file(layer_config, framework_config)
    elif src_type == "db":
        register_bronze_db(layer_config, framework_config)
    else:
        raise ValueError(f"Unknown source_type '{src_type}'")

# COMMAND ----------
# ── 6. DLT router (bronze-only) ─────────────────────────────
#
# dlt_layers above is already pre-filtered to dlt_mode=="bronze" only,
# so in normal operation this only ever sees "bronze". The explicit
# else/raise is defensive — it means a filtering bug would fail loudly
# here rather than silently mis-registering a silver layer as bronze.

def register_dlt_layer(layer_config: dict, framework_config: dict):
    dlt_mode = layer_config.get("dlt_mode")
    log("DLT_ROUTER", f"Registering: {layer_config['name']} ({dlt_mode})")
    if dlt_mode == "bronze":
        register_bronze_layer(layer_config, framework_config)
    else:
        raise ValueError(
            f"register_dlt_layer (bronze_pipeline_1.py) received dlt_mode="
            f"'{dlt_mode}' for layer '{layer_config.get('name')}' — this "
            f"pipeline only registers dlt_mode=='bronze'. Non-bronze modes "
            f"belong in silver_pipeline_1.py; check the dlt_layers filter "
            f"above for a bug if this fires."
        )

# COMMAND ----------
# ── 7. Register all bronze layers ───────────────────────────
#
# See the equivalent note in silver_pipeline_1.py and the original
# combined file's history: spark.sql() DDL/DML is rejected inside a
# Lakeflow/SDP pipeline source file (UNSUPPORTED_SPARK_SQL_COMMAND), so
# this file cannot write to control tables itself — that still happens
# entirely in gold_pipeline.py, reading this pipeline's event log after
# the run completes.

failed_layers = []
failure_details = []
log("LAYER_REGISTRATION", f"Starting registration of {len(dlt_layers)} bronze DLT layer(s)")
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

log("LAYER_REGISTRATION", f"Registered {len(dlt_layers) - len(failed_layers)}/{len(dlt_layers)} bronze DLT layers successfully")

if failed_layers:
    # IMPORTANT: keep this on ONE line — see the note in the original
    # combined file; the Databricks run banner truncates at the first
    # newline.
    detail_str = "  |  ".join(failure_details)
    log("LAYER_REGISTRATION", f"{len(failed_layers)} layer(s) failed to register — {detail_str}", level="ERROR")
    raise RuntimeError(
        f"{len(failed_layers)} bronze layer(s) failed to register — {detail_str}"
    )
else:
    log("PIPELINE_END", f"Bronze pipeline definition completed successfully. Log file: {_LOG_FILE_PATH}")
