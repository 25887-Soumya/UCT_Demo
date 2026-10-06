# Databricks notebook source
# DBTITLE 1,Silver -> Gold
#
# This is a PLAIN notebook (not a Lakeflow Declarative Pipeline) run as a
# Job task, chained AFTER the bronze/silver Lakeflow pipeline task. Gold
# needs imperative control — arbitrary custom SQL, joins across multiple
# silver/bronze sources, and incremental MERGE — none of which map cleanly
# onto Lakeflow's declarative table-function model (see the earlier
# discussion: dp.create_auto_cdc_flow is built for CDC key+sequence
# semantics tied to one source, not arbitrary business SQL).
#
# Reads the SAME config file the bronze/silver pipeline used (same
# resolver: latest file in {config_out_dir}/{pipeline_id}/), and only
# processes layers with layer_type == "spark" (gold).
#
import re
import json
import uuid
from datetime import datetime, timedelta
import hashlib
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
import os

# Default — overridden by widget values below in section 1.
CTRL = "rgaplxdatabricks.uct_demo"

# ── 0. File-based logging setup ─────────────────────────────
#
# Every stage below already prints to stdout (visible in the job run's
# driver logs). This adds a SECOND sink: every print is mirrored, via
# log(), into a per-run file under LOG_DIR. Volumes paths are
# FUSE-mounted under /Volumes, so plain Python file I/O works here.
#
# SHARED FILE WITH lakeflow_pipeline.py: see the matching comment there
# — the filename is timestamped with the RESOLVED CONFIG's own
# timestamp (config_ts), not datetime.now(), so this task (which runs
# SECOND, after the bronze/silver Lakeflow pipeline task) lands on the
# exact same path lakeflow_pipeline.py already wrote to, as long as no
# new config was registered in between. Construction is deferred until
# after config resolution (section 2).
# Default — overridden by widget values below in section 1.
LOG_DIR        = "/Volumes/rgaplxdatabricks/temp_source/source_files/logs"
COMPONENT      = "GOLD"
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
    Called once config_ts is known (section 2, below). Reads any content
    ALREADY at the shared path first (written by lakeflow_pipeline.py,
    which runs before this task) and prepends it to this process's
    buffer, so gold's own lines are appended to — not written over —
    whatever bronze/silver already logged. See the matching function in
    lakeflow_pipeline.py for the full rationale.
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

# ── 1. Widgets ───────────────────────────────────────────────
dbutils.widgets.text("environment",       "",  "Environment")
dbutils.widgets.text("pipeline_id",       "",  "Pipeline Id")
dbutils.widgets.text("config_out_dir",    "",  "Config Dir (passed from job)")
dbutils.widgets.text("dlt_pipeline_uuid", "",  "Bronze/Silver Lakeflow Pipeline UUID (optional)")
dbutils.widgets.text("catalog",           "rgaplxdatabricks", "Target Catalog")
dbutils.widgets.text("control_schema",    "uct_demo",         "Control Schema")
dbutils.widgets.text("base_volume",       "",  "Framework volume path (passed from job)")

environment       = dbutils.widgets.get("environment").strip()
pipeline_id       = dbutils.widgets.get("pipeline_id").strip()
config_out_dir    = dbutils.widgets.get("config_out_dir").strip()
dlt_pipeline_uuid = dbutils.widgets.get("dlt_pipeline_uuid").strip()
_catalog          = dbutils.widgets.get("catalog").strip()
_control_schema   = dbutils.widgets.get("control_schema").strip()
_base_volume      = dbutils.widgets.get("base_volume").strip()

# Override module-level defaults with widget-resolved values
if _catalog and _control_schema:
    CTRL = f"{_catalog}.{_control_schema}"
if _base_volume:
    LOG_DIR = f"{_base_volume.rstrip('/')}/logs"

if not pipeline_id:
    raise ValueError("Widget 'pipeline_id' is required.")
if not environment:
    raise ValueError("Widget 'environment' is required.")

try:
    os.makedirs(LOG_DIR, exist_ok=True)
except Exception as _log_init_ex:
    print(f"  WARNING: could not initialize log directory {LOG_DIR}: {_log_init_ex}")

log("WIDGETS", f"Widgets resolved -> pipeline_id={pipeline_id!r} environment={environment!r} "
    f"dlt_pipeline_uuid={dlt_pipeline_uuid!r}")
log("WIDGETS", "Log file path pending — finalized once config_ts is resolved (section 2)")

# ── 2. Resolve latest config (same logic/layout as lakeflow_pipeline.py) ──
log("CONFIG_RESOLVE", f"Resolving latest config in {config_out_dir.rstrip('/')}/{pipeline_id}")
pipeline_config_dir = f"{config_out_dir.rstrip('/')}/{pipeline_id}"

_FNAME_RE = re.compile(
    rf"^{re.escape(pipeline_id)}_{re.escape(environment)}_(\d{{8}}_\d{{6}})_config\.json$"
)

def _resolve_latest_config(config_dir: str, fname_re: re.Pattern):
    try:
        entries = dbutils.fs.ls(config_dir)
    except Exception as ex:
        raise FileNotFoundError(f"Could not list config dir '{config_dir}': {ex}")
    candidates = []
    for f in entries:
        if f.name == "archive" or f.name.endswith("/"):
            continue
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
    candidates.sort(key=lambda t: t[0])
    return candidates[-1][1], candidates[-1][0]

config_path, config_ts = _resolve_latest_config(pipeline_config_dir, _FNAME_RE)
if not config_path:
    raise FileNotFoundError(
        f"No config found matching '{pipeline_id}_{environment}_<timestamp>_config.json' "
        f"in {pipeline_config_dir}."
    )

_finalize_log_file(pipeline_id, environment, config_ts)
log("CONFIG_RESOLVE", f"Config resolved: {config_path} (timestamp={config_ts})")
log("CONFIG_RESOLVE", f"Log file: {_LOG_FILE_PATH}  (shared with lakeflow_pipeline.py — "
                       f"appended to, not overwritten, if that task already wrote to it)")
config_str = "\n".join(r.value for r in spark.read.text(config_path).collect())
config = json.loads(config_str)
log("CONFIG_RESOLVE", f"Config loaded and parsed ({len(config_str)} bytes)")

gold_layers = [l for l in config["layers"] if l.get("layer_type") == "spark"]

log("PIPELINE_START", "=" * 70)
log("PIPELINE_START", "SILVER -> GOLD START")
log("PIPELINE_START", "=" * 70)
log("PIPELINE_START", f"Pipeline    : {pipeline_id}")
log("PIPELINE_START", f"Environment : {environment}")
log("PIPELINE_START", f"Config      : {config_path}  (timestamp={config_ts})")
log("PIPELINE_START", f"Gold layers : {len(gold_layers)}")
for l in gold_layers:
    log("PIPELINE_START", f"  {l['name']}  ({l.get('materialization','table')}, load_type={l.get('load_type','full')})")
log("PIPELINE_START", "=" * 70)

# COMMAND ----------

# ── 3. Logging helpers (pipeline_runs / pipeline_alerts) ─────────────

def _sql_lit(v) -> str:
    if v is None:
        return "null"
    return f"'{str(v).replace(chr(39), chr(39)*2)}'"

log("LOG_TABLE_SETUP", f"Ensuring {CTRL}.pipeline_runs exists")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CTRL}.pipeline_runs (
        run_id            STRING,
        run_group_id      STRING,
        pipeline_id       STRING,
        pipeline_name     STRING,
        environment       STRING,
        layer_name        STRING,
        layer_type        STRING,
        stage             STRING,
        source_path       STRING,
        bronze_table      STRING,
        silver_table      STRING,
        gold_table        STRING,
        config_version_id STRING,
        status            STRING,
        rows_read         LONG,
        rows_written      LONG,
        rows_failed       LONG,
        rows_rejected     LONG,
        started_at        TIMESTAMP,
        completed_at      TIMESTAMP,
        duration_seconds  LONG,
        error_message     STRING,
        error_type        STRING,
        triggered_by      STRING,
        created_at        TIMESTAMP
    ) USING DELTA
""")
log("LOG_TABLE_SETUP", f"{CTRL}.pipeline_runs ready")

log("LOG_TABLE_SETUP", f"Ensuring {CTRL}.pipeline_reconciliation exists")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CTRL}.pipeline_reconciliation (
        reconciliation_id     STRING,
        run_group_id          STRING,
        pipeline_id           STRING,
        pipeline_name         STRING,
        environment           STRING,
        config_version_id     STRING,
        source_path           STRING,
        bronze_table          STRING,
        silver_table          STRING,
        gold_table            STRING,
        bronze_count          LONG,
        silver_count          LONG,
        silver_agg_count      LONG,
        gold_count            LONG,
        count_diff_pct        DOUBLE,
        tolerance_pct         DOUBLE,
        reconciliation_status STRING,
        computed_at           TIMESTAMP,
        created_at            TIMESTAMP
    ) USING DELTA
""")
# ALTER, not just CREATE IF NOT EXISTS: the table already exists in every
# deployed environment without this column — CREATE IF NOT EXISTS is a
# no-op against an existing table, so the column would silently never
# appear without this.
try:
    spark.sql(f"ALTER TABLE {CTRL}.pipeline_reconciliation ADD COLUMN silver_agg_count LONG")
    log("LOG_TABLE_SETUP", "Added silver_agg_count column to pipeline_reconciliation")
except Exception as exc:
    if "already exists" not in str(exc).lower() and "duplicate column" not in str(exc).lower():
        log("LOG_TABLE_SETUP", f"Could not add silver_agg_count column: {exc}", level="WARNING")
log("LOG_TABLE_SETUP", f"{CTRL}.pipeline_reconciliation ready")

# Redefinition of what bronze_count / silver_count mean, as of this
# version:
#   bronze_count     = rows that landed in bronze THIS RUN (not table total)
#   silver_count     = rows TOUCHED in silver THIS RUN — inserted + updated
#                       + deleted for upsert/history (SCD) layers, or rows
#                       appended for snapshot/delta layers
#   silver_agg_count = silver table's TOTAL row count (what bronze_count/
#                       silver_count used to mean before this change)
#   gold_count       = gold table's TOTAL row count (unchanged)
#   count_diff_pct   = computed between silver_agg_count and gold_count
#                       (previously bronze_count vs gold_count) — the two
#                       numbers that both represent "current total state"
#                       are the ones that should reconcile against each
#                       other; bronze_count/silver_count are now
#                       per-run throughput figures, not reconcilable
#                       totals.

def _log_run(run_id, layer_name, layer_type, status, started_at,
             completed_at=None, rows_read=None, rows_written=None,
             rows_failed=None, rows_rejected=None,
             error_message=None, error_type=None,
             triggered_by="gold_pipeline",
             run_group_id=None, stage=None, source_path=None,
             bronze_table=None, silver_table=None, gold_table=None):
    duration = None
    if completed_at is not None:
        duration = int((completed_at - started_at).total_seconds())
    spark.sql(f"""
        INSERT INTO {CTRL}.pipeline_runs
        (run_id, run_group_id, pipeline_id, pipeline_name, environment,
         layer_name, layer_type, stage, source_path, bronze_table,
         silver_table, gold_table,
         config_version_id, status, rows_read, rows_written, rows_failed,
         rows_rejected, started_at, completed_at, duration_seconds,
         error_message, error_type, triggered_by, created_at)
        VALUES (
            {_sql_lit(run_id)}, {_sql_lit(run_group_id)},
            {_sql_lit(pipeline_id)}, {_sql_lit(config.get('pipeline_name'))},
            {_sql_lit(environment)}, {_sql_lit(layer_name)}, {_sql_lit(layer_type)},
            {_sql_lit(stage)}, {_sql_lit(source_path)}, {_sql_lit(bronze_table)},
            {_sql_lit(silver_table)}, {_sql_lit(gold_table)},
            {_sql_lit(config.get('version_number'))}, {_sql_lit(status)},
            {rows_read if rows_read is not None else "null"},
            {rows_written if rows_written is not None else "null"},
            {rows_failed if rows_failed is not None else "null"},
            {rows_rejected if rows_rejected is not None else "null"},
            timestamp({_sql_lit(started_at.isoformat())}),
            {f"timestamp({_sql_lit(completed_at.isoformat())})" if completed_at else "null"},
            {duration if duration is not None else "null"},
            {_sql_lit(error_message[:2000] if error_message else None)},
            {_sql_lit(error_type)}, {_sql_lit(triggered_by)}, current_timestamp()
        )
    """)

def _log_alert(layer_name, subject, body, severity="critical"):
    log("ALERT", f"Raising {severity} alert for layer '{layer_name}': {subject}", level="WARNING")
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CTRL}.pipeline_alerts (
                alert_id STRING, pipeline_id STRING, environment STRING,
                alert_to STRING, subject STRING, body STRING, severity STRING,
                resolved BOOLEAN, resolved_at TIMESTAMP, resolved_by STRING,
                alert_time TIMESTAMP, created_at TIMESTAMP
            ) USING DELTA
        """)
        alert_id = f"{pipeline_id}_{environment}_{layer_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        spark.sql(f"""
            INSERT INTO {CTRL}.pipeline_alerts
            (alert_id, pipeline_id, environment, alert_to, subject, body, severity,
             resolved, resolved_at, resolved_by, alert_time, created_at)
            VALUES (
                {_sql_lit(alert_id)}, {_sql_lit(pipeline_id)}, {_sql_lit(environment)},
                null, {_sql_lit(subject[:500])}, {_sql_lit(body[:2000])}, {_sql_lit(severity)},
                false, null, null, current_timestamp(), current_timestamp()
            )
        """)
    except Exception as exc:
        log("ALERT", f"could not write pipeline_alerts for '{layer_name}': {exc}", level="WARNING")

def _safe_count(table_name: str):
    if not table_name:
        return None
    try:
        return spark.sql(f"SELECT COUNT(*) AS c FROM {table_name}").collect()[0]["c"]
    except Exception as exc:
        log("COUNT", f"Could not count {table_name}: {exc}", level="WARNING")
        return None

def _previous_run_completed_at(layer_name):
    """Last successful completed_at logged for this layer_name, from a run
    OTHER than the current one — the baseline this run's Delta commit
    history is diffed against to isolate "what changed THIS run" (our
    working definition of "today"). None means either this is the very
    first run ever for this layer, or the lookup failed — either way the
    caller falls back to reading the table's FULL commit history, which
    for a first run is the correct thing anyway (everything in the table
    came in "this run")."""
    try:
        row = spark.sql(f"""
            SELECT MAX(completed_at) AS ts
            FROM {CTRL}.pipeline_runs
            WHERE layer_name = {_sql_lit(layer_name)}
              AND status = 'success'
              AND run_group_id != {_sql_lit(run_group_id)}
        """).collect()
        return row[0]["ts"] if row and row[0]["ts"] else None
    except Exception as exc:
        log("DELTA_HISTORY", f"Could not resolve previous run for '{layer_name}': {exc}", level="WARNING")
        return None

def _delta_commit_metrics(table_name, since_ts, is_merge_mode):
    """Sums Delta's OWN operationMetrics (from DESCRIBE HISTORY) for every
    commit to table_name after since_ts — reliable regardless of whether
    Lakeflow logged a populated flow_progress metric for that commit,
    since Delta always writes operationMetrics on every commit itself.
    since_ts=None reads the table's entire history (used on a layer's
    first-ever run).

    is_merge_mode=True (upsert/history/SCD layers): reads MERGE commits'
    numTargetRowsInserted/Updated/Deleted. is_merge_mode=False (bronze,
    snapshot/delta silver): reads WRITE/STREAMING UPDATE/APPEND commits'
    numOutputRows.

    Returns None if the table can't be read/has no history yet (e.g. not
    created), otherwise a dict:
        written  — rows inserted+updated (merge modes) or rows appended
                    (append modes) — "records upserted/appended/inserted"
        touched  — written + deleted (merge modes only; equals written
                    for append modes, since there's nothing to delete)
    """
    if not table_name:
        return None
    try:
        hist = spark.sql(f"DESCRIBE HISTORY {table_name}")
        if since_ts is not None:
            hist = hist.where(f"timestamp > timestamp({_sql_lit(since_ts.isoformat())})")
        rows = hist.select("operation", "operationMetrics").collect()
    except Exception as exc:
        log("DELTA_HISTORY", f"Could not read history for {table_name}: {exc}", level="WARNING")
        return None

    inserted = updated = deleted = output_rows = 0
    for r in rows:
        op = r["operation"]
        m = r["operationMetrics"] or {}
        if op == "MERGE":
            inserted += int(m.get("numTargetRowsInserted", 0) or 0)
            updated  += int(m.get("numTargetRowsUpdated", 0) or 0)
            deleted  += int(m.get("numTargetRowsDeleted", 0) or 0)
        elif op in ("WRITE", "STREAMING UPDATE", "APPEND"):
            output_rows += int(m.get("numOutputRows", 0) or 0)
        # OPTIMIZE / VACUUM / SET TBLPROPERTIES / etc. don't add or change
        # data rows and are ignored.

    if is_merge_mode:
        return {"written": inserted + updated, "touched": inserted + updated + deleted}
    else:
        return {"written": output_rows, "touched": output_rows}

# today_metrics: keyed by table name, populated as each bronze/silver flow
# is harvested below. Lets a downstream layer's rows_read be a genuine
# pass-through of its upstream layer's rows_written for THIS SAME RUN,
# rather than an independently (and differently) computed number:
#   silver.rows_read = today_metrics[bronze_table]["written"]
#   gold.rows_read   = today_metrics[silver_table]["written"]
today_metrics = {}

# COMMAND ----------

# ── 4. Harvest REAL bronze/silver metrics from the Lakeflow event log ─
#
# lakeflow_pipeline.py can NEVER write to pipeline_runs/pipeline_alerts
# directly — spark.sql() DDL/DML is rejected inside a Lakeflow/SDP
# pipeline source file no matter where it's placed (confirmed:
# UNSUPPORTED_SPARK_SQL_COMMAND — SDP evaluates that whole file as a
# declarative graph; dataset functions may only define/return
# DataFrames). So this step is the ONLY place bronze/silver get logged
# at all — it INSERTs fresh rows (there is nothing pre-existing to
# UPDATE), reading real per-table status straight from the event log
# (event log is now used for STATUS/TIMING/rows_rejected only — see
# _delta_commit_metrics above for why row VOLUMES come from Delta's own
# transaction log instead), and raises a pipeline_alerts row for anything
# that failed.
#
# Needs the actual Databricks Lakeflow pipeline UUID (not our pipeline_id
# string) — pass it via the dlt_pipeline_uuid widget/job parameter. If
# omitted, this step is skipped with a warning; gold still runs, but
# bronze/silver won't have any pipeline_runs rows for that run.
#
# IMPORTANT: this query is filtered to run_group_id (the MOST RECENT
# update_id on this pipeline) — earlier versions of this harvest had no
# such filter and re-inserted a fresh pipeline_runs row for the ENTIRE
# event log history on every single gold_pipeline.py run, duplicating
# without bound. run_group_id is also what ties this run's bronze/silver
# rows to its gold rows (section 6) and its reconciliation rows
# (section 5/7) into one queryable unit.

layer_by_target = {
    l["target"]["table_name"]: l
    for l in config["layers"]
    if l.get("layer_type") == "dlt"
}

def _lineage_for_dlt_layer(layer: dict):
    """Returns (stage, source_path, bronze_table, silver_table) for a
    bronze or silver dlt layer, based on its dlt_mode."""
    dlt_mode = layer.get("dlt_mode")
    if dlt_mode == "bronze":
        src = layer.get("source", {})
        path = src.get("path") if isinstance(src, dict) else None
        return "source_to_bronze", path, layer["target"]["table_name"], None
    elif dlt_mode in ("snapshot", "upsert", "history", "delta", "full"):
        src = layer.get("source", {})
        bronze_tbl = src.get("table_name") if isinstance(src, dict) else None
        return "bronze_to_silver", None, bronze_tbl, layer["target"]["table_name"]
    else:
        return None, None, None, None

run_group_id = None

if dlt_pipeline_uuid:
    log("EVENT_LOG_HARVEST", f"Resolving latest update_id on pipeline {dlt_pipeline_uuid} "
        f"as this run's run_group_id")
    try:
        latest_row = spark.sql(f"""
            SELECT origin.update_id AS update_id
            FROM event_log('{dlt_pipeline_uuid}')
            WHERE origin.update_id IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
        """).collect()
        run_group_id = latest_row[0]["update_id"] if latest_row else None
    except Exception as exc:
        log("EVENT_LOG_HARVEST", f"Could not resolve latest update_id: {exc}", level="WARNING")

    if not run_group_id:
        log("EVENT_LOG_HARVEST",
            "No update_id found in event log yet — falling back to a fresh "
            "run_group_id; bronze/silver rows for this run may be missing "
            "if the DLT update hasn't produced any events.", level="WARNING")
        run_group_id = str(uuid.uuid4())
    else:
        log("EVENT_LOG_HARVEST", f"run_group_id = {run_group_id}")

    log("EVENT_LOG_HARVEST", f"Harvesting bronze/silver metrics from "
        f"event_log('{dlt_pipeline_uuid}') for update_id={run_group_id}")
    try:
        # Event log is used for TIMING and FAILURE metrics only now —
        # MIN/MAX(timestamp) per flow gives a real started_at/completed_at,
        # and dropped_records is a genuine event-log-only signal (data
        # quality expectation failures aren't visible any other way).
        #
        # It is deliberately NOT used for row counts anymore. Lakeflow
        # only emits num_output_rows on SOME flow_progress events (roughly:
        # one per micro-batch) — for small/fast flows that can complete
        # without ever logging a tick that carries a populated metrics
        # object, so SUM(num_output_rows) silently comes back 0 even when
        # real rows were written. rows_read fares even worse: Lakeflow's
        # FlowMetrics schema has no top-level "rows read" field at all
        # (only num_output_rows / num_upserted_rows / num_deleted_rows /
        # num_output_bytes + backlog_* are documented), so it can never be
        # populated from here regardless of which events are inspected.
        #
        # rows_written is instead taken as a live COUNT(*) against the
        # actual table the flow writes to, below — the same proven,
        # reliable method already used for pipeline_reconciliation's
        # bronze_count/silver_count.
        event_df = spark.sql(f"""
            WITH flow_events AS (
                SELECT
                    origin.flow_name AS flow_name,
                    details:flow_progress.status AS status,
                    TRY_CAST(details:flow_progress.data_quality.dropped_records AS BIGINT) AS rows_rejected,
                    timestamp
                FROM event_log('{dlt_pipeline_uuid}')
                WHERE event_type = 'flow_progress'
                  AND origin.update_id = '{run_group_id}'
                  AND origin.flow_name IS NOT NULL
            )
            SELECT
                flow_name,
                MIN(timestamp) AS started_at,
                MAX(timestamp) AS completed_at,
                SUM(COALESCE(rows_rejected, 0)) AS rows_rejected,
                MAX(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS had_failure,
                COUNT(CASE WHEN status IN ('COMPLETED', 'FAILED') THEN 1 END) AS reached_terminal
            FROM flow_events
            GROUP BY flow_name
            ORDER BY started_at
        """)
        # ORDER BY started_at: bronze flows commit before the silver flows
        # that read from them within the same update, so processing rows
        # in that order means today_metrics[bronze_table] is already
        # populated by the time we reach that bronze table's silver flow
        # and need to pass its rows_read through.
        rows = event_df.collect()
        harvested = 0
        for r in rows:
            layer_name    = r["flow_name"]
            if r["reached_terminal"] == 0:
                # Flow has progress events this update but hasn't reached
                # COMPLETED/FAILED yet (still running, or update still in
                # flight) — skip it this run rather than log a bogus
                # "success" for a flow that isn't actually done.
                log("EVENT_LOG_HARVEST",
                    f"Flow '{layer_name}' has no terminal event yet — skipping "
                    f"(will be picked up on a later gold_pipeline.py run)",
                    level="WARNING")
                continue
            status        = "failed" if r["had_failure"] else "success"
            rows_rejected = int(r["rows_rejected"]) if r["rows_rejected"] is not None else None
            started_at    = r["started_at"]
            completed_at  = r["completed_at"]

            layer_def = layer_by_target.get(layer_name)
            if layer_def:
                stage, source_path, bronze_table, silver_table = _lineage_for_dlt_layer(layer_def)
                dlt_mode = layer_def.get("dlt_mode")
            else:
                stage, source_path, bronze_table, silver_table, dlt_mode = None, None, None, None, None
                log("EVENT_LOG_HARVEST",
                    f"Flow '{layer_name}' not found in config['layers'] — "
                    f"logging without lineage enrichment", level="WARNING")

            own_table = bronze_table if stage == "source_to_bronze" else silver_table
            is_merge_mode = dlt_mode in ("upsert", "history")

            commit_metrics = None
            if status == "success" and own_table:
                since_ts = _previous_run_completed_at(layer_name)
                commit_metrics = _delta_commit_metrics(own_table, since_ts, is_merge_mode)

            if stage == "source_to_bronze":
                # rows_written = rows that landed in bronze this run.
                # rows_read = what Auto Loader actually read from the
                # parquet files this run — landed rows + rows the same run
                # rejected via expectations (rescue/quarantine doesn't drop
                # good rows, so read == written + rejected).
                rows_written = commit_metrics["written"] if commit_metrics else None
                rows_read = (rows_written + rows_rejected) if (
                    rows_written is not None and rows_rejected is not None) else rows_written
            elif stage == "bronze_to_silver":
                # rows_written = records upserted/appended/inserted into
                # silver this run (deletes excluded — matches how you
                # defined bronze_to_silver rows_written).
                # rows_read = pass-through of bronze's rows_written from
                # this SAME harvest pass — "what came into bronze this
                # run" is exactly "what silver read from bronze this run".
                rows_written = commit_metrics["written"] if commit_metrics else None
                rows_read = today_metrics.get(bronze_table, {}).get("written")
            else:
                rows_written, rows_read = None, None

            if own_table:
                today_metrics[own_table] = {
                    "written": rows_written,
                    "touched": commit_metrics["touched"] if commit_metrics else None,
                }

            run_id = str(uuid.uuid4())
            log("EVENT_LOG_HARVEST", f"Flow '{layer_name}' [{stage}]: status={status}, "
                f"rows_read={rows_read}, rows_written={rows_written}, rows_rejected={rows_rejected}, "
                f"duration={(completed_at - started_at).total_seconds():.1f}s")

            _log_run(
                run_id, layer_name, "dlt", status, started_at,
                completed_at=completed_at, rows_read=rows_read, rows_written=rows_written,
                rows_rejected=rows_rejected,
                triggered_by="gold_pipeline_harvest",
                run_group_id=run_group_id, stage=stage, source_path=source_path,
                bronze_table=bronze_table, silver_table=silver_table,
            )

            if status == "failed":
                _log_alert(
                    layer_name,
                    subject=f"[{environment.upper()}] Bronze/silver layer failed — {layer_name}",
                    body=(f"Pipeline: {pipeline_id}\nLayer: {layer_name}\nStage: {stage}\n"
                          f"Environment: {environment}\nDLT pipeline UUID: {dlt_pipeline_uuid}\n"
                          f"Event window: {started_at} to {completed_at}\n\n"
                          f"See event_log('{dlt_pipeline_uuid}') for full details."),
                )

            harvested += 1
        log("EVENT_LOG_HARVEST", f"Harvested {harvested} flow(s) from event log into pipeline_runs")
    except Exception as exc:
        log("EVENT_LOG_HARVEST", f"could not harvest event log metrics: {exc}", level="WARNING")
else:
    log("EVENT_LOG_HARVEST",
        "dlt_pipeline_uuid not provided — skipping event log metrics harvest "
        "for bronze/silver (gold will still run, but bronze/silver won't be "
        "logged to pipeline_runs for this run).", level="WARNING")
    run_group_id = str(uuid.uuid4())
    log("EVENT_LOG_HARVEST", f"Using standalone run_group_id for gold-only rows: {run_group_id}")


# COMMAND ----------

# ── 5. Reconciliation helpers ─────────────────────────────────
#
# bronze_count / silver_count meaning changed — see the note by the
# pipeline_reconciliation DDL above. Quick recap:
#   bronze_count     = rows that landed in bronze THIS RUN
#                       (today_metrics[bronze_table]["written"])
#   silver_count     = rows TOUCHED in silver THIS RUN — inserted+updated
#                       +deleted for upsert/history, or rows appended for
#                       snapshot/delta (today_metrics[silver_table]["touched"])
#   silver_agg_count = silver table's TOTAL row count (live COUNT(*))
#   gold_count       = gold table's TOTAL row count (live COUNT(*))
#   count_diff_pct   = |silver_agg_count - gold_count| / silver_agg_count
#                       — the two TOTAL-state numbers, not the per-run ones
#
# Two writes per lineage chain, but now seeded upfront for EVERY gold
# layer in one pass — BEFORE any gold layer runs at all, not just before
# each layer's own run — per your instruction that bronze/silver get
# done before "the gold pipeline" (the whole stage) starts:
#   1. _seed_reconciliation() — bronze_count/silver_count/silver_agg_count,
#      called for every gold layer in section 5a below, all before
#      section 6 (gold execution) begins. gold_count stays NULL,
#      status='PENDING'.
#   2. _finalize_reconciliation() — gold_count + diff/status, written the
#      instant THAT gold layer finishes, in the section 6 loop.

TOLERANCE_PCT = 0.0
# _safe_count() is defined in section 3, shared with the pipeline_runs harvest above.

def _seed_reconciliation(source_path, bronze_table, silver_table, gold_table):
    """Insert the reconciliation row NOW, with whatever's already
    knowable. Returns (reconciliation_id, silver_agg_count) — silver_agg_count
    is held onto until that gold layer finishes, since count_diff_pct
    compares it against gold_count."""
    bronze_count     = today_metrics.get(bronze_table, {}).get("written")
    silver_count     = today_metrics.get(silver_table, {}).get("touched")
    silver_agg_count = _safe_count(silver_table)
    reconciliation_id = str(uuid.uuid4())

    log("RECONCILIATION", f"Seeding {gold_table}: bronze_count={bronze_count} (this run), "
        f"silver_count={silver_count} (this run), silver_agg_count={silver_agg_count} (total), gold=pending")

    spark.sql(f"""
        INSERT INTO {CTRL}.pipeline_reconciliation
        (reconciliation_id, run_group_id, pipeline_id, pipeline_name, environment,
         config_version_id, source_path, bronze_table, silver_table, gold_table,
         bronze_count, silver_count, silver_agg_count, gold_count, count_diff_pct,
         tolerance_pct, reconciliation_status, computed_at, created_at)
        VALUES (
            {_sql_lit(reconciliation_id)}, {_sql_lit(run_group_id)},
            {_sql_lit(pipeline_id)}, {_sql_lit(config.get('pipeline_name'))},
            {_sql_lit(environment)}, {_sql_lit(config.get('version_number'))},
            {_sql_lit(source_path)}, {_sql_lit(bronze_table)}, {_sql_lit(silver_table)},
            {_sql_lit(gold_table)},
            {bronze_count if bronze_count is not None else "null"},
            {silver_count if silver_count is not None else "null"},
            {silver_agg_count if silver_agg_count is not None else "null"},
            null, null, {TOLERANCE_PCT},
            'PENDING', current_timestamp(), current_timestamp()
        )
    """)
    return reconciliation_id, silver_agg_count

def _finalize_reconciliation(reconciliation_id, gold_table, silver_agg_count,
                              bronze_table, silver_table):
    """Called the moment THIS gold layer finishes — counts gold now and
    updates just this one row in place. Diffs silver_agg_count (total,
    seeded earlier) against gold_count (total, just counted) — both are
    "current state" numbers, unlike bronze_count/silver_count which are
    per-run throughput and aren't meaningfully reconcilable against a
    table total."""
    gold_count = _safe_count(gold_table)

    if silver_agg_count is None or gold_count is None:
        count_diff_pct = None
        recon_status = "UNKNOWN"
    elif silver_agg_count == 0:
        count_diff_pct = 0.0 if gold_count == 0 else 100.0
        recon_status = "PASS" if count_diff_pct <= TOLERANCE_PCT else "FAIL"
    else:
        count_diff_pct = abs(silver_agg_count - gold_count) / silver_agg_count * 100.0
        recon_status = "PASS" if count_diff_pct <= TOLERANCE_PCT else "FAIL"

    log("RECONCILIATION", f"Finalizing {gold_table}: silver_agg_count={silver_agg_count}, "
        f"gold={gold_count}, status={recon_status}")

    spark.sql(f"""
        UPDATE {CTRL}.pipeline_reconciliation
        SET gold_count = {gold_count if gold_count is not None else "null"},
            count_diff_pct = {count_diff_pct if count_diff_pct is not None else "null"},
            reconciliation_status = {_sql_lit(recon_status)},
            computed_at = current_timestamp()
        WHERE reconciliation_id = {_sql_lit(reconciliation_id)}
    """)

    if recon_status == "FAIL":
        _log_alert(
            gold_table,
            subject=f"[{environment.upper()}] Reconciliation FAILED — {gold_table}",
            body=(f"Pipeline: {pipeline_id}\nSilver (total): {silver_table} ({silver_agg_count})\n"
                  f"Gold (total): {gold_table} ({gold_count})\n"
                  f"count_diff_pct={count_diff_pct:.2f}%  tolerance_pct={TOLERANCE_PCT}%"),
            severity="critical",
        )
    return gold_count

# COMMAND ----------

# ── 5b. Gold execution helpers ────────────────────────────────

def _lineage_for_gold_layer(layer: dict):
    """
    Resolves a gold layer's full lineage back to bronze, using
    layer_by_target (built in section 4) to trace through the config —
    NOT by parsing sql_transform.query, which is arbitrary user SQL and
    not reliable to introspect.

    Returns (stage, source_path, bronze_table, silver_table, gold_table).
    stage is 'silver_to_gold' when sourced from a silver layer (the
    common case), or 'bronze_to_gold' when a gold layer reads bronze
    directly (delta/delta_merge gold layers with from_bronze). Even in
    the silver_to_gold case, bronze_table and source_path are filled in
    by tracing one hop further back through that silver layer's own
    source — needed for pipeline_reconciliation, which validates bronze
    all the way through to gold, not just the immediate hop.
    """
    gold_table = layer["target"]["table_name"]
    src = layer.get("source", {})
    src_table = src.get("table_name") if isinstance(src, dict) else None

    silver_table, bronze_table, source_path = None, None, None

    if src_table and src_table in layer_by_target:
        src_layer = layer_by_target[src_table]
        if src_layer.get("dlt_mode") == "bronze":
            bronze_table = src_table
            source_path = (src_layer.get("source") or {}).get("path")
        else:
            silver_table = src_table
            silver_src = (src_layer.get("source") or {}).get("table_name")
            if silver_src and silver_src in layer_by_target:
                bronze_layer = layer_by_target[silver_src]
                bronze_table = silver_src
                source_path = (bronze_layer.get("source") or {}).get("path")

    stage = "silver_to_gold" if silver_table else ("bronze_to_gold" if bronze_table else None)
    return stage, source_path, bronze_table, silver_table, gold_table


def _create_join_temp_views(layer_config: dict):
    """
    join_sources entries (beyond the primary source) get materialized as
    TEMP VIEWs named by their 'alias', so custom Gold_sql_file SQL can
    just reference the bare alias instead of a fully-qualified name.
    """
    for js in layer_config.get("join_sources", []):
        alias = js["alias"]
        table = js["table_name"]
        log("GOLD_LAYER", f"Creating join temp view {alias} <- {table}")
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {alias} AS SELECT * FROM {table}")

_MV_HASH_PROP = "framework.definition_hash"
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _inline_join_sources(query: str, join_sources: list) -> str:
    """A materialized view can't reference temp views, so join_sources
    aliases become CTEs of the view's own query instead:
        WITH <alias> AS (SELECT * FROM <table>) <query>
    merged into the query's own WITH clause if it already has one."""
    query = query.strip().rstrip(";").strip()
    ctes = []
    for js in join_sources or []:
        alias, table = js["alias"], js["table_name"]
        if not _IDENT_RE.match(alias):
            raise ValueError(f"join_sources alias '{alias}' is not a valid identifier")
        ctes.append(f"{alias} AS (SELECT * FROM {table})")
    if not ctes:
        return query
    m = re.match(r"(?is)^\s*WITH\s+(?!RECURSIVE\b)", query)
    if m:
        return f"WITH {', '.join(ctes)},\n{query[m.end():]}"
    return f"WITH {', '.join(ctes)}\n{query}"


def _run_materialized_view(name: str, target: str, query: str, layer_config: dict):
    """CREATE OR REPLACE when the view is new or its definition changed;
    REFRESH (incremental where Databricks can) when it's unchanged. The
    definition's hash is stored as a table property to tell the two apart."""
    mv_query = _inline_join_sources(query, layer_config.get("join_sources", []))
    parts    = layer_config.get("partition_cols") or []
    digest   = hashlib.sha256(json.dumps({"q": mv_query, "p": parts}).encode()).hexdigest()[:16]

    cat, sch, tbl = target.split(".")
    rows = spark.sql(f"SELECT table_type FROM {cat}.information_schema.tables "
                     f"WHERE table_schema = '{sch}' AND table_name = '{tbl}'").collect()
    exists = bool(rows)
    is_mv  = exists and (rows[0]["table_type"] or "").upper() == "MATERIALIZED_VIEW"
    if exists and not is_mv:
        raise ValueError(f"Gold layer '{name}': {target} exists as {rows[0]['table_type']}, "
                         f"not a materialized view — drop it first")
    stored = None
    if is_mv:
        stored = next((r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()
                       if r["key"] == _MV_HASH_PROP), None)

    if is_mv and stored == digest:
        log("GOLD_LAYER", f"Materialized view '{target}' unchanged — REFRESH")
        spark.sql(f"REFRESH MATERIALIZED VIEW {target}")
    else:
        log("GOLD_LAYER", f"Materialized view '{target}' "
            f"{'definition changed' if is_mv else 'is new'} — CREATE OR REPLACE")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch}")
        partition = f"PARTITIONED BY ({', '.join(parts)})\n" if parts else ""
        spark.sql(f"CREATE OR REPLACE MATERIALIZED VIEW {target}\n{partition}"
                  f"TBLPROPERTIES ('{_MV_HASH_PROP}' = '{digest}')\nAS\n{mv_query}")
    return {"rows_read": None, "rows_written": None}

def _run_gold_layer(layer_config: dict):
    name            = layer_config["name"]
    target          = layer_config["target"]["table_name"]
    materialization = layer_config.get("materialization", "table")
    load_type       = layer_config.get("load_type", "full")
    query           = layer_config.get("sql_transform", {}).get("query")
    if not query:
        raise ValueError(f"Gold layer '{name}' has no sql_transform.query in config")

    log("GOLD_LAYER", f"Running gold layer '{name}': materialization={materialization}, load_type={load_type}")
    
    if materialization == "materialized_view":
        if load_type != "full":
            raise ValueError(f"Gold layer '{name}': a materialized view is refreshed from its "
                            f"query — load_type must be 'full', got '{load_type}'")
        return _run_materialized_view(name, target, query, layer_config)

    _create_join_temp_views(layer_config)
    

    if materialization == "view":
        # Views are always full — no incremental semantics, no physical
        # rows of their own. rows_written is left NULL deliberately: a
        # COUNT(*) here would re-run the (possibly expensive) query a
        # second time just to report a number, doubling the cost of
        # every gold refresh for a metric of limited value on a view.
        spark.sql(f"CREATE OR REPLACE VIEW {target} AS {query}")
        log("GOLD_LAYER", f"View '{target}' created/replaced")
        return {"rows_read": None, "rows_written": None}
    
    
    if materialization == "table":
        catalog_schema = ".".join(target.split(".")[:2])
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema}")

        df = spark.sql(query)
        rows_read = df.count()
        log("GOLD_LAYER", f"Query for '{name}' returned {rows_read} row(s)")

    if load_type == "full":
        log("GOLD_LAYER", f"Full overwrite: writing {rows_read} row(s) to {target}")
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        rows_written = rows_read
        log("GOLD_LAYER", f"Full overwrite complete: {target} ({rows_written} row(s))")

    elif load_type == "append":
        log("GOLD_LAYER", f"Append: writing {rows_read} row(s) to {target}")
        df.write.format("delta").mode("append").saveAsTable(target)
        rows_written = rows_read
        log("GOLD_LAYER", f"Append complete: {target} ({rows_written} row(s))")

    elif load_type in ("delta", "delta_merge"):
        wm_col = layer_config.get("sequence_by")
        if not wm_col:
            raise ValueError(f"Gold layer '{name}': load_type='{load_type}' requires sequence_by (watermark)")

        table_exists = spark.catalog.tableExists(target)
        if table_exists:
            last_val = spark.sql(f"SELECT MAX({wm_col}) AS m FROM {target}").collect()[0]["m"]
        else:
            last_val = None
        log("GOLD_LAYER", f"Incremental load on {target}: table_exists={table_exists}, "
            f"watermark_col={wm_col}, last_val={last_val}")

        incr_df = df
        if last_val is not None:
            incr_df = df.filter(df[wm_col] > last_val)
        rows_read = incr_df.count()
        log("GOLD_LAYER", f"Incremental filter on '{name}' yielded {rows_read} row(s)")

        if load_type == "delta":
            log("GOLD_LAYER", f"Delta append: writing {rows_read} row(s) to {target}")
            incr_df.write.format("delta").mode("append").saveAsTable(target)
            rows_written = rows_read
            log("GOLD_LAYER", f"Delta append complete: {target} ({rows_written} row(s))")
        else:  # delta_merge
            merge_keys = layer_config.get("merge_keys", [])
            if not merge_keys:
                raise ValueError(f"Gold layer '{name}': load_type='delta_merge' requires merge_keys")
            if not table_exists:
                log("GOLD_LAYER", f"Delta merge target {target} doesn't exist yet — initial write "
                    f"of {rows_read} row(s)")
                incr_df.write.format("delta").saveAsTable(target)
                rows_written = rows_read
            else:
                log("GOLD_LAYER", f"Delta merge into {target} on keys={merge_keys} "
                    f"({rows_read} candidate row(s))")
                cond = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
                (DeltaTable.forName(spark, target).alias("t")
                    .merge(incr_df.alias("s"), cond)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute())
                rows_written = rows_read
                log("GOLD_LAYER", f"Delta merge complete: {target}")
    else:
        raise ValueError(f"Gold layer '{name}': unknown load_type '{load_type}'")

    return {"rows_read": rows_read, "rows_written": rows_written}

# COMMAND ----------

# ── 5a. Seed reconciliation for EVERY gold layer, upfront ────────
#
# All bronze_count/silver_count/silver_agg_count values are locked in
# and written here, for the whole gold stage, before a single gold
# layer runs — not just before each layer's own run. Per-layer lineage
# is already knowable from config alone (no gold execution needed to
# resolve it), so there's no reason this has to interleave with
# execution.

seeded = {}  # gold_table -> (reconciliation_id, silver_agg_count), for section 6 to finalize
log("RECONCILIATION", f"Seeding reconciliation for {len(gold_layers)} gold layer(s) "
    f"before any gold layer runs")
for layer_config in gold_layers:
    stage, source_path, bronze_table, silver_table, gold_table = _lineage_for_gold_layer(layer_config)
    reconciliation_id, silver_agg_count = _seed_reconciliation(
        source_path, bronze_table, silver_table, gold_table)
    seeded[gold_table] = (reconciliation_id, silver_agg_count)
log("RECONCILIATION", f"Seeded {len(seeded)} reconciliation row(s)")

# COMMAND ----------

# ── 6. Run all gold layers ────────────────────────────────────

failed_layers = []
log("GOLD_LAYER_RUN", f"Starting run of {len(gold_layers)} gold layer(s)")
for layer_config in gold_layers:
    name       = layer_config["name"]
    run_id     = str(uuid.uuid4())
    started_at = datetime.now()
    stage, source_path, bronze_table, silver_table, gold_table = _lineage_for_gold_layer(layer_config)
    log("GOLD_LAYER_RUN", f"Gold: {name} ({layer_config.get('materialization','table')}, "
        f"load_type={layer_config.get('load_type','full')}) — run_id={run_id}  "
        f"lineage: {bronze_table} -> {silver_table} -> {gold_table}")

    reconciliation_id, silver_agg_count = seeded[gold_table]
    # rows_read for gold = pass-through of silver's rows_written from
    # THIS run's harvest (section 4) — "records upserted/appended/
    # inserted in silver". rows_written for gold is always left NULL now
    # (per your spec) rather than computed from the query result.
    rows_read = today_metrics.get(silver_table, {}).get("written")

    try:
        _run_gold_layer(layer_config)  # return value no longer used for logging — see below
        completed_at = datetime.now()
        _log_run(run_id, name, "spark", "success", started_at, completed_at,
                  rows_read=rows_read, rows_written=None,
                  run_group_id=run_group_id, stage=stage, source_path=source_path,
                  bronze_table=bronze_table, silver_table=silver_table, gold_table=gold_table)
        log("GOLD_LAYER_RUN", f"Gold layer '{name}' succeeded "
            f"(rows_read={rows_read}, duration={(completed_at - started_at).total_seconds():.1f}s)")

        # Gold table/view exists now — count it and close out this row
        # immediately, rather than waiting for the rest of the batch.
        _finalize_reconciliation(reconciliation_id, gold_table, silver_agg_count,
                                  bronze_table, silver_table)
    except Exception as exc:
        completed_at = datetime.now()
        _log_run(run_id, name, "spark", "failed", started_at, completed_at,
                  error_message=str(exc), error_type=type(exc).__name__,
                  run_group_id=run_group_id, stage=stage, source_path=source_path,
                  bronze_table=bronze_table, silver_table=silver_table, gold_table=gold_table)
        _log_alert(
            name,
            subject=f"[{environment.upper()}] Gold layer failed — {name}",
            body=(f"Pipeline: {pipeline_id}\nLayer: {name}\nEnvironment: {environment}\n"
                  f"Config: {config_path}\n\nError: {exc}"),
        )
        # Gold never materialized — leave gold_count null rather than
        # comparing against a table that failed to build (spurious FAIL).
        # Mark the seeded row so it reads distinctly from a still-running one.
        spark.sql(f"""
            UPDATE {CTRL}.pipeline_reconciliation
            SET reconciliation_status = 'GOLD_FAILED', computed_at = current_timestamp()
            WHERE reconciliation_id = {_sql_lit(reconciliation_id)}
        """)
        failed_layers.append(name)
        log("GOLD_LAYER_RUN", f"FAILED: {name}: {exc}", level="ERROR")
        # Continue to the next gold layer — one failing gold table/view
        # shouldn't block the others in the same pipeline.

log("GOLD_LAYER_RUN", f"Completed {len(gold_layers) - len(failed_layers)}/{len(gold_layers)} gold layer(s)")

# COMMAND ----------

# ── 7. Wrap up ─────────────────────────────────────────────────
#
# Reconciliation rows are no longer built here — each one was already
# seeded (bronze/silver counts) and finalized (gold count) inline, per
# layer, in section 6 above. Query the run as a unit via:
#   SELECT * FROM pipeline_reconciliation WHERE run_group_id = '<uuid>';

if failed_layers:
    log("PIPELINE_END", f"{len(failed_layers)} gold layer(s) failed: {failed_layers}", level="ERROR")
    raise RuntimeError(
        f"{len(failed_layers)} gold layer(s) failed: {failed_layers}. "
        f"See pipeline_runs/pipeline_alerts for details."
    )
else:
    log("PIPELINE_END", f"Silver -> Gold pipeline completed successfully. Log file: {_LOG_FILE_PATH}")