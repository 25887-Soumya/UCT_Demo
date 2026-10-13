# Databricks notebook source
# DBTITLE 1,jdbc_extract_to_landing
# ══════════════════════════════════════════════════════════════════
# JDBC EXTRACT -> LANDING VOLUME (parquet) — pipeline-driven
#
# Runs in DATA_LOAD_SOURCE_TO_GOLD after Check_skip_source_to_landing,
# when the If_skip_source_to_landing condition is false (see
# check_skip_source_to_landing.py and resources/jobs.yml).
#
# Loops over every active row in pipeline_table_config for this
# pipeline_id where skip_source_to_landing = false, extracts it via JDBC
# and writes it as parquet to {landing_base_path}/{source_path} - the
# same landing location the bronze layer reads from.
#
# JDBC read pattern: spark.read.jdbc(url=, table=, properties=) with a
# connection_properties dict, NOT .format("jdbc").option(...).load().
#
# All locations come from job parameters (catalog, control_schema,
# landing_base_path), so the same notebook runs in dev, qa and prod.
# ══════════════════════════════════════════════════════════════════

import json
import re
import logging

logger = logging.getLogger("jdbc_extract_to_landing")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_h)

# Source schema used when a connection's Config does not set "schema"
# (SQL Server default).
DEFAULT_SOURCE_SCHEMA = "dbo"

# Same assumption as pipeline_registry.py's patch: Config.connection's
# "user"/"password" are secret KEY NAMES inside "scope", not literal
# credential values. Flip to False if your tbl_connection rows instead
# store literal username/password strings directly.
CREDENTIALS_ARE_SECRET_KEYS = True

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("pipeline_id", "", "Pipeline id (matches pipeline_table_config.pipeline_name)")
dbutils.widgets.text("environment", "", "Environment (logging only — pipeline_table_config isn't environment-scoped)")
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("control_schema", "", "Control schema")
dbutils.widgets.text("landing_base_path", "", "Landing zone root that source_path is relative to")

pipeline_id       = dbutils.widgets.get("pipeline_id").strip()
environment       = dbutils.widgets.get("environment").strip()
catalog           = dbutils.widgets.get("catalog").strip()
control_schema    = dbutils.widgets.get("control_schema").strip()
LANDING_BASE_PATH = dbutils.widgets.get("landing_base_path").strip()

for _name, _val in (("pipeline_id", pipeline_id), ("catalog", catalog),
                    ("control_schema", control_schema), ("landing_base_path", LANDING_BASE_PATH)):
    if not _val:
        raise ValueError(f"Widget '{_name}' is required (it is a job parameter in resources/jobs.yml).")

CTRL_SCHEMA           = f"{catalog}.{control_schema}"
CONNECTION_TABLE      = f"{CTRL_SCHEMA}.tbl_connection"
PIPELINE_TABLE_CONFIG = f"{CTRL_SCHEMA}.pipeline_table_config"

logger.info(f"Starting JDBC-to-landing extraction. pipeline_id={pipeline_id!r} environment={environment!r}")
logger.info(f"Control schema: {CTRL_SCHEMA} | landing: {LANDING_BASE_PATH}")

# COMMAND ----------

# DBTITLE 1,Resolve tables to extract
def _sql_escape(s: str) -> str:
    return s.replace("'", "''")

rows = spark.sql(f"""
    SELECT table_name, source_path, connection_name
    FROM {PIPELINE_TABLE_CONFIG}
    WHERE pipeline_name = '{_sql_escape(pipeline_id)}'
      AND is_active = true
      AND skip_source_to_landing = false
""").collect()

if not rows:
    logger.info(
        f"No active rows with skip_source_to_landing=false for pipeline_id="
        f"'{pipeline_id}' in {PIPELINE_TABLE_CONFIG} — nothing to extract. "
        f"(If you expected rows here, this task shouldn't have been scheduled "
        f"to run at all — check the If_skip_source_to_landing condition.)"
    )
    dbutils.notebook.exit("no_tables_to_extract")

logger.info(f"Found {len(rows)} table(s) to extract for pipeline_id='{pipeline_id}': "
            f"{[r['table_name'] for r in rows]}")

# COMMAND ----------

# DBTITLE 1,Connection resolution helper
_connection_cache = {}  # connection_name -> (jdbc_url, driver, connection_properties, source_schema)

def _resolve_connection(connection_name: str):
    if connection_name in _connection_cache:
        return _connection_cache[connection_name]

    conn_rows = spark.sql(f"""
        SELECT Name, Type, Config
        FROM {CONNECTION_TABLE}
        WHERE Name = '{_sql_escape(connection_name)}'
    """).collect()

    if not conn_rows:
        raise ValueError(f"No row in {CONNECTION_TABLE} where Name = '{connection_name}'.")
    if len(conn_rows) > 1:
        raise ValueError(f"Multiple rows in {CONNECTION_TABLE} share Name = '{connection_name}'.")

    conn_row = conn_rows[0]
    if (conn_row["Type"] or "").upper() != "JDBC":
        raise ValueError(f"Connection '{connection_name}' has Type={conn_row['Type']!r}, not 'JDBC'.")

    raw_config = conn_row["Config"]
    if raw_config is None:
        raise ValueError(f"Connection '{connection_name}' has a NULL Config column.")

    # Defensive: tolerate a trailing comma before a closing brace (invalid
    # strict JSON but easy to end up with if Config is hand-edited).
    cleaned_config = re.sub(r",\s*([}\]])", r"\1", raw_config)
    try:
        conn_cfg = json.loads(cleaned_config).get("connection", {})
    except json.JSONDecodeError as ex:
        raise ValueError(f"Could not parse Config JSON for connection '{connection_name}': {ex}")

    required = ["url", "driver"]
    missing = [k for k in required if not conn_cfg.get(k)]
    if missing:
        raise ValueError(f"Connection '{connection_name}' Config.connection missing {missing}.")

    jdbc_url = conn_cfg["url"]
    driver = conn_cfg.get("driver", "")

    if CREDENTIALS_ARE_SECRET_KEYS:
        scope = conn_cfg.get("scope")
        username_key = conn_cfg.get("user")
        password_key = conn_cfg.get("password")
        if not scope or not username_key or not password_key:
            raise ValueError(
                f"Connection '{connection_name}' Config.connection missing "
                f"'scope'/'user'/'password' needed to resolve secrets.")
        username = dbutils.secrets.get(scope=scope, key=username_key)
        password = dbutils.secrets.get(scope=scope, key=password_key)
    else:
        username = conn_cfg.get("user", "")
        password = conn_cfg.get("password", "")

    connection_properties = {"user": username, "password": password, "driver": driver}
    source_schema = (conn_cfg.get("schema") or DEFAULT_SOURCE_SCHEMA).strip()
    result = (jdbc_url, driver, connection_properties, source_schema)
    _connection_cache[connection_name] = result
    return result

# COMMAND ----------

# DBTITLE 1,Extract each table via JDBC and write to landing
results = []
for r in rows:
    table_name = r["table_name"]
    source_path = r["source_path"]
    connection_name = r["connection_name"]

    if not connection_name:
        logger.warning(f"'{table_name}': needs a JDBC extract but connection_name is "
                        f"blank in {PIPELINE_TABLE_CONFIG} — skipping this table.")
        results.append((table_name, "skipped_no_connection", 0))
        continue
    if not source_path:
        logger.warning(f"'{table_name}': source_path is blank in {PIPELINE_TABLE_CONFIG} — "
                        f"skipping this table (no landing destination).")
        results.append((table_name, "skipped_no_source_path", 0))
        continue

    try:
        jdbc_url, driver, connection_properties, source_schema = _resolve_connection(connection_name)

        sql_query = f"SELECT * FROM {source_schema}.{table_name}"
        logger.info(f"'{table_name}': source query (JDBC): {sql_query[:200]}...")

        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"({sql_query}) AS src_query",
            properties=connection_properties,
        )

        row_count = df.count()
        logger.info(f"'{table_name}': extracted {row_count} row(s) via connection '{connection_name}'")

        # source_path may be comma-separated per metadata_loader.py's
        # convention (one or more paths relative to LANDING_BASE_PATH) —
        # a JDBC extract only ever has one destination, so take the first.
        rel_path = source_path.split(",")[0].strip()
        landing_path = f"{LANDING_BASE_PATH.rstrip('/')}/{rel_path.lstrip('/')}"

        df.coalesce(1).write.mode("append").parquet(landing_path)
        logger.info(f"'{table_name}': wrote {row_count} row(s) to {landing_path}")
        results.append((table_name, "success", row_count))

    except Exception as exc:
        logger.error(f"'{table_name}': extraction FAILED — {exc}")
        results.append((table_name, "failed", 0))

# COMMAND ----------

# DBTITLE 1,Summary
failed = [t for t, status, _ in results if status == "failed"]
logger.info("=" * 70)
logger.info(f"JDBC-to-landing extraction summary for pipeline_id='{pipeline_id}':")
for table_name, status, row_count in results:
    logger.info(f"  {table_name}: {status} ({row_count} row(s))")
logger.info("=" * 70)

if failed:
    raise RuntimeError(f"{len(failed)} table(s) failed JDBC extraction: {failed}")