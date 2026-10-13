# Databricks notebook source
# ════════════════════════════════════════════════════════════════════════
# Post-deploy smoke test
#
# Checks that a deployment is wired to the right environment and that the
# objects the framework needs are reachable. Read-only: it never runs the
# pipeline or writes data.
#
#   FAIL  -> the run fails, the GitHub workflow fails, promotion stops
#   WARN  -> printed only (for example data not loaded yet in a new env)
#
# Parameters arrive as job parameters (see resources/smoke_test.yml).
# ════════════════════════════════════════════════════════════════════════

# COMMAND ----------

def param(name, default=""):
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name).strip()
    except Exception:
        return default

EXPECTED_ENV  = param("expected_environment")
ENVIRONMENT   = param("environment")
CATALOG       = param("catalog")
CTRL_SCHEMA   = param("control_schema")
PIPELINE_NAME = param("pipeline_id")
FRAMEWORK_ROOT = (param("framework_root")
                  or f"/Volumes/{CATALOG}/{CTRL_SCHEMA}/landing").rstrip("/")
PIPELINES     = {"bronze": param("bronze_pipeline_uuid"),
                 "silver": param("silver_pipeline_uuid")}
CONFIG_DIR    = f"{FRAMEWORK_ROOT}/configs"

CTRL = f"{CATALOG}.{CTRL_SCHEMA}"

# Tables the framework reads or writes without creating them first
REQUIRED_CONTROL_TABLES = [
    "pipeline_table_config", "sap_metadata_landing", "pipeline_definitions",
    "sources", "connections", "transformation_rules", "metric_definitions",
    "pipeline_config_versions", "pipeline_staging",
]

results = []   # (status, check, detail)

def ok(check, detail=""):   results.append(("PASS", check, detail))
def fail(check, detail):    results.append(("FAIL", check, detail))
def warn(check, detail):    results.append(("WARN", check, detail))

print(f"Smoke test for target '{EXPECTED_ENV}' | catalog={CATALOG} | pipeline_id={PIPELINE_NAME}")

# COMMAND ----------

# DBTITLE 1,1. Parameters point at the right environment
if not CATALOG or not CTRL_SCHEMA or not all(PIPELINES.values()):
    fail("parameters", "catalog, control_schema, bronze_pipeline_uuid and silver_pipeline_uuid must all be set")
else:
    ok("parameters present")

if ENVIRONMENT == EXPECTED_ENV:
    ok("environment matches target", ENVIRONMENT)
else:
    fail("environment matches target",
         f"environment parameter is '{ENVIRONMENT}' but this is the '{EXPECTED_ENV}' target")

# COMMAND ----------

# DBTITLE 1,2. Catalog, control schema and control tables
try:
    spark.sql(f"DESCRIBE SCHEMA `{CATALOG}`.`{CTRL_SCHEMA}`")
    ok("control schema exists", CTRL)
except Exception as e:
    fail("control schema exists", f"{CTRL}: {str(e)[:150]}")

missing = [t for t in REQUIRED_CONTROL_TABLES if not spark.catalog.tableExists(f"{CTRL}.{t}")]
if missing:
    fail("required control tables", f"missing in {CTRL}: {', '.join(missing)}")
else:
    ok("required control tables", f"{len(REQUIRED_CONTROL_TABLES)} found")

# COMMAND ----------

# DBTITLE 1,3. Volumes are reachable
def can_list(path):
    try:
        dbutils.fs.ls(path)
        return True, ""
    except Exception as e:
        return False, str(e)[:150]

good, err = can_list(FRAMEWORK_ROOT)
ok("framework root", FRAMEWORK_ROOT) if good else fail("framework root", f"{FRAMEWORK_ROOT}: {err}")

good, _ = can_list(f"{FRAMEWORK_ROOT}/sql_files")
ok("sql_files folder", f"{FRAMEWORK_ROOT}/sql_files") if good else warn(
    "sql_files folder", "not found - upload the gold SQL files")

good, _ = can_list(CONFIG_DIR)
ok("configs folder", CONFIG_DIR) if good else warn(
    "configs folder", f"{CONFIG_DIR} not found yet - run Config_file_generator")

# COMMAND ----------

# DBTITLE 1,4. Deployed bronze and silver pipelines are wired to this environment
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
expected_conf = {
    "pipeline_id":    PIPELINE_NAME,
    "environment":    ENVIRONMENT,
    "catalog":        CATALOG,
    "control_schema": CTRL_SCHEMA,
    "framework_root": FRAMEWORK_ROOT,
}
for label, uuid in PIPELINES.items():
    try:
        p = w.pipelines.get(pipeline_id=uuid)
        spec = p.spec
        conf = spec.configuration or {}

        if spec.catalog == CATALOG:
            ok(f"{label} pipeline catalog", spec.catalog)
        else:
            fail(f"{label} pipeline catalog", f"writes to '{spec.catalog}', expected '{CATALOG}'")

        wrong = {k: (conf.get(k, "").rstrip("/") if k == "framework_root" else conf.get(k), v)
                 for k, v in expected_conf.items()}
        wrong = {k: gv for k, gv in wrong.items() if gv[0] != gv[1]}
        if wrong:
            fail(f"{label} pipeline configuration",
                 "; ".join(f"{k}={got!r} (expected {exp!r})" for k, (got, exp) in wrong.items()))
        else:
            ok(f"{label} pipeline configuration", ", ".join(expected_conf))

        last = (p.latest_updates or [None])[0]
        if last is not None and str(getattr(last, "state", "")).endswith("FAILED"):
            warn(f"{label} last update", f"most recent update {last.update_id} FAILED")
        else:
            ok(f"{label} last update", str(getattr(last, "state", "no runs yet")))
    except Exception as e:
        fail(f"{label} pipeline lookup", f"{uuid}: {str(e)[:200]}")

# COMMAND ----------

# DBTITLE 1,5. Registered tables and gold SQL files (warnings only)
try:
    rows = spark.sql(f"""
        SELECT * FROM {CTRL}.pipeline_table_config
        WHERE pipeline_name = '{PIPELINE_NAME}'
    """).collect()
except Exception as e:
    rows = []
    warn("pipeline_table_config rows", str(e)[:150])

if not rows:
    warn("pipeline_table_config rows",
         f"no rows for pipeline_name='{PIPELINE_NAME}' - run Load_pipeline_table_config")
else:
    ok("pipeline_table_config rows", f"{len(rows)} registered")
    for r in rows:
        d = r.asDict()
        if d.get("is_active") is False:
            continue
        silver = f"{CATALOG}.{d.get('silver_schema')}.{d.get('silver_table')}"
        if d.get("silver_schema") and d.get("silver_table"):
            if spark.catalog.tableExists(silver):
                ok("silver table", silver)
            else:
                warn("silver table", f"{silver} not created yet - run the main job")
        sql_file = d.get("gold_sql_file")
        if sql_file:
            path = f"{FRAMEWORK_ROOT}/sql_files/{sql_file}"
            good, _ = can_list(path)
            ok("gold SQL file", path) if good else fail(
                "gold SQL file", f"{path} is registered but missing - the gold step would fail")

# COMMAND ----------

# DBTITLE 1,Result
width = max(len(c) for _, c, _ in results)
for status, check, detail in results:
    print(f"{status:<5} {check:<{width}}  {detail}")

failures = [r for r in results if r[0] == "FAIL"]
warnings = [r for r in results if r[0] == "WARN"]
summary = f"{len(results) - len(failures) - len(warnings)} passed, {len(warnings)} warnings, {len(failures)} failed"
print("\n" + summary)

if failures:
    raise Exception("Smoke test failed: " + "; ".join(f"{c}: {d}" for _, c, d in failures))

dbutils.notebook.exit(summary)
