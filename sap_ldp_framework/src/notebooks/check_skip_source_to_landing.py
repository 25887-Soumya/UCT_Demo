# Databricks notebook source
# DBTITLE 1,check_skip_source_to_landing
# ══════════════════════════════════════════════════════════════════
# Resolves skip_source_to_landing for this pipeline and publishes it as
# a task value, so a downstream condition_task in the job can branch on
# it (Databricks condition_task compares values known at the Jobs
# level — job parameters or dbutils.jobs.taskValues — not arbitrary
# content buried in a JSON file, so this task's only job is to surface
# that one boolean up to the Jobs layer).
#
# Queries pipeline_table_config DIRECTLY (not the generated config.json)
# so the decision is always current, even if a fresh config.json hasn't
# been regenerated since the flag was last changed via
# load_pipeline_table_config.sql.
#
# Semantics: true (skip the JDBC task) only when no active row for this
# pipeline_id has skip_source_to_landing = false. Rows that need a JDBC
# extract are exactly the ones jdbc_extract_to_landing.py processes.
# ══════════════════════════════════════════════════════════════════


# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("pipeline_id", "", "Pipeline id (matches pipeline_table_config.pipeline_name)")
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("control_schema", "", "Control schema")
pipeline_id    = dbutils.widgets.get("pipeline_id").strip()
catalog        = dbutils.widgets.get("catalog").strip()
control_schema = dbutils.widgets.get("control_schema").strip()

if not pipeline_id:
    raise ValueError("Widget 'pipeline_id' is required.")
if not catalog or not control_schema:
    raise ValueError("Widgets 'catalog' and 'control_schema' are required.")

PIPELINE_TABLE_CONFIG = f"{catalog}.{control_schema}.pipeline_table_config"

# COMMAND ----------

# DBTITLE 1,Resolve flag and publish as a task value
try:
    # Skip the JDBC task only when NO active row needs a JDBC extract.
    # A row needs one when skip_source_to_landing is explicitly false -
    # the same rule jdbc_extract_to_landing.py uses to pick its tables,
    # so a pipeline that mixes both kinds of rows still gets extracted.
    row = spark.sql(f"""
        SELECT count_if(skip_source_to_landing = false) AS needs_extract
        FROM {PIPELINE_TABLE_CONFIG}
        WHERE pipeline_name = '{pipeline_id.replace(chr(39), chr(39) * 2)}'
          AND is_active = true
    """).collect()
    needs_extract = int(row[0]["needs_extract"] or 0) if row else 0
    skip_source_to_landing = needs_extract == 0
except Exception as exc:
    print(f"WARNING: could not resolve skip_source_to_landing for pipeline_id="
          f"'{pipeline_id}' (defaulting to False): {exc}")
    needs_extract = -1
    skip_source_to_landing = False

print(f"pipeline_id='{pipeline_id}' -> {needs_extract} table(s) need a JDBC extract "
      f"-> skip_source_to_landing={skip_source_to_landing}")

dbutils.jobs.taskValues.set(key="skip_source_to_landing", value=str(skip_source_to_landing).lower())