# Databricks notebook source
# DBTITLE 1,configure_pipeline
# ── Why this notebook exists ────────────────────────────────────────
#
# Lakeflow/DLT pipeline tasks in a Job do NOT receive the job's own
# `parameters:` block the way notebook tasks do via base_parameters.
# For a Python-source pipeline (like lakeflow_pipeline.py), the ONLY
# thing it can read at runtime is spark.conf.get(...), which is backed
# exclusively by the pipeline resource's own "Configuration" key-value
# settings (Pipeline Settings -> Advanced -> Configuration) — a property
# of the pipeline itself, not of any job that happens to trigger it.
# (The Beta "pipeline task parameters" override field is SQL-source-only
# per Databricks' docs, so it doesn't help here even when enabled.)
#
# This notebook is the workaround: run it as a task BEFORE the pipeline
# task. It reads THIS job's parameters (which DO push into notebook
# tasks normally), then calls the Databricks SDK to update the target
# pipeline's Configuration to match — so by the time the pipeline task
# starts, its Configuration reflects the values this specific job run
# was actually invoked with, not just whatever static defaults are
# sitting in the pipeline's Settings page.
#
# Wire this into the job as:
#   Configure_pipeline  ->  Source_to_silver  ->  Silver_to_gold
#
# See the matching job YAML change at the bottom of this file (as a
# comment) for exactly how to wire the dependency and parameter
# pushdown.

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("dlt_pipeline_uuid", "",
    "Databricks pipeline resource id (e.g. c4e23f05-5690-4102-aa60-acde9036d2ad) — NOT the app-level pipeline_id")
dbutils.widgets.text("pipeline_id", "sap_demo_uct", "App-level pipeline_id")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("config_out_dir",
    "/Volumes/rgaplxdatabricks/temp_source/source_files/configs", "Config output dir")

dlt_pipeline_uuid = dbutils.widgets.get("dlt_pipeline_uuid").strip()
pipeline_id       = dbutils.widgets.get("pipeline_id").strip()
environment       = dbutils.widgets.get("environment").strip()
config_out_dir    = dbutils.widgets.get("config_out_dir").strip()

if not dlt_pipeline_uuid:
    raise ValueError(
        "Widget 'dlt_pipeline_uuid' is required — this must be the "
        "Databricks PIPELINE RESOURCE id (the value currently hardcoded "
        "on pipeline_task.pipeline_id in your job YAML), not the "
        "app-level pipeline_id string used inside lakeflow_pipeline.py."
    )

new_config_values = {
    "pipeline_id":    pipeline_id,
    "environment":    environment,
    "config_out_dir": config_out_dir,
}

print("=" * 70)
print("CONFIGURE PIPELINE — push job parameters into pipeline Configuration")
print("=" * 70)
print(f"Target pipeline (Databricks resource id): {dlt_pipeline_uuid}")
print(f"New configuration values to merge in     : {new_config_values}")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Connect
# WorkspaceClient() auto-authenticates using this notebook's own execution
# context when run inside a Databricks job/notebook — no token, host, or
# secret scope setup required.
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Fetch current spec, merge configuration, update
# The Pipelines update API is a full replace, not a partial patch —
# omitting a field (clusters, libraries, catalog, etc.) would wipe it
# out. So: fetch the current spec exactly as-is (as a plain dict via the
# raw api_client, deliberately NOT the typed w.pipelines.get()/update()
# convenience methods — those round-trip inconsistently across SDK
# versions: get() may hand back a dict OR a dataclass depending on
# version, and even when it's a dataclass, .as_dict() flattens NESTED
# fields like clusters/libraries into plain dicts that update()'s typed
# kwargs then can't re-serialize, raising this same AttributeError one
# level down. The raw api_client sidesteps all of that: GET returns a
# plain dict, mutate ONLY the "configuration" key inside it, PUT the
# same dict back — every other setting on the pipeline stays exactly
# as it was, and nothing here depends on SDK dataclass internals.

current = w.api_client.do("GET", f"/api/2.0/pipelines/{dlt_pipeline_uuid}")
if not isinstance(current, dict) or "spec" not in current:
    raise RuntimeError(
        f"GET /api/2.0/pipelines/{dlt_pipeline_uuid} returned an unexpected "
        f"shape — got keys: {list(current.keys()) if isinstance(current, dict) else type(current)}"
    )
spec_dict = current["spec"]

existing_config = spec_dict.get("configuration") or {}
merged_config   = {**existing_config, **new_config_values}
spec_dict["configuration"] = merged_config

# Some Databricks update endpoints validate/require the id in the body
# too, even though it's already in the URL — include it explicitly
# rather than relying on it having been present in the GET response's
# nested "spec" object (it's typically only at the top level there).
spec_dict["pipeline_id"] = dlt_pipeline_uuid

print(f"Existing configuration on pipeline : {existing_config}")
print(f"Merged configuration to apply      : {merged_config}")

w.api_client.do("PUT", f"/api/2.0/pipelines/{dlt_pipeline_uuid}", body=spec_dict)

print(f"\n✓ Pipeline {dlt_pipeline_uuid} configuration updated.")

# COMMAND ----------

# DBTITLE 1,Verify
verify = w.api_client.do("GET", f"/api/2.0/pipelines/{dlt_pipeline_uuid}")
verify_config = (verify.get("spec") or {}).get("configuration") or {}

print(f"Verified configuration on pipeline: {verify_config}")

_mismatches = {
    k: (v, verify_config.get(k))
    for k, v in new_config_values.items()
    if verify_config.get(k) != v
}
if _mismatches:
    raise RuntimeError(
        f"Verification failed — expected vs actual: {_mismatches}"
    )

print("All target configuration values confirmed present on the pipeline.")
print("Safe to proceed to the pipeline task.")