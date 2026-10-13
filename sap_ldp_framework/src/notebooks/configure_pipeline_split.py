# Databricks notebook source
# DBTITLE 1,configure_pipeline
# ── Why this notebook exists ────────────────────────────────────────
#
# Lakeflow/DLT pipeline tasks in a Job do NOT receive the job's own
# `parameters:` block the way notebook tasks do via base_parameters.
# For a Python-source pipeline, the ONLY thing it can read at runtime
# is spark.conf.get(...), which is backed exclusively by the pipeline
# resource's own "Configuration" key-value settings (Pipeline Settings
# -> Advanced -> Configuration) — a property of the pipeline itself,
# not of any job that happens to trigger it.
#
# This notebook is the workaround: run it as a task BEFORE the bronze
# and silver pipeline tasks. It reads THIS job's parameters, then calls
# the Databricks SDK to update BOTH target pipelines' Configuration to
# match — so by the time either pipeline task starts, its Configuration
# reflects the values this specific job run was invoked with.
#
# UPDATED for the bronze/silver split: there are now TWO Lakeflow
# pipeline resources (bronze_pipeline_1.py, silver_pipeline_1.py)
# instead of one, so this notebook now takes two UUID widgets and
# loops over both, applying the identical config payload to each.
#
# With the bundle, both pipelines are already deployed with these values
# in their Configuration (resources/pipelines.yml). This task keeps a run
# with OVERRIDDEN job parameters (Run now with different parameters)
# consistent: the pipelines then pick up the overridden values too.
#
# Wiring: resources/jobs.yml (Configure_pipeline runs first).

# COMMAND ----------
# DBTITLE 1,Widgets
dbutils.widgets.text("bronze_pipeline_uuid", "",
    "Bronze Lakeflow pipeline resource id — NOT the app-level pipeline_id")
dbutils.widgets.text("silver_pipeline_uuid", "",
    "Silver Lakeflow pipeline resource id — NOT the app-level pipeline_id")
dbutils.widgets.text("pipeline_id", "", "App-level pipeline_id")
dbutils.widgets.text("environment", "", "Environment")
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("control_schema", "", "Control schema")
dbutils.widgets.text("framework_root", "", "Framework root (default /Volumes/<catalog>/<control_schema>/landing)")

bronze_pipeline_uuid = dbutils.widgets.get("bronze_pipeline_uuid").strip()
silver_pipeline_uuid = dbutils.widgets.get("silver_pipeline_uuid").strip()
pipeline_id           = dbutils.widgets.get("pipeline_id").strip()
environment           = dbutils.widgets.get("environment").strip()
catalog               = dbutils.widgets.get("catalog").strip()
control_schema        = dbutils.widgets.get("control_schema").strip()
framework_root        = dbutils.widgets.get("framework_root").strip()

for _name, _val in (("pipeline_id", pipeline_id), ("environment", environment),
                    ("catalog", catalog), ("control_schema", control_schema)):
    if not _val:
        raise ValueError(f"Widget '{_name}' is required (it is a job parameter in resources/jobs.yml).")
framework_root = (framework_root or f"/Volumes/{catalog}/{control_schema}/landing").rstrip("/")

if not bronze_pipeline_uuid:
    raise ValueError(
        "Widget 'bronze_pipeline_uuid' is required — the Databricks PIPELINE "
        "RESOURCE id of the bronze_pipeline_1.py Lakeflow pipeline."
    )
if not silver_pipeline_uuid:
    raise ValueError(
        "Widget 'silver_pipeline_uuid' is required — the Databricks PIPELINE "
        "RESOURCE id of the silver_pipeline_1.py Lakeflow pipeline."
    )

# The same five keys the pipelines read with spark.conf.get(...).
# Paths (configs/, logs/, schema/) are derived from framework_root inside
# the pipelines, so config_out_dir is no longer pushed.
new_config_values = {
    "pipeline_id":    pipeline_id,
    "environment":    environment,
    "catalog":        catalog,
    "control_schema": control_schema,
    "framework_root": framework_root,
}

targets = [
    ("bronze", bronze_pipeline_uuid),
    ("silver", silver_pipeline_uuid),
]

print("=" * 70)
print("CONFIGURE PIPELINE — push job parameters into pipeline Configuration")
print("=" * 70)
for label, uuid_ in targets:
    print(f"Target {label} pipeline (Databricks resource id): {uuid_}")
print(f"New configuration values to merge in: {new_config_values}")
print("=" * 70)

# COMMAND ----------
# DBTITLE 1,Connect
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------
# DBTITLE 1,Fetch current spec, merge configuration, update — for each pipeline
# The Pipelines update API is a full replace, not a partial patch —
# omitting a field (clusters, libraries, catalog, etc.) would wipe it
# out. So: fetch the current spec exactly as-is (as a plain dict via the
# raw api_client, deliberately NOT the typed w.pipelines.get()/update()
# convenience methods — see the original notebook's comment for why),
# mutate ONLY the "configuration" key, PUT the same dict back.

def _configure_one(label: str, dlt_pipeline_uuid: str):
    current = w.api_client.do("GET", f"/api/2.0/pipelines/{dlt_pipeline_uuid}")
    if not isinstance(current, dict) or "spec" not in current:
        raise RuntimeError(
            f"GET /api/2.0/pipelines/{dlt_pipeline_uuid} ({label}) returned an "
            f"unexpected shape — got keys: "
            f"{list(current.keys()) if isinstance(current, dict) else type(current)}"
        )
    spec_dict = current["spec"]

    existing_config = spec_dict.get("configuration") or {}
    merged_config   = {**existing_config, **new_config_values}
    spec_dict["configuration"] = merged_config
    spec_dict["pipeline_id"] = dlt_pipeline_uuid

    print(f"[{label}] Existing configuration : {existing_config}")
    print(f"[{label}] Merged configuration    : {merged_config}")

    w.api_client.do("PUT", f"/api/2.0/pipelines/{dlt_pipeline_uuid}", body=spec_dict)
    print(f"[{label}] ✓ Pipeline {dlt_pipeline_uuid} configuration updated.")

    verify = w.api_client.do("GET", f"/api/2.0/pipelines/{dlt_pipeline_uuid}")
    verify_config = (verify.get("spec") or {}).get("configuration") or {}
    mismatches = {
        k: (v, verify_config.get(k))
        for k, v in new_config_values.items()
        if verify_config.get(k) != v
    }
    if mismatches:
        raise RuntimeError(f"[{label}] Verification failed — expected vs actual: {mismatches}")
    print(f"[{label}] Verified — all target configuration values confirmed present.\n")


for label, uuid_ in targets:
    _configure_one(label, uuid_)

print("Safe to proceed to the bronze pipeline task.")
