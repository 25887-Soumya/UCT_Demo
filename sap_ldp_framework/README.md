# sap_ldp_framework

## Getting Started

To deploy and manage this bundle, follow these steps:

### 1. Deployment

- Click the **deployment rocket** 🚀 in the left sidebar to open the **Deployments** panel, then click **Deploy**.

### 2. Running Jobs & Pipelines

- To run a deployed job or pipeline, hover over the resource in the **Deployments** panel and click the **Run** button.

### 3. Managing Resources

- Use the **Add** dropdown to add resources to the bundle.
- Click **Schedule** on a notebook within the bundle to create a **job definition** that schedules the notebook.

## Bundle Variables — Quick Reference

All variables are defined in `databricks.yml` and can be overridden per
target under `targets.<target>.variables`.

### Identity variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `catalog` | `rgaplxdatabricks` | UC catalog for all output and control tables |
| `schema` | `sap_demo_bronze` | Default schema for the Lakeflow pipeline (bronze target) |
| `control_schema` | `uct_demo` | Schema holding `pipeline_table_config`, `sap_metadata_landing`, `pipeline_alerts` |
| `pipeline_id` | `uct_test_bundle` | Pipeline name registered in `pipeline_table_config` |
| `environment` | `dev` | `dev` \| `qa` \| `prod` — read by every notebook |

### Path variables — three separate UC Volumes

The project reads/writes across **three distinct Unity Catalog Volumes**.
Each variable controls a different volume.

| # | Variable | Default path | UC Volume | What lives there | Notebooks that use it |
| --- | --- | --- | --- | --- | --- |
| 1 | `landing_base_path` | `/Volumes/ingestion_test/landing/landing_volume/` | `ingestion_test.landing.landing_volume` | Raw SAP Datasphere partfiles + `.sap.partfile.metadata` | `metadataloader_updated` only |
| 2 | `base_volume` | `/Volumes/{catalog}/temp_source/source_files` | `{catalog}.temp_source.source_files` | Auto Loader schema checkpoints (`schema/`), run logs (`logs/`) | `sap_lakeflow_pipeline`, `gold_pipeline`, `configure_pipeline` |
| 3 | `config_out_dir` | `/Volumes/{catalog}/{control_schema}/landing/configs` | `{catalog}.{control_schema}.landing` | Pipeline config JSON files | `metadataloader_updated` (writes), `sap_lakeflow_pipeline` + `gold_pipeline` (read), `configure_pipeline` (passes to pipeline) |

> **Note:** `config_out_dir` lives on the *control* volume (Volume 3), not
> on `base_volume` (Volume 2). They are separate volumes on different
> schemas.

### How variables flow at runtime

```
databricks.yml variables
    │
    ├── pipeline.yml configuration  ──────→  sap_lakeflow_pipeline
    │     (ctrl_schema, base_volume,          (reads via spark.conf.get)
    │      config_out_dir)
    │
    ├── jobs.yml parameters  ─────────────→  configure_pipeline, gold_pipeline
    │     (catalog, control_schema,           (read via dbutils.widgets)
    │      base_volume, config_out_dir)
    │
    └── control_jobs.yml parameters  ─────→  metadataloader_updated,
          (catalog, control_schema,              load_pipeline_table_config
           landing_base_path)                   (read via dbutils.widgets)
```

### Switching to production

Uncomment the `prod` target in `databricks.yml` and override the variables
that differ:

```yaml
targets:
  prod:
    mode: production
    variables:
      environment: prod
      catalog: <prod_catalog>
      landing_base_path: /Volumes/<prod_catalog>/landing/landing_volume/
      # base_volume and config_out_dir auto-derive from catalog +
      # control_schema — override only if the prod layout differs.
```

## Documentation

- For information on using **Declarative Automation Bundles in the workspace**, see: [Declarative Automation Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- For details on the **Declarative Automation Bundles format** used in this bundle, see: [Declarative Automation Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
