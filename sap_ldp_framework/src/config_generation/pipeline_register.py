# Databricks notebook source
"""
PIPELINE REGISTRY — merged version
====================================
"""

import json
import logging
import os
import re
import uuid
from datetime import datetime, timedelta
from typing import Optional, Union

logger = logging.getLogger("pipeline_registry")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)

CTRL = "rgaplxdatabricks.uct_demo"

# ── The menu of allowed "how does this table refresh" options ───────────────
# Keeping this as a fixed list (instead of letting people type anything) means
# a typo gets caught immediately when someone registers a pipeline, instead of
# quietly breaking a run at 3am.
#
#   full        -> wipe the table and reload everything, every run.
#   scd1        -> keep only the latest version of each record (upsert).
#   scd2        -> keep full history of every change to each record.
#   delta       -> only bring in new rows since last run (no updates/dedup).
#   delta_merge -> only bring in new/changed rows since last run, and
#                  update existing rows in place (upsert) rather than
#                  reloading the whole table. Mainly used for summary
#                  ("gold") tables that are too big to fully recompute
#                  every run.
VALID_SOURCE_LOAD_TYPES = {"full", "scd1", "scd2", "delta", "delta_merge"}
# "delta" added: the metadata loader registers silver with type="delta" for
# Load_type=delta (plain incremental append, no keys/merge). Previously this
# raised RegistrationError for every delta row.
VALID_SILVER_LOAD_TYPES = {"full", "scd1", "scd2", "delta"}

# Silver type -> dlt_mode written to pipeline_definitions / the config file.
# Single source of truth — used when writing definitions, resolving the
# silver source block, and counting layers.
# NOTE: confirm these names against what lakeflow_pipeline.py dispatches on.
SILVER_DLT_MODE_MAP = {"full": "full", "scd1": "snapshot", "scd2": "history", "delta": "delta"}
SILVER_DLT_MODES    = set(SILVER_DLT_MODE_MAP.values())

# Gold is a 1:1 copy of silver with light transforms (casts, renames), not an
# aggregate. Default for SAP gold is "materialized_view".
VALID_GOLD_MATERIALIZATIONS = {"table", "view", "materialized_view"}

# Load types whose silver/bronze actually need keys (merge/dedup on keys).
KEYED_LOAD_TYPES = {"scd1", "scd2", "delta_merge"}

# pipeline_id is embedded in table names ({pipeline_id}_{entity}_bronze),
# so it must be a valid unquoted identifier fragment. It now comes from a
# free-text widget in the config table, so validate it here.
PIPELINE_ID_RE = re.compile(r"^[A-Za-z0-9_]+$")
VALID_GOLD_LOAD_TYPES   = {"full", "append", "delta", "delta_merge"}

# How we recognize a config filename and pull the "generated at" timestamp
# out of it, e.g. "sales_prod_20260828_143000_config.json"
CONFIG_TS_RE = re.compile(r"_(\d{8}_\d{6})_config\.json$")


class RegistrationError(Exception):
    pass


class PipelineRegistry:
    """
    One PipelineRegistry = one pipeline's full setup: what data it reads
    (sources), how it cleans/dedups that data (silver), what business-ready
    tables it produces (gold), and what metrics get calculated from those
    tables. Call register() once everything is described, and it writes all
    of that into the control tables plus a JSON "instruction sheet" (the
    config file) that the actual data pipeline reads at run time.
    """

    ALERT_DL          = "data-engineering-alerts@company.com"
    SMTP_HOST         = "smtp.office365.com"
    SMTP_PORT         = 587
    SMTP_FROM         = "databricks-alerts@company.com"
    SMTP_SECRET_SCOPE = "email-alerts"
    SMTP_SECRET_KEY   = "smtp-password"

    def __init__(self, spark, dbutils,
                 pipeline_id:    str,
                 pipeline_name:  str,
                 environment:    str,
                 target_catalog: str,
                 bronze_schema:  str,
                 silver_schema:  str,
                 gold_schema:    str,
                 config_dir:     str,
                 alert_email:    str = "",
                 notes:          str = ""):

        if not pipeline_id.strip():
            raise RegistrationError("pipeline_id is required")
        if not PIPELINE_ID_RE.match(pipeline_id.strip()):
            raise RegistrationError(
                f"pipeline_id '{pipeline_id}' may only contain letters, digits "
                f"and underscores — it is used in table names."
            )
        if not pipeline_name.strip():
            raise RegistrationError("pipeline_name is required")
        if environment.strip() not in ("dev", "qa", "prod"):
            raise RegistrationError(
                f"environment must be dev|qa|prod — got '{environment}'"
            )

        self.spark          = spark
        self.dbutils        = dbutils
        self.pipeline_id    = pipeline_id.strip()
        self.pipeline_name  = pipeline_name.strip()
        self.environment    = environment.strip()
        self.target_catalog = target_catalog.strip()
        self.bronze_schema  = bronze_schema.strip()
        self.silver_schema  = silver_schema.strip()
        self.gold_schema    = gold_schema.strip()
        self.config_dir     = config_dir.strip()
        self.alert_email    = alert_email.strip() or self.ALERT_DL
        self.notes          = notes.strip()

        self._sources          = {}
        self._silver           = {}
        self._gold              = {}
        self._metrics          = {}
        self._silver_layer_map = {}

        logger.info(f"PipelineRegistry: {self.pipeline_id} [{self.environment}]")

    # ── Naming helpers — just building consistent table/id names ────────────

    def _src_id(self, entity: str) -> str:
        return f"src_{self.pipeline_id}_{entity}"

    def _layer(self, entity: str, suffix: str) -> str:
        return f"{self.pipeline_id}_{entity}_{suffix}"

    def _full(self, schema: str, tbl: str) -> str:
        return f"{self.target_catalog}.{schema}.{tbl}"

    def _rule_id(self, layer_nm: str, order: int) -> str:
        return f"{self.pipeline_id}_{layer_nm}_{order}"[:50]

    # ── Small utilities ───────────────────────────────────────────────────

    @staticmethod
    def _schema_json(schema: dict) -> str:
        """Turn a simple {column: type} dict into the structured format the
        pipeline runtime expects. A trailing "!" on a column name means
        "this column cannot be empty."""
        fields = []
        for col, typ in schema.items():
            nullable  = not col.endswith("!")
            clean_col = col.rstrip("!")
            resolved = typ.lower()
            fields.append({
                "name":     clean_col,
                "type":     resolved,
                "nullable": nullable
            })
        return json.dumps({"fields": fields})

    @staticmethod
    def _clean_json(clean: dict) -> str:
        return json.dumps([
            {"target_column": c, "transform_rule": e}
            for c, e in clean.items()
        ])

    @staticmethod
    def _q(val) -> str:
        """Wraps a plain value in quotes so it's safe to drop into a SQL
        statement. Only ever use this for simple values (names, ids, flags)
        — never for a whole block of SQL someone wrote, since doubling
        quotes would mangle it (see _write_rules below for why gold SQL is
        NEVER passed through this)."""
        if val is None:
            return "null"
        return f"'{str(val).replace(chr(39), chr(39)*2)}'"

    @staticmethod
    def _normalize_delete_condition(dc: Optional[str]) -> Optional[str]:
        """
        Turns a simple `column = value` soft-delete condition into valid SQL
        with the value in single quotes, whatever quoting it arrived with:

            __operation_type=D        -> __operation_type = 'D'
            __operation_type = 'D'    -> __operation_type = 'D'
            __operation_type = "D"    -> __operation_type = 'D'
            __operation_type = 'D     -> __operation_type = 'D'   (stray quote)

        Same behaviour as the original split-on-"=" version, plus two guards:
        conditions with other operators (!=, <>, <=, >=, ==) or with
        AND/OR/IN/LIKE/NOT/IS/BETWEEN are left exactly as written, since
        splitting those on "=" would corrupt them. Idempotent, so running it
        on an already-normalized condition changes nothing.
        """
        if not dc or not dc.strip():
            return dc
        dc = dc.strip()
        if re.search(r"!=|<>|<=|>=|==", dc):
            return dc
        if re.search(r"\b(AND|OR|IN|LIKE|NOT|IS|BETWEEN)\b", dc, flags=re.IGNORECASE):
            return dc
        parts = dc.split("=")
        if len(parts) != 2:
            return dc
        col = parts[0].strip()
        val = parts[1].strip().strip("'").strip('"').strip()
        if not col or not val:
            return dc
        return f"{col} = '{val.replace(chr(39), chr(39) * 2)}'"

    def _get_current_user(self) -> str:
        try:
            return (
                self.dbutils.notebook.entry_point
                .getDbutils().notebook().getContext()
                .userName().get()
            )
        except Exception:
            return "unknown"

    def _write_json(self, path: str, data: dict, _pre_serialized: Optional[str] = None):
        content = _pre_serialized if _pre_serialized is not None else json.dumps(data, indent=2)
        if path.startswith("/Volumes/"):
            self.dbutils.fs.put(path, content, overwrite=True)
        else:
            dbfs_path = f"/dbfs{path}" if not path.startswith("/dbfs") else path
            try:
                os.makedirs(os.path.dirname(dbfs_path), exist_ok=True)
                with open(dbfs_path, "w") as f:
                    f.write(content)
            except Exception as exc:
                raise RegistrationError(
                    f"Cannot write config to '{path}'. Use /Volumes/ path. Error: {exc}"
                )
        logger.info(f"  Config written : {path}")

    def _ensure_dir(self, path: str):
        """Make sure a folder exists before we try to write into it. Always
        goes through self.dbutils so this works the same way no matter where
        the code is actually running (notebook, job, or a test)."""
        try:
            self.dbutils.fs.mkdirs(path)
        except Exception as exc:
            raise RegistrationError(f"Cannot create folder '{path}'. Error: {exc}")

    # ── Alert system — emails + logs a permanent record any time a
    #    registration step fails, so nothing fails silently ─────────────────

    def _send_alert(self, subject: str, body: str):
        full_subject = (
            f"[{self.environment.upper()}] CRITICAL — "
            f"PipelineRegistry: {self.pipeline_id} — {subject}"
        )
        full_body = (
            f"PIPELINE REGISTRATION FAILURE\n{'='*50}\n"
            f"Pipeline    : {self.pipeline_id}\n"
            f"Name        : {self.pipeline_name}\n"
            f"Environment : {self.environment}\n"
            f"Alert DL    : {self.alert_email}\n"
            f"Time        : {datetime.now().isoformat()}\n"
            f"{'='*50}\n\nFAILED STEP : {subject}\n\n"
            f"ERROR:\n{body}\n\n{'='*50}\n"
            f"Fix the error and call register(overwrite=True) to retry.\n"
        )
        try:
            import smtplib
            from email.mime.text      import MIMEText
            from email.mime.multipart import MIMEMultipart
            try:
                smtp_password = self.dbutils.secrets.get(
                    scope=self.SMTP_SECRET_SCOPE, key=self.SMTP_SECRET_KEY)
            except Exception:
                smtp_password = None
            msg = MIMEMultipart()
            msg["From"]    = self.SMTP_FROM
            msg["To"]      = self.alert_email
            msg["Subject"] = full_subject
            msg.attach(MIMEText(full_body, "plain"))
            with smtplib.SMTP(self.SMTP_HOST, self.SMTP_PORT) as server:
                server.ehlo()
                server.starttls()
                if smtp_password:
                    server.login(self.SMTP_FROM, smtp_password)
                server.sendmail(self.SMTP_FROM, [self.alert_email], msg.as_string())
            logger.info(f"  Alert sent → {self.alert_email}")
        except Exception as smtp_err:
            logger.warning(f"  SMTP failed: {smtp_err}")
        try:
            self._write_alert_to_delta(full_subject, full_body)
        except Exception as e:
            logger.warning(f"  Delta alert log failed: {e}")
        logger.error("=" * 60)
        logger.error(f"CRITICAL: {full_subject}")
        logger.error(full_body)
        logger.error("=" * 60)

    def _write_alert_to_delta(self, subject: str, body: str):
        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {CTRL}.pipeline_alerts (
                    alert_id    STRING,
                    pipeline_id STRING,
                    environment STRING,
                    alert_to    STRING,
                    subject     STRING,
                    body        STRING,
                    alert_time  TIMESTAMP,
                    created_at  TIMESTAMP
                ) USING DELTA
            """)
            alert_id = (
                f"{self.pipeline_id}_{self.environment}_"
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            q = self._q
            self.spark.sql(f"""
                INSERT INTO {CTRL}.pipeline_alerts
                (alert_id, pipeline_id, environment, alert_to,
                 subject, body, alert_time, created_at)
                VALUES (
                    {q(alert_id)}, {q(self.pipeline_id)},
                    {q(self.environment)}, {q(self.alert_email)},
                    {q(subject[:500])}, {q(body[:2000])},
                    current_timestamp(), current_timestamp()
                )
            """)
        except Exception as e:
            logger.warning(f"  pipeline_alerts write failed: {e}")

    def _critical_alert(self, step: str, error: str):
        self._send_alert(step, error)

    # ── Public API — this is what someone calls to describe their pipeline ──

    def add_source(self, entity: str, path: str, keys: str,
                   watermark: str, format: str = "parquet",
                   source_type: str = "file",
                   schema: Optional[dict] = None,
                   clean: Optional[dict] = None,
                   expect: Optional[list] = None,
                   connection_id: Optional[str] = None,
                   load_type: str = "full",
                   operation_col: Optional[str] = None,
                   sequence_col: Optional[str] = None,
                   soft_delete_condition: Optional[str] = None):
        """Register one raw data source (a file location or a database
        table) that will land in the "bronze" layer."""
        entity = entity.strip()
        if entity in self._sources:
            raise RegistrationError(f"Source '{entity}' already added.")
        load_type = (load_type or "full").strip().lower()
        if load_type not in VALID_SOURCE_LOAD_TYPES:
            raise RegistrationError(
                f"Source '{entity}': load_type must be one of "
                f"{sorted(VALID_SOURCE_LOAD_TYPES)} — got '{load_type}'")
        self._sources[entity] = dict(
            entity=entity, path=path.strip(), keys=keys.strip(),
            watermark=watermark.strip(), format=format.strip(),
            source_type=source_type.strip(), schema=schema,
            clean=clean, expect=expect, connection_id=connection_id,
            load_type=load_type,
            operation_col=(operation_col or None),
            sequence_col=(sequence_col or None),
            soft_delete_condition=(soft_delete_condition or None),
        )
        logger.info(f"  + source  : {entity} ({source_type}, load_type={load_type})")

    def add_silver(self, name: str, type: str,
                   from_entity: Union[str, list],
                   keys: str, watermark: str,
                   soft_delete: Optional[str] = None,
                   join_type: str = "inner",
                   join_on: Optional[str] = None):
        """Register a cleaned-up, deduplicated table (the "silver" layer)
        built from one or more sources."""
        name = name.strip()
        if name in self._silver:
            raise RegistrationError(f"Silver '{name}' already added.")
        type = (type or "").strip().lower()
        if type not in VALID_SILVER_LOAD_TYPES:
            raise RegistrationError(
                f"Silver '{name}': type must be one of "
                f"{sorted(VALID_SILVER_LOAD_TYPES)} — got '{type}'")
        entities = (
            [from_entity.strip()] if isinstance(from_entity, str)
            else [e.strip() for e in from_entity]
        )
        is_multi = len(entities) > 1
        if is_multi and not join_on:
            raise RegistrationError(
                f"Silver '{name}': join_on required for multiple entities")
        self._silver[name] = dict(
            name=name, type=type, entities=entities,
            keys=keys.strip(), watermark=watermark.strip(),
            soft_delete=soft_delete, is_multi=is_multi,
            join_type=join_type, join_on=join_on,
        )
        logger.info(f"  + silver  : {name} ({type}) ← {entities}")

    def add_gold(self, name: str, sql: str,
                 from_silver: Optional[str] = None,
                 from_bronze: Optional[Union[str, list]] = None,
                 join_sources: Optional[list] = None,
                 load_type: str = "full",
                 merge_keys: Optional[str] = None,
                 watermark: Optional[str] = None,
                 partition_cols: Optional[str] = None,
                 materialization: str = "table"):
        """
        Register a business-ready summary table (the "gold" layer).

        load_type:
          full        -> wipe and reload every run (default).
          append      -> just add new rows on top, no dedup.
          delta       -> only bring in rows newer than `watermark`.
          delta_merge -> only bring in newer rows AND update existing ones
                         in place, using `merge_keys` to match rows.
        """
        name = name.strip()
        if name in self._gold:
            raise RegistrationError(f"Gold '{name}' already added.")
        if not from_silver and not from_bronze:
            raise RegistrationError(f"Gold '{name}': specify from_silver or from_bronze")
        if from_silver and from_bronze:
            raise RegistrationError(f"Gold '{name}': from_silver OR from_bronze, not both")
        load_type = (load_type or "full").strip().lower()
        if load_type not in VALID_GOLD_LOAD_TYPES:
            raise RegistrationError(
                f"Gold '{name}': load_type must be one of "
                f"{sorted(VALID_GOLD_LOAD_TYPES)} — got '{load_type}'")
        if load_type in ("delta", "delta_merge") and not watermark:
            raise RegistrationError(
                f"Gold '{name}': load_type='{load_type}' requires watermark")
        if load_type == "delta_merge" and not merge_keys:
            raise RegistrationError(
                f"Gold '{name}': load_type='delta_merge' requires merge_keys")
        materialization = (materialization or "table").strip().lower()
        if materialization not in VALID_GOLD_MATERIALIZATIONS:
            raise RegistrationError(
                f"Gold '{name}': materialization must be one of "
                f"{sorted(VALID_GOLD_MATERIALIZATIONS)} — got '{materialization}'")
        if materialization in ("view", "materialized_view") and load_type != "full":
            raise RegistrationError(
                f"Gold '{name}': a {materialization} is refreshed from its query, "
                f"not merged/appended — "
                f"load_type must be 'full', got '{load_type}'")
        if join_sources:
            for item in join_sources:
                if not all(k in item for k in ("entity", "type", "alias")):
                    raise RegistrationError(
                        f"Gold '{name}': join_sources needs entity, type, alias")
                if item["type"] not in ("silver", "bronze"):
                    raise RegistrationError(
                        f"Gold '{name}': join type must be silver|bronze")
        self._gold[name] = dict(
            name=name, sql=sql.strip(),
            from_silver=from_silver,
            from_bronze=(from_bronze if isinstance(from_bronze, str) else None),
            join_sources=join_sources or [],
            load_type=load_type,
            merge_keys=(merge_keys.strip() if merge_keys else None),
            watermark=(watermark.strip() if watermark else None),
            partition_cols=partition_cols,
            materialization=materialization,
        )
        logger.info(f"  + gold    : {name} ← {from_silver or from_bronze} "
                    f"(load_type={load_type}, {materialization})")

    def add_metric(self, metric_name: str, source_gold: str,
                   expression: str, dimensions: list = None,
                   metric_filter: str = None, description: str = "",
                   metric_group: str = "default",
                   aggregation_type: str = "sum",
                   output_view: str = None,
                   refresh_type: str = "view"):
        """Register a business metric (e.g. "total revenue") calculated from
        a gold table, and the view that will expose it."""
        metric_name = metric_name.strip()
        if metric_name in self._metrics:
            raise RegistrationError(f"Metric '{metric_name}' already added.")
        if source_gold not in self._gold:
            raise RegistrationError(
                f"Metric '{metric_name}': source_gold '{source_gold}' "
                f"not found. Available: {sorted(self._gold)}")
        self._metrics[metric_name] = {
            "metric_name":      metric_name,
            "source_gold":      source_gold,
            "expression":       expression.strip(),
            "dimensions":       dimensions or [],
            "metric_filter":    metric_filter,
            "description":      description,
            "metric_group":     metric_group,
            "aggregation_type": aggregation_type,
            "output_view":      output_view or f"metric_{metric_name}",
            "refresh_type":     refresh_type,
        }
        logger.info(f"  + metric  : {metric_name}")

    # ── register() — the main "go" button ───────────────────────────────────
    # Runs every step in order. If a step fails, we send an alert email/log
    # entry naming exactly which step broke, then stop.

    def register(self, overwrite: bool = False) -> str:
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        vid = f"{self.pipeline_id}_{self.environment}_{ts}"

        logger.info("=" * 60)
        logger.info(f"Registering : {self.pipeline_id} [{self.environment}]")
        logger.info("=" * 60)

        try:
            self._validate()
        except RegistrationError as exc:
            self._critical_alert("Step 1: Validation", str(exc)); raise

        try:
            self._check_existing(overwrite)
        except RegistrationError as exc:
            self._critical_alert("Step 2: Existing row check", str(exc)); raise

        next_version = 1
        if overwrite:
            try:
                next_version = self._rotate_versions()
            except Exception as exc:
                self._critical_alert("Step 3: Version rotation", str(exc)); raise
        else:
            self._clean_metadata_rows()

        try:
            self._create_schemas()
        except Exception as exc:
            self._critical_alert("Step 4: Schema creation", str(exc)); raise

        try:
            self._write_sources()
            self._write_definitions()
            self._write_rules()
            self._write_metrics()
        except Exception as exc:
            self._critical_alert("Steps 5-7: Metadata write", str(exc))
            logger.error("Partial write may have occurred. Fix and retry.")
            raise

        if overwrite:
            try:
                self._clean_schema_locations()
            except Exception as exc:
                logger.warning(f"Schema location cleanup failed (non-blocking): {exc}")

        try:
            config, config_path = self._generate_config(ts, vid, next_version)
        except Exception as exc:
            self._critical_alert("Step 8: Config generation", str(exc)); raise

        try:
            self._write_audit_log(vid, config, config_path, next_version)
        except Exception as exc:
            logger.warning(f"Audit log write failed (non-blocking): {exc}")

        self._print_summary(config, config_path, vid, next_version)
        return config_path

    # ── Validation — catch mistakes before anything gets written anywhere ──

    def _validate(self):
        logger.info("Step 1/9: Validating...")
        errors = []

        def e(layer, name, msg):
            errors.append(f"  [{layer.upper():6s}] {name}: {msg}")

        if not self._sources:
            errors.append("  [PIPELINE] At least one add_source() required")

        for entity, s in self._sources.items():
            if not s["path"]:      e("bronze", entity, "path required")
            if s["load_type"] in KEYED_LOAD_TYPES and not s["keys"]:
                e("bronze", entity,
                  f"keys required for load_type={s['load_type']} (no primary key in "
                  f"SAP metadata and no merge_keys in the config table)")
            if not s["watermark"]: e("bronze", entity, "watermark required")
            if s["source_type"] not in ("file", "db"):
                e("bronze", entity, f"source_type must be file|db")
            if s["source_type"] == "file" and s["format"] not in (
                    "parquet", "csv", "json", "delta"):
                e("bronze", entity, "format must be parquet|csv|json|delta")
            if s["source_type"] == "db" and not s["connection_id"]:
                e("bronze", entity, "connection_id required for db source")

            if s["expect"]:
                for rule in s["expect"]:
                    if "name" not in rule or "constraint" not in rule:
                        e("bronze", entity, "expect needs name and constraint")
                    if rule.get("action", "drop") not in ("drop", "warn", "fail"):
                        e("bronze", entity, "expect action must be drop|warn|fail")

        for name, sl in self._silver.items():
            for entity in sl["entities"]:
                if entity not in self._sources:
                    e("silver", name, f"from_entity '{entity}' not in add_source")
            if sl["type"] in ("scd1", "scd2") and not sl["keys"]:
                e("silver", name, f"keys required for silver type={sl['type']}")
            if not sl["watermark"]: e("silver", name, "watermark required")
            if sl["is_multi"] and not sl["join_on"]:
                e("silver", name, "join_on required for multiple entities")

        for name, gl in self._gold.items():
            if not gl["sql"]: e("gold", name, "sql required")
            if gl["from_silver"] and gl["from_silver"] not in self._silver:
                e("gold", name, f"from_silver '{gl['from_silver']}' not in add_silver")
            if gl["from_bronze"] and gl["from_bronze"] not in self._sources:
                e("gold", name, f"from_bronze '{gl['from_bronze']}' not in add_source")
            for js in gl["join_sources"]:
                if js["type"] == "silver" and js["entity"] not in self._silver:
                    e("gold", name, f"join entity '{js['entity']}' not in add_silver")
                if js["type"] == "bronze" and js["entity"] not in self._sources:
                    e("gold", name, f"join entity '{js['entity']}' not in add_source")

        for mname, m in self._metrics.items():
            if not m["expression"]:
                e("metric", mname, "expression required")
            if m["source_gold"] not in self._gold:
                e("metric", mname, f"source_gold '{m['source_gold']}' not in add_gold")

        if errors:
            msg = (
                f"\nVALIDATION FAILED — {len(errors)} error(s):\n"
                + "\n".join(errors)
                + "\n\nFix errors and call register() again."
            )
            raise RegistrationError(msg)

        logger.info(
            f"  ✓ {len(self._sources)} source(s), {len(self._silver)} silver, "
            f"{len(self._gold)} gold, {len(self._metrics)} metric(s)"
        )

    # ── Check existing ──────────────────────────────────────────────────────

    def _check_existing(self, overwrite: bool):
        logger.info("Step 2/9: Checking existing rows...")
        try:
            count = self.spark.sql(f"""
                SELECT COUNT(*) AS c FROM {CTRL}.pipeline_definitions
                WHERE pipeline_id = '{self.pipeline_id}'
                AND   environment = '{self.environment}'
            """).collect()[0]["c"]
        except Exception as exc:
            raise RegistrationError(f"Could not query pipeline_definitions: {exc}")

        if count > 0 and not overwrite:
            raise RegistrationError(
                f"'{self.pipeline_id}' has {count} existing rows "
                f"for env='{self.environment}'. Call register(overwrite=True) to replace.")
        logger.info(
            f"  {count} existing rows — will rotate + replace"
            if count > 0 else "  No existing rows — first registration (v1)"
        )

    # ── Version rotation ─────────────────────────────────────────────────────
    # This ONLY updates status flags in the database (current -> previous ->
    # archived). It never touches the actual config files on disk. That's on
    # purpose: cleaning up old files is a separate, schedulable job
    # (purge_old_configs, at the bottom of this file) so that registering a
    # pipeline stays fast and never risks deleting a file something else is
    # mid-way through reading.

    def _rotate_versions(self) -> int:
        logger.info("Step 3/9: Rotating versions...")
        try:
            rows = self.spark.sql(f"""
                SELECT config_version_id, version_number, version_status, config_path
                FROM   {CTRL}.pipeline_config_versions
                WHERE  pipeline_id = '{self.pipeline_id}'
                AND    environment = '{self.environment}'
                ORDER  BY version_number DESC
            """).collect()
        except Exception as exc:
            logger.warning(f"  Could not read versions ({exc}) — v1")
            self._clean_metadata_rows()
            return 1

        if not rows:
            self._clean_metadata_rows()
            return 1

        current_row  = next((r for r in rows if r["version_status"] == "current"),  None)
        previous_row = next((r for r in rows if r["version_status"] == "previous"), None)
        max_version  = max(r["version_number"] for r in rows)
        next_version = max_version + 1

        logger.info(
            f"  current=v{current_row['version_number'] if current_row else 'none'}  "
            f"previous=v{previous_row['version_number'] if previous_row else 'none'}  "
            f"next=v{next_version}"
        )

        if previous_row:
            logger.info(f"  Marking v{previous_row['version_number']} as archived "
                        f"(file itself is swept up later by purge_old_configs)")
            try:
                self.spark.sql(f"""
                    UPDATE {CTRL}.pipeline_config_versions
                    SET    version_status = 'archived',
                           is_active      = false
                    WHERE  config_version_id = '{previous_row['config_version_id']}'
                """)
            except Exception as exc:
                logger.warning(f"  Could not archive previous row: {exc}")

        if current_row:
            logger.info(f"  Moving v{current_row['version_number']} status to 'previous' "
                        f"(file untouched: {current_row['config_path']})")
            try:
                self.spark.sql(f"""
                    UPDATE {CTRL}.pipeline_config_versions
                    SET    version_status = 'previous',
                           is_active      = false
                    WHERE  config_version_id = '{current_row['config_version_id']}'
                """)
            except Exception as exc:
                logger.warning(f"  Could not promote current: {exc}")

        self._clean_metadata_rows()
        logger.info(f"  ✓ Rotation done — next: v{next_version}")
        return next_version

    def _clean_metadata_rows(self):
        """Permanently removes this pipeline's old rows before writing fresh
        ones. This is a real delete, not a "mark as inactive" — so the
        control tables don't slowly fill up with dead rows."""
        logger.info("  Cleaning existing metadata rows (hard delete)...")
        self.spark.sql(f"""
            DELETE FROM {CTRL}.pipeline_definitions
            WHERE pipeline_id = '{self.pipeline_id}'
            AND   environment = '{self.environment}'
        """)
        self.spark.sql(f"""
            DELETE FROM {CTRL}.sources
            WHERE pipeline_id = '{self.pipeline_id}'
            AND   environment = '{self.environment}'
        """)
        self.spark.sql(f"""
            DELETE FROM {CTRL}.transformation_rules
            WHERE pipeline_id = '{self.pipeline_id}'
        """)
        try:
            self.spark.sql(f"""
                DELETE FROM {CTRL}.metric_definitions
                WHERE pipeline_id = '{self.pipeline_id}'
                AND   environment = '{self.environment}'
            """)
        except Exception as exc:
            logger.warning(f"  Could not delete metric rows: {exc}")
        logger.info("  ✓ Metadata rows cleaned")

    # ── Create schemas ──────────────────────────────────────────────────────

    def _create_schemas(self):
        logger.info("Step 4/9: Creating schemas...")
        for sc in [self.bronze_schema, self.silver_schema, self.gold_schema]:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.target_catalog}.{sc}")
        logger.info(
            f"  ✓ {self.target_catalog}: "
            f"{self.bronze_schema} / {self.silver_schema} / {self.gold_schema}"
        )

    # ── Write sources ───────────────────────────────────────────────────────
    # We spell out every column's type by hand (instead of letting Spark
    # guess) before saving this data. Without that, saving would fail
    # whenever a value is blank/empty, because Spark can't figure out on its
    # own what type an empty value is supposed to be.

    def _write_sources(self):
        logger.info("Step 5/9: Writing sources...")
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, BooleanType
        )
        from pyspark.sql.functions import current_timestamp as _cts

        schema = StructType([
            StructField("source_id",             StringType(),  True),
            StructField("pipeline_id",           StringType(),  True),
            StructField("entity_name",           StringType(),  True),
            StructField("source_name",           StringType(),  True),
            StructField("environment",           StringType(),  True),
            StructField("source_type",           StringType(),  True),
            StructField("connection_id",         StringType(),  True),
            StructField("source_path",           StringType(),  True),
            StructField("file_format",           StringType(),  True),
            StructField("watermark_col",         StringType(),  True),
            StructField("merge_keys",            StringType(),  True),
            StructField("schema_json",           StringType(),  True),
            StructField("clean_rules",           StringType(),  True),
            StructField("expect_rules",          StringType(),  True),
            StructField("load_type",             StringType(),  True),
            StructField("operation_col",         StringType(),  True),
            StructField("sequence_col",          StringType(),  True),
            StructField("soft_delete_condition", StringType(),  True),
            StructField("version",               IntegerType(), True),
            StructField("is_active",             BooleanType(), True),
        ])

        for entity, s in self._sources.items():
            schema_j = self._schema_json(s["schema"]) if s["schema"] else None
            clean_j  = self._clean_json(s["clean"])   if s["clean"]  else None
            expect_j = json.dumps(s["expect"])         if s["expect"] else None
            try:
                data = [(
                    self._src_id(entity),
                    self.pipeline_id,
                    entity,
                    f"{self.pipeline_name} — {entity}",
                    self.environment,
                    s["source_type"],
                    s["connection_id"],
                    s["path"],
                    s["format"],
                    s["watermark"],
                    s["keys"],
                    schema_j,
                    clean_j,
                    expect_j,
                    s["load_type"],
                    s["operation_col"],
                    s["sequence_col"],
                    s["soft_delete_condition"],
                    1,
                    True,
                )]
                (
                    self.spark.createDataFrame(data, schema=schema)
                    .withColumn("created_at", _cts())
                    .withColumn("updated_at", _cts())
                    .write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{CTRL}.sources")
                )
                logger.info(f"  source : {self._src_id(entity)}")
            except Exception as exc:
                raise RegistrationError(f"Failed to write source '{entity}': {exc}")

    # ── Write pipeline_definitions ──────────────────────────────────────────
    # These are simple values (ids, names, flags) so it's safe to quote them
    # with _q(). Full SQL bodies never go through here — see _write_rules.

    def _write_definitions(self):
        logger.info("Step 6/9: Writing pipeline_definitions...")
        q     = self._q
        order = 1

        def insert(layer_nm, layer_order, source_id_val,
                   target_sch, target_tbl, layer_tp,
                   dlt_md=None, src_tp=None, load_tp=None,
                   wm=None, mk=None, del_cond=None,
                   deps=None, silver_srcs=None, partition_c=None):
            sv = q(json.dumps(silver_srcs)) if silver_srcs else "null"
            try:
                self.spark.sql(f"""
                    INSERT INTO {CTRL}.pipeline_definitions
                    (pipeline_id, pipeline_name, environment,
                     layer_name, layer_order, layer_type, dlt_mode,
                     source_id, silver_sources,
                     target_catalog, target_schema, target_table,
                     source_type, load_type,
                     watermark_col, merge_keys, lookback_window,
                     delete_condition, depends_on, partition_cols,
                     continue_on_failure, version, is_active,
                     created_at, updated_at)
                    VALUES (
                        {q(self.pipeline_id)}, {q(self.pipeline_name)},
                        {q(self.environment)},
                        {q(layer_nm)}, {layer_order},
                        {q(layer_tp)}, {q(dlt_md)},
                        {q(source_id_val)}, {sv},
                        {q(self.target_catalog)},
                        {q(target_sch)}, {q(target_tbl)},
                        {q(src_tp)}, {q(load_tp)},
                        {q(wm)}, {q(mk)}, '1',
                        {q(del_cond)}, {q(deps)}, {q(partition_c)},
                        false, 1, true,
                        current_timestamp(), current_timestamp()
                    )
                """)
                tag = dlt_md or load_tp or ""
                logger.info(f"  [{layer_tp}|{tag}] {layer_nm}")
            except Exception as exc:
                raise RegistrationError(f"Failed to write layer '{layer_nm}': {exc}")

        # Bronze — one row per raw source
        for entity, s in self._sources.items():
            insert(
                layer_nm=self._layer(entity, "bronze"),
                layer_order=order,
                source_id_val=self._src_id(entity),
                target_sch=self.bronze_schema,
                target_tbl=self._layer(entity, "bronze"),
                layer_tp="dlt", dlt_md="bronze",
                src_tp=s["source_type"], load_tp=s["load_type"],
                wm=s["watermark"], mk=s["keys"],
            )
            order += 1

        # Silver — one row per cleaned/deduped table
        self._silver_layer_map = {}
        for name, sl in self._silver.items():
            dlt_md   = SILVER_DLT_MODE_MAP[sl["type"]]
            entities = sl["entities"]

            if sl["is_multi"]:
                join_view_nm  = self._layer(name, "bronze_join")
                bronze_deps   = [self._layer(e, "bronze") for e in entities]
                bronze_tables = [
                    self._full(self.bronze_schema, self._layer(e, "bronze"))
                    for e in entities
                ]
                insert(
                    layer_nm=join_view_nm, layer_order=order,
                    source_id_val=self._src_id(entities[0]),
                    target_sch=self.bronze_schema, target_tbl=join_view_nm,
                    layer_tp="dlt", dlt_md="bronze_join",
                    deps=",".join(bronze_deps), silver_srcs=bronze_tables,
                )
                order += 1
                deps       = join_view_nm
                silver_src = [self._full(self.bronze_schema, join_view_nm)]
            else:
                deps       = self._layer(entities[0], "bronze")
                silver_src = [
                    self._full(self.bronze_schema, self._layer(entities[0], "bronze"))
                ]

            layer_nm = self._layer(name, "silver")
            self._silver_layer_map[name] = layer_nm

            # Convert the friendly "col=value" shorthand into valid SQL
            # before it gets stored. See _normalize_delete_condition above
            # for exactly what this does and doesn't touch.
            del_cond = self._normalize_delete_condition(sl["soft_delete"])

            insert(
                layer_nm=layer_nm, layer_order=order,
                source_id_val=self._src_id(entities[0]),
                target_sch=self.silver_schema,
                target_tbl=self._layer(name, "silver"),
                layer_tp="dlt", dlt_md=dlt_md, load_tp=sl["type"],
                wm=sl["watermark"], mk=sl["keys"],
                del_cond=del_cond,
                deps=deps, silver_srcs=silver_src,
            )
            order += 1

        # Gold — one row per business-ready table
        for name, gl in self._gold.items():
            layer_nm = self._layer(name, "gold")
            if gl["from_silver"]:
                silver_lyr = self._silver_layer_map.get(gl["from_silver"])
                deps       = silver_lyr
                silver_ref = [silver_lyr]
            else:
                bronze_lyr = self._layer(gl["from_bronze"], "bronze")
                deps       = bronze_lyr
                silver_ref = [f"__bronze__:{bronze_lyr}"]

            insert(
                layer_nm=layer_nm, layer_order=order,
                source_id_val=self._src_id(next(iter(self._sources))),
                target_sch=self.gold_schema,
                target_tbl=self._layer(name, "gold"),
                layer_tp="spark", load_tp=gl["load_type"],
                wm=gl.get("watermark"), mk=gl.get("merge_keys"),
                deps=deps, silver_srcs=silver_ref,
                partition_c=gl["partition_cols"],
            )
            order += 1

    # ── Write transformation_rules ──────────────────────────────────────────
    # IMPORTANT: the SQL a person writes in add_gold() is stored EXACTLY as
    # written — it never goes through _q(). _q() doubles up single quotes,
    # which is right for a simple value but wrong for a chunk of SQL: it
    # would turn something like DATE '2020-01-01' into broken SQL. So gold
    # SQL bypasses that quoting step entirely.

    def _write_rules(self):
        logger.info("Step 7/9: Writing transformation_rules...")
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, BooleanType
        )
        from pyspark.sql.functions import current_timestamp as _cts

        schema = StructType([
            StructField("rule_id",          StringType(),  True),
            StructField("pipeline_id",      StringType(),  True),
            StructField("layer_name",       StringType(),  True),
            StructField("step_name",        StringType(),  True),
            StructField("transform_type",   StringType(),  True),
            StructField("sql_query",        StringType(),  True),
            StructField("join_sources",     StringType(),  True),
            StructField("output_view_name", StringType(),  True),
            StructField("target_column",    StringType(),  True),
            StructField("transform_rule",   StringType(),  True),
            StructField("custom_module",    StringType(),  True),
            StructField("custom_fn",        StringType(),  True),
            StructField("rule_order",       IntegerType(), True),
            StructField("is_active",        BooleanType(), True),
        ])

        for name, gl in self._gold.items():
            layer_nm = self._layer(name, "gold")
            rid      = self._rule_id(layer_nm, 1)

            resolved_joins = []
            for js in gl["join_sources"]:
                entity = js["entity"]
                alias  = js["alias"]
                jtype  = js["type"]
                table_name = (
                    self._full(self.silver_schema, self._layer(entity, "silver"))
                    if jtype == "silver"
                    else self._full(self.bronze_schema, self._layer(entity, "bronze"))
                )
                resolved_joins.append({
                    "table_name": table_name,
                    "alias":      alias,
                    "entity":     entity,
                    "type":       jtype,
                })

            join_j = json.dumps(resolved_joins) if resolved_joins else None

            try:
                data = [(
                    rid,
                    self.pipeline_id,
                    layer_nm,
                    "primary_sql",
                    "sql",
                    gl["sql"],    # exact SQL as written — no escaping
                    join_j,
                    None, None, None, None, None,
                    1,
                    True,
                )]
                (
                    self.spark.createDataFrame(data, schema=schema)
                    .withColumn("created_at", _cts())
                    .write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{CTRL}.transformation_rules")
                )
                logger.info(f"  rule : {rid}")
            except Exception as exc:
                raise RegistrationError(f"Failed to write rule for gold '{name}': {exc}")

    # ── Write metrics ───────────────────────────────────────────────────────

    def _write_metrics(self):
        if not self._metrics:
            return
        logger.info("Step 7A/9: Writing metrics...")
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, BooleanType
        )
        from pyspark.sql.functions import current_timestamp as _cts

        schema = StructType([
            StructField("metric_id",         StringType(),  True),
            StructField("pipeline_id",       StringType(),  True),
            StructField("pipeline_name",     StringType(),  True),
            StructField("environment",       StringType(),  True),
            StructField("metric_name",       StringType(),  True),
            StructField("metric_group",      StringType(),  True),
            StructField("description",       StringType(),  True),
            StructField("source_gold_layer", StringType(),  True),
            StructField("source_table",      StringType(),  True),
            StructField("metric_expression", StringType(),  True),
            StructField("aggregation_type",  StringType(),  True),
            StructField("dimensions",        StringType(),  True),
            StructField("metric_filter",     StringType(),  True),
            StructField("output_view_name",  StringType(),  True),
            StructField("refresh_type",      StringType(),  True),
            StructField("version",           IntegerType(), True),
            StructField("is_active",         BooleanType(), True),
            StructField("created_by",        StringType(),  True),
        ])

        for metric_name, m in self._metrics.items():
            source_gold_layer = self._layer(m["source_gold"], "gold")
            source_table      = self._full(self.gold_schema, source_gold_layer)
            metric_id         = f"{self.pipeline_id}_{metric_name}"
            try:
                data = [(
                    metric_id,
                    self.pipeline_id,
                    self.pipeline_name,
                    self.environment,
                    metric_name,
                    m["metric_group"],
                    m["description"],
                    source_gold_layer,
                    source_table,
                    m["expression"],
                    m["aggregation_type"],
                    json.dumps(m["dimensions"]),
                    m["metric_filter"],
                    m["output_view"],
                    m["refresh_type"],
                    1,
                    True,
                    self._get_current_user(),
                )]
                (
                    self.spark.createDataFrame(data, schema=schema)
                    .withColumn("created_at", _cts())
                    .write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{CTRL}.metric_definitions")
                )
                logger.info(f"  metric : {metric_id}")
            except Exception as exc:
                logger.warning(f"  metric write skipped: {metric_name} — {exc}")

            try:
                self._generate_metric_view(m)
            except Exception as exc:
                logger.warning(
                    f"  metric view skipped (gold not yet created): "
                    f"{m['output_view']} — {exc}"
                )

    def _generate_metric_view(self, metric: dict):
        source_gold_layer = self._layer(metric["source_gold"], "gold")
        source_table      = self._full(self.gold_schema, source_gold_layer)
        dimensions        = metric["dimensions"]
        select_parts      = list(dimensions) + [
            f'{metric["expression"]} AS {metric["metric_name"]}'
        ]
        select_sql   = ",\n            ".join(select_parts)
        where_clause = f"\nWHERE {metric['metric_filter']}" if metric["metric_filter"] else ""
        group_clause = f"\nGROUP BY {', '.join(dimensions)}" if dimensions else ""
        view_name    = f"{self.target_catalog}.{self.gold_schema}.{metric['output_view']}"
        try:
            self.spark.sql(f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT {select_sql}
                FROM {source_table}
                {where_clause}
                {group_clause}
            """)
            logger.info(f"  metric view : {view_name}")
        except Exception as exc:
            logger.warning(
                f"  metric view skipped (gold table not yet created): "
                f"{view_name} — {exc}"
            )

    # ── Schema location cleanup ────────────────────────────────────────────
    # Autoloader (the tool that watches for new files) remembers the shape
    # of the data it last saw. If someone changes a source's expected
    # columns, we need to clear that memory out, otherwise it gets confused
    # and can create duplicate rows. We only ever touch this "memory" folder
    # — never the checkpoint that tracks which files have already been
    # processed.

    def _clean_schema_locations(self):
        logger.info("  Clearing schema locations for changed sources...")
        base_volume = self.config_dir.rstrip("/").rsplit("/", 1)[0]
        schema_base = f"{base_volume}/schema"
        cleared = 0
        skipped = 0

        for entity, s in self._sources.items():
            if s["source_type"] != "file":
                continue
            layer_name  = self._layer(entity, "bronze")
            schema_path = f"{schema_base}/{layer_name}"
            try:
                try:
                    self.dbutils.fs.ls(schema_path)
                    exists = True
                except Exception:
                    exists = False

                if not exists:
                    logger.info(f"  Schema location first run (no clear needed): {layer_name}")
                    skipped += 1
                    continue

                prev_schema = self._get_stored_schema_hints(schema_path)
                curr_hints  = self._build_schema_hints(s["schema"])

                if prev_schema != curr_hints:
                    self.dbutils.fs.rm(schema_path, recurse=True)
                    logger.info(f"  Schema location cleared : {layer_name}")
                    cleared += 1
                else:
                    logger.info(f"  Schema location intact  : {layer_name}")
                    skipped += 1
            except Exception as exc:
                logger.warning(f"  Could not process schema for '{layer_name}': {exc}")
                skipped += 1

        logger.info(f"  ✓ Schema locations: {cleared} cleared, {skipped} unchanged")

    def _build_schema_hints(self, schema: Optional[dict]) -> str:
        if not schema:
            return ""
        return ", ".join(
            f"{col.rstrip('!')} {typ.upper()}"
            for col, typ in schema.items()
        )

    def _get_stored_schema_hints(self, schema_path: str) -> str:
        try:
            content = self.dbutils.fs.head(f"{schema_path}/_schema.json", 100_000)
            if not content:
                return ""
            stored = json.loads(content)
            parts  = []
            for f in stored.get("fields", []):
                name = f.get("name", "")
                typ  = f.get("type", "")
                if name.startswith("_"):
                    continue
                if isinstance(typ, str):
                    parts.append(f"{name} {typ.upper()}")
                elif isinstance(typ, dict):
                    parts.append(f"{name} {typ.get('typeName', 'STRING').upper()}")
            return ", ".join(parts)
        except Exception:
            return "__UNKNOWN__"

    # ── Generate config ─────────────────────────────────────────────────────
    # Builds the final "instruction sheet" JSON that the actual data pipeline
    # reads at run time, and saves it under a folder dedicated to this one
    # pipeline: {config_dir}/{pipeline_id}/{pipeline_id}_{env}_{timestamp}_config.json
    #
    # We never overwrite or rename an existing file here — every run gets its
    # own permanent, uniquely-named file. Which one is "the current one" is
    # tracked in the pipeline_config_versions table, not by the filename.
    # Sweeping up old files is handled separately by purge_old_configs().

    def _generate_config(self, ts: str, vid: str, version_number: int):
        logger.info(f"Step 8/9: Generating config v{version_number}...")

        from pyspark.sql.window    import Window
        from pyspark.sql.functions import row_number, desc

        def rg(row, key, default=None):
            try:
                v = row[key]
                return v if v is not None else default
            except Exception:
                return default

        layers_df = self.spark.table(f"{CTRL}.pipeline_definitions")
        srcs_df   = self.spark.table(f"{CTRL}.sources")
        conns_df  = self.spark.table(f"{CTRL}.connections")
        rules_df  = self.spark.table(f"{CTRL}.transformation_rules")

        w = Window.partitionBy("pipeline_id", "layer_name").orderBy(desc("created_at"))

        layers = (
            layers_df
            .filter(f"pipeline_id='{self.pipeline_id}' AND environment='{self.environment}'")
            .withColumn("rn", row_number().over(w))
            .filter("rn=1").drop("rn")
            .orderBy("layer_order")
            .collect()
        )

        if not layers:
            raise RegistrationError(
                f"No layers found for '{self.pipeline_id}' [{self.environment}]")

        config_layers = []
        gold_by_layer = {self._layer(n, "gold"): g for n, g in self._gold.items()}

        for layer in layers:
            source_id  = rg(layer, "source_id")
            layer_nm   = rg(layer, "layer_name")
            layer_type = rg(layer, "layer_type")
            dlt_mode   = rg(layer, "dlt_mode")

            source = srcs_df.filter(f"source_id='{source_id}'").first()
            if not source:
                raise RegistrationError(f"Source '{source_id}' not found for '{layer_nm}'")

            connection = None
            conn_id = rg(source, "connection_id")
            if conn_id:
                connection = conns_df.filter(f"connection_id='{conn_id}'").first()
                if not connection:
                    raise RegistrationError(f"Connection '{conn_id}' not found for '{layer_nm}'")

            transforms = (
                rules_df
                .filter(f"pipeline_id='{self.pipeline_id}' AND layer_name='{layer_nm}'")
                .orderBy("rule_order")
                .collect()
            )

            wm         = rg(layer, "watermark_col") or rg(source, "watermark_col")
            mk         = rg(layer, "merge_keys")    or rg(source, "merge_keys")
            merge_keys = [k.strip() for k in mk.split(",") if k.strip()] if mk else []
            deps_str   = rg(layer, "depends_on")
            depends_on = [d.strip() for d in deps_str.split(",") if d.strip()] if deps_str else []

            lc = {
                "name":       layer_nm,
                "layer_type": layer_type,
                "source":     self._build_source_block(
                    rg, source, connection, layer, layers, dlt_mode, layer_type),
                "target":     {"table_name": (
                    f"{rg(layer,'target_catalog')}."
                    f"{rg(layer,'target_schema')}."
                    f"{rg(layer,'target_table')}"
                )},
                "continue_on_failure": bool(rg(layer, "continue_on_failure", False))
            }

            if depends_on: lc["depends_on"] = depends_on

            if layer_type == "dlt":
                lc["dlt_mode"]  = dlt_mode
                layer_load_type = rg(layer, "load_type")
                if layer_load_type:
                    lc["load_type"] = layer_load_type
                if dlt_mode == "bronze":
                    lc["source_type"] = rg(layer, "source_type") or rg(source, "source_type")
                    for key, col in [("schema", "schema_json"),
                                     ("clean_rules", "clean_rules"),
                                     ("expect_rules", "expect_rules")]:
                        raw = rg(source, col)
                        if raw:
                            try:    lc[key] = json.loads(raw)
                            except: logger.warning(f"Cannot parse {col} for {layer_nm}")
                    cdc_op  = rg(source, "operation_col")
                    cdc_seq = rg(source, "sequence_col")
                    if cdc_op:  lc["operation_col"] = cdc_op
                    if cdc_seq: lc["sequence_col"]  = cdc_seq
                elif dlt_mode == "bronze_join":
                    sv_raw = rg(layer, "silver_sources")
                    if sv_raw:
                        try: lc["bronze_tables"] = json.loads(sv_raw)
                        except: pass
                if merge_keys: lc["keys"]        = merge_keys
                if wm:         lc["sequence_by"] = wm
                dc = rg(layer, "delete_condition")
                if dc:
                    # Split and add quotes if it contains an equals sign
                    parts = dc.split("=")
                    if len(parts) == 2:
                        col = parts[0].strip()
                        val = parts[1].strip().strip("'").strip('"')
                        dc = f"{col} = '{val}'"
                        
                    lc["delete_condition"] = dc

            elif layer_type == "spark":
                spark_load_type = rg(layer, "load_type") or "full"
                lc["load_type"] = spark_load_type
                lc["audit"]     = {"add_ingestion_time": True, "add_source_file": False}
                g = gold_by_layer.get(layer_nm)
                lc["materialization"] = g["materialization"] if g else "table"
                pc = rg(layer, "partition_cols")
                if pc:
                    lc["partition_cols"] = [p.strip() for p in pc.split(",") if p.strip()]
                if spark_load_type in ("delta", "delta_merge"):
                    if wm:
                        lc["sequence_by"] = wm
                    if spark_load_type == "delta_merge" and merge_keys:
                        lc["merge_keys"] = merge_keys

            sql_steps, col_maps, custom_steps = [], [], []
            for rule in transforms:
                t = rg(rule, "transform_type")
                if t == "sql":
                    js = rg(rule, "join_sources")
                    sql_steps.append({
                        "step_name":    rg(rule, "step_name"),
                        "sql":          rg(rule, "sql_query"),
                        "join_sources": json.loads(js) if js else None,
                        "output_view":  rg(rule, "output_view_name"),
                        "rule_order":   rg(rule, "rule_order"),
                    })
                elif t == "column":
                    col_maps.append({
                        "target_column":  rg(rule, "target_column"),
                        "transform_rule": rg(rule, "transform_rule"),
                    })
                elif t == "custom":
                    custom_steps.append({
                        "step_name": rg(rule, "step_name"),
                        "module":    rg(rule, "custom_module"),
                        "fn":        rg(rule, "custom_fn"),
                        "order":     rg(rule, "rule_order"),
                    })

            if len(sql_steps) == 1 and not sql_steps[0]["output_view"]:
                lc["sql_transform"] = {"query": sql_steps[0]["sql"]}
                if sql_steps[0]["join_sources"]:
                    lc["join_sources"] = sql_steps[0]["join_sources"]
            elif sql_steps:
                lc["sql_steps"] = sql_steps
            if col_maps:     lc["transformations"]  = {"mappings": col_maps}
            if custom_steps: lc["custom_transforms"] = custom_steps

            config_layers.append(lc)

        config = {
            "pipeline_id":    self.pipeline_id,
            "pipeline_name":  self.pipeline_name,
            "environment":    self.environment,
            "version_number": version_number,
            "generated_at":   ts,
            "layers":         config_layers,
        }
        

        # Define nested pipeline-specific subfolder and archive folder
        pipeline_config_dir = f"{self.config_dir}/{self.pipeline_id}"
        archive_dir = f"{pipeline_config_dir}/archive"

        # New active config path (ts is computed once, earlier)
        config_path = f"{pipeline_config_dir}/{self.pipeline_id}_{self.environment}_{ts}_config.json"

        # Matches e.g. uct_test_dev_20261005_092856_config.json
        prefix = f"{self.pipeline_id}_{self.environment}_"
        pattern = re.compile(rf"^{re.escape(prefix)}\d{{8}}_\d{{6}}_config\.json$")

        # 1. If an active config already exists, MOVE it to the archive folder before writing the new one
        try:
            existing_files = [
                f.path for f in dbutils.fs.ls(pipeline_config_dir)
                if not f.isDir() and pattern.match(f.name)
            ]

            if existing_files:
                dbutils.fs.mkdirs(archive_dir)  # make sure the archive folder exists

                for src in existing_files:
                    archived_path = f"{archive_dir}/{src.split('/')[-1]}"
                    dbutils.fs.mv(src, archived_path)
                    logger.info(f"  Existing config moved to archive -> {archived_path}")
        except Exception as exc:
            logger.warning(f"  Could not archive existing config (might be first run): {exc}")

        # 2. Write the brand new config to the active path
        self._write_json(config_path, config)
        # # One folder per pipeline keeps things easy to browse as the number
        # # of pipelines grows, instead of every pipeline's files being mixed
        # # together in one shared folder.
        # pipeline_dir = f"{self.config_dir}/{self.pipeline_id}"
        # self._ensure_dir(pipeline_dir)

        # config_path = (
        #     f"{pipeline_dir}/{self.pipeline_id}_{self.environment}_{ts}_config.json")
        config_json = json.dumps(config, indent=2)
        self._write_json(config_path, config, _pre_serialized=config_json)
        config_size_bytes = len(config_json.encode("utf-8"))

        try:
            self.spark.sql(f"""
                UPDATE {CTRL}.pipeline_config_versions
                SET    is_active = false
                WHERE  pipeline_id    = '{self.pipeline_id}'
                AND    environment    = '{self.environment}'
                AND    version_status = 'current'
                AND    is_active      = true
            """)
        except Exception as exc:
            logger.warning(f"Could not deactivate old version: {exc}")

        bronze_cnt = sum(1 for l in config_layers if l.get("dlt_mode") == "bronze")
        silver_cnt = sum(1 for l in config_layers if l.get("dlt_mode") in SILVER_DLT_MODES)
        gold_cnt   = sum(1 for l in config_layers if l.get("layer_type") == "spark")

        try:
            self.spark.sql(f"""
                INSERT INTO {CTRL}.pipeline_config_versions
                (config_version_id, pipeline_id, pipeline_name,
                 environment, version_number, version_status,
                 config_path, layer_count, bronze_count,
                 silver_count, gold_count, config_size_bytes,
                 generated_at, generated_by, notes, is_active, created_at)
                VALUES (
                    {self._q(vid)},
                    {self._q(self.pipeline_id)},
                    {self._q(self.pipeline_name)},
                    {self._q(self.environment)},
                    {version_number}, 'current',
                    {self._q(config_path)},
                    {len(config_layers)},
                    {bronze_cnt}, {silver_cnt}, {gold_cnt},
                    {config_size_bytes},
                    current_timestamp(),
                    {self._q(self._get_current_user())},
                    {self._q(self.notes) if self.notes else 'null'},
                    true, current_timestamp()
                )
            """)
            logger.info(f"  Version : v{version_number} [current]  ({config_size_bytes:,} bytes)")
        except Exception as exc:
            raise RegistrationError(f"Failed to write pipeline_config_versions: {exc}")

        return config, config_path

    # ── Source block builder ────────────────────────────────────────────────

    def _build_source_block(self, rg, source, connection,
                            layer, layers, dlt_mode, layer_type):
        # All silver modes (incl. "full" and "delta") read from bronze. Before,
        # only snapshot/history were matched here, so full/delta silver fell
        # through to the autoloader block and re-read the raw landing files.
        if dlt_mode in SILVER_DLT_MODES:
            raw = rg(layer, "silver_sources")
            if raw:
                try:
                    refs = json.loads(raw)
                    if refs and "." in refs[0]:
                        return {"type": "table", "table_name": refs[0]}
                except Exception: pass
            bl = next((l for l in layers if rg(l, "dlt_mode") == "bronze"), None)
            if bl:
                return {"type": "table", "table_name": (
                    f"{rg(bl,'target_catalog')}.{rg(bl,'target_schema')}.{rg(bl,'target_table')}")}

        if dlt_mode == "bronze_join":
            raw    = rg(layer, "silver_sources")
            tables = json.loads(raw) if raw else []
            return {"type": "bronze_join", "tables": tables}

        if layer_type == "spark":
            raw = rg(layer, "silver_sources")
            if raw:
                try:
                    refs = json.loads(raw)
                    if refs:
                        ref = refs[0]
                        if ref.startswith("__bronze__:"):
                            bronze_layer_nm = ref.split(":", 1)[1]
                            match = next(
                                (l for l in layers if rg(l, "layer_name") == bronze_layer_nm), None)
                            if match:
                                return {"type": "table", "table_name": (
                                    f"{rg(match,'target_catalog')}."
                                    f"{rg(match,'target_schema')}."
                                    f"{rg(match,'target_table')}")}
                            raise RegistrationError(
                                f"Bronze layer '{bronze_layer_nm}' not found")
                        match = next(
                            (l for l in layers if rg(l, "layer_name") == ref), None)
                        if match:
                            return {"type": "table", "table_name": (
                                f"{rg(match,'target_catalog')}."
                                f"{rg(match,'target_schema')}."
                                f"{rg(match,'target_table')}")}
                except RegistrationError: raise
                except Exception: pass

            if not raw:
                sl = next(
                    (l for l in layers if rg(l, "dlt_mode") in SILVER_DLT_MODES), None)
                if sl:
                    return {"type": "table", "table_name": (
                        f"{rg(sl,'target_catalog')}.{rg(sl,'target_schema')}.{rg(sl,'target_table')}")}
            bl = next((l for l in layers if rg(l, "dlt_mode") == "bronze"), None)
            if bl:
                return {"type": "table", "table_name": (
                    f"{rg(bl,'target_catalog')}.{rg(bl,'target_schema')}.{rg(bl,'target_table')}")}

        st = rg(layer, "source_type") or rg(source, "source_type")
        if st == "file":
            return {
                "type":   "autoloader",
                "format": rg(source, "file_format", "parquet"),
                "path":   rg(source, "source_path"),
            }
        if st == "db":
            block = {
                "type":    "jdbc",
                "dbtable": rg(source, "source_path"),
                "connection": {
                    "url": (
                        f"jdbc:{rg(connection,'connection_type')}://"
                        f"{rg(connection,'host')}:{rg(connection,'port')}/"
                        f"{rg(connection,'database_name')}"
                    ),
                    "secret_scope": rg(connection, "secret_scope"),
                    "secret_key":   rg(connection, "secret_key"),
                }
            }
            wm = rg(layer, "watermark_col") or rg(source, "watermark_col")
            lw = rg(layer, "lookback_window", "0")
            if wm:
                block["incremental_strategy"] = {
                    "watermark_column": wm, "lookback_window": lw}
            return block

        return {"type": "table", "table_name": rg(source, "source_path")}

    # ── Audit log ───────────────────────────────────────────────────────────

    def _write_audit_log(self, vid, config, config_path, version_number):
        logger.info("Step 9/9: Writing audit log...")
        sid = (
            f"{self.pipeline_id}_{self.environment}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        definition = {
            "sources": list(self._sources),
            "silver":  list(self._silver),
            "gold":    list(self._gold),
            "metrics": list(self._metrics),
            "config_path": config_path,
        }
        self.spark.sql(f"""
            INSERT INTO {CTRL}.pipeline_staging
            (staging_id, pipeline_id, pipeline_name, environment,
             registered_at, registered_by, config_version_id,
             version_number, layer_count, definition_json,
             notes, created_at)
            VALUES (
                {self._q(sid)},
                {self._q(self.pipeline_id)},
                {self._q(self.pipeline_name)},
                {self._q(self.environment)},
                current_timestamp(),
                {self._q(self._get_current_user())},
                {self._q(vid)},
                {version_number},
                {len(config['layers'])},
                {self._q(json.dumps(definition))},
                {self._q(self.notes) if self.notes else 'null'},
                current_timestamp()
            )
        """)
        logger.info(f"  Audit : {sid}")

    # ── Summary ─────────────────────────────────────────────────────────────

    def _print_summary(self, config, config_path, vid, version_number):
        layers = config["layers"]
        bronze = [l for l in layers if l.get("dlt_mode") == "bronze"]
        joins  = [l for l in layers if l.get("dlt_mode") == "bronze_join"]
        silver = [l for l in layers if l.get("dlt_mode") in SILVER_DLT_MODES]
        gold   = [l for l in layers if l.get("layer_type") == "spark"]

        logger.info("=" * 60)
        logger.info(f"COMPLETE : {self.pipeline_id} [{self.environment}]")
        logger.info("=" * 60)
        logger.info(f"  Version : v{version_number} [current]")
        logger.info(f"  ID      : {vid}")
        logger.info(f"  Config  : {config_path}")
        logger.info(
            f"  Layers  : {len(layers)} total "
            f"({len(bronze)} bronze / {len(joins)} join / "
            f"{len(silver)} silver / {len(gold)} gold)"
        )
        logger.info(f"  Metrics : {len(self._metrics)}")
        logger.info("")
        for l in bronze: logger.info(f"  [bronze] {l['target']['table_name']}")
        for l in joins:  logger.info(f"  [join  ] {l['target']['table_name']}")
        for l in silver:
            mode = l.get("load_type") or l.get("dlt_mode")
            logger.info(f"  [{mode:6s}] {l['target']['table_name']}")
        for l in gold:
            logger.info(f"  [gold  ] {l['target']['table_name']} ({l.get('materialization', 'table')})")
        for mname, m in self._metrics.items():
            logger.info(f"  [metric] {m['output_view']} ← {m['source_gold']}")
        logger.info("")
        logger.info(f"  1. DLT → config_path = {config_path}")
        logger.info("  2. Run DLT triggered run (bronze + silver)")
        logger.info(f"  3. run_pipeline.py → pipeline_id = {self.pipeline_id}")
        logger.info(f"  Alert DL : {self.alert_email}")
        logger.info("=" * 60)


# ── Config archival / cleanup ─────────────────────────────────────────────
# This is a separate, scheduled housekeeping job (meant to run once a day on
# its own schedule) — NOT something that happens automatically inside
# register(). Its job: move config files older than `retention_days` out of
# the way into a dated "archive" folder, using a metadata-only rename
# (dbutils.fs.mv) rather than reading and rewriting file contents, so it
# stays fast even with thousands of files piled up over time.
#
# Because config files now live in one subfolder per pipeline (see
# _generate_config above), this job first lists each pipeline's folder, then
# looks inside each one — instead of assuming every file sits directly in
# one shared top-level folder.
#
# A file only ever gets archived if BOTH are true:
#   1. its filename timestamp is older than `retention_days`
#   2. it is NOT the file currently marked "version_status = 'current'" for
#      any pipeline/environment (so something still in active use is never
#      touched, no matter how old it is)

def purge_old_configs(spark, dbutils, config_dir: str,
                       retention_days: int = 7,
                       archive_dir: Optional[str] = None) -> dict:
    """
    Sweep every pipeline's config folder under `config_dir`, moving files
    older than `retention_days` into `{pipeline_folder}/archive/{YYYY}/{MM}/`
    (or into a single shared `archive_dir` if one is explicitly supplied).
    Every move is logged to {CTRL}.config_archive_log.

    Returns: {"moved": int, "skipped_active": int, "skipped_recent": int, "errors": int}
    """
    cutoff = datetime.now() - timedelta(days=retention_days)

    logger.info("=" * 60)
    logger.info(f"Config purge — retention={retention_days}d  dir={config_dir}")
    logger.info("=" * 60)

    # One query for every pipeline's "currently active" file, so we're not
    # doing a separate lookup per file — this scales with pipeline count,
    # not file count.
    try:
        active_paths = {
            r["config_path"] for r in spark.sql(f"""
                SELECT config_path FROM {CTRL}.pipeline_config_versions
                WHERE version_status = 'current' AND config_path IS NOT NULL
            """).collect()
        }
    except Exception as exc:
        logger.error(f"  Could not read active config paths — aborting purge: {exc}")
        return {"moved": 0, "skipped_active": 0, "skipped_recent": 0, "errors": 1}

    try:
        pipeline_folders = [e for e in dbutils.fs.ls(config_dir) if e.isDir()]
    except Exception as exc:
        logger.error(f"  Could not list {config_dir}: {exc}")
        return {"moved": 0, "skipped_active": 0, "skipped_recent": 0, "errors": 1}

    moved = skipped_active = skipped_recent = errors = 0

    for pf in pipeline_folders:
        pipeline_folder = pf.path.rstrip("/")

        # Skip a pipeline's own "archive" folder if it happens to appear at
        # this level — it only ever holds already-archived files.
        if pipeline_folder.endswith("/archive"):
            continue

        try:
            entries = dbutils.fs.ls(pipeline_folder)
        except Exception as exc:
            errors += 1
            logger.warning(f"  Could not list {pipeline_folder}: {exc}")
            continue

        pipeline_archive_base = (archive_dir or f"{pipeline_folder}/archive").rstrip("/")

        for entry in entries:
            if entry.isDir() or not entry.name.endswith("_config.json"):
                continue

            m = CONFIG_TS_RE.search(entry.name)
            if not m:
                logger.warning(f"  Skipping unrecognized filename: {entry.name}")
                continue

            file_ts = datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")

            if file_ts >= cutoff:
                skipped_recent += 1
                continue

            src_path = entry.path.rstrip("/")
            if src_path in active_paths:
                skipped_active += 1
                logger.info(f"  Skipping active config: {entry.name}")
                continue

            dest_dir  = f"{pipeline_archive_base}/{file_ts:%Y}/{file_ts:%m}"
            dest_path = f"{dest_dir}/{entry.name}"

            try:
                dbutils.fs.mkdirs(dest_dir)
                dbutils.fs.mv(src_path, dest_path)   # metadata-only move
                moved += 1
                logger.info(f"  Archived: {entry.name} → {dest_path}")

                spark.sql(f"""
                    INSERT INTO {CTRL}.config_archive_log
                    (archive_id, config_path, archive_path,
                     file_generated_at, archived_at)
                    VALUES (
                        '{uuid.uuid4()}',
                        '{src_path}',
                        '{dest_path}',
                        '{file_ts.isoformat()}',
                        current_timestamp()
                    )
                """)
            except Exception as exc:
                errors += 1
                logger.warning(f"  Could not archive {entry.name}: {exc}")

    logger.info("─" * 60)
    logger.info(
        f"Purge done — moved={moved}  skipped_active={skipped_active}  "
        f"skipped_recent={skipped_recent}  errors={errors}"
    )
    logger.info("=" * 60)
    return {
        "moved": moved, "skipped_active": skipped_active,
        "skipped_recent": skipped_recent, "errors": errors,
    }