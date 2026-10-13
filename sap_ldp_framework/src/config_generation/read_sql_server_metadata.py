# Databricks notebook source
# ══════════════════════════════════════════════════════════════════
# Counterpart to read_sap_metadata.py for SQL-Server-sourced tables
# (connection_name = 'SQL_SERVER' in pipeline_table_config).
#
# read_sap_metadata() reads a .sap.partfile.metadata file and returns a
# dict of {format, load_type, columns, primary_keys, key_str, watermark,
# operation_col, sequence_col, soft_delete_condition, schema_hints}.
# read_sql_server_metadata() below returns the EXACT SAME SHAPE, built
# instead from INFORMATION_SCHEMA.COLUMNS over JDBC — so it's a drop-in
# alternative wherever metadata_loader.py currently calls
# read_sap_metadata(), with no changes needed to _land_sap_metadata(),
# add_source(), add_silver(), etc.
#
# Populated from INFORMATION_SCHEMA over JDBC: column name/order, data type,
# precision, scale, nullability, max length and PRIMARY KEY columns
# (TABLE_CONSTRAINTS + KEY_COLUMN_USAGE). Decimal/numeric/money map to
# DECIMAL(p,s); a trailing "!" on a schema_hints key marks NOT NULL.
#
# Still returned empty (not derivable from the database): watermark,
# operation_col, sequence_col, soft_delete_condition. They are optional and
# supplied through pipeline_table_config (Watermark_col, Operation_col,
# Sequence_col, Soft_delete_condition) — metadata_loader.py applies them.
# ══════════════════════════════════════════════════════════════════

import json
import re
import logging

logger = logging.getLogger("sap_ingestion_orchestrator")  # same logger as metadata_loader.py
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_h)

# Connection table lives in the control schema. Left as None so it follows
# CTRL ("<catalog>.<control_schema>"), which metadata_loader.py sets from its
# job parameters; set a full table name here only to override that.
CONNECTION_TABLE = None

# Source schema when neither the caller nor the connection's Config sets one
# (SQL Server default). A connection can set "schema" in Config.connection.
DEFAULT_SOURCE_SCHEMA = "dbo"


def _connection_table() -> str:
    if CONNECTION_TABLE:
        return CONNECTION_TABLE
    ctrl = globals().get("CTRL")
    if not ctrl:
        raise ValueError("CTRL is not set - run this through metadata_loader.py, "
                         "which sets it from the catalog/control_schema parameters.")
    return f"{ctrl}.tbl_connection"

# Same assumption as jdbc_extract_to_landing.py / pipeline_registry.py's
# skip_source_to_landing patch: Config.connection's "user"/"password" are
# secret KEY NAMES inside "scope", not literal credential values.
CREDENTIALS_ARE_SECRET_KEYS = True

# INFORMATION_SCHEMA.COLUMNS.DATA_TYPE (SQL Server) -> Databricks type.
# decimal/numeric/money/smallmoney are handled separately as DECIMAL(p,s).
SQL_SERVER_TYPE_MAP = {
    "varchar": "STRING", "nvarchar": "STRING", "char": "STRING", "nchar": "STRING",
    "text": "STRING", "ntext": "STRING", "xml": "STRING",
    "uniqueidentifier": "STRING", "binary": "STRING", "varbinary": "STRING", "image": "STRING",
    "time": "STRING",

    "int": "INTEGER", "smallint": "INTEGER", "tinyint": "INTEGER",
    "bigint": "BIGINT",
    "bit": "BOOLEAN",

    "float": "DOUBLE", "real": "DOUBLE",

    "date": "DATE",
    "datetime": "TIMESTAMP_NTZ", "datetime2": "TIMESTAMP_NTZ",
    "smalldatetime": "TIMESTAMP_NTZ", "datetimeoffset": "TIMESTAMP_NTZ",
}
_DECIMAL_TYPES = {"decimal", "numeric", "money", "smallmoney"}
_MONEY_PRECISION = {"money": (19, 4), "smallmoney": (10, 4)}


def _to_int(v):
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _databricks_type(raw_type: str, precision, scale):
    """SQL Server type -> Databricks DDL type (None if unmapped)."""
    t = (raw_type or "").lower()
    if t in _DECIMAL_TYPES:
        p, sc = precision, scale
        if t in _MONEY_PRECISION and (p is None or sc is None):
            p, sc = _MONEY_PRECISION[t]
        if p is None:
            return "DOUBLE"
        return f"DECIMAL({p},{sc if sc is not None else 0})"
    return SQL_SERVER_TYPE_MAP.get(t)


def _columns_from_rows(col_rows, pk_names):
    """Pure function: INFORMATION_SCHEMA rows (dicts) + PK names -> column dicts."""
    pk_set = {n.lower() for n in pk_names}
    cols = []
    for r in col_rows:
        raw = (r["DATA_TYPE_NAME"] or "")
        prec, scale = _to_int(r.get("NUMERIC_PRECISION")), _to_int(r.get("NUMERIC_SCALE"))
        cols.append({
            "name":           r["COLUMN_NAME"],
            "dataType":       raw.upper(),
            "databricksType": _databricks_type(raw, prec, scale),
            "precision":      prec,
            "scale":          scale,
            "maxLength":      _to_int(r.get("CHARACTER_MAXIMUM_LENGTH")),
            "nullable":       str(r.get("IS_NULLABLE") or "YES").upper() != "NO",
            "primaryKey":     r["COLUMN_NAME"].lower() in pk_set,
            "properties":     [],
        })
    return cols


def build_schema_hints(all_cols, log_unmapped=True):
    """{column: type}; key gets a trailing '!' when the column is NOT NULL."""
    hints = {}
    for c in all_cols:
        typ = c.get("databricksType")
        if typ is None:
            if log_unmapped:
                logger.info(f"  Unmapped type  : {c['name']} ({c['dataType']}) — left for Parquet/Delta to infer")
            continue
        hints[c["name"] + ("" if c["nullable"] else "!")] = typ
    return hints


def _sql_escape(s: str) -> str:
    return s.replace("'", "''")


def _resolve_jdbc_connection(connection_name: str):
    """Returns (jdbc_url, driver, connection_properties, source_schema) for a connection_name
    row in tbl_connection. Same resolution logic as jdbc_extract_to_landing.py —
    kept self-contained here rather than shared, to avoid a cross-notebook
    %run dependency for a handful of lines."""
    connection_table = _connection_table()
    conn_rows = spark.sql(f"""
        SELECT Name, Type, Config
        FROM {connection_table}
        WHERE Name = '{_sql_escape(connection_name)}'
    """).collect()

    if not conn_rows:
        raise ValueError(f"No row in {connection_table} where Name = '{connection_name}'.")
    if len(conn_rows) > 1:
        raise ValueError(f"Multiple rows in {connection_table} share Name = '{connection_name}'.")

    conn_row = conn_rows[0]
    if (conn_row["Type"] or "").upper() != "JDBC":
        raise ValueError(f"Connection '{connection_name}' has Type={conn_row['Type']!r}, not 'JDBC'.")

    raw_config = conn_row["Config"]
    if raw_config is None:
        raise ValueError(f"Connection '{connection_name}' has a NULL Config column.")

    cleaned_config = re.sub(r",\s*([}\]])", r"\1", raw_config)  # tolerate trailing comma
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
    return jdbc_url, driver, connection_properties, source_schema


def read_sql_server_metadata(table_name: str, connection_name: str, schema_name: str = None) -> dict:
    """
    Counterpart to read_sap_metadata(metadata_path) — same return shape,
    built from INFORMATION_SCHEMA.COLUMNS over JDBC instead of a
    .sap.partfile.metadata file.

    schema_name: pass it explicitly, or leave it empty to use the
    connection's Config "schema" (same setting jdbc_extract_to_landing.py
    uses), falling back to DEFAULT_SOURCE_SCHEMA ('dbo').
    """
    jdbc_url, driver, connection_properties, conn_schema = _resolve_jdbc_connection(connection_name)
    schema_name = (schema_name or conn_schema).strip()
    logger.info(f"  Reading metadata via JDBC: {connection_name} / {schema_name}.{table_name}")

    sch, tbl = _sql_escape(schema_name), _sql_escape(table_name)

    def _jdbc_query(q, alias):
        return spark.read.jdbc(url=jdbc_url, table=f"({q}) AS {alias}", properties=connection_properties)

    cols_query = f"""
        SELECT TABLE_SCHEMA AS SCHEMA_NAME, TABLE_NAME AS VIEW_NAME,
               COLUMN_NAME AS COLUMN_NAME, ORDINAL_POSITION AS POSITION,
               DATA_TYPE AS DATA_TYPE_NAME, IS_NULLABLE AS IS_NULLABLE,
               NUMERIC_PRECISION AS NUMERIC_PRECISION, NUMERIC_SCALE AS NUMERIC_SCALE,
               CHARACTER_MAXIMUM_LENGTH AS CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{sch}' AND TABLE_NAME = '{tbl}'
    """
    col_rows = [r.asDict() for r in _jdbc_query(cols_query, "columns_query").orderBy("POSITION").collect()]

    if not col_rows:
        raise ValueError(
            f"INFORMATION_SCHEMA.COLUMNS returned no rows for "
            f"{schema_name}.{table_name} via connection '{connection_name}' — "
            f"check the table exists and the connection's user has access."
        )

    pk_query = f"""
        SELECT kcu.COLUMN_NAME AS COLUMN_NAME, kcu.ORDINAL_POSITION AS KEY_POSITION
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
          ON  tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
          AND tc.TABLE_SCHEMA    = kcu.TABLE_SCHEMA
          AND tc.TABLE_NAME      = kcu.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND tc.TABLE_SCHEMA = '{sch}' AND tc.TABLE_NAME = '{tbl}'
    """
    pk_names = []
    try:
        pk_rows = _jdbc_query(pk_query, "pk_query").orderBy("KEY_POSITION").collect()
        pk_names = [r["COLUMN_NAME"] for r in pk_rows]
    except Exception as ex:
        logger.warning(f"  Could not read primary keys for {schema_name}.{table_name}: {ex}")

    all_cols = _columns_from_rows(col_rows, pk_names)
    schema_hints = build_schema_hints(all_cols)

    logger.info("=" * 65)
    logger.info(f"  Source          : SQL_SERVER ({connection_name})")
    logger.info(f"  Schema.Table    : {schema_name}.{table_name}")
    logger.info(f"  Total cols      : {len(schema_hints)}")
    logger.info(f"  Primary keys    : {pk_names or '(none)'}")
    logger.info("  Not populated   : watermark, operation_col, sequence_col, "
                "soft_delete_condition (optional — from pipeline_table_config)")
    logger.info("=" * 65)

    return {
        "format":               "parquet",  # bronze reads parquet from landing regardless of
                                             # source type (see jdbc_extract_to_landing.py)
        "load_type":            "UNKNOWN",
        "columns":              all_cols,
        "primary_keys":         pk_names,
        "key_str":              ",".join(pk_names),
        "watermark":            None,
        "operation_col":        None,
        "sequence_col":         None,
        "soft_delete_condition": None,
        "schema_hints":         schema_hints,
    }
