# Databricks notebook source
# DBTITLE 1,Read SAP Metadata for Ingestion
import json
import logging
from datetime import datetime
from pyspark.sql.functions import current_timestamp

# ============================================================================
# LOGGING SETUP
# Configure a dedicated logger for the SAP metadata ingestion process.
# This ensures all log messages are timestamped and clearly labelled.
# ============================================================================
logger = logging.getLogger("sap_ingestion_orchestrator")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_h)

# ============================================================================
# SAP-TO-SPARK DATA TYPE MAPPING
# Translates SAP/HANA native data types into their equivalent Spark SQL types.
# Used when building the Delta table schema from the SAP metadata file.
# ============================================================================
TYPE_MAP = {
    # String / text types
    "NVARCHAR":   "STRING", "VARCHAR":    "STRING", "CHAR":       "STRING",
    "NCHAR":      "STRING", "TEXT":       "STRING", "ALPHANUM":   "STRING",
    "CLOB":       "STRING", "BLOB":       "STRING", "SSTRING":    "STRING",
    "STRING":     "STRING",
    # Time-as-string types (stored as STRING since Spark has no native TIME type)
    "TIME":       "STRING", "TIMESHORT":  "STRING",
    "TIMELONG":   "STRING", "TIMEOFDAY":  "STRING", "TIMS":       "STRING",
    "TIMSLONG":   "STRING",
    # Floating-point types
    "FLOAT":      "DOUBLE", "DOUBLE":     "DOUBLE", "REAL":       "DOUBLE",
    # Integer types
    "INTEGER":    "INTEGER", "INT":        "INTEGER",
    "INT4":       "INTEGER", "INT2":       "INTEGER", "INT1":       "INTEGER",
    "TINYINT":    "INTEGER", "SMALLINT":   "INTEGER",
    # Big integer types
    "BIGINT":     "BIGINT", "INT8":       "BIGINT",
    # Boolean types
    "BOOLEAN":    "BOOLEAN", "ABAP_BOOL":  "BOOLEAN",
    # Date and timestamp types
    "DATE":       "DATE",    "DATS":       "DATE",
    "TIMESTAMP":  "TIMESTAMP_NTZ", "TIMESTAMPL": "TIMESTAMP_NTZ"
}

# ============================================================================
# SYSTEM COLUMNS
# These are automatically added to every ingested table for CDC tracking.
#   - __timestamp      : When the record was captured from SAP
#   - __operation_type : The change operation (I=Insert, U=Update, D=Delete)
# ============================================================================
SYSTEM_COL_MAP = {
    "__timestamp":      "TIMESTAMP_NTZ",
    "__operation_type": "STRING",
}


def read_sap_metadata(metadata_path: str) -> dict:
    """
    Parse an SAP metadata JSON file and extract everything needed to set up
    the Delta table ingestion pipeline.

    Parameters
    ----------
    metadata_path : str
        The DBFS or cloud storage path to the SAP metadata JSON file.
        Example: "dbfs:/mnt/raw/sap/VBAK/_metadata.json"

    Returns
    -------
    dict with keys:
        - format             : File format of the source data (e.g. "parquet")
        - load_type          : SAP load type ("FULL" or "DELTA")
        - columns            : Raw column definitions from the metadata file
        - primary_keys       : List of primary key column names
        - key_str            : Comma-separated primary keys (for MERGE statements)
        - watermark          : Column used to track incremental changes
        - operation_col      : Column indicating Insert/Update/Delete operation
        - sequence_col       : Column used to order changes (tie-breaking)
        - soft_delete_condition : SQL condition identifying deleted records
        - schema_hints       : Dict mapping column names to Spark SQL types
                               (primary key columns are suffixed with "!")
    """

    # --- Step 1: Read the metadata file from storage ---
    logger.info(f"  Reading metadata: {metadata_path}")
    try:
        content = dbutils.fs.head(metadata_path, 1024 * 1024)  # Read up to 1 MB
    except Exception as e:
        raise ValueError(f"Could not read metadata file: {metadata_path} — {e}")

    if not content.strip():
        raise ValueError(f"Metadata file is empty or unreadable: {metadata_path}")

    # --- Step 2: Parse JSON and extract top-level properties ---
    raw = json.loads(content)
    fmt       = raw.get("format", "PARQUET").lower()       # Source file format
    props     = {p["name"]: p["value"] for p in raw.get("properties", [])}
    load_type = props.get("loadType", "UNKNOWN")           # FULL or DELTA
    all_cols  = raw.get("columns", [])                     # Full column list

    # --- Step 3: Detect CDC-related columns ---
    # SAP marks CDC columns with semantic type "_change_mode".
    # We identify three special columns by their data type:
    #   - TIMESTAMP  → watermark (tracks when changes occurred)
    #   - VARCHAR(1) → operation flag (I/U/D)
    #   - DECIMAL    → sequence number (ordering tie-breaker)
    watermark_col  = None
    operation_col  = None
    sequence_col   = None

    for col in all_cols:
        col_name    = col.get("name", "")
        col_dtype   = col.get("dataType", "").upper()
        col_props   = {f"{p.get('namespace','')}.{p.get('name','')}": p.get("value", "")
                      for p in col.get("properties", [])}
        semantic = col_props.get("com.sap.rms.semanticType", "")

        if semantic == "_change_mode":
            if col_dtype == "TIMESTAMP":
                watermark_col = col_name
            elif col_dtype in ("NVARCHAR", "VARCHAR") and col.get("length") == 1:
                operation_col = col_name
            elif col_dtype == "DECIMAL":
                sequence_col  = col_name

    # --- Step 4: Build soft-delete condition ---
    # If an operation column exists, records with value 'D' are soft-deleted.
    soft_delete_condition = None
    if operation_col:
        soft_delete_condition = f"{operation_col} = 'D'"

    logger.info(f"  Watermark col   : {watermark_col or 'not found'}")
    logger.info(f"  Operation col   : {operation_col or 'not found'}")
    logger.info(f"  Sequence col    : {sequence_col  or 'not found'}")
    if soft_delete_condition:
        logger.info(f"  Soft delete     : {soft_delete_condition}")

    # --- Step 5: Extract primary keys ---
    primary_keys = [c["name"] for c in all_cols if c.get("primaryKey")]

    # --- Step 6: Build schema hints (column name → Spark type) ---
    # Schema hints are used downstream to create the Delta table DDL.
    # Primary key columns are marked with a trailing "!" in their key name.
    schema_hints = {}

    # Include system columns first
    for col_name, col_type in SYSTEM_COL_MAP.items():
        schema_hints[col_name] = col_type

    # Map each SAP column to its Spark type (skip unsupported TIME types)
    for col in all_cols:
        name      = col.get("name", "")
        data_type = col.get("dataType", "").upper()
        is_pk     = col.get("primaryKey", False)

        # Skip columns with unmapped data types
        if data_type not in TYPE_MAP:
            continue
        # Skip TIME types — they are mapped to STRING but excluded from schema
        if data_type in ("TIME", "TIMESHORT", "TIMELONG", "TIMEOFDAY", "TIMS", "TIMSLONG"):
            continue

        mapped_type = TYPE_MAP[data_type]
        key = f"{name}!" if is_pk else name  # Suffix "!" marks primary keys
        schema_hints[key] = mapped_type

    # --- Step 7: Log summary ---
    logger.info("=" * 65)
    logger.info(f"  Format          : {fmt.upper()}")
    logger.info(f"  Load type       : {load_type.upper()}")
    logger.info(f"  Primary keys    : {primary_keys}")
    logger.info(f"  Total cols      : {len(schema_hints)}")
    logger.info("=" * 65)

    # --- Step 8: Return structured metadata dictionary ---
    return {
        "format":               fmt,
        "load_type":            load_type,
        "columns":              all_cols,
        "primary_keys":         primary_keys,
        "key_str":              ",".join(primary_keys),
        "watermark":            watermark_col or "__timestamp",
        "operation_col":        operation_col,
        "sequence_col":         sequence_col,
        "soft_delete_condition": soft_delete_condition,
        "schema_hints":         schema_hints,
    }

