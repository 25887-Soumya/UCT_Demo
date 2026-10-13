-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("pipeline_name", "")
-- MAGIC dbutils.widgets.text("table_name", "")
-- MAGIC dbutils.widgets.text("source_path", "")
-- MAGIC dbutils.widgets.text("bronze_schema", "")
-- MAGIC dbutils.widgets.text("silver_schema", "")
-- MAGIC dbutils.widgets.text("gold_schema", "")
-- MAGIC dbutils.widgets.text("silver_table", "")
-- MAGIC dbutils.widgets.text("gold_view", "")
-- MAGIC dbutils.widgets.text("gold_sql_file", "")
-- MAGIC dbutils.widgets.text("depends_on_silvers", "")
-- MAGIC
-- MAGIC # Defaults
-- MAGIC dbutils.widgets.text("load_type", "full")
-- MAGIC dbutils.widgets.text("soft_delete_condition", "")
-- MAGIC dbutils.widgets.dropdown("is_active", "true", ["true", "false"])
-- MAGIC
-- MAGIC # NEW: skip_source_to_landing — when true, this table's landing drop is
-- MAGIC # produced by jdbc_extract_to_landing.py instead of an external process.
-- MAGIC # connection_name — which rgaplxdatabricks.uct_demo.tbl_connection row to
-- MAGIC # use for the JDBC extract. Only meaningful when skip_source_to_landing=true,
-- MAGIC # but always accepted so it's set before the flag is flipped on.
-- MAGIC dbutils.widgets.dropdown("skip_source_to_landing", "false", ["true", "false"])
-- MAGIC dbutils.widgets.text("connection_name", "")
-- MAGIC
-- MAGIC # OPTIONAL overrides (leave blank = NULL). SQL_SERVER: used if present, else NULL.
-- MAGIC # SAP: if present, OVERWRITES the value read from .sap.partfile.metadata.
-- MAGIC dbutils.widgets.text("watermark_col", "")
-- MAGIC dbutils.widgets.text("operation_col", "")
-- MAGIC dbutils.widgets.text("sequence_col", "")

-- COMMAND ----------

MERGE INTO rgaplxdatabricks.uct_demo.pipeline_table_config AS target
USING (
    SELECT
        '${pipeline_name}' AS pipeline_name,
        '${table_name}' AS table_name,
        '${source_path}' AS source_path,
        '${bronze_schema}' AS bronze_schema,
        '${silver_schema}' AS silver_schema,
        '${gold_schema}' AS gold_schema,
        '${silver_table}' AS silver_table,
        '${gold_view}' AS gold_view,
        CASE
            WHEN '${load_type}' = '' THEN 'full'
            ELSE '${load_type}'
        END AS load_type,
        NULLIF('${soft_delete_condition}', '') AS soft_delete_condition,
        CAST('${is_active}' AS BOOLEAN) AS is_active,
        CAST('${skip_source_to_landing}' AS BOOLEAN) AS skip_source_to_landing,
        NULLIF('${connection_name}', '') AS connection_name,
        NULLIF('${watermark_col}', '') AS watermark_col,
        NULLIF('${operation_col}', '') AS operation_col,
        NULLIF('${sequence_col}', '') AS sequence_col,
		NULLIF('${gold_sql_file}', '') AS gold_sql_file,
        NULLIF('${depends_on_silvers}', '') AS depends_on_silvers
) AS source

ON  target.pipeline_name = source.pipeline_name
AND target.table_name = source.table_name

WHEN MATCHED THEN
    UPDATE SET
        target.source_path             = source.source_path,
        target.bronze_schema           = source.bronze_schema,
        target.silver_schema           = source.silver_schema,
        target.gold_schema             = source.gold_schema,
        target.silver_table            = source.silver_table,
        target.gold_view               = source.gold_view,
        target.load_type               = source.load_type,
        target.soft_delete_condition   = source.soft_delete_condition,
        target.is_active               = source.is_active,
        target.skip_source_to_landing  = source.skip_source_to_landing,
        target.connection_name         = source.connection_name,
        target.watermark_col           = source.watermark_col,
        target.operation_col           = source.operation_col,
        target.sequence_col            = source.sequence_col,
		target.gold_sql_file 		   = COALESCE(source.gold_sql_file, target.gold_sql_file),
        target.depends_on_silvers      = COALESCE(source.depends_on_silvers, target.depends_on_silvers)

WHEN NOT MATCHED THEN
    INSERT (
        pipeline_name,
        table_name,
        source_path,
        bronze_schema,
        silver_schema,
        gold_schema,
        silver_table,
        gold_view,
        load_type,
        soft_delete_condition,
        is_active,
        skip_source_to_landing,
        connection_name,
        watermark_col,
        operation_col,
        sequence_col,
		gold_sql_file,
        depends_on_silvers
    )
    VALUES (
        source.pipeline_name,
        source.table_name,
        source.source_path,
        source.bronze_schema,
        source.silver_schema,
        source.gold_schema,
        source.silver_table,
        source.gold_view,
        source.load_type,
        source.soft_delete_condition,
        source.is_active,
        source.skip_source_to_landing,
        source.connection_name,
        source.watermark_col,
        source.operation_col,
        source.sequence_col,
		source.gold_sql_file,
        source.depends_on_silvers
    );
