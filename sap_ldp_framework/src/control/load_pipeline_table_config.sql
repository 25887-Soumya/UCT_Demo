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

-- COMMAND ----------

MERGE INTO rgaplxdatabricks.uct_demo.pipeline_table_config AS target
USING (
    SELECT
        '${pipeline_name}' AS pipeline_name,
        '${table_name}' AS table_name,

        NULLIF('${source_path}', '') AS source_path,
        NULLIF('${bronze_schema}', '') AS bronze_schema,
        NULLIF('${silver_schema}', '') AS silver_schema,
        NULLIF('${gold_schema}', '') AS gold_schema,
        NULLIF('${silver_table}', '') AS silver_table,
        NULLIF('${gold_view}', '') AS gold_view,
        NULLIF('${load_type}', '') AS load_type,
        NULLIF('${soft_delete_condition}', '') AS soft_delete_condition,
        NULLIF('${gold_sql_file}', '') AS gold_sql_file,
        NULLIF('${depends_on_silvers}', '') AS depends_on_silvers,

        CASE
            WHEN '${is_active}' = '' THEN NULL
            ELSE CAST('${is_active}' AS BOOLEAN)
        END AS is_active

) AS source

ON target.pipeline_name = source.pipeline_name
AND target.table_name = source.table_name

WHEN MATCHED THEN
    UPDATE SET
        target.source_path =
            COALESCE(source.source_path, target.source_path),

        target.bronze_schema =
            COALESCE(source.bronze_schema, target.bronze_schema),

        target.silver_schema =
            COALESCE(source.silver_schema, target.silver_schema),

        target.gold_schema =
            COALESCE(source.gold_schema, target.gold_schema),

        target.silver_table =
            COALESCE(source.silver_table, target.silver_table),

        target.gold_view =
            COALESCE(source.gold_view, target.gold_view),

        target.load_type =
            COALESCE(source.load_type, target.load_type),

        target.soft_delete_condition =
            COALESCE(source.soft_delete_condition, target.soft_delete_condition),

        target.is_active =
            COALESCE(source.is_active, target.is_active),

        target.gold_sql_file =
            COALESCE(source.gold_sql_file, target.gold_sql_file),

        target.depends_on_silvers =
            COALESCE(source.depends_on_silvers, target.depends_on_silvers)

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

        COALESCE(source.load_type, 'full'),

        source.soft_delete_condition,
        source.is_active,
        source.gold_sql_file,
        source.depends_on_silvers
    );

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

