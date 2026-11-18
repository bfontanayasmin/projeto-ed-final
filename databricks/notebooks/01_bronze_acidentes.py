# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: acidentes PRF
# MAGIC
# MAGIC Le os CSVs originais (2023 a 2026) do volume e grava a tabela Delta `bronze.acidentes`.
# MAGIC
# MAGIC Particularidades dos arquivos da PRF:
# MAGIC - separador `;`
# MAGIC - encoding **ISO-8859-1** (latin1) - sem isso os acentos quebram
# MAGIC - decimais com virgula (`km`, `latitude`, `longitude`) - tratados so na silver
# MAGIC
# MAGIC Na bronze tudo fica como **string**, mais as colunas de controle `_arquivo_origem` e `_data_ingestao`.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("volume_raw", "raw")

catalog = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
volume_raw = dbutils.widgets.get("volume_raw")

raw_path = f"/Volumes/{catalog}/{schema_bronze}/{volume_raw}/acidentes"
tabela_bronze = f"`{catalog}`.`{schema_bronze}`.`acidentes`"

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .option("quote", '"')
    .option("inferSchema", "false")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.csv")
    .load(raw_path)
    .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
    .withColumn("_data_ingestao", F.current_timestamp())
)

if not df.columns or df.limit(1).count() == 0:
    raise ValueError(f"Nenhum CSV encontrado em {raw_path}. Veja databricks/README.md (envio dos datasets).")

# COMMAND ----------

(
    df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tabela_bronze)
)

spark.sql(
    f"COMMENT ON TABLE {tabela_bronze} IS "
    "'Acidentes PRF 2023-2026 (por pessoa envolvida), dado bruto do CSV, todas as colunas string'"
)

# COMMAND ----------

display(
    spark.table(tabela_bronze)
    .groupBy("_arquivo_origem")
    .count()
    .orderBy("_arquivo_origem")
)
