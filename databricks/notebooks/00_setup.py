# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup do ambiente
# MAGIC
# MAGIC Cria os schemas da arquitetura medalhao (bronze / silver / gold) e o volume
# MAGIC onde ficam os CSVs originais dos acidentes da PRF.
# MAGIC
# MAGIC Pode ser rodado varias vezes sem problema (tudo e `IF NOT EXISTS`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_gold", "gold")
dbutils.widgets.text("volume_raw", "raw")

catalog = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold = dbutils.widgets.get("schema_gold")
volume_raw = dbutils.widgets.get("volume_raw")

# COMMAND ----------

for schema, comentario in [
    (schema_bronze, "Camada bronze: dado bruto dos CSVs da PRF, sem tratamento"),
    (schema_silver, "Camada silver: dado limpo e tipado"),
    (schema_gold, "Camada gold: agregacoes e indicadores para analise"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` COMMENT '{comentario}'")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema_bronze}`.`{volume_raw}` "
    "COMMENT 'Arquivos originais (CSV) dos acidentes da PRF'"
)

# COMMAND ----------

raw_path = f"/Volumes/{catalog}/{schema_bronze}/{volume_raw}/acidentes"
dbutils.fs.mkdirs(raw_path)

print(f"Schemas prontos em `{catalog}`: {schema_bronze}, {schema_silver}, {schema_gold}")
print(f"Envie os CSVs para: {raw_path}")

display(dbutils.fs.ls(raw_path))
