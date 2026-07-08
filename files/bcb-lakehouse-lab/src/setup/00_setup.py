# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 0 — Setup do Unity Catalog (Exame S1/S7)
# MAGIC Cria os schemas das camadas Medallion e os **Volumes** (armazenamento governado
# MAGIC para dados NÃO tabulares — aqui, a landing zone de arquivos e checkpoints).
# MAGIC Hierarquia UC: metastore → catálogo → schema → tabela/view/volume/função.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env_prefix", "bcb_dev")
catalog = dbutils.widgets.get("catalog")
prefix = dbutils.widgets.get("env_prefix")

# COMMAND ----------
for camada in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{prefix}_{camada} "
              f"COMMENT 'Camada {camada} do lakehouse BCB'")

# Volumes gerenciados: landing zone (arquivos brutos) e checkpoints (streaming)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{prefix}_bronze.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{prefix}_bronze.checkpoints")

display(spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE '{prefix}*'"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Grants básicos (S7)
# MAGIC O modelo do UC é **aditivo**: só existem `GRANT` e `REVOKE` — não há `DENY`
# MAGIC (distrator clássico de prova). Privilégios exigem a cadeia completa:
# MAGIC `USE CATALOG` + `USE SCHEMA` + privilégio no objeto (ex.: `SELECT`).

# COMMAND ----------
spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `account users`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{prefix}_gold TO `account users`")
# SELECT em todo o schema gold (aplica-se às tabelas atuais e futuras do schema):
spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.{prefix}_gold TO `account users`")
display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalog}.{prefix}_gold"))

# COMMAND ----------
print(f"Setup concluído: {catalog}.{prefix}_(bronze|silver|gold) + volumes landing/checkpoints")
