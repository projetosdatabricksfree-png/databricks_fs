# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 1 — CTAS × COPY INTO × Auto Loader (Exame S2)
# MAGIC A matriz de decisão que a prova cobra:
# MAGIC | Método | Incremental? | Escala | Quando usar |
# MAGIC |---|---|---|---|
# MAGIC | `CREATE TABLE AS` + `read_files` | Não (recarga total) | baixa | carga única/exploração |
# MAGIC | `COPY INTO` | Sim (idempotente por arquivo) | milhares de arquivos | ingestão SQL simples e agendada |
# MAGIC | Auto Loader (`cloudFiles`) | Sim (checkpoint) | milhões de arquivos | produção, streaming/near-real-time, schema evolution |

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env_prefix", "bcb_dev")
cat = dbutils.widgets.get("catalog")
pfx = dbutils.widgets.get("env_prefix")
landing = f"/Volumes/{cat}/{pfx}_bronze/landing"
ckpt = f"/Volumes/{cat}/{pfx}_bronze/checkpoints"

# Semente: catálogo de séries SGS como CSV (dimensão de referência)
import pathlib
seed_dir = pathlib.Path(f"{landing}/seed_dim_series"); seed_dir.mkdir(parents=True, exist_ok=True)
(seed_dir / "dim_series.csv").write_text(
    "codigo_serie,nome_serie,unidade,periodicidade\n"
    "1,Dólar comercial (venda),BRL,diaria\n"
    "11,Taxa Selic,% a.d.,diaria\n"
    "433,IPCA,% a.m.,mensal\n"
)

# COMMAND ----------
# MAGIC %md ## 1) CTAS + read_files — carga única (recria tudo a cada execução)

# COMMAND ----------
spark.sql(f"""
  CREATE OR REPLACE TABLE {cat}.{pfx}_bronze.dim_series_ctas AS
  SELECT * FROM read_files(
    '{landing}/seed_dim_series/', format => 'csv', header => true
  )
""")
display(spark.table(f"{cat}.{pfx}_bronze.dim_series_ctas"))

# COMMAND ----------
# MAGIC %md ## 2) COPY INTO — incremental e idempotente por arquivo
# MAGIC Rode a célula **duas vezes**: a 2ª carrega `num_inserted_rows = 0`
# MAGIC (arquivos já processados são ignorados). `mergeSchema` habilita evolução.

# COMMAND ----------
spark.sql(f"CREATE TABLE IF NOT EXISTS {cat}.{pfx}_bronze.dim_series_copy")
r = spark.sql(f"""
  COPY INTO {cat}.{pfx}_bronze.dim_series_copy
  FROM '{landing}/seed_dim_series/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
  COPY_OPTIONS ('mergeSchema' = 'true')
""")
display(r)

# COMMAND ----------
# MAGIC %md ## 3) Auto Loader clássico (fora do SDP) — `cloudFiles` + checkpoint
# MAGIC - `cloudFiles.schemaLocation`: onde a inferência/evolução de schema é persistida
# MAGIC - `trigger(availableNow=True)`: processa todo o backlog em micro-lotes e **para**
# MAGIC   (ingestão incremental agendada — padrão batch do Auto Loader)
# MAGIC - `addNewColumns` (default): o stream **falha** na 1ª coluna nova e, ao reiniciar,
# MAGIC   segue com o schema evoluído; `rescue` congela o schema e desvia o excedente
# MAGIC   para `_rescued_data` — que também captura violações de tipo.

# COMMAND ----------
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", f"{ckpt}/sgs_demo_schema")
   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
   .load(f"{landing}/sgs/")
   .selectExpr("*", "_metadata.file_path AS _arquivo_origem")
   .writeStream
   .option("checkpointLocation", f"{ckpt}/sgs_demo")
   .trigger(availableNow=True)
   .toTable(f"{cat}.{pfx}_bronze.sgs_autoloader_demo")
).awaitTermination()

display(spark.sql(f"""
  SELECT _arquivo_origem, count(*) qtd, max(_rescued_data) exemplo_rescued
  FROM {cat}.{pfx}_bronze.sgs_autoloader_demo GROUP BY 1"""))

# COMMAND ----------
# MAGIC %md ### Outras fontes do guia (S2): JDBC/ODBC e REST
# MAGIC REST está implementado em `src/ingestion/*`. Para bancos relacionais, o padrão
# MAGIC em notebook é `spark.read.jdbc(url, tabela, properties)` orquestrado por Job —
# MAGIC e, em produção, os **Managed Connectors do Lakeflow Connect** (SQL Server,
# MAGIC Salesforce etc.) substituem esse código. No Free Edition não há como testar
# MAGIC JDBC contra um banco externo; estude a sintaxe e os trade-offs na documentação.
