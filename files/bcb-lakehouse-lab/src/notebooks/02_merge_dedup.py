# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 2 — MERGE INTO + estratégias de deduplicação (Exame S2/S3)
# MAGIC O pipeline SDP resolve dedup com AUTO CDC; aqui você domina o **MERGE manual**
# MAGIC (e quando cada abordagem é a resposta certa na prova).

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env_prefix", "bcb_dev")
cat, pfx = dbutils.widgets.get("catalog"), dbutils.widgets.get("env_prefix")
from pyspark.sql import functions as F, Window as W

# COMMAND ----------
# MAGIC %md ## 1) Dedup: dropDuplicates × janela determinística
# MAGIC `dropDuplicates(["chave"])` mantém uma linha **arbitrária** por chave — ok quando
# MAGIC as linhas são idênticas. Com versões conflitantes, use `row_number()` ordenado
# MAGIC pelo critério de desempate (ex.: mais recente vence) — determinístico e auditável.

# COMMAND ----------
stg = spark.table(f"{cat}.{pfx}_silver.sgs_observacoes_stg")
print("linhas stg:", stg.count(),
      "| chaves distintas:", stg.select("codigo_serie", "data_ref").distinct().count())

dedup_arbitrario = stg.dropDuplicates(["codigo_serie", "data_ref"])

janela = W.partitionBy("codigo_serie", "data_ref").orderBy(F.col("_ingerido_em").desc())
dedup_deterministico = (stg.withColumn("rn", F.row_number().over(janela))
                           .filter("rn = 1").drop("rn"))
assert dedup_arbitrario.count() == dedup_deterministico.count()
display(dedup_deterministico.orderBy("codigo_serie", F.col("data_ref").desc()).limit(10))

# COMMAND ----------
# MAGIC %md ## 2) MERGE INTO (upsert SCD1 manual)
# MAGIC Regra de ouro: **deduplique a fonte antes do MERGE** — múltiplas linhas de origem
# MAGIC casando com a mesma linha de destino causam erro em runtime.

# COMMAND ----------
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {cat}.{pfx}_silver.sgs_merge_scd1
  (codigo_serie INT, data_ref DATE, valor DOUBLE, _atualizado_em TIMESTAMP)
""")
dedup_deterministico.createOrReplaceTempView("fonte_dedup")

resultado = spark.sql(f"""
  MERGE INTO {cat}.{pfx}_silver.sgs_merge_scd1 AS t
  USING (SELECT codigo_serie, data_ref, valor FROM fonte_dedup) AS s
    ON  t.codigo_serie = s.codigo_serie AND t.data_ref = s.data_ref
  WHEN MATCHED AND t.valor <> s.valor THEN
    UPDATE SET t.valor = s.valor, t._atualizado_em = current_timestamp()
  WHEN NOT MATCHED THEN
    INSERT (codigo_serie, data_ref, valor, _atualizado_em)
    VALUES (s.codigo_serie, s.data_ref, s.valor, current_timestamp())
""")
display(resultado)  # métricas: num_inserted_rows / num_updated_rows

# COMMAND ----------
# MAGIC %md
# MAGIC Reexecute a célula acima: `num_inserted/updated = 0` → **MERGE idempotente**
# MAGIC (a condição `t.valor <> s.valor` evita update inútil e reescrita de arquivos).
# MAGIC
# MAGIC Variações que caem em prova:
# MAGIC - `WHEN NOT MATCHED BY SOURCE THEN DELETE` — remove do destino o que sumiu da
# MAGIC   fonte (sincronização completa; use com fonte FULL, nunca com incremental!)
# MAGIC - `MERGE WITH SCHEMA EVOLUTION INTO ...` — evolui o schema do destino durante o merge
# MAGIC - `DELETE` lógico via CDC → no SDP, `APPLY AS DELETE WHEN` (veja 04_cdc_scd2)
