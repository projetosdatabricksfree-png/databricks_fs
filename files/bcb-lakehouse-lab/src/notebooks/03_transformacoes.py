# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 3 — Transformações PySpark + parâmetros de tuning (Exame S3/S6)

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env_prefix", "bcb_dev")
cat, pfx = dbutils.widgets.get("catalog"), dbutils.widgets.get("env_prefix")
from pyspark.sql import functions as F

obs = spark.table(f"{cat}.{pfx}_silver.sgs_observacoes")
dim = spark.table(f"{cat}.{pfx}_bronze.dim_series_ctas") \
           .withColumn("codigo_serie", F.col("codigo_serie").cast("int"))

# COMMAND ----------
# MAGIC %md ## 1) Joins — inner, left, broadcast e múltiplas chaves
# MAGIC `broadcast(dim)` replica a tabela pequena em cada executor e **elimina o shuffle**
# MAGIC do lado grande. Automático quando a tabela < `spark.sql.autoBroadcastJoinThreshold`
# MAGIC (default 10 MB); o hint força o comportamento.

# COMMAND ----------
enriquecido = obs.join(F.broadcast(dim), on="codigo_serie", how="left") \
                 .select(obs["*"], dim["unidade"], dim["periodicidade"])
enriquecido.explain()      # procure BroadcastHashJoin no plano físico
display(enriquecido.limit(5))

anti = dim.join(obs, "codigo_serie", "left_anti")   # séries sem observações
multi = obs.alias("a").join(obs.alias("b"),
        on=[F.col("a.codigo_serie") == F.col("b.codigo_serie"),
            F.col("a.data_ref") == F.date_add(F.col("b.data_ref"), 1)],
        how="inner")                                # join em múltiplas condições

# COMMAND ----------
# MAGIC %md ## 2) union × unionByName
# MAGIC `union` casa colunas **por posição** (perigoso); `unionByName` casa por nome e,
# MAGIC com `allowMissingColumns=True`, preenche ausentes com NULL.

# COMMAND ----------
a = obs.select("codigo_serie", "data_ref", "valor")
b = obs.select("codigo_serie", "data_ref").withColumn("fonte", F.lit("reprocesso"))
unido = a.unionByName(b, allowMissingColumns=True)
print(unido.columns)

# COMMAND ----------
# MAGIC %md ## 3) Agregações e manipulação de colunas do guia

# COMMAND ----------
resumo = (obs.groupBy("codigo_serie")
    .agg(F.count("*").alias("qtd"),
         F.countDistinct("data_ref").alias("dias_exatos"),        # exato: caro (shuffle)
         F.approx_count_distinct("data_ref").alias("dias_aprox"), # HyperLogLog: barato
         F.round(F.avg("valor"), 4).alias("media"),
         F.min("valor").alias("min"), F.max("valor").alias("max")))
display(resumo)
display(obs.select("valor").summary())      # count/mean/stddev/quartis

renome = (obs.withColumn("ano", F.year("data_ref"))
             .withColumnRenamed("valor", "valor_indicador")
             .withColumn("partes_nome", F.split("nome_serie", "_"))
             .filter(F.col("ano") >= 2024)
             .drop("_ingerido_em"))
display(renome.limit(5))

# COMMAND ----------
# MAGIC %md ## 4) Semiestruturado: dot notation, `:` e VARIANT

# COMMAND ----------
display(spark.sql(f"""
  SELECT payload.`@odata.context`                        AS contexto,     -- dot notation
         payload.value[0].Indicador                      AS primeiro_indicador,
         to_json(payload.value[0])                       AS como_json,
         to_json(payload.value[0]):Mediana :: double     AS mediana_via_dois_pontos
  FROM {cat}.{pfx}_bronze.expectativas_raw LIMIT 3
"""))

# COMMAND ----------
# MAGIC %md ## 5) Parâmetros de tuning (S3/S6) — teoria obrigatória
# MAGIC | Parâmetro | Controla | Default |
# MAGIC |---|---|---|
# MAGIC | `spark.sql.shuffle.partitions` | nº de partições pós-shuffle (joins/agg) | 200 (AUTO c/ AQE) |
# MAGIC | `spark.default.parallelism` | paralelismo default de RDDs | nº de cores |
# MAGIC | `spark.sql.autoBroadcastJoinThreshold` | limite p/ broadcast automático | 10 MB |
# MAGIC | `spark.executor.memory` / `spark.driver.memory` | heap de executor/driver | por node type |
# MAGIC No **serverless** (Free Edition) esses knobs são gerenciados pela plataforma —
# MAGIC exatamente o trade-off que o exame cobra: serverless = zero tuning manual;
# MAGIC clusters clássicos = controle fino + responsabilidade de dimensionar.

# COMMAND ----------
for k in ("spark.sql.shuffle.partitions",
          "spark.sql.adaptive.enabled",
          "spark.sql.autoBroadcastJoinThreshold"):
    try:
        print(k, "=", spark.conf.get(k))
    except Exception as e:
        print(k, "→ gerenciado/indisponível no serverless:", type(e).__name__)
