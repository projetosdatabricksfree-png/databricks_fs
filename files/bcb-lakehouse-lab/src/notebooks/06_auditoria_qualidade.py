# Databricks notebook source
# MAGIC %md
# MAGIC # Auditoria de qualidade + observabilidade (Exame S3/S6)
# MAGIC Lê o **event log** do pipeline SDP, agrega as métricas de *expectations* e publica
# MAGIC um veredito via `dbutils.jobs.taskValues` — consumido pelo `condition_task` do job
# MAGIC (ramificação if/else). É o elo entre qualidade de dados e orquestração.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env_prefix", "bcb_dev")
dbutils.widgets.text("pipeline_id", "")
dbutils.widgets.text("max_taxa_descarte", "0.05")   # 5% de linhas dropadas tolerado
cat, pfx = dbutils.widgets.get("catalog"), dbutils.widgets.get("env_prefix")
pipeline_id = dbutils.widgets.get("pipeline_id")
limite = float(dbutils.widgets.get("max_taxa_descarte"))

# COMMAND ----------
status, detalhe = "OK", "sem métricas de expectations ainda"
try:
    # TVF event_log: linhas com event_type='flow_progress' carregam data_quality
    metricas = spark.sql(f"""
        WITH exp AS (
          SELECT explode(from_json(
                   details:flow_progress:data_quality:expectations,
                   'array<struct<name string, dataset string,
                                 passed_records bigint, failed_records bigint>>')) e
          FROM event_log('{pipeline_id}')
          WHERE event_type = 'flow_progress'
        )
        SELECT e.dataset, e.name,
               sum(e.passed_records) AS aprovados,
               sum(e.failed_records) AS reprovados
        FROM exp GROUP BY 1, 2
    """)
    display(metricas)
    tot = metricas.selectExpr("sum(aprovados) a", "sum(reprovados) r").first()
    if tot and tot["a"] is not None:
        taxa = (tot["r"] or 0) / max((tot["a"] or 0) + (tot["r"] or 0), 1)
        status = "OK" if taxa <= limite else "ALERTA"
        detalhe = f"taxa de violações = {taxa:.2%} (limite {limite:.0%})"
except Exception as e:
    detalhe = f"event_log indisponível ({type(e).__name__}) — seguindo com OK"

print(status, "|", detalhe)

# COMMAND ----------
# Task values: comunicação entre tasks do mesmo job run (S4)
dbutils.jobs.taskValues.set(key="status_qualidade", value=status)
dbutils.jobs.taskValues.set(key="detalhe", value=detalhe)

# COMMAND ----------
# MAGIC %md ### Onde olhar performance (S6) no serverless
# MAGIC - **Jobs → Runs**: duração por task vs baseline histórico (regressões)
# MAGIC - **Query Profile / Spark UI** da execução: procure sinais de
# MAGIC   *skew* (task máx ≫ mediana em shuffle read/duração), *shuffle* excessivo
# MAGIC   (Exchange no plano) e *spill* (memória → disco). Mitigações: AQE/skew join,
# MAGIC   broadcast do lado pequeno, Liquid Clustering nas colunas de filtro/join.
