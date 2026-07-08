# Databricks notebook source
# MAGIC %md # Publicação Gold (ramo "qualidade OK" do condition_task)

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env_prefix", "bcb_dev")
cat, pfx = dbutils.widgets.get("catalog"), dbutils.widgets.get("env_prefix")

for t in ("indicadores_mensais", "ipca_expectativa_vs_realizado"):
    spark.sql(f"COMMENT ON TABLE {cat}.{pfx}_gold.{t} IS "
              f"'Certificada pela auditoria de qualidade em {{ts}}'"
              .replace("{ts}", spark.sql("SELECT current_timestamp()").first()[0].isoformat()))
    print(f"gold.{t} certificada")
display(spark.table(f"{cat}.{pfx}_gold.indicadores_mensais").orderBy("mes", ascending=False).limit(12))
