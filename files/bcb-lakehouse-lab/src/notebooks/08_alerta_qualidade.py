# Databricks notebook source
# MAGIC %md # Alerta (ramo "qualidade ALERTA" do condition_task)

# COMMAND ----------
detalhe = dbutils.jobs.taskValues.get(taskKey="auditoria_qualidade",
                                      key="detalhe", default="sem detalhe")
msg = f"⚠️ Qualidade fora do limite — Gold NÃO certificada. {detalhe}"
print(msg)
# Em produção: notification destinations / e-mail do job. Falhar a task torna o
# problema visível no run e habilita o REPAIR RUN após correção (S4):
raise Exception(msg)
