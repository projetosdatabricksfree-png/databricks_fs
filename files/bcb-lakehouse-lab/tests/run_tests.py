# Databricks notebook source
# MAGIC %md # Runner de testes no Databricks (Exame S5)
# MAGIC Executa o pytest dentro do workspace (serverless). Nos notebooks recentes,
# MAGIC o diretório de trabalho é a pasta do próprio notebook — os testes estão ao lado.

# COMMAND ----------
# MAGIC %pip install -q pytest

# COMMAND ----------
import os, sys, pytest
sys.dont_write_bytecode = True                      # workspace files são read-only p/ .pyc
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])
assert retcode == 0, f"pytest falhou (retcode={retcode}) — veja o log acima"
print("✅ Todos os testes passaram")
