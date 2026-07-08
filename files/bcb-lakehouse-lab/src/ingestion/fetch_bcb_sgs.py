# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão SGS/BCB → landing zone (Exame S2)
# MAGIC Busca uma série temporal na API pública SGS e grava o payload como **arquivo JSON**
# MAGIC em um Volume UC. Padrão realista: sistemas-fonte "dropam" arquivos; o Auto Loader
# MAGIC ingere incrementalmente. A janela móvel de N dias gera sobreposição proposital
# MAGIC → duplicatas na Bronze → tratadas na Silver (dedup/upsert via AUTO CDC SCD1).
# MAGIC API: https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json

# COMMAND ----------
dbutils.widgets.text("serie_codigo", "11")
dbutils.widgets.text("serie_nome", "selic_diaria")
dbutils.widgets.text("dias_backfill", "30")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env_prefix", "bcb_dev")

import json, pathlib, requests
from datetime import date, datetime, timedelta, timezone

codigo = int(dbutils.widgets.get("serie_codigo"))
nome = dbutils.widgets.get("serie_nome")
dias = int(dbutils.widgets.get("dias_backfill"))
landing = (f"/Volumes/{dbutils.widgets.get('catalog')}/"
           f"{dbutils.widgets.get('env_prefix')}_bronze/landing")

# COMMAND ----------
fim, ini = date.today(), date.today() - timedelta(days=dias)
url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
       f"?formato=json&dataInicial={ini:%d/%m/%Y}&dataFinal={fim:%d/%m/%Y}")
resp = requests.get(url, timeout=30)
resp.raise_for_status()
observacoes = resp.json()  # [{"data": "01/07/2026", "valor": "..."}, ...]
if not observacoes:
    dbutils.notebook.exit(f"Série {codigo}: sem observações na janela — nada a gravar.")

envelope = {
    "codigo_serie": codigo,
    "nome_serie": nome,
    "coletado_em": datetime.now(timezone.utc).isoformat(),
    "fonte": url,
    "observacoes": observacoes,   # array aninhado → prática de explode na Silver
}

# COMMAND ----------
ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
destino = pathlib.Path(f"{landing}/sgs/serie={codigo}")
destino.mkdir(parents=True, exist_ok=True)
arquivo = destino / f"sgs_{codigo}_{ts}.json"
arquivo.write_text(json.dumps(envelope, ensure_ascii=False))
print(f"{len(observacoes)} observações → {arquivo}")
