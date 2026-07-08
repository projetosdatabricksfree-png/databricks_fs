# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão Expectativas de Mercado (Olinda/BCB) → landing (Exame S2)
# MAGIC API OData com resposta **aninhada** (`{"@odata.context": ..., "value": [...]}`)
# MAGIC — matéria-prima para flatten/explode e para o join Gold expectativa × realizado.
# MAGIC Este notebook também exemplifica ingestão **via REST em notebook orquestrado por
# MAGIC Job** — um dos padrões listados no guia do exame ao lado de JDBC/ODBC.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env_prefix", "bcb_dev")
dbutils.widgets.text("top", "300")

import json, pathlib, requests
from datetime import datetime, timezone

landing = (f"/Volumes/{dbutils.widgets.get('catalog')}/"
           f"{dbutils.widgets.get('env_prefix')}_bronze/landing")
url = ("https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
       "ExpectativaMercadoMensais?$format=json&$orderby=Data%20desc"
       f"&$top={dbutils.widgets.get('top')}"
       "&$filter=Indicador%20eq%20'IPCA'")

resp = requests.get(url, timeout=60)
resp.raise_for_status()
envelope = {"coletado_em": datetime.now(timezone.utc).isoformat(),
            "endpoint": url,
            "payload": resp.json()}

ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
destino = pathlib.Path(f"{landing}/expectativas")
destino.mkdir(parents=True, exist_ok=True)
(destino / f"expectativas_ipca_{ts}.json").write_text(
    json.dumps(envelope, ensure_ascii=False))
print(f"{len(envelope['payload'].get('value', []))} registros gravados na landing")
