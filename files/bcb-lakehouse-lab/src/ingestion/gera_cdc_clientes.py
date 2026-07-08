# Databricks notebook source
# MAGIC %md
# MAGIC # Gerador de feed CDC sintético — clientes (Exame S3: AUTO CDC / SCD2)
# MAGIC Simula eventos de change data capture (INSERT/UPDATE/DELETE) com coluna de
# MAGIC sequenciamento `ts_evento`, gravados como JSON na landing. O pipeline SDP
# MAGIC aplica `AUTO CDC INTO ... STORED AS SCD TYPE 2` sobre esse feed.
# MAGIC Nota: o AUTO CDC tem semântica de upsert — um UPDATE de chave inexistente insere.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env_prefix", "bcb_dev")
dbutils.widgets.text("qtd_eventos", "40")

import json, pathlib, random
from datetime import datetime, timezone

landing = (f"/Volumes/{dbutils.widgets.get('catalog')}/"
           f"{dbutils.widgets.get('env_prefix')}_bronze/landing")
random.seed()  # execuções diferentes → histórias SCD2 diferentes

UFS = ["SP", "RJ", "MG", "RS", "PR", "BA"]
SEGMENTOS = ["varejo", "private", "corporate"]
eventos = []
for _ in range(int(dbutils.widgets.get("qtd_eventos"))):
    cid = random.randint(1, 20)
    op = random.choices(["INSERT", "UPDATE", "DELETE"], weights=[3, 6, 1])[0]
    eventos.append({
        "cliente_id": cid,
        "nome": f"Cliente {cid:03d}",
        "uf": random.choice(UFS),
        "segmento": random.choice(SEGMENTOS),
        "email": f"cliente{cid:03d}@exemplo.com.br",
        "cpf": f"{random.randint(100, 999)}.***.***-{random.randint(10, 99)}",
        "operacao": op,
        "ts_evento": datetime.now(timezone.utc).isoformat(),
    })

ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S%f")
destino = pathlib.Path(f"{landing}/clientes_cdc")
destino.mkdir(parents=True, exist_ok=True)
(destino / f"cdc_{ts}.json").write_text(
    "\n".join(json.dumps(e, ensure_ascii=False) for e in eventos))
print(f"{len(eventos)} eventos CDC gravados em {destino}")
