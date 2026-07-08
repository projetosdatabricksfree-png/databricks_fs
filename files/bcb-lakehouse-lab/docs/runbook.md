# Runbook operacional

## Deploy e execução
```bash
databricks bundle validate                 # lint da configuração
# BOOTSTRAP (workspace nova): o job_orquestracao tem file-arrival trigger que exige
# o volume landing JÁ existente no deploy. Crie schemas+volumes ANTES do 1º deploy:
databricks api post /api/2.0/sql/statements --json '{"warehouse_id":"<id>","statement":"CREATE SCHEMA IF NOT EXISTS workspace.bcb_dev_bronze","wait_timeout":"30s"}'
# ... idem silver, gold e CREATE VOLUME landing/checkpoints (ver bcb_dev_bronze.landing).
databricks bundle deploy -t dev            # sincroniza código + recursos
databricks bundle run job_setup -t dev     # 1ª vez por target (idempotente: reforça schemas/volumes/grants)
databricks bundle run job_ingestao -t dev  # carga (ou aguardar cron)
databricks bundle run job_orquestracao -t dev
databricks bundle run job_testes -t dev
databricks bundle summary -t dev           # o que está implantado
databricks bundle destroy -t dev           # remove recursos do target
```

> **CLI + Terraform (erro `openpgp: key expired`):** algumas versões da CLI falham ao
> baixar/verificar o Terraform. Contorne apontando um binário local:
> `export DATABRICKS_TF_EXEC_PATH=/caminho/terraform DATABRICKS_TF_VERSION=<versão>`.

## Procedimentos
**Repair run (S4):** Jobs → run com falha → corrigir a causa → botão *Repair run*
→ somente as tasks falhas (e dependentes) reexecutam; as verdes são preservadas.

**Backfill/reprocesso total:** rodar o pipeline com *Full refresh* (UI) ou
`pipeline_task.full_refresh: true` temporariamente. Atenção: full refresh trunca e
reconstrói as streaming tables a partir da landing (a landing é a fonte da verdade).

**Evolução de schema na fonte:** esperado que o update falhe 1× (addNewColumns) e
recupere no retry. Se a coluna nova for lixo, mude a fonte para `rescue` e trate via
`_rescued_data`.

**Duplicata na Gold:** conferir `SEQUENCE BY` do AUTO CDC e se alguém fez DML manual
na tabela alvo (proibido — ver ADR-002).

## Troubleshooting no Free Edition
| Sintoma | Causa provável | Ação |
|---|---|---|
| `PERMISSION_DENIED` ao criar catálogo | FE restringe criação | usar catálogo `workspace` (default do projeto) |
| file arrival não dispara | trigger pausado (mode development) | rodar manualmente ou `deploy -t prod` |
| `spark.conf.set` falha | serverless gerencia configs | comportamento esperado (tópico de prova) |
| event_log() vazio | pipeline ainda sem updates | rodar o pipeline 1× antes da auditoria |
| `_rescued_data` vazio apesar de dado "errado" | Auto Loader JSON usa `inferColumnTypes=false` → escalares viram STRING (não há conflito de tipo a resgatar) | resgate de tipo aparece no SDP Bronze (`read_files` infere tipos) ou habilite `cloudFiles.inferColumnTypes=true` |
| `job_ingestao` falha em série mensal (IPCA/433) | API SGS retorna **404** para janela sem observação (não array vazio) | tratado em `fetch_bcb_sgs.py` (404 → skip); janela precisa cobrir uma divulgação |

## Higiene
- `VACUUM` manual é dispensável em managed tables (Predictive Optimization cuida);
  ao praticar, nunca reduza a retenção < 7 dias em tabela com time travel em uso.
- Landing: arquivos podem ser arquivados após N dias (não deletar antes de um
  eventual full refresh planejado).
