# Runbook operacional

## Deploy e execução
```bash
databricks bundle validate                 # lint da configuração
databricks bundle deploy -t dev            # sincroniza código + recursos
databricks bundle run job_setup -t dev     # 1ª vez por target
databricks bundle run job_ingestao -t dev  # carga (ou aguardar cron)
databricks bundle run job_orquestracao -t dev
databricks bundle run job_testes -t dev
databricks bundle summary -t dev           # o que está implantado
databricks bundle destroy -t dev           # remove recursos do target
```

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

## Higiene
- `VACUUM` manual é dispensável em managed tables (Predictive Optimization cuida);
  ao praticar, nunca reduza a retenção < 7 dias em tabela com time travel em uso.
- Landing: arquivos podem ser arquivados após N dias (não deletar antes de um
  eventual full refresh planejado).
