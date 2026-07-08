# BCB Lakehouse Lab — Projeto de estudo para a Databricks Certified Data Engineer Associate

Lakehouse completo (Medallion) no **Databricks Free Edition**, usando dados públicos reais do
**Banco Central do Brasil** (API SGS e API Olinda de Expectativas de Mercado) + um feed CDC
sintético de clientes. Cada fase do projeto mapeia uma seção do guia oficial do exame
(versão vigente, mai/2026) e uma semana do plano de estudos.

```
                         ┌────────────────────── Lakeflow Jobs ──────────────────────┐
 APIs BCB ──► job_ingestao (cron, for_each, retries) ──► /Volumes/.../landing/*.json │
                                                              │ (file arrival trigger)
                                                              ▼
                                    job_orquestracao ──► Pipeline Lakeflow SDP
                                    (pipeline_task →         │
                                     auditoria →       BRONZE (Auto Loader/read_files, _metadata,
                                     condition_task →         _rescued_data, schema evolution)
                                     publicar | alerta)      │
                                                        SILVER (expectations WARN/DROP/FAIL,
                                                              explode, cast, AUTO CDC SCD1 = dedup/upsert,
                                                              AUTO CDC SCD2 = histórico de clientes)
                                                             │
                                                        GOLD  (Materialized Views: agregações, joins)
```

## Pré-requisitos

1. Conta no **Databricks Free Edition** → https://www.databricks.com/learn/free-edition
2. **Databricks CLI** ≥ 0.230 → https://docs.databricks.com/aws/en/dev-tools/cli/install
3. Git + (opcional) conta GitHub para praticar Git Folders e CI.

## Setup (15 min)

```bash
# 1. Autentique a CLI no seu workspace Free Edition (cria o profile DEFAULT)
databricks auth login --host https://<seu-workspace>.cloud.databricks.com

# 2. Valide e implante o bundle no target dev
databricks bundle validate
databricks bundle deploy -t dev

# 3. Crie schemas e volumes (bronze/silver/gold + landing/checkpoints)
databricks bundle run job_setup -t dev

# 4. Primeira carga: busca as séries do BCB e grava JSON na landing zone
databricks bundle run job_ingestao -t dev

# 5. Rode o pipeline + auditoria (ou aguarde o file arrival trigger disparar)
databricks bundle run job_orquestracao -t dev
```

> `mode: development` prefixa os recursos com `[dev <seu_user>]` e pausa schedules/triggers
> automaticamente — comportamento cobrado no exame (Seção 5). Em `-t prod` nada é pausado.

## Roadmap de fases (= semanas do plano de estudos)

| Fase | Semana | O que fazer | Arquivos | Seções do exame |
|---|---|---|---|---|
| 0 | 1 | Setup, explorar UC, Delta/time travel | `src/setup/00_setup.py`, `src/notebooks/05_manutencao_delta.sql` | 1 |
| 1 | 2 | Comparar CTAS × COPY INTO × Auto Loader | `src/notebooks/01_ingestao_comparativo.py` | 2 |
| 2 | 3 | Ingestão de APIs, JSON aninhado, MERGE | `src/ingestion/*`, `src/notebooks/02_merge_dedup.py` | 2 |
| 3 | 4 | Transformações PySpark + tuning | `src/notebooks/03_transformacoes.py` | 3, 6 |
| 4 | 5 | Pipeline SDP: expectations + AUTO CDC | `src/pipelines/sdp/*` | 3 |
| 5 | 6 | Jobs (DAG, triggers, repair) + DABs + pytest | `resources/*.yml`, `tests/*`, `.github/workflows/ci.yml` | 4, 5 |
| 6 | 7 | Governança (grants, masks, tags) + observabilidade | `src/notebooks/04_governanca.sql`, `06_auditoria_qualidade.py` | 6, 7 |

## Checklist de cobertura do blueprint (mai/2026)

- [x] S1 — Tipos de compute e serverless: discutido em `03_transformacoes.py` e `docs/ADR-001`
- [x] S2 — CTAS, COPY INTO, Auto Loader (schema inference/evolution, `_rescued_data`, `_metadata`), JDBC/REST, MERGE, matriz de decisão
- [x] S3 — Medallion, explode/flatten, joins/broadcast, union, dedup, `approx_count_distinct`, ST × MV × view, expectations (3 modos), AUTO CDC SCD1/SCD2, parâmetros de tuning
- [x] S4 — Job multi-task, `for_each`, `condition_task`, retries, cron × file arrival × table update, repair run (runbook)
- [x] S5 — Git Folders, DABs (targets, variables, overrides), CLI `bundle validate/deploy/run`, pytest
- [x] S6 — Event log, run history, skew/shuffle/spill, Liquid Clustering, Predictive Optimization
- [x] S7 — Managed × external, GRANT/REVOKE, row filter, column mask, tags/ABAC, lineage, UNDROP

## Limitações do Free Edition (e como este projeto contorna)

O Free Edition é 100% **serverless**: não há clusters clássicos, portanto configuração de cluster,
bibliotecas de cluster e a maior parte dos `spark.conf` são gerenciados pela plataforma
(isso, em si, é um tópico do exame). Não há external locations → tabelas **external** são cobertas
por documentação e demonstração conceitual em `04_governanca.sql`, não por lab. Grants são
demonstrados contra o grupo `account users` (workspace de usuário único).

## Documentação de referência

Delta: https://docs.databricks.com/aws/en/delta/ · Auto Loader: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/ · SDP: https://docs.databricks.com/aws/en/dlt/ · Jobs: https://docs.databricks.com/aws/en/jobs/ · Bundles: https://docs.databricks.com/aws/en/dev-tools/bundles/ · Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/

## Documentação do projeto

PRD: `docs/PRD.md` · Roadmap com hardgates: `docs/ROADMAP.md` · Decisões: `docs/ADR-*.md` · Contratos de dados: `docs/data-contracts.md` · Operação: `docs/runbook.md`
