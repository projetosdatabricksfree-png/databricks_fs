# PRD — BCB Lakehouse Lab
**Versão:** 1.0 · **Data:** 07/07/2026 · **Owner:** Diego Costa · **Status:** Aprovado

## 1. Contexto e problema
A certificação Databricks Certified Data Engineer Associate (guia vigente de 04/05/2026)
exige domínio prático de 7 domínios. Cursos e leitura de documentação não fixam sintaxe
nem trade-offs; falta um ambiente onde cada objetivo do blueprint seja **exercitado em
código de produção**, com custo zero (Databricks Free Edition) e dados reais brasileiros.

## 2. Objetivo do produto
Um lakehouse Medallion completo, implantável via IaC (Databricks Asset Bundles) no Free
Edition, que sirva simultaneamente como (a) laboratório cobrindo 100% do blueprint do
exame e (b) peça de portfólio nível staff (padrões de produção: qualidade declarativa,
CI/CD, observabilidade, governança, documentação de decisão).

## 3. Não-objetivos
- Não é um produto para usuários finais nem serviço com SLA.
- Não cobre tópicos exclusivos de clusters clássicos com prática (impossível no Free
  Edition) — esses ficam em estudo teórico dirigido (ver ROADMAP, Fase 3).
- Não usa serviços pagos de nuvem (S3/ADLS próprios, external locations).

## 4. Usuário e caso de uso
Engenheiro de dados sênior (background SQL Server) estudando para o exame: percorre o
roadmap fase a fase, desenvolve/roda cada componente, quebra e conserta o pipeline, e
usa os hardgates de cada fase como critério de avanço.

## 5. Métricas de sucesso
| Métrica | Alvo |
|---|---|
| Cobertura do blueprint (checklist do README) | 100% dos objetivos com artefato associado |
| Hardgates das fases do ROADMAP | 8/8 aprovados |
| Testes unitários (pytest) | 100% verdes, local e no `job_testes` |
| Pipeline SDP | update verde com expectations reportando no event log |
| `databricks bundle deploy` | dev e prod sem erros, com isolamento verificado |
| Simulados pós-projeto | ≥ 85% |

## 6. Requisitos funcionais
| ID | Requisito | Seção do exame | Artefato |
|---|---|---|---|
| RF-01 | Ingerir séries SGS/BCB como arquivos JSON em Volume UC (landing zone), com janela móvel que gera duplicatas propositais | S2 | `src/ingestion/fetch_bcb_sgs.py` |
| RF-02 | Ingerir API OData (payload aninhado) via REST orquestrado por Job | S2 | `src/ingestion/fetch_bcb_expectativas.py` |
| RF-03 | Gerar feed CDC sintético (INSERT/UPDATE/DELETE + sequência) | S3 | `src/ingestion/gera_cdc_clientes.py` |
| RF-04 | Bronze incremental com Auto Loader (`STREAM read_files` e `cloudFiles`), `_metadata`, `_rescued_data`, schema evolution (`addNewColumns` e `rescue`) | S2 | `sdp/01_bronze.sql`, `sdp/05_bronze_expectativas.py` |
| RF-05 | Comparativo executável CTAS × COPY INTO × Auto Loader com prova de idempotência | S2 | `notebooks/01_ingestao_comparativo.py` |
| RF-06 | Silver com flatten (`explode`), tipagem e expectations nos 3 modos (WARN/DROP/FAIL) | S3 | `sdp/02_silver.sql` |
| RF-07 | Dedup/upsert declarativo via `AUTO CDC ... STORED AS SCD TYPE 1` | S3 | `sdp/02_silver.sql` |
| RF-08 | Dimensão histórica via `AUTO CDC ... STORED AS SCD TYPE 2` com `APPLY AS DELETE WHEN` | S3 | `sdp/04_cdc_scd2_clientes.sql` |
| RF-09 | MERGE INTO idempotente + dedup determinístico (window) e `dropDuplicates` | S2/S3 | `notebooks/02_merge_dedup.py` |
| RF-10 | Gold como Materialized Views (agregações, `MAX_BY`, join expectativa×realizado) | S3 | `sdp/03_gold.sql` |
| RF-11 | Transformações PySpark do guia: joins/broadcast, union, agregações, `approx_count_distinct`, semiestruturado (`.`, `:`, VARIANT), parâmetros de tuning | S3/S6 | `notebooks/03_transformacoes.py` |
| RF-12 | Job time-based com cron, `for_each_task`, retries e parâmetros | S4 | `resources/job_ingestao.yml` |
| RF-13 | Job data-driven com `file_arrival` trigger, `pipeline_task`, task values, `condition_task` e ramos de sucesso/alerta (+ `table_update` documentado) | S4 | `resources/job_orquestracao.yml` |
| RF-14 | Auditoria de qualidade lendo o event log do SDP e publicando veredito | S3/S6 | `notebooks/06_auditoria_qualidade.py` |
| RF-15 | IaC completo: bundle com variables, targets dev/prod e overrides | S5 | `databricks.yml`, `resources/` |
| RF-16 | Testes: funções puras + pytest local, em CI (GitHub Actions) e no workspace | S5 | `src/lib/`, `tests/`, `.github/workflows/ci.yml` |
| RF-17 | Governança: GRANT/REVOKE, row filter, column mask, tags/ABAC, UNDROP, lineage, views seguras | S7 | `notebooks/04_governanca.sql`, `setup/00_setup.py` |
| RF-18 | Delta ops: time travel, RESTORE, OPTIMIZE, VACUUM, Liquid Clustering (`CLUSTER BY`/`AUTO`) | S1/S6 | `notebooks/05_manutencao_delta.sql` |

## 7. Requisitos não funcionais
- **Custo zero:** tudo roda no Free Edition (serverless); nenhuma dependência paga.
- **Idempotência:** reexecutar qualquer job/pipeline não duplica dados (Auto Loader
  checkpoint, COPY INTO por arquivo, AUTO CDC por sequência, MERGE condicional).
- **Reprodutibilidade:** ambiente 100% declarado em DAB; `deploy -t dev|prod` isola
  ambientes por prefixo de schema e modo do bundle.
- **Qualidade observável:** toda regra de qualidade é expectation com métrica no event
  log; nenhum filtro "silencioso".
- **Documentação viva:** decisões em ADRs; contratos de dados versionados; runbook
  operacional; CLAUDE.md ≤ 200 linhas (índice, padrão token-economy).

## 8. Restrições e premissas
1. Free Edition: sem clusters clássicos, sem external locations, usuário único,
   limites de recursos serverless — ver seção "Limitações" do README.
2. APIs BCB são públicas e sem autenticação; sujeitas a indisponibilidade eventual
   (mitigado por retries no job).
3. Catálogo padrão `workspace`; prefixos `bcb_dev_*`/`bcb_*` simulam ambientes.

## 9. Riscos
| Risco | Prob. | Impacto | Mitigação |
|---|---|---|---|
| Mudança de sintaxe SDP (produto em evolução rápida) | média | médio | ADRs citam a doc oficial; validar contra release notes antes da prova |
| API BCB fora do ar durante estudo | baixa | baixo | retries; reprocessar depois — nada é perdido (landing) |
| Recursos indisponíveis no Free Edition (ex.: ABAC beta) | média | baixo | tópicos marcados como teóricos no ROADMAP |

## 10. Referências
Guia do exame (mai/2026), documentação oficial (links no README), plano de estudos
de 7 semanas (documento companheiro deste projeto).
