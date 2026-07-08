# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Layout do repositório

O projeto real é **`files/bcb-lakehouse-lab/`** — lakehouse Medallion no Databricks Free Edition,
laboratório para a certificação Databricks Data Engineer Associate e peça de portfólio. Trabalhe
sempre de dentro dessa pasta.

- `PRD_CASE.MD` (raiz) — atribuições da vaga/case de arquitetura que motiva o projeto; contexto, não código.
- `files/PRD-bcb-lakehouse-lab.md` e `files/ROADMAP-bcb-lakehouse-lab.md` — **cópias idênticas** de
  `files/bcb-lakehouse-lab/docs/{PRD,ROADMAP}.md`. Trate `docs/` como fonte da verdade e replique
  qualquer edição na cópia correspondente.

## Stack

- Databricks Free Edition (serverless-only) + Unity Catalog (catálogo `workspace`).
- Lakeflow: SDP (pipelines declarativos), Jobs, Auto Loader via `read_files`.
- Deploy: Databricks Asset Bundles (DAB) + Databricks CLI.
- Dados: APIs públicas do BCB (SGS, Olinda) + feed CDC sintético de clientes.

## Comandos

Todos rodam de dentro de `files/bcb-lakehouse-lab/`:

```bash
databricks bundle validate            # lint da configuração DAB
databricks bundle deploy -t dev      # target dev é o default
databricks bundle run <job> -t dev   # job_setup | job_ingestao | job_orquestracao | job_testes
pytest tests/ -v                     # testes unitários locais (funções puras, sem Spark)
pytest tests/test_transforms.py::test_para_data_sgs -v   # um único teste
```

`job_setup` roda uma vez por target (cria schemas/volumes). CI (GitHub Actions) executa pytest e
`bundle validate` (exige secrets `DATABRICKS_HOST`/`DATABRICKS_TOKEN`).

## Arquitetura (visão geral)

Fluxo de dados: `job_ingestao` (cron, `for_each_task` sobre as séries SGS, retries — código em
`src/ingestion/`) busca APIs públicas do BCB e grava JSON em
`/Volumes/<catalog>/<env_prefix>_bronze/landing/`. O trigger de file arrival dispara
`job_orquestracao`: pipeline SDP → auditoria do event log (`06_auditoria_qualidade.py` publica
task value `status_qualidade`) → `condition_task` → `publicar_gold` ou `alerta_qualidade`.

Pipeline SDP (`src/pipelines/sdp/`, um único pipeline publica nas 3 camadas via nomes totalmente
qualificados): Bronze (Auto Loader `STREAM read_files`, schema evolution, `_rescued_data`) →
Silver (explode/cast, expectations WARN/DROP/FAIL, dedup via `AUTO CDC ... STORED AS SCD TYPE 1`,
histórico de clientes via SCD TYPE 2) → Gold (Materialized Views).

Fluxo de parâmetros (padrão transversal a todos os arquivos):
- `databricks.yml` define `variables` (`catalog`, `env_prefix`, `series_sgs`); targets dev/prod fazem override.
- Jobs (`resources/*.yml`) repassam via `base_parameters` → notebooks leem com `dbutils.widgets`.
- O pipeline repassa via `configuration` → SQL usa substituição `${...}`; Python SDP usa `spark.conf.get()`.

Isolamento de ambientes: mesmo catálogo (`workspace` no Free Edition), schemas prefixados
`bcb_dev_*` (dev) / `bcb_*` (prod) via `var.env_prefix`; `mode: development` prefixa recursos com
`[dev <user>]` e pausa schedules/triggers automaticamente.

Testes: lógica de negócio vive como funções puras em `src/lib/transforms.py` (sem dependência de
Spark) para rodar via pytest local, no CI e no workspace (`job_testes`). Lógica nova segue esse padrão.

Restrições que afetam código:
- Free Edition é serverless-only: sem configuração de cluster; `spark.conf.set` de tuning falha (esperado).
- Tabelas alvo de `AUTO CDC` são gerenciadas pelo pipeline — nunca aplicar DML manual (ADR-002).
- Bronze é imutável/auditável; duplicatas lá são propositais e resolvidas na Silver (ADR-001).
- A landing zone é a fonte da verdade para full refresh — não deletar arquivos dela.

## Convenções

- Schemas: `bcb_dev_{bronze|silver|gold}` (dev) / `bcb_{...}` (prod) — via `var.env_prefix`.
- Nomes de tabelas/colunas em pt-BR snake_case; colunas técnicas com prefixo `_`.
- SQL onde possível; Python quando necessário (mesma regra do exame).
- Commits: conventional commits.

## Decisões arquiteturais (resumo — detalhes em docs/ADR-*.md)

- Auto Loader (streaming `read_files`) na Bronze, não COPY INTO: escala, checkpoint, schema evolution.
- Dedup/upsert na Silver com AUTO CDC SCD1 (declarativo) em vez de MERGE em notebook; MERGE fica
  didático em `02_merge_dedup.py`.
- Um único pipeline SDP publica nas 3 camadas via nomes qualificados + parâmetros `${...}`.

Documentação: decisões em `docs/ADR-*.md` · contratos por camada em `docs/data-contracts.md` ·
operação e troubleshooting em `docs/runbook.md` · fases e critérios de avanço em `docs/ROADMAP.md`.

## Regras de sessão (Claude Code)

- Seguir skill `token-economy`: Sonnet padrão, Plan Mode antes de implementar, `/compact` a ~60%,
  referências cirúrgicas (`@arquivo`), `/clear` ao trocar de fase do roadmap.
