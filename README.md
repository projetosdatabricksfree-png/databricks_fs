# BCB Lakehouse Lab

**Lakehouse Medallion de ponta a ponta no Databricks, com dados macroeconômicos reais do Banco Central do Brasil — implantado 100% como código e validado por CI, a custo zero (Free Edition serverless).**

![CI](https://github.com/projetosdatabricksfree-png/databricks_fs/actions/workflows/ci.yml/badge.svg)
![Plataforma](https://img.shields.io/badge/Databricks-Free%20Edition%20(serverless)-FF3621)
![IaC](https://img.shields.io/badge/IaC-Asset%20Bundles-informational)
![Custo](https://img.shields.io/badge/custo-R%240-brightgreen)

Pipeline de dados declarativo, versionado e governado que ingere indicadores públicos do BCB
(câmbio, Selic, IPCA e expectativas de mercado), aplica qualidade de dados **observável** em cada
camada e entrega tabelas prontas para análise. Construído como **peça de portfólio de engenharia
de dados** e como laboratório completo para a certificação
*Databricks Certified Data Engineer Associate*.

> **📂 O projeto vive em [`files/bcb-lakehouse-lab/`](https://github.com/projetosdatabricksfree-png/databricks_fs/tree/main/files/bcb-lakehouse-lab).**
> Comece pelo **[README completo do projeto »](https://github.com/projetosdatabricksfree-png/databricks_fs/blob/main/files/bcb-lakehouse-lab/README.md)** — visão de
> negócio, arquitetura detalhada, o porquê de cada etapa e o passo a passo de execução.

---

## Arquitetura em uma imagem

Orquestração em duas camadas do Lakeflow: **Jobs** (o "quando" e o "controle") disparam o
**Pipeline SDP** (o "o quê" e a "qualidade"). O acoplamento é um *file arrival trigger* — o dado
chegando na landing zone é o próprio gatilho, não um relógio.

```
                         APIs públicas do BCB
              SGS (câmbio, Selic, IPCA)   Olinda/OData (Focus)      feed CDC sintético
                        └───────────┬────────────┴───────────┬───────────┘
                                    ▼                         ▼
              job_ingestao (cron · for_each · retries) ─► landing zone (Volume UC, fonte da verdade)
                                    │  (file arrival trigger)
                                    ▼
              job_orquestracao: pipeline ─► auditoria (event log) ─► condition_task
                                                          │
                                       OK ─► publicar_gold │ falha ─► alerta_qualidade
                                    ▼
        Pipeline SDP (1 pipeline, 3 camadas):
          BRONZE  ── Auto Loader (STREAM read_files) · schema evolution · _rescued_data · imutável
          SILVER  ── explode/cast · expectations WARN/DROP/FAIL · AUTO CDC SCD1 (dedup) · SCD2 (histórico)
          GOLD    ── Materialized Views · row filter + column mask sobre PII
```

## Stack

- **Databricks Free Edition** (serverless-only) + **Unity Catalog**
- **Lakeflow**: SDP (pipelines declarativos), Jobs, Auto Loader via `read_files`
- **IaC**: Databricks Asset Bundles (DAB) + Databricks CLI
- **CI**: GitHub Actions (`pytest` + `databricks bundle validate`)
- **Dados**: APIs públicas do BCB (SGS, Olinda) + feed CDC sintético de clientes

## Navegação

| Onde | O quê |
|------|-------|
| **[README completo »](https://github.com/projetosdatabricksfree-png/databricks_fs/blob/main/files/bcb-lakehouse-lab/README.md)** | Documentação principal do projeto |
| [`files/bcb-lakehouse-lab/src/`](https://github.com/projetosdatabricksfree-png/databricks_fs/tree/main/files/bcb-lakehouse-lab/src) | Ingestão, pipeline SDP (Bronze/Silver/Gold), notebooks, funções puras |
| [`files/bcb-lakehouse-lab/resources/`](https://github.com/projetosdatabricksfree-png/databricks_fs/tree/main/files/bcb-lakehouse-lab/resources) | Jobs e pipeline declarados como IaC |
| [`files/bcb-lakehouse-lab/docs/`](https://github.com/projetosdatabricksfree-png/databricks_fs/tree/main/files/bcb-lakehouse-lab/docs) | PRD · ROADMAP · ADRs · data-contracts · runbook |
| [`files/bcb-lakehouse-lab/tests/`](https://github.com/projetosdatabricksfree-png/databricks_fs/tree/main/files/bcb-lakehouse-lab/tests) | Testes `pytest` (local + CI + workspace) |

## Execução rápida

```bash
cd files/bcb-lakehouse-lab
databricks auth login --host https://<seu-workspace>.cloud.databricks.com
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run job_setup -t dev      # cria schemas/volumes (1× por target)
```

Passo a passo completo no **[README do projeto](https://github.com/projetosdatabricksfree-png/databricks_fs/blob/main/files/bcb-lakehouse-lab/README.md#7-como-executar)**.
