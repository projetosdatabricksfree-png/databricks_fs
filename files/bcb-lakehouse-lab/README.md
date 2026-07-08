# BCB Lakehouse Lab

**Lakehouse Medallion de ponta a ponta no Databricks, com dados macroeconômicos reais do Banco Central do Brasil.**

![CI](https://github.com/projetosdatabricksfree-png/databricks_fs/actions/workflows/ci.yml/badge.svg)
![Plataforma](https://img.shields.io/badge/Databricks-Free%20Edition%20(serverless)-FF3621)
![IaC](https://img.shields.io/badge/IaC-Asset%20Bundles-informational)
![Custo](https://img.shields.io/badge/custo-R%240-brightgreen)

Pipeline de dados declarativo, versionado e governado que ingere indicadores públicos do BCB
(câmbio, Selic, IPCA e expectativas de mercado), aplica qualidade de dados observável em cada
camada e entrega tabelas prontas para análise — implantado 100% como código (Databricks Asset
Bundles) e validado por CI. Construído como **peça de portfólio de engenharia de dados** e como
laboratório completo para a certificação *Databricks Certified Data Engineer Associate*.

---

## 1. Visão estratégica de negócio

**O problema que o projeto resolve.** Indicadores macroeconômicos (dólar, Selic, inflação) são
insumo crítico para pricing, risco, planejamento financeiro e relatórios regulatórios. As fontes
oficiais (APIs do BCB) entregam dados **crus, em formatos heterogêneos, com janelas que se
sobrepõem e geram duplicatas**, e sem nenhuma garantia de qualidade. Consumir isso direto no BI é
frágil: quebra a cada mudança de schema, mistura dado válido com lixo e não deixa rastro de
auditoria.

**A proposta de valor.** Um lakehouse que transforma esse fluxo bruto em um **produto de dados
confiável**: cada indicador chega, é validado contra um contrato explícito, deduplicado de forma
determinística e publicado em tabelas de consumo com linhagem, controle de acesso e um SLO de
qualidade auditável. O negócio consome a camada Gold sabendo que **o que está lá passou por
regras que reportam suas próprias violações** — nada é filtrado em silêncio.

| Decisão de negócio | Como o lakehouse habilita |
|---|---|
| Pricing / risco precisam de séries confiáveis | Silver com expectations (WARN/DROP/FAIL) + dedup determinístico |
| "Qual foi a surpresa inflacionária do mês?" | Gold `ipca_expectativa_vs_realizado`: mediana Focus × IPCA realizado |
| Análise mensal de câmbio e juros | Gold `indicadores_mensais`: avg/min/max/fechamento por série |
| Cliente 360 com histórico | Dimensão SCD Type 2 (`clientes_scd2`) reconstrói qualquer estado passado |
| Conformidade / LGPD | Row filter + column mask sobre PII, GRANT/REVOKE e linhagem no Unity Catalog |
| Custo sob controle | Serverless puro (paga-por-uso), Predictive Optimization, sem cluster ocioso |

**Por que "custo zero" importa estrategicamente.** Todo o projeto roda no Databricks Free Edition
(serverless). Isso prova que os padrões de produção — qualidade declarativa, CI/CD, governança,
IaC — não dependem de infraestrutura cara: são **decisões de arquitetura**, não de orçamento.

---

## 2. Arquitetura

Orquestração em duas camadas do Lakeflow: **Jobs** (o "quando" e o "controle") disparam o
**Pipeline SDP** (o "o quê" e a "qualidade"). O acoplamento entre eles é um *file arrival trigger*
— o dado chegando na landing zone é o próprio gatilho, não um relógio.

```
                         APIs públicas do BCB
              SGS (câmbio, Selic, IPCA)   Olinda/OData (Focus)      feed CDC sintético
                        │                        │                       │
                        └───────────┬────────────┴───────────┬───────────┘
                                    ▼                         ▼
                    ┌──────────────────────────── Lakeflow Jobs ────────────────────────────┐
                    │  job_ingestao  (cron · for_each sobre as séries · retries)             │
                    │        │  grava JSON cru                                               │
                    │        ▼                                                               │
                    │  /Volumes/<catalog>/<env>_bronze/landing/*.json   ◄── fonte da verdade │
                    │        │  (file arrival trigger)                                       │
                    │        ▼                                                               │
                    │  job_orquestracao:                                                     │
                    │     pipeline_task ─► auditoria (event log) ─► condition_task ─┐         │
                    │                                                    │          │         │
                    │                            status_qualidade OK ◄───┘          │ falha   │
                    │                                    │                          ▼         │
                    │                             publicar_gold              alerta_qualidade  │
                    └────────────────────────────────────┼──────────────────────────────────┘
                                                          ▼
        ┌──────────────────────────── Pipeline Lakeflow SDP (1 pipeline, 3 camadas) ─────────────────────┐
        │                                                                                                 │
        │  BRONZE  ── Auto Loader (STREAM read_files / cloudFiles), _metadata, _rescued_data,              │
        │             schema evolution · imutável e auditável (duplicatas propositais)                    │
        │                    │                                                                            │
        │  SILVER  ── explode/flatten + cast · expectations WARN/DROP/FAIL                                 │
        │             AUTO CDC SCD Type 1 (dedup/upsert de fatos por sequência)                           │
        │             AUTO CDC SCD Type 2 (histórico de clientes, __START_AT/__END_AT)                    │
        │                    │                                                                            │
        │  GOLD    ── Materialized Views (agregações, joins, MAX_BY) · row filter + column mask sobre PII │
        └─────────────────────────────────────────────────────────────────────────────────────────────┘
```

Padrão transversal — **fluxo de parâmetros único** (evita hardcode e habilita dev/prod):
`databricks.yml` define `variables` → targets fazem override → Jobs repassam via `base_parameters`
(lidos por `dbutils.widgets`) → Pipeline repassa via `configuration` (SQL usa `${...}`, Python usa
`spark.conf.get()`).

---

## 3. Fluxo de dados e o porquê de cada etapa

Cada decisão abaixo é deliberada; as de maior impacto têm um ADR formal em `docs/`.

### Ingestão → landing zone (Bronze crua)
- **O quê:** `job_ingestao` roda por cron, usa `for_each_task` para varrer as séries SGS
  (`dólar_venda`=1, `selic_diaria`=11, `ipca_mensal`=433) e grava o JSON **cru** num Volume UC.
- **Por quê:** a landing zone é a **fonte da verdade** para full refresh — reprocessar é sempre
  possível sem re-chamar a API. A janela móvel gera duplicatas de propósito: elas exercitam a
  dedup a jusante em vez de mascarar o problema. `for_each` + retries dão tolerância a falha de
  API sem código imperativo.

### Bronze — Auto Loader, não COPY INTO (ADR-001)
- **Por quê:** o Auto Loader rastreia arquivos já processados em *checkpoint* (RocksDB) — não
  relista o diretório inteiro a cada execução, então **escala e custa menos** conforme o volume
  cresce. Ganha de graça *schema inference/evolution* + `_rescued_data` (captura dados fora do
  contrato sem derrubar o pipeline) e, dentro do SDP, herda retries, event log e expectations.
  COPY INTO fica para *seed* de referência; o comparativo executável dos três métodos vive em
  `notebooks/01_ingestao_comparativo.py`.
- **Consequência assumida:** Bronze é **imutável e permite duplicatas** — corrigi-las aqui
  destruiria a auditabilidade. A limpeza é responsabilidade da Silver.

### Silver — qualidade observável + dedup declarativo (ADR-002)
- **Expectations nos 3 modos:** `FAIL` (viola contrato → derruba o update), `DROP` (descarta a
  linha ruim, conta a violação) e `WARN` (deixa passar, mas registra). **Nenhum filtro é
  silencioso** — toda violação vira métrica no event log.
- **Dedup via `AUTO CDC ... STORED AS SCD TYPE 1`, não MERGE manual:** o AUTO CDC aplica eventos
  **pela sequência mesmo chegando fora de ordem** (vence a maior `SEQUENCE BY _ingerido_em`) e
  elimina o código estado-a-estado que é fonte clássica de bug. O `MERGE INTO` permanece como
  material didático em `notebooks/02_merge_dedup.py`.
- **Histórico de clientes via `SCD TYPE 2`:** o SDP gera `__START_AT`/`__END_AT` corretamente e
  trata deleção com `APPLY AS DELETE WHEN operacao='DELETE'` — reconstrói qualquer estado passado
  da dimensão sem lógica manual de fechamento de vigência.
- **Restrição operacional:** tabelas alvo de AUTO CDC são gerenciadas pelo pipeline — **nunca**
  receber DML manual (quebraria a reconciliação declarativa).

### Auditoria → decisão → Gold
- **O quê:** após o pipeline, `06_auditoria_qualidade.py` lê o **event log** do SDP, calcula a
  taxa de violações e publica um task value `status_qualidade`. Um `condition_task` ramifica:
  aprovado → `publicar_gold`; reprovado → `alerta_qualidade`.
- **Por quê:** a Gold só é "certificada" se o **SLO de qualidade** (violações ≤ 5% por update) for
  atingido. Isso transforma qualidade de dados de aspiração em **gate automatizado** — o mesmo
  princípio de um quality gate de CI, aplicado a dados.

### Gold — produto de dados para consumo
- Materialized Views: `indicadores_mensais` (métricas mensais por série) e
  `ipca_expectativa_vs_realizado` (mediana Focus × IPCA realizado + surpresa). PII é servida por
  `clientes_atual` (row filter + column mask) e `vw_clientes_seguro` (view dinâmica condicionada a
  grupo). É a fronteira onde governança e negócio se encontram.

---

## 4. Contratos de dados e SLO de qualidade

Cada camada tem um contrato versionado (`docs/data-contracts.md`). Resumo do que a qualidade
garante:

| Camada | Garantia | Mecanismo |
|---|---|---|
| Bronze | Nada se perde; tudo é rastreável | Imutável + `_metadata`, `_rescued_data`, `_ingerido_em` |
| Silver | 1 linha por chave; dado tipado e validado | Expectations (3 modos) + AUTO CDC SCD1/SCD2 |
| Gold | Só publica se qualidade ≥ SLO | Auditoria do event log + `condition_task` |

**SLO:** taxa de violações (dropped/total) **≤ 5% por update**. Acima disso, a Gold não é
publicada e o ramo de alerta é acionado.

---

## 5. Engenharia de plataforma: IaC, ambientes e CI/CD

- **Infraestrutura como código (Databricks Asset Bundles).** Todo o ambiente — jobs, pipeline,
  schemas, volumes, permissões — é declarado em `databricks.yml` + `resources/*.yml`. Nada é
  criado "na mão" no workspace.
- **Isolamento dev/prod sem duplicar código.** Mesmo catálogo (`workspace`), schemas prefixados
  por `var.env_prefix`: `bcb_dev_*` (dev) vs. `bcb_*` (prod). O target `dev` usa
  `mode: development` (prefixa recursos com `[dev <user>]` e **pausa** schedules/triggers para
  iterar sem efeito colateral); `prod` usa `mode: production` (sem prefixo, triggers ativos,
  validações extras de deploy).
- **Testabilidade.** A lógica de negócio vive como **funções puras** em `src/lib/transforms.py`
  (sem dependência de Spark), então roda por `pytest` local, no CI (GitHub Actions) e no workspace
  (`job_testes`) — a mesma regra de teste em três lugares.
- **CI.** Cada push valida `pytest` + `databricks bundle validate` (badge no topo).

---

## 6. Estrutura do repositório

```
bcb-lakehouse-lab/
├── databricks.yml              # Bundle: variables, targets dev/prod, overrides
├── resources/                  # Jobs e pipeline declarados como IaC
│   ├── job_setup.yml           #   cria schemas/volumes/grants (1× por target)
│   ├── job_ingestao.yml        #   cron + for_each sobre as séries + retries
│   ├── job_orquestracao.yml    #   file arrival → pipeline → auditoria → condition
│   ├── job_testes.yml          #   pytest no workspace
│   └── pipeline_sdp.yml        #   configuração do pipeline SDP
├── src/
│   ├── ingestion/              # fetch SGS / Olinda / gerador CDC sintético
│   ├── pipelines/sdp/          # Bronze/Silver/Gold + SCD2 (SQL + Python)
│   ├── notebooks/              # comparativos, MERGE, transformações, governança, auditoria
│   ├── setup/00_setup.py       # bootstrap de schemas/volumes/grants
│   └── lib/transforms.py       # funções puras (testáveis sem Spark)
├── tests/                      # pytest (unitário, local + CI + workspace)
└── docs/                       # PRD · ROADMAP · ADRs · data-contracts · runbook
```

---

## 7. Como executar

**Pré-requisitos:** conta no [Databricks Free Edition](https://www.databricks.com/learn/free-edition),
[Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) ≥ 0.230, Git.

```bash
# 1. Autentique a CLI no seu workspace Free Edition
databricks auth login --host https://<seu-workspace>.cloud.databricks.com

# 2. Valide e implante o bundle no target dev
databricks bundle validate
databricks bundle deploy -t dev

# 3. Bootstrap: schemas + volumes (bronze/silver/gold + landing/checkpoints)
databricks bundle run job_setup -t dev

# 4. Primeira carga: busca as séries do BCB e grava JSON na landing
databricks bundle run job_ingestao -t dev

# 5. Roda o pipeline + auditoria (ou aguarde o file arrival trigger)
databricks bundle run job_orquestracao -t dev

# 6. (opcional) testes unitários no workspace
databricks bundle run job_testes -t dev
```

Testes locais (funções puras, sem Spark): `pytest tests/ -v`.
Operação, repair run, backfill e troubleshooting: **`docs/runbook.md`**.

> **Bootstrap numa workspace nova:** o `job_orquestracao` tem *file arrival trigger* que exige o
> volume `landing` **já existente** no deploy. Crie os schemas/volumes antes do 1º deploy (passo
> documentado no runbook) — depois disso, o ciclo acima é idempotente.

---

## 8. Governança e segurança (Unity Catalog)

- **Controle de acesso** aditivo via `GRANT`/`REVOKE` (o UC não tem `DENY` — negação é ausência de
  grant; a segurança é a interseção dos grants efetivos).
- **PII protegida em duas frentes:** *row filter* + *column mask* sobre a dimensão de clientes e
  *dynamic view* condicionada a grupo (`is_account_group_member`).
- **Linhagem** ponta a ponta no Catalog Explorer e recuperação com `UNDROP` / time travel /
  `RESTORE`.
- Detalhes e demonstração: `src/notebooks/04_governanca.sql`.

---

## 9. Decisões arquiteturais (ADRs)

| ADR | Decisão | Trade-off central |
|---|---|---|
| [ADR-001](docs/ADR-001-ingestao.md) | Auto Loader (streaming) na Bronze, não COPY INTO | Escala/checkpoint/schema-evolution vs. simplicidade do COPY INTO |
| [ADR-002](docs/ADR-002-dedup-scd.md) | Dedup/histórico com AUTO CDC (SCD1/SCD2), não MERGE manual | Aplicação declarativa fora-de-ordem vs. controle imperativo do MERGE |

---

## 10. Limitações do Free Edition (e como o projeto as trata)

O Free Edition é **100% serverless**: sem clusters clássicos, sem *external locations*, usuário
único. Isso é uma feature, não um bug, para os propósitos do projeto:

- Configuração de cluster e a maioria dos `spark.conf` de tuning são gerenciados pela plataforma —
  tentar setá-los falha **de propósito** (tópico de exame; documentado no runbook).
- Tabelas *external* e ABAC beta são cobertas conceitualmente em `04_governanca.sql`, não por lab.
- Grants são demonstrados contra o grupo `account users` (workspace de usuário único).

O ponto: os padrões de produção deste repositório **não dependem** de infraestrutura paga.

---

## 11. Trilha de certificação (uso secundário)

O projeto também é um laboratório que cobre 100% do blueprint da *Databricks Certified Data
Engineer Associate* (guia vigente, mai/2026). O mapeamento fase → semana → seção do exame, os
hardgates de cada fase e o checklist de cobertura estão em **`docs/ROADMAP.md`** e **`docs/PRD.md`**.

## 12. Documentação

PRD: [`docs/PRD.md`](docs/PRD.md) · Roadmap com hardgates: [`docs/ROADMAP.md`](docs/ROADMAP.md) ·
Decisões: [`docs/ADR-001-ingestao.md`](docs/ADR-001-ingestao.md),
[`docs/ADR-002-dedup-scd.md`](docs/ADR-002-dedup-scd.md) ·
Contratos de dados: [`docs/data-contracts.md`](docs/data-contracts.md) ·
Operação e troubleshooting: [`docs/runbook.md`](docs/runbook.md)

**Referência técnica (docs oficiais):**
[Delta](https://docs.databricks.com/aws/en/delta/) ·
[Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/) ·
[Lakeflow SDP](https://docs.databricks.com/aws/en/dlt/) ·
[Jobs](https://docs.databricks.com/aws/en/jobs/) ·
[Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) ·
[Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/)
