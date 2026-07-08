# ROADMAP — BCB Lakehouse Lab
Fases = sprints de estudo (≈1 semana cada, compressível). **Hardgate**: critérios
objetivos para encerrar a fase. Não avance com hardgate reprovado.

---
## Fase 0 — Fundação (Semana 1 · Exame S1)
**Objetivo:** ambiente pronto e fundamentos de plataforma dominados.
**Atividades:** criar conta Free Edition; instalar/autenticar CLI; `bundle validate`
+ `deploy -t dev`; rodar `job_setup`; estudar tipos de compute e modelo DBU;
executar `05_manutencao_delta.sql` (time travel/RESTORE).
**Hardgate:** ✅ deploy dev sem erros · ✅ schemas+volumes criados · ✅ explicar em 30s
serverless × job cluster × all-purpose × SQL warehouse · ✅ RESTORE executado.

## Fase 1 — Padrões de ingestão (Semana 2 · S2)
**Objetivo:** dominar a matriz CTAS × COPY INTO × Auto Loader.
**Atividades:** `job_ingestao` (primeira carga); `01_ingestao_comparativo.py`;
provocar evolução de schema (editar um JSON da landing acrescentando campo) e
observar `addNewColumns` × `rescue`/`_rescued_data`.
**Hardgate:** ✅ COPY INTO reexecutado com 0 inseridos · ✅ Auto Loader `availableNow`
sem reprocessar backlog · ✅ `_rescued_data` populado num caso forjado.

## Fase 2 — Fontes corporativas + MERGE (Semana 3 · S2/S3)
**Objetivo:** ingestão REST/JDBC (conceito), flatten de JSON e upsert manual.
**Atividades:** ler código de `fetch_bcb_expectativas.py`; estudar Managed
Connectors e sintaxe `spark.read.jdbc`; executar `02_merge_dedup.py`.
**Hardgate:** ✅ MERGE idempotente comprovado (2ª execução: 0/0) · ✅ explicar por que
a fonte deve ser deduplicada antes do MERGE · ✅ dedup por janela reproduzido.

## Fase 3 — Transformações e tuning (Semana 4 · S3/S6)
**Objetivo:** PySpark do guia + parâmetros de tuning (teoria serverless × clássico).
**Atividades:** `03_transformacoes.py`; conferir `BroadcastHashJoin` no `explain()`;
memorizar tabela de parâmetros; ler doc de AQE.
**Hardgate:** ✅ citar de cabeça os 4 parâmetros e o que controlam · ✅ diferenciar
`approx_count_distinct` × `countDistinct` (custo/precisão) · ✅ union × unionByName.

## Fase 4 — Pipeline declarativo (Semana 5 · S3)
**Objetivo:** SDP ponta a ponta com qualidade e CDC.
**Atividades:** rodar `job_orquestracao` (ou aguardar file arrival); inspecionar
pipeline graph e event log; injetar registro inválido e ver DROP/WARN; rodar o
gerador CDC 3× e consultar `clientes_scd2` (`__START_AT`/`__END_AT`).
**Hardgate:** ✅ update verde nas 3 camadas · ✅ os 3 modos de expectation observados
no event log · ✅ SCD2 com ≥2 versões de um mesmo cliente · ✅ dedup SCD1 comprovado
(count stg > count final).

## Fase 5 — Orquestração + CI/CD (Semana 6 · S4/S5)
**Objetivo:** jobs de produção e promoção entre ambientes.
**Atividades:** estudar os YAMLs de `resources/`; forçar falha (alerta_qualidade) e
praticar **Repair run**; `deploy -t prod` e comparar isolamento dev/prod; publicar no
GitHub, abrir PR via Git Folders, ver CI rodar; `bundle run job_testes`.
**Hardgate:** ✅ repair run reexecutando só a task falha · ✅ prod sem prefixo [dev] e
schedule ativo · ✅ pytest verde local + workspace · ✅ explicar variables/overrides.

## Fase 6 — Governança e segurança (Semana 7 · S7)
**Objetivo:** UC de ponta a ponta.
**Atividades:** `04_governanca.sql` completo; testar lineage no Catalog Explorer;
revisar managed × external e audit logs (JSON, latência, sobrescrita).
**Hardgate:** ✅ row filter+mask funcionando · ✅ SHOW GRANTS antes/depois de REVOKE ·
✅ UNDROP executado · ✅ explicar por que não existe DENY no UC.

## Fase 7 — Revisão e prova (Semana 7 · todas)
**Atividades:** refazer as 10 questões oficiais comentadas; simulado cronometrado;
reler o guia marcando lacunas; agendar no Webassessor; system check Kryterion.
**Hardgate:** ✅ simulado ≥ 85% · ✅ checklist do README 100% · ✅ prova agendada.

---
**Definition of Done global:** todos os hardgates ✅ + repositório publicado com CI
verde + você consegue reconstruir o ambiente do zero em <30 min só com o README.
