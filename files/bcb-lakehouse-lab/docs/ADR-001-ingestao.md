# ADR-001 — Estratégia de ingestão da Bronze
**Status:** Aceito · **Data:** 07/07/2026

## Contexto
As fontes (APIs BCB, feed CDC) produzem arquivos JSON em Volume UC. Opções de
ingestão: CTAS/read_files (batch total), COPY INTO (incremental SQL) e Auto Loader
(incremental com checkpoint, via `cloudFiles` ou `STREAM read_files` no SDP).

## Decisão
Auto Loader como padrão da Bronze, dentro do pipeline SDP (`STREAM read_files`).
COPY INTO permanece para cargas de referência simples (seed de dimensão) e o
comparativo dos três métodos vive em `notebooks/01_ingestao_comparativo.py`.

## Justificativa
1. Escala e custo de descoberta: Auto Loader rastreia arquivos processados em
   checkpoint (RocksDB) sem listar tudo a cada execução; COPY INTO degrada com
   milhões de arquivos.
2. Schema drift: inference/evolution nativos + `_rescued_data` para dados fora do
   contrato — requisito explícito do exame.
3. Operação: dentro do SDP, o Auto Loader herda retries, event log e expectations.

## Consequências
- Duplicatas são esperadas na Bronze (janela móvel da fonte) → resolvidas na Silver
  (ADR-002). Bronze permanece imutável/auditável.
- `addNewColumns` faz o stream falhar na 1ª ocorrência de coluna nova e seguir após
  restart — comportamento documentado, tratado por retries do job.
