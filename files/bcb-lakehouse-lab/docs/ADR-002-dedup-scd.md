# ADR-002 — Dedup e dimensões: AUTO CDC vs MERGE
**Status:** Aceito · **Data:** 07/07/2026

## Contexto
A Silver precisa de (a) 1 linha por (série, data) apesar das duplicatas da Bronze e
(b) histórico de clientes. Opções: MERGE INTO em notebooks agendados ou `AUTO CDC
INTO` (SDP; sucessor declarativo do `APPLY CHANGES INTO`).

## Decisão
`AUTO CDC ... STORED AS SCD TYPE 1` para dedup/upsert de fatos (vence a maior
`SEQUENCE BY _ingerido_em`) e `SCD TYPE 2` para a dimensão clientes
(`APPLY AS DELETE WHEN operacao='DELETE'`). MERGE INTO fica como material didático
(`notebooks/02_merge_dedup.py`) e como padrão para cenários fora do SDP.

## Justificativa
1. Ordenação fora de ordem: AUTO CDC aplica eventos pela sequência mesmo chegando
   atrasados; MERGE manual exigiria lógica extra.
2. Menos código estado-a-estado: SCD2 manual (fechar vigência, abrir nova) é fonte
   clássica de bugs; o SDP gera `__START_AT`/`__END_AT` corretamente.
3. Exame: AUTO CDC/SDP é o caminho recomendado no blueprint atual; MERGE segue
   cobrado como fundamento — por isso mantemos os dois no projeto.

## Consequências
- Tabelas alvo do AUTO CDC são gerenciadas pelo pipeline (não receber DML externo).
- O critério de desempate precisa ser monotônico por chave (`_ingerido_em` atende).
