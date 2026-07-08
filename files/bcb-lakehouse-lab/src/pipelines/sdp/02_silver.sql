-- ============================================================================
-- SILVER (Exame S3): limpeza, tipagem, flatten e QUALIDADE via expectations.
-- Os 3 comportamentos de expectation (decorar para a prova):
--   EXPECT (cond)                          → WARN: linha ENTRA, métrica no event log
--   EXPECT (cond) ON VIOLATION DROP ROW    → linha descartada + métrica
--   EXPECT (cond) ON VIOLATION FAIL UPDATE → update do pipeline FALHA
-- ============================================================================

-- Staging: explode do array aninhado `observacoes` + cast de tipos.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${schema_silver}.sgs_observacoes_stg (
  CONSTRAINT serie_presente  EXPECT (codigo_serie IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valor_valido    EXPECT (valor IS NOT NULL AND valor >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT serie_catalogada EXPECT (codigo_serie IN (1, 11, 433))    -- warn (monitoria)
)
COMMENT 'Silver stg: observações SGS explodidas e tipadas (append; pode ter duplicatas)'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  CAST(codigo_serie AS INT)             AS codigo_serie,
  nome_serie,
  TO_DATE(obs.data, 'dd/MM/yyyy')       AS data_ref,
  TRY_CAST(obs.valor AS DOUBLE)         AS valor,
  TO_TIMESTAMP(coletado_em)             AS coletado_em,
  _arquivo_origem,
  _ingerido_em
FROM STREAM(${catalog}.${schema_bronze}.sgs_raw)
LATERAL VIEW explode(observacoes) o AS obs;

-- DEDUP/UPSERT declarativo: a janela móvel da ingestão gera a MESMA observação
-- em vários arquivos. AUTO CDC com SCD TYPE 1 mantém 1 linha por chave,
-- vencendo pela maior sequência (_ingerido_em) — dedup idempotente sem MERGE manual.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${schema_silver}.sgs_observacoes
COMMENT 'Silver: 1 linha por (série, data) — deduplicada via AUTO CDC SCD1';

CREATE FLOW dedup_sgs AS AUTO CDC INTO ${catalog}.${schema_silver}.sgs_observacoes
FROM STREAM(${catalog}.${schema_silver}.sgs_observacoes_stg)
KEYS (codigo_serie, data_ref)
SEQUENCE BY _ingerido_em
COLUMNS * EXCEPT (_arquivo_origem)
STORED AS SCD TYPE 1;
