-- ============================================================================
-- AUTO CDC / SCD TYPE 2 (Exame S3): histórico completo de mudanças de clientes.
-- O SDP cria as colunas __START_AT / __END_AT; a versão vigente tem __END_AT NULL.
-- (Sintaxe legada equivalente: APPLY CHANGES INTO — pode aparecer como distrator.)
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE ${catalog}.${schema_silver}.clientes_cdc_stg (
  CONSTRAINT chave_presente  EXPECT (cliente_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT email_valido    EXPECT (email LIKE '%@%')       ON VIOLATION DROP ROW,
  CONSTRAINT uf_conhecida    EXPECT (uf IN ('SP','RJ','MG','RS','PR','BA'))  -- warn
)
COMMENT 'Silver stg: eventos CDC validados'
AS SELECT
  CAST(cliente_id AS INT)     AS cliente_id,
  nome, uf, segmento, email, cpf,
  operacao,
  TO_TIMESTAMP(ts_evento)     AS ts_evento
FROM STREAM(${catalog}.${schema_bronze}.clientes_cdc_raw);

CREATE OR REFRESH STREAMING TABLE ${catalog}.${schema_silver}.clientes_scd2
COMMENT 'Silver: dimensão clientes com histórico SCD Tipo 2 (__START_AT/__END_AT)';

CREATE FLOW cdc_clientes AS AUTO CDC INTO ${catalog}.${schema_silver}.clientes_scd2
FROM STREAM(${catalog}.${schema_silver}.clientes_cdc_stg)
KEYS (cliente_id)
APPLY AS DELETE WHEN operacao = 'DELETE'
SEQUENCE BY ts_evento
COLUMNS * EXCEPT (operacao)
STORED AS SCD TYPE 2;
