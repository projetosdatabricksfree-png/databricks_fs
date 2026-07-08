-- ============================================================================
-- BRONZE (Exame S2/S3): ingestão incremental com Auto Loader via STREAM read_files
-- Princípios da camada: dados crus + colunas técnicas de auditoria (_metadata),
-- schema evolution automática e captura de dados fora do schema em _rescued_data.
-- Os parâmetros ${...} vêm de `configuration` no resources/pipeline_sdp.yml.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE ${catalog}.${schema_bronze}.sgs_raw
COMMENT 'Bronze: envelopes SGS crus (Auto Loader; 1 linha por arquivo coletado)'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path              AS _arquivo_origem,
  _metadata.file_modification_time AS _arquivo_modificado_em,
  current_timestamp()              AS _ingerido_em
FROM STREAM read_files(
  '${volume_landing}/sgs/',
  format => 'json',
  schemaEvolutionMode => 'addNewColumns'   -- novas colunas entram no schema;
);                                          -- tipos incompatíveis vão p/ _rescued_data

CREATE OR REFRESH STREAMING TABLE ${catalog}.${schema_bronze}.clientes_cdc_raw
COMMENT 'Bronze: eventos CDC de clientes (JSON lines), insumo do AUTO CDC SCD2'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path AS _arquivo_origem,
  current_timestamp() AS _ingerido_em
FROM STREAM read_files(
  '${volume_landing}/clientes_cdc/',
  format => 'json',
  schemaEvolutionMode => 'rescue'   -- schema congelado; tudo que for novo/
);                                   -- incompatível cai em _rescued_data
