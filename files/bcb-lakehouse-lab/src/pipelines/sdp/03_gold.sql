-- ============================================================================
-- GOLD (Exame S3): agregações de negócio como MATERIALIZED VIEWS.
-- Tabela × View × Materialized View × Streaming Table (decorar):
--   View                → só a query salva; recomputa a cada leitura
--   Materialized View   → resultado pré-computado, atualizado pelo pipeline
--                         (refresh incremental quando possível) — ideal p/ BI
--   Streaming Table     → append incremental de fonte streaming (Bronze/Silver)
--   Tabela (Delta)      → armazenamento gerenciado genérico
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${schema_gold}.indicadores_mensais
COMMENT 'Gold: métricas mensais por série (média, extremos, fechamento)'
AS SELECT
  codigo_serie,
  nome_serie,
  DATE_TRUNC('month', data_ref)      AS mes,
  ROUND(AVG(valor), 4)               AS valor_medio,
  MIN(valor)                         AS valor_minimo,
  MAX(valor)                         AS valor_maximo,
  MAX_BY(valor, data_ref)            AS valor_fechamento,
  COUNT(*)                           AS qtd_observacoes
FROM ${catalog}.${schema_silver}.sgs_observacoes
GROUP BY codigo_serie, nome_serie, DATE_TRUNC('month', data_ref);

-- Join analítico: expectativa de mercado (Focus) × IPCA realizado (série 433).
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${schema_gold}.ipca_expectativa_vs_realizado
COMMENT 'Gold: mediana das expectativas Focus vs IPCA realizado, por mês de referência'
AS
WITH realizado AS (
  SELECT DATE_TRUNC('month', data_ref) AS mes, MAX_BY(valor, data_ref) AS ipca_realizado
  FROM ${catalog}.${schema_silver}.sgs_observacoes
  WHERE codigo_serie = 433
  GROUP BY 1
),
expectativa AS (
  SELECT mes_referencia AS mes,
         ROUND(AVG(mediana), 4) AS mediana_focus,
         COUNT(*)               AS qtd_boletins
  FROM ${catalog}.${schema_silver}.expectativas_ipca
  GROUP BY 1
)
SELECT COALESCE(r.mes, e.mes) AS mes,
       e.mediana_focus,
       r.ipca_realizado,
       ROUND(r.ipca_realizado - e.mediana_focus, 4) AS surpresa
FROM realizado r
FULL OUTER JOIN expectativa e USING (mes);
