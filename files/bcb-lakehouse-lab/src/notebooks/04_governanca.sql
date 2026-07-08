-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Fase 6 — Governança e Segurança no Unity Catalog (Exame S7)
-- MAGIC Ajuste `workspace`/`bcb_dev` abaixo se usou outro catálogo/prefixo.

-- COMMAND ----------
USE CATALOG workspace;

-- COMMAND ----------
-- MAGIC %md ## 1) Managed × External (decorar)
-- MAGIC | | Managed | External |
-- MAGIC |---|---|---|
-- MAGIC | Localização | storage gerenciado pelo UC | `LOCATION` em External Location sua |
-- MAGIC | `DROP TABLE` | apaga metadados **e dados** (recuperável ~7d via `UNDROP`) | apaga **só metadados**; arquivos ficam |
-- MAGIC | Otimizações automáticas (Predictive Optimization) | sim | limitadas |
-- MAGIC No Free Edition não há external locations (sem credencial de nuvem própria),
-- MAGIC então external tables ficam na teoria; toda a prática abaixo usa managed.

-- COMMAND ----------
DESCRIBE EXTENDED bcb_dev_silver.sgs_observacoes;  -- Type: MANAGED + Location gerenciada

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS bcb_dev_gold.tmp_undrop_demo AS SELECT 1 AS x;
DROP TABLE bcb_dev_gold.tmp_undrop_demo;
UNDROP TABLE bcb_dev_gold.tmp_undrop_demo;          -- recupera managed table dropada
SELECT * FROM bcb_dev_gold.tmp_undrop_demo;

-- COMMAND ----------
-- MAGIC %md ## 2) GRANT / REVOKE (modelo aditivo — não existe DENY no UC)
-- MAGIC Privilégios-chave: `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`, `CREATE TABLE`,
-- MAGIC `ALL PRIVILEGES`, `MANAGE`. Principals: usuários, **grupos** e **service principals**.

-- COMMAND ----------
GRANT SELECT ON TABLE bcb_dev_gold.indicadores_mensais TO `account users`;
SHOW GRANTS ON TABLE bcb_dev_gold.indicadores_mensais;

-- COMMAND ----------
REVOKE SELECT ON TABLE bcb_dev_gold.indicadores_mensais FROM `account users`;
SHOW GRANTS ON TABLE bcb_dev_gold.indicadores_mensais;

-- COMMAND ----------
-- MAGIC %md ## 3) Row filter + Column mask (segurança em nível de linha/coluna)
-- MAGIC São **funções SQL** anexadas à tabela. Abaixo: quem não for do grupo `admins`
-- MAGIC só vê clientes de SP e enxerga o CPF mascarado.

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS bcb_dev_gold.clientes_atual AS
SELECT cliente_id, nome, uf, segmento, email, cpf
FROM bcb_dev_silver.clientes_scd2 WHERE `__END_AT` IS NULL;

CREATE OR REPLACE FUNCTION bcb_dev_gold.filtro_uf(uf STRING)
RETURN is_account_group_member('admins') OR uf = 'SP';

CREATE OR REPLACE FUNCTION bcb_dev_gold.mascara_cpf(cpf STRING)
RETURN CASE WHEN is_account_group_member('admins') THEN cpf
            ELSE concat('***.***.***-', right(cpf, 2)) END;

ALTER TABLE bcb_dev_gold.clientes_atual SET ROW FILTER bcb_dev_gold.filtro_uf ON (uf);
ALTER TABLE bcb_dev_gold.clientes_atual
  ALTER COLUMN cpf SET MASK bcb_dev_gold.mascara_cpf;

SELECT * FROM bcb_dev_gold.clientes_atual;   -- observe filtro + máscara aplicados

-- COMMAND ----------
-- MAGIC %md ## 4) Tags e ABAC
-- MAGIC Tags classificam objetos; **ABAC** (attribute-based access control) define
-- MAGIC políticas centrais que aplicam filtros/máscaras a tudo que carrega a tag
-- MAGIC (ex.: `pii`), em vez de tabela a tabela. Verifique a disponibilidade no seu
-- MAGIC workspace: https://docs.databricks.com/aws/en/data-governance/abac/

-- COMMAND ----------
ALTER TABLE bcb_dev_gold.clientes_atual SET TAGS ('camada' = 'gold', 'pii' = 'true');
ALTER TABLE bcb_dev_gold.clientes_atual ALTER COLUMN cpf SET TAGS ('classe' = 'pii_cpf');

-- COMMAND ----------
-- MAGIC %md ## 5) Views como camada de segurança + lineage + auditoria
-- MAGIC View dinâmica (padrão pré-masks, ainda cobrado): `is_account_group_member` na
-- MAGIC própria view. **Lineage**: Catalog Explorer → aba Lineage (tabela e coluna) —
-- MAGIC gerado automaticamente pelo UC. **Audit logs**: entregues como **JSON**, com
-- MAGIC latência de minutos, consultáveis via system tables (habilitação por admin).

-- COMMAND ----------
CREATE OR REPLACE VIEW bcb_dev_gold.vw_clientes_seguro AS
SELECT cliente_id, nome, uf, segmento,
       CASE WHEN is_account_group_member('admins') THEN email
            ELSE regexp_replace(email, '^[^@]+', '***') END AS email
FROM bcb_dev_gold.clientes_atual;

-- system tables (se habilitadas): auditoria e lineage
-- SELECT event_time, action_name, request_params
-- FROM system.access.audit ORDER BY event_time DESC LIMIT 20;
-- SELECT * FROM system.access.table_lineage
-- WHERE target_table_name = 'indicadores_mensais' LIMIT 20;
