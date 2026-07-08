-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Fase 0/6 — Delta Lake: time travel, manutenção e Liquid Clustering (S1/S6)

-- COMMAND ----------
USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS bcb_dev_sandbox;
CREATE OR REPLACE TABLE bcb_dev_sandbox.lab_delta (id INT, valor DOUBLE)
CLUSTER BY (id);                          -- Liquid Clustering na criação

-- COMMAND ----------
INSERT INTO bcb_dev_sandbox.lab_delta VALUES (1, 10.0), (2, 20.0);
UPDATE bcb_dev_sandbox.lab_delta SET valor = 99.9 WHERE id = 1;
DELETE FROM bcb_dev_sandbox.lab_delta WHERE id = 2;

-- COMMAND ----------
-- MAGIC %md ## Transaction log e time travel
-- MAGIC Cada operação = 1 versão no `_delta_log`. Leia versões antigas com
-- MAGIC `VERSION AS OF` / `TIMESTAMP AS OF`; desfaça com `RESTORE`.

-- COMMAND ----------
DESCRIBE HISTORY bcb_dev_sandbox.lab_delta;

-- COMMAND ----------
SELECT * FROM bcb_dev_sandbox.lab_delta VERSION AS OF 1;   -- antes do UPDATE/DELETE

-- COMMAND ----------
RESTORE TABLE bcb_dev_sandbox.lab_delta TO VERSION AS OF 1;
SELECT * FROM bcb_dev_sandbox.lab_delta;

-- COMMAND ----------
-- MAGIC %md ## OPTIMIZE, VACUUM e Predictive Optimization
-- MAGIC - `OPTIMIZE` compacta arquivos pequenos (e agrupa por cluster keys no Liquid)
-- MAGIC - `VACUUM` remove arquivos não referenciados além da retenção (**default 7 dias**;
-- MAGIC   reduzir a retenção quebra time travel — pegadinha clássica)
-- MAGIC - Em managed tables no UC, a **Predictive Optimization** roda OPTIMIZE/VACUUM
-- MAGIC   automaticamente com base no padrão de uso — você raramente agenda isso à mão.

-- COMMAND ----------
OPTIMIZE bcb_dev_sandbox.lab_delta;
VACUUM bcb_dev_sandbox.lab_delta;                 -- respeita retenção default
-- VACUUM bcb_dev_sandbox.lab_delta DRY RUN;      -- lista o que seria removido

-- COMMAND ----------
-- MAGIC %md ## Liquid Clustering (substitui partições fixas + ZORDER)
-- MAGIC `CLUSTER BY AUTO` delega a escolha das chaves à plataforma (Predictive
-- MAGIC Optimization analisa os filtros mais usados). Chaves podem mudar sem rewrite.

-- COMMAND ----------
ALTER TABLE bcb_dev_sandbox.lab_delta CLUSTER BY (valor);   -- troca de chave: só metadado
ALTER TABLE bcb_dev_sandbox.lab_delta CLUSTER BY AUTO;
DESCRIBE DETAIL bcb_dev_sandbox.lab_delta;                  -- veja clusteringColumns
