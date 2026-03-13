-- Databricks notebook source
-- MAGIC %md
-- MAGIC Objectif :
-- MAGIC
-- MAGIC répondre à des questions métier.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Revenus par type de police

-- COMMAND ----------

SELECT
policy_type,
SUM(premium_amount) AS total_premium
FROM insurance_catalog.silver.insurance_claims_clean
GROUP BY policy_type
ORDER BY total_premium DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SUM -> additionne les valeurs.
-- MAGIC
-- MAGIC GROUP BY -> regroupe les données par catégorie.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Score de risque moyen

-- COMMAND ----------

SELECT
driving_record,
AVG(credit_score) AS avg_credit_score
FROM insurance_catalog.silver.insurance_claims_clean
GROUP BY driving_record;