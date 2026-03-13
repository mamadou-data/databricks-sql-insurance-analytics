-- Databricks notebook source
-- MAGIC %md
-- MAGIC Objectif :
-- MAGIC
-- MAGIC montrer des compétences SQL avancées.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Window Function
-- MAGIC ### Classement des clients par couverture

-- COMMAND ----------

SELECT
customer_id,
coverage_amount,

RANK() OVER(
ORDER BY coverage_amount DESC
) AS coverage_rank

FROM insurance_catalog.silver.insurance_claims_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC RANK() -> Permet de classer les lignes.
-- MAGIC
-- MAGIC OVER() -> Définit la fenêtre d'analyse.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTE

-- COMMAND ----------

WITH high_value_customers AS (

SELECT
customer_id,
coverage_amount
FROM insurance_catalog.silver.insurance_claims_clean
WHERE coverage_amount > 500000

)

SELECT *
FROM high_value_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC WITH -> Permet de créer une table temporaire appelée CTE.