-- Databricks notebook source
-- MAGIC %md
-- MAGIC Objectif :
-- MAGIC
-- MAGIC - créer des variables analytiques
-- MAGIC - segmentation client

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Catégoriser l’âge

-- COMMAND ----------

SELECT
customer_id,
age,

CASE
WHEN age < 30 THEN 'Young'
WHEN age BETWEEN 30 AND 50 THEN 'Adult'
ELSE 'Senior'
END AS age_group

FROM insurance_catalog.silver.insurance_claims_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CASE WHEN équivalent de "if / else"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ratio prime / couverture

-- COMMAND ----------

SELECT
customer_id,
premium_amount,
coverage_amount,

premium_amount / coverage_amount AS premium_ratio

FROM insurance_catalog.silver.insurance_claims_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC On crée une nouvelle métrique analytique.