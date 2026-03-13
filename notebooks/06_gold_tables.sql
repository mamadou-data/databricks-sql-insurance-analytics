-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Créer le schema GOLD

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS insurance_catalog.gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Créer la table GOLD customer_risk_analysis

-- COMMAND ----------

CREATE OR REPLACE TABLE insurance_catalog.gold.customer_risk_analysis AS

SELECT
customer_id,
age,
risk_profile,
credit_score,
coverage_amount,
premium_amount

FROM insurance_catalog.silver.insurance_claims_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Vérifier la table créée

-- COMMAND ----------

SELECT *
FROM insurance_catalog.gold.customer_risk_analysis
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Créer la table GOLD customer_segmentation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Objectif :
-- MAGIC
-- MAGIC Segmenter les clients selon :
-- MAGIC
-- MAGIC - âge
-- MAGIC - revenu
-- MAGIC - score de crédit
-- MAGIC - profil de risque
-- MAGIC
-- MAGIC Cela permet d’identifier :
-- MAGIC
-- MAGIC - clients premium
-- MAGIC - clients à risque
-- MAGIC - segments marketing

-- COMMAND ----------

CREATE OR REPLACE TABLE insurance_catalog.gold.customer_segmentation AS

SELECT
    customer_id,
    age,
    income_level,
    credit_score,
    risk_profile,

    -- segmentation par âge
    CASE
        WHEN age < 30 THEN 'Young'
        WHEN age BETWEEN 30 AND 50 THEN 'Middle Age'
        ELSE 'Senior'
    END AS age_group,

    -- segmentation par revenu
    CASE
        WHEN income_level < 40000 THEN 'Low Income'
        WHEN income_level BETWEEN 40000 AND 80000 THEN 'Middle Income'
        ELSE 'High Income'
    END AS income_segment,

    -- segmentation du risque client
    CASE
        WHEN credit_score >= 750 THEN 'Low Risk'
        WHEN credit_score BETWEEN 600 AND 749 THEN 'Medium Risk'
        ELSE 'High Risk'
    END AS credit_risk_segment,

    coverage_amount,
    premium_amount

FROM insurance_catalog.silver.insurance_claims_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Pourquoi cette table est utile**
-- MAGIC
-- MAGIC Elle permettra de répondre à :
-- MAGIC
-- MAGIC - quels segments sont les plus rentables ?
-- MAGIC
-- MAGIC - quels segments sont les plus risqués ?
-- MAGIC
-- MAGIC - quels segments acheter plus de produits ?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Créer la table GOLD policy_performance

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Objectif :
-- MAGIC
-- MAGIC Analyser la performance des **types de police d’assurance**.

-- COMMAND ----------

CREATE OR REPLACE TABLE insurance_catalog.gold.policy_performance AS

SELECT
    policy_type,

    COUNT(*) AS number_of_customers,

    AVG(premium_amount) AS avg_premium,

    AVG(coverage_amount) AS avg_coverage,

    AVG(credit_score) AS avg_credit_score,

    AVG(risk_profile) AS avg_risk_profile

FROM insurance_catalog.silver.insurance_claims_clean

GROUP BY policy_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Vérifier les tables GOLD

-- COMMAND ----------

SHOW TABLES IN insurance_catalog.gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tester les tables
-- MAGIC * Segmentation client

-- COMMAND ----------

SELECT *
FROM insurance_catalog.gold.customer_segmentation
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Performance des polices

-- COMMAND ----------

SELECT *
FROM insurance_catalog.gold.policy_performance;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ------------------------------------------------------------------------------------------------------------------------------

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analyse business intéressante

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - segment le plus rentable

-- COMMAND ----------

SELECT
income_segment,
AVG(premium_amount) AS avg_premium
FROM insurance_catalog.gold.customer_segmentation
GROUP BY income_segment
ORDER BY avg_premium DESC;