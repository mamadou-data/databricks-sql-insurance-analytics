-- Databricks notebook source
-- MAGIC %md
-- MAGIC insurance_catalog
-- MAGIC
-- MAGIC │
-- MAGIC
-- MAGIC └── gold
-- MAGIC
-- MAGIC       
-- MAGIC        ├── fact_policy
-- MAGIC
-- MAGIC        ├── dim_customer
-- MAGIC
-- MAGIC        ├── dim_location
-- MAGIC
-- MAGIC        └── dim_policy
-- MAGIC
-- MAGIC Ces tables seront construites à partir de "insurance_catalog.silver.insurance_claims_clean"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1️⃣ Dimension Customer

-- COMMAND ----------

CREATE OR REPLACE TABLE insurance_catalog.gold.dim_customer AS

SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    customer_id,
    age,
    gender,
    marital_status,
    occupation,
    education_level,
    income_level,
    credit_score,
    risk_profile

FROM (
    SELECT DISTINCT *
    FROM insurance_catalog.silver.insurance_claims_clean
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2️⃣ Dimension Location
-- MAGIC
-- MAGIC Objectif : stocker les informations géographiques.

-- COMMAND ----------

CREATE OR REPLACE TABLE insurance_catalog.gold.dim_location AS

SELECT
    ROW_NUMBER() OVER (ORDER BY location) AS location_key,
    location,
    region

FROM (
    SELECT DISTINCT location, region
    FROM insurance_catalog.silver.insurance_claims_clean
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Pourquoi une dimension location** ?
-- MAGIC
-- MAGIC Permet de faire des analyses :
-- MAGIC
-- MAGIC - sinistres par région
-- MAGIC - revenus par zone
-- MAGIC - risque géographique

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3️⃣ Dimension Policy
-- MAGIC
-- MAGIC Objectif : stocker les caractéristiques des polices d’assurance.

-- COMMAND ----------

CREATE OR REPLACE TABLE insurance_catalog.gold.dim_policy AS

SELECT
    ROW_NUMBER() OVER (ORDER BY policy_type) AS policy_key,
    policy_type

FROM (
    SELECT DISTINCT policy_type
    FROM insurance_catalog.silver.insurance_claims_clean
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Cette table permettra d’analyser :
-- MAGIC
-- MAGIC - performance par type de police
-- MAGIC - risque par police

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4️⃣ Table de faits (Fact Table)
-- MAGIC
-- MAGIC Objectif :
-- MAGIC
-- MAGIC Stocker les mesures analytiques :
-- MAGIC
-- MAGIC - premium
-- MAGIC - coverage
-- MAGIC - claim history

-- COMMAND ----------

CREATE OR REPLACE TABLE insurance_catalog.gold.fact_policy AS

WITH cleaned_dates AS (

SELECT
    customer_id,
    location,
    policy_type,
    coverage_amount,
    premium_amount,
    deductible,
    claim_history,
    previous_claims,

    -- conversion des dates
    COALESCE(
        TRY_TO_DATE(policy_start_date, 'dd-MM-yyyy'),
        TRY_TO_DATE(policy_start_date, 'M/d/yyyy')
    ) AS policy_start_date,

    COALESCE(
        TRY_TO_DATE(policy_renewal_date, 'dd-MM-yyyy'),
        TRY_TO_DATE(policy_renewal_date, 'M/d/yyyy')
    ) AS policy_renewal_date

FROM insurance_catalog.silver.insurance_claims_clean

)

SELECT

    c.customer_key,
    l.location_key,
    p.policy_key,
    d.date_key,

    coverage_amount,
    premium_amount,
    deductible,
    claim_history,
    previous_claims,

    policy_start_date,
    policy_renewal_date,

    YEAR(policy_start_date) AS policy_year,
    MONTH(policy_start_date) AS policy_month,
    QUARTER(policy_start_date) AS policy_quarter

FROM cleaned_dates f

LEFT JOIN insurance_catalog.gold.dim_customer c
ON f.customer_id = c.customer_id

LEFT JOIN insurance_catalog.gold.dim_location l
ON f.location = l.location

LEFT JOIN insurance_catalog.gold.dim_policy p
ON f.policy_type = p.policy_type

LEFT JOIN insurance_catalog.gold.dim_date d
ON f.policy_start_date = d.full_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Créer la table dim_date

-- COMMAND ----------

CREATE OR REPLACE TABLE insurance_catalog.gold.dim_date AS

SELECT
    ROW_NUMBER() OVER (ORDER BY full_date) AS date_key,
    full_date,
    year,
    month,
    month_name,
    quarter,
    day_of_month,
    day_of_week,
    day_name

FROM insurance_catalog.gold.dim_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Vérifier les tables

-- COMMAND ----------

SHOW TABLES IN insurance_catalog.gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---------------------------------------------------------------------------------------------------------

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exemple d'analyse avec le Star Schema
-- MAGIC - Premium par région

-- COMMAND ----------

SELECT
    l.region,
    SUM(f.premium_amount) AS total_premium

FROM insurance_catalog.gold.fact_policy f

JOIN insurance_catalog.gold.dim_location l
ON f.location = l.location

GROUP BY l.region
ORDER BY total_premium DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exemple analyse par type de police

-- COMMAND ----------

SELECT
    p.policy_type,
    AVG(f.premium_amount) AS avg_premium

FROM insurance_catalog.gold.fact_policy f

JOIN insurance_catalog.gold.dim_policy p
ON f.policy_type = p.policy_type

GROUP BY p.policy_type;