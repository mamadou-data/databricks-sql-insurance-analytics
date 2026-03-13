-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 1️⃣ Top N clients par couverture
-- MAGIC
-- MAGIC Objectif : trouver les clients avec la plus grande couverture.

-- COMMAND ----------

-- DBTITLE 1,Classement clients selon couverture
-- classement des clients selon leur couverture

SELECT
    customer_key,
    coverage_amount,

    RANK() OVER(
        ORDER BY coverage_amount DESC
    ) AS coverage_rank

FROM insurance_catalog.gold.fact_policy;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2️⃣ Top client par région
-- MAGIC
-- MAGIC Objectif : identifier le meilleur client dans chaque région.

-- COMMAND ----------

-- DBTITLE 1,Cell 4
WITH ranked_clients AS (

SELECT
    f.customer_key,
    l.region,
    f.coverage_amount,

    ROW_NUMBER() OVER(
        PARTITION BY l.region
        ORDER BY f.coverage_amount DESC
    ) AS rank_region

FROM insurance_catalog.gold.fact_policy f

JOIN insurance_catalog.gold.dim_location l
ON f.location_key = l.location_key

)

SELECT *
FROM ranked_clients
WHERE rank_region = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3️⃣ Analyse temporelle des polices
-- MAGIC
-- MAGIC Objectif : voir l’évolution des polices dans le temps.

-- COMMAND ----------

SELECT
    YEAR(policy_start_date) AS year,
    COUNT(*) AS number_of_policies

FROM insurance_catalog.gold.fact_policy

GROUP BY YEAR(policy_start_date)
ORDER BY year;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4️⃣ Running Total des premiums
-- MAGIC
-- MAGIC Objectif : calculer un revenu cumulé.

-- COMMAND ----------

SELECT
    policy_start_date,
    premium_amount,

    SUM(premium_amount) OVER(
        ORDER BY policy_start_date
    ) AS cumulative_premium

FROM insurance_catalog.gold.fact_policy;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5️⃣ Analyse du risque client
-- MAGIC
-- MAGIC Objectif : voir quels profils sont les plus risqués.

-- COMMAND ----------

-- DBTITLE 1,Cell 10
SELECT
    d.risk_profile,
    AVG(f.coverage_amount) AS avg_coverage,
    AVG(d.credit_score) AS avg_credit_score

FROM insurance_catalog.gold.fact_policy f

JOIN insurance_catalog.gold.dim_customer d
ON f.customer_key = d.customer_key

GROUP BY d.risk_profile;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6️⃣ Customer Lifetime Value (LTV)
-- MAGIC
-- MAGIC Objectif : calculer la valeur client totale.

-- COMMAND ----------

-- DBTITLE 1,Cell 12
SELECT
    customer_key,
    SUM(premium_amount) AS customer_lifetime_value

FROM insurance_catalog.gold.fact_policy

GROUP BY customer_key
ORDER BY customer_lifetime_value DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7️⃣ Analyse des polices par segment

-- COMMAND ----------

-- DBTITLE 1,Cell 14
SELECT
    policy_key,
    COUNT(*) AS number_of_policies,
    AVG(premium_amount) AS avg_premium,
    AVG(coverage_amount) AS avg_coverage

FROM insurance_catalog.gold.fact_policy

GROUP BY policy_key
ORDER BY number_of_policies DESC;