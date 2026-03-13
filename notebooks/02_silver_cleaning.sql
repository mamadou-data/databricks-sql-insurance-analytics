-- Databricks notebook source
-- MAGIC %md
-- MAGIC Objectif :
-- MAGIC
-- MAGIC - nettoyer les données
-- MAGIC - supprimer les colonnes inutiles
-- MAGIC - créer la table Silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Création du schema Silver

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS insurance_catalog.silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Création de la table nettoyée

-- COMMAND ----------

CREATE OR REPLACE TABLE insurance_catalog.silver.insurance_claims_clean AS

SELECT
    `Customer ID`            AS customer_id,
    Age                      AS age,
    Gender                   AS gender,
    `Marital Status`         AS marital_status,
    Occupation               AS occupation,
    `Income Level`           AS income_level,
    `Education Level`        AS education_level,
    `Geographic Information` AS region,
    Location                 AS location,
    `Coverage Amount`        AS coverage_amount,
    `Premium Amount`         AS premium_amount,
    Deductible               AS deductible,
    `Policy Type`            AS policy_type,
    `Claim History`          AS claim_history,
    `Previous Claims History` AS previous_claims,
    `Credit Score`           AS credit_score,
    `Driving Record`         AS driving_record,
    `Risk Profile`           AS risk_profile,
    `Policy Start Date`      AS policy_start_date,
    `Policy Renewal Date`    AS policy_renewal_date

FROM insurance_catalog.raw.insurance_claims;