-- Databricks notebook source
-- MAGIC %md
-- MAGIC Objectif :
-- MAGIC
-- MAGIC - comprendre le dataset
-- MAGIC - vérifier les données
-- MAGIC - apprendre les bases SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Voir les données

-- COMMAND ----------

-- afficher les premières lignes

SELECT *
FROM insurance_catalog.raw.insurance_claims
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Compter le nombre de lignes

-- COMMAND ----------

-- nombre total d'observations

SELECT
COUNT(*) AS total_rows
FROM insurance_catalog.raw.insurance_claims;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Vérifier les valeurs manquantes

-- COMMAND ----------

-- vérifier les valeurs nulles dans certaines colonnes

SELECT
COUNT(*) AS total_rows,
COUNT(`Income Level`) AS income_not_null,
COUNT(`Credit Score`) AS credit_not_null
FROM insurance_catalog.raw.insurance_claims;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC COUNT(colonne) compte uniquement les valeurs non nulles.