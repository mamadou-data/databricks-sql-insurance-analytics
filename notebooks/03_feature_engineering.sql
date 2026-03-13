-- Databricks notebook source
-- MAGIC Objectif :
-- créer des variables analytiques
-- MAGIC - segmentation client

--## Catégoriser l’âge

SELECT
customer_id,
age,

CASE
WHEN age < 30 THEN 'Young'
WHEN age BETWEEN 30 AND 50 THEN 'Adult'
ELSE 'Senior'
END AS age_group

FROM insurance_catalog.silver.insurance_claims_clean;

-- CASE WHEN équivalent de "if / else"

-- ## Ratio prime / couverture

SELECT
customer_id,
premium_amount,
coverage_amount,

premium_amount / coverage_amount AS premium_ratio

FROM insurance_catalog.silver.insurance_claims_clean;

-- On crée une nouvelle métrique analytique.
