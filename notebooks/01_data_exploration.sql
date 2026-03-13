-- Databricks notebook source
-- Objectif :
-- comprendre le dataset
-- vérifier les données
-- apprendre les bases SQL

-- ## Voir les données

-- afficher les premières lignes

SELECT *
FROM insurance_catalog.raw.insurance_claims
LIMIT 20;

-- ## Compter le nombre de lignes

-- nombre total d'observations

SELECT
COUNT(*) AS total_rows
FROM insurance_catalog.raw.insurance_claims;


-- ## Vérifier les valeurs manquantes

-- vérifier les valeurs nulles dans certaines colonnes

SELECT
COUNT(*) AS total_rows,
COUNT(`Income Level`) AS income_not_null,
COUNT(`Credit Score`) AS credit_not_null
FROM insurance_catalog.raw.insurance_claims;

-- COUNT(colonne) compte uniquement les valeurs non nulles.
