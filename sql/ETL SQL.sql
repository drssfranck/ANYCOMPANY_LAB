---------------------------------------------------------------
-- ANYCOMPANY DATA PIPELINE - PHASE 1: DATA INGESTION & CLEANING
-- Script complet de préparation et nettoyage des données
-- Date: 31/01/2026
-- Auteur: Franck MBE
---------------------------------------------------------------

---------------------------------------------------------------
-- SECTION 1: ENVIRONMENT SETUP
---------------------------------------------------------------

-- Création de la base de données principale
CREATE OR REPLACE DATABASE ANYCOMPANY_LAB;
USE DATABASE ANYCOMPANY_LAB;

-- Schéma BRONZE : données brutes (landing zone)
CREATE OR REPLACE SCHEMA BRONZE;

-- Schéma SILVER : données nettoyées et validées
CREATE OR REPLACE SCHEMA SILVER;

COMMENT ON SCHEMA BRONZE IS 'Zone de données brutes - Raw data from external sources';
COMMENT ON SCHEMA SILVER IS 'Zone de données nettoyées - Cleaned and validated data';

---------------------------------------------------------------
-- SECTION 2: EXTERNAL STAGE CONFIGURATION
---------------------------------------------------------------

-- Création du stage externe pointant vers le bucket S3
CREATE OR REPLACE STAGE BRONZE.food_beverage_stage
URL = 's3://logbrain-datalake/datasets/food-beverage/'
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"')
COMMENT = 'Stage externe pour les datasets food & beverage';

---------------------------------------------------------------
-- SECTION 3: BRONZE LAYER - RAW TABLES CREATION
---------------------------------------------------------------

-- 3.1 Table des données démographiques clients
CREATE OR REPLACE TABLE BRONZE.customer_demographics (
    customer_id NUMBER,
    name STRING,
    date_of_birth DATE,
    gender STRING,
    region STRING,
    country STRING,
    city STRING,
    marital_status STRING,
    annual_income NUMBER
)
COMMENT = 'Données démographiques brutes des clients';

-- 3.2 Table des interactions avec le service client
CREATE OR REPLACE TABLE BRONZE.customer_service_interactions (
    interaction_id STRING,
    interaction_date DATE,
    interaction_type STRING,
    issue_category STRING,
    description STRING,
    duration_minutes NUMBER,
    resolution_status STRING,
    follow_up_required STRING,
    customer_satisfaction NUMBER
)
COMMENT = 'Interactions brutes du service client';

-- 3.3 Table des transactions financières
CREATE OR REPLACE TABLE BRONZE.financial_transactions (
    transaction_id STRING,
    transaction_date DATE,
    transaction_type STRING,
    amount STRING, -- Stocké en STRING pour le nettoyage ultérieur
    payment_method STRING,
    entity STRING,
    region STRING,
    account_code STRING
)
COMMENT = 'Transactions financières brutes';

-- 3.4 Table des promotions
CREATE OR REPLACE TABLE BRONZE.promotions_data (
    promotion_id STRING,
    product_category STRING,
    promotion_type STRING,
    discount_percentage FLOAT,
    start_date DATE,
    end_date DATE,
    region STRING
)
COMMENT = 'Données brutes des promotions';

-- 3.5 Table des campagnes marketing
CREATE OR REPLACE TABLE BRONZE.marketing_campaigns (
    campaign_id STRING,
    campaign_name STRING,
    campaign_type STRING,
    product_category STRING,
    target_audience STRING,
    start_date DATE,
    end_date DATE,
    region STRING,
    budget NUMBER,
    reach NUMBER,
    conversion_rate FLOAT
)
COMMENT = 'Données brutes des campagnes marketing';

-- 3.6 Table de logistique et expédition
CREATE OR REPLACE TABLE BRONZE.logistics_and_shipping (
    shipment_id STRING,
    order_id NUMBER,
    ship_date DATE,
    estimated_delivery DATE,
    shipping_method STRING,
    status STRING,
    shipping_cost STRING, -- Stocké en STRING pour nettoyage
    destination_region STRING,
    destination_country STRING,
    carrier STRING
)
COMMENT = 'Données brutes de logistique et expédition';

-- 3.7 Table d'information sur les fournisseurs
CREATE OR REPLACE TABLE BRONZE.supplier_information (
    supplier_id STRING,
    supplier_name STRING,
    product_category STRING,
    region STRING,
    country STRING,
    city STRING,
    lead_time NUMBER,
    reliability_score FLOAT,
    quality_rating STRING
)
COMMENT = 'Informations brutes sur les fournisseurs';

-- 3.8 Table des dossiers employés
CREATE OR REPLACE TABLE BRONZE.employee_records (
    employee_id STRING,
    name STRING,
    date_of_birth DATE,
    hire_date DATE,
    department STRING,
    job_title STRING,
    salary STRING, -- Stocké en STRING pour nettoyage
    region STRING,
    country STRING,
    email STRING
)
COMMENT = 'Dossiers bruts des employés';

-- 3.9 Table des avis produits (format brut)
CREATE OR REPLACE TABLE BRONZE.product_reviews (
    raw_line STRING
)
COMMENT = 'Avis produits en format brut (une ligne complète)';

-- 3.10 Format JSON pour les fichiers structurés
CREATE OR REPLACE FILE FORMAT BRONZE.json_format
TYPE = 'JSON'
COMMENT = 'Format pour fichiers JSON';

-- 3.11 Tables de staging pour données JSON
CREATE OR REPLACE TABLE BRONZE.inventory_staging (
    raw_data VARIANT
)
COMMENT = 'Staging pour données JSON d''inventaire';

CREATE OR REPLACE TABLE BRONZE.store_locations_staging (
    raw_data VARIANT
)
COMMENT = 'Staging pour données JSON des magasins';

-- 3.12 Format CSV pour lignes complètes (reviews)
CREATE OR REPLACE FILE FORMAT BRONZE.csv_full_line
TYPE = 'CSV'
FIELD_DELIMITER = NONE
SKIP_HEADER = 1
COMMENT = 'Format CSV pour lignes non délimitées (avis produits)';

---------------------------------------------------------------
-- SECTION 4: DATA LOADING INTO BRONZE LAYER
---------------------------------------------------------------

-- 4.1 Chargement des données démographiques clients
COPY INTO BRONZE.customer_demographics
FROM @BRONZE.food_beverage_stage/customer_demographics.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- 4.2 Chargement des interactions service client
COPY INTO BRONZE.customer_service_interactions
FROM @BRONZE.food_beverage_stage/customer_service_interactions.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- 4.3 Chargement des transactions financières
COPY INTO BRONZE.financial_transactions
FROM @BRONZE.food_beverage_stage/financial_transactions.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- 4.4 Chargement des données de promotions
COPY INTO BRONZE.promotions_data
FROM @BRONZE.food_beverage_stage/promotions-data.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- 4.5 Chargement des campagnes marketing
COPY INTO BRONZE.marketing_campaigns
FROM @BRONZE.food_beverage_stage/marketing_campaigns.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- 4.6 Chargement des données de logistique
COPY INTO BRONZE.logistics_and_shipping
FROM @BRONZE.food_beverage_stage/logistics_and_shipping.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- 4.7 Chargement des informations fournisseurs
COPY INTO BRONZE.supplier_information
FROM @BRONZE.food_beverage_stage/supplier_information.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- 4.8 Chargement des dossiers employés
COPY INTO BRONZE.employee_records
FROM @BRONZE.food_beverage_stage/employee_records.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- 4.9 Chargement des avis produits
COPY INTO BRONZE.product_reviews
FROM @BRONZE.food_beverage_stage/product_reviews.csv
FILE_FORMAT = BRONZE.csv_full_line
ON_ERROR = 'CONTINUE';

-- 4.10 Chargement des données JSON
COPY INTO BRONZE.inventory_staging
FROM @BRONZE.food_beverage_stage/inventory.json
FILE_FORMAT = BRONZE.json_format
ON_ERROR = 'CONTINUE';

COPY INTO BRONZE.store_locations_staging
FROM @BRONZE.food_beverage_stage/store_locations.json
FILE_FORMAT = BRONZE.json_format
ON_ERROR = 'CONTINUE';

---------------------------------------------------------------
-- SECTION 5: SILVER LAYER - DATA CLEANING AND TRANSFORMATION
---------------------------------------------------------------

-- 5.1 Nettoyage des transactions financières
-- Convertit le champ amount de string à numérique et filtre les valeurs invalides
CREATE OR REPLACE TABLE SILVER.financial_transactions_clean AS
SELECT
    transaction_id,
    transaction_date,
    transaction_type,
    TRY_TO_NUMBER(REPLACE(amount, ' ', '')) AS amount, -- Nettoyage des espaces
    payment_method,
    entity,
    region,
    account_code
FROM BRONZE.financial_transactions
WHERE TRY_TO_NUMBER(REPLACE(amount, ' ', '')) > 0 -- Exclut les montants nuls/négatifs
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_date) = 1; -- Dédoublonnage

COMMENT ON TABLE SILVER.financial_transactions_clean IS 'Transactions financières nettoyées et validées';

-- 5.2 Nettoyage des promotions
-- Valide les plages de dates et dédoublonne
CREATE OR REPLACE TABLE SILVER.promotions_clean AS
SELECT DISTINCT
    promotion_id,
    product_category,
    promotion_type,
    discount_percentage,
    start_date,
    end_date,
    region
FROM BRONZE.promotions_data
WHERE start_date <= end_date -- Validation de la cohérence des dates
  AND discount_percentage BETWEEN 0 AND 100; -- Validation du pourcentage

COMMENT ON TABLE SILVER.promotions_clean IS 'Promotions nettoyées avec dates validées';

-- 5.3 Nettoyage des données démographiques
-- Supprime les doublons et les enregistrements incomplets
CREATE OR REPLACE TABLE SILVER.customer_demographics_clean AS
SELECT DISTINCT
    customer_id,
    name,
    date_of_birth,
    gender,
    region,
    country,
    city,
    marital_status,
    annual_income,
    -- Calcul de l'âge à partir de la date de naissance
    DATEDIFF('year', date_of_birth, CURRENT_DATE()) AS age
FROM BRONZE.customer_demographics
WHERE customer_id IS NOT NULL -- Exclut les enregistrements sans ID
  AND date_of_birth IS NOT NULL
  AND annual_income >= 0; -- Validation du revenu

COMMENT ON TABLE SILVER.customer_demographics_clean IS 'Démographiques clients nettoyés et validés';

-- 5.4 Nettoyage des dossiers employés
CREATE OR REPLACE TABLE SILVER.employee_records_clean AS
SELECT
    employee_id,
    name,
    date_of_birth,
    hire_date,
    department,
    job_title,
    TRY_TO_NUMBER(REPLACE(salary, ' ', '')) AS salary, -- Nettoyage du salaire
    region,
    country,
    REPLACE(email, 'mailto:', '') AS email, -- Nettoyage de l'email
    -- Calcul de l'ancienneté en années
    DATEDIFF('year', hire_date, CURRENT_DATE()) AS tenure_years,
    -- Calcul de l'âge de l'employé
    DATEDIFF('year', date_of_birth, CURRENT_DATE()) AS age
FROM BRONZE.employee_records
WHERE TRY_TO_NUMBER(REPLACE(salary, ' ', '')) > 0 -- Validation du salaire
  AND employee_id IS NOT NULL
  AND hire_date IS NOT NULL;

COMMENT ON TABLE SILVER.employee_records_clean IS 'Dossiers employés nettoyés avec calcul d''ancienneté';

-- 5.5 Nettoyage des informations fournisseurs
CREATE OR REPLACE TABLE SILVER.supplier_information_clean AS
SELECT DISTINCT
    supplier_id,
    supplier_name,
    product_category,
    region,
    country,
    city,
    lead_time,
    reliability_score,
    quality_rating,
    -- Catégorisation basée sur le score de fiabilité
    CASE
        WHEN reliability_score >= 0.9 THEN 'Excellent'
        WHEN reliability_score >= 0.7 THEN 'Bon'
        WHEN reliability_score >= 0.5 THEN 'Moyen'
        ELSE 'À améliorer'
    END AS reliability_category
FROM BRONZE.supplier_information
WHERE lead_time > 0 -- Validation du délai de livraison
  AND reliability_score BETWEEN 0 AND 1 -- Validation du score
  AND supplier_id IS NOT NULL;

COMMENT ON TABLE SILVER.supplier_information_clean IS 'Informations fournisseurs nettoyées avec catégorisation';

-- 5.6 Nettoyage des données de logistique
CREATE OR REPLACE TABLE SILVER.logistics_and_shipping_clean AS
SELECT
    shipment_id,
    order_id,
    ship_date,
    estimated_delivery,
    shipping_method,
    status,
    TRY_TO_NUMBER(REPLACE(shipping_cost, ' ', '')) AS shipping_cost, -- Nettoyage du coût
    NULLIF(destination_region, '') AS destination_region, -- Gestion des valeurs vides
    NULLIF(destination_country, '') AS destination_country,
    carrier,
    -- Calcul du délai de livraison en jours
    DATEDIFF('day', ship_date, estimated_delivery) AS estimated_delivery_days,
    -- Indicateur de retard (si la date actuelle dépasse la date estimée)
    CASE
        WHEN CURRENT_DATE() > estimated_delivery THEN 'En retard'
        ELSE 'Dans les temps'
    END AS delivery_status
FROM BRONZE.logistics_and_shipping
WHERE TRY_TO_NUMBER(REPLACE(shipping_cost, ' ', '')) IS NOT NULL
  AND shipment_id IS NOT NULL
  AND ship_date <= estimated_delivery; -- Validation de la cohérence des dates

COMMENT ON TABLE SILVER.logistics_and_shipping_clean IS 'Données logistiques nettoyées avec indicateurs de performance';

-- 5.7 Nettoyage des interactions service client
CREATE OR REPLACE TABLE SILVER.customer_service_interactions_clean AS
SELECT
    interaction_id,
    interaction_date,
    interaction_type,
    issue_category,
    description,
    duration_minutes,
    resolution_status,
    follow_up_required,
    customer_satisfaction,
    -- Catégorisation de la satisfaction
    CASE
        WHEN customer_satisfaction >= 4 THEN 'Très satisfait'
        WHEN customer_satisfaction >= 3 THEN 'Satisfait'
        WHEN customer_satisfaction >= 2 THEN 'Neutre'
        ELSE 'Insatisfait'
    END AS satisfaction_category,
    -- Indicateur de résolution
    CASE
        WHEN resolution_status ILIKE '%resolved%' OR resolution_status ILIKE '%closed%' THEN 1
        ELSE 0
    END AS is_resolved
FROM BRONZE.customer_service_interactions
WHERE interaction_id IS NOT NULL
  AND duration_minutes >= 0; -- Validation de la durée

COMMENT ON TABLE SILVER.customer_service_interactions_clean IS 'Interactions service client nettoyées avec indicateurs';

-- 5.8 Nettoyage des campagnes marketing (CORRIGÉ)
CREATE OR REPLACE TABLE SILVER.marketing_campaigns_clean AS
SELECT
    campaign_id,
    campaign_name,
    campaign_type,
    product_category,
    target_audience,
    start_date,
    end_date,
    region,
    budget,
    reach,
    conversion_rate,
    -- Calcul du coût par acquisition (si applicable)
    CASE
        WHEN conversion_rate > 0 AND budget > 0 THEN budget / (reach * conversion_rate)
        ELSE NULL
    END AS cpa,
    -- Indicateur d'efficacité
    CASE
        WHEN conversion_rate > 0.1 THEN 'Très efficace'
        WHEN conversion_rate > 0.05 THEN 'Efficace'
        WHEN conversion_rate > 0.02 THEN 'Moyenne'
        ELSE 'Peu efficace'
    END AS efficiency_category
FROM BRONZE.marketing_campaigns
WHERE start_date <= end_date
  AND budget >= 0
  AND reach >= 0
  AND conversion_rate BETWEEN 0 AND 1;

COMMENT ON TABLE SILVER.marketing_campaigns_clean IS 'Campagnes marketing nettoyées avec indicateurs d''efficacité';

-- 5.9 Nettoyage des avis produits (SIMPLIFIÉ - nécessite parsing spécifique)
CREATE OR REPLACE TABLE SILVER.product_reviews_clean AS
SELECT
    -- Extraction basique des données (à adapter selon le format réel)
    SPLIT_PART(raw_line, ',', 1) AS reviewer_id,
    SPLIT_PART(raw_line, ',', 2) AS reviewer_name,
    TRY_TO_NUMBER(SPLIT_PART(raw_line, ',', 3)) AS rating,
    TRY_TO_DATE(SPLIT_PART(raw_line, ',', 4)) AS review_date,
    SPLIT_PART(raw_line, ',', 5) AS product_category,
    -- Classification du sentiment basé sur la note
    CASE
        WHEN TRY_TO_NUMBER(SPLIT_PART(raw_line, ',', 3)) >= 4 THEN 'Positive'
        WHEN TRY_TO_NUMBER(SPLIT_PART(raw_line, ',', 3)) = 3 THEN 'Neutral'
        WHEN TRY_TO_NUMBER(SPLIT_PART(raw_line, ',', 3)) <= 2 THEN 'Negative'
        ELSE 'Unknown'
    END AS sentiment
FROM BRONZE.product_reviews
WHERE raw_line IS NOT NULL
  AND raw_line != ''
QUALIFY ROW_NUMBER() OVER (PARTITION BY SPLIT_PART(raw_line, ',', 1) 
                          ORDER BY TRY_TO_DATE(SPLIT_PART(raw_line, ',', 4)) DESC) = 1;

COMMENT ON TABLE SILVER.product_reviews_clean IS 'Avis produits nettoyés avec analyse de sentiment';

-- 5.10 Transformation des données d'inventaire JSON
CREATE OR REPLACE TABLE SILVER.inventory_clean AS
SELECT 
    f.value:product_id::VARCHAR(50) AS product_id,
    f.value:product_category::VARCHAR(100) AS product_category,
    f.value:region::VARCHAR(100) AS region,
    f.value:country::VARCHAR(100) AS country,
    f.value:warehouse::VARCHAR(100) AS warehouse,
    f.value:current_stock::NUMBER AS current_stock,
    f.value:reorder_point::NUMBER AS reorder_point,
    f.value:lead_time::NUMBER AS lead_time,
    f.value:last_restock_date::DATE AS last_restock_date,
    -- Calcul du niveau de stock
    CASE
        WHEN current_stock <= reorder_point THEN 'Critique'
        WHEN current_stock <= reorder_point * 1.5 THEN 'Bas'
        WHEN current_stock <= reorder_point * 3 THEN 'Normal'
        ELSE 'Élevé'
    END AS stock_level,
    -- Calcul des jours depuis le dernier réapprovisionnement
    DATEDIFF('day', last_restock_date, CURRENT_DATE()) AS days_since_restock
FROM BRONZE.inventory_staging,
LATERAL FLATTEN(input => raw_data) f
WHERE f.value:product_id IS NOT NULL;

COMMENT ON TABLE SILVER.inventory_clean IS 'Inventaire structuré avec indicateurs de stock';

-- 5.11 Transformation des données des magasins JSON
CREATE OR REPLACE TABLE SILVER.store_locations_clean AS
SELECT 
    f.value:store_id::VARCHAR(50) AS store_id,
    f.value:store_name::VARCHAR(100) AS store_name,
    f.value:store_type::VARCHAR(50) AS store_type,
    f.value:region::VARCHAR(100) AS region,
    f.value:country::VARCHAR(100) AS country,
    f.value:city::VARCHAR(100) AS city,
    f.value:address::VARCHAR(200) AS address,
    f.value:postal_code::NUMBER AS postal_code,
    f.value:square_footage::DECIMAL(10,2) AS square_footage,
    f.value:employee_count::NUMBER AS employee_count,
    -- Calcul de la densité d'employés
    CASE
        WHEN square_footage > 0 THEN employee_count / square_footage
        ELSE NULL
    END AS employee_density
FROM BRONZE.store_locations_staging,
LATERAL FLATTEN(input => raw_data) f
WHERE f.value:store_id IS NOT NULL;

COMMENT ON TABLE SILVER.store_locations_clean IS 'Localisations magasins avec indicateurs opérationnels';

---------------------------------------------------------------
-- SECTION 6: DATA QUALITY CHECKS
---------------------------------------------------------------

-- 6.1 Vérification des comptes par table (ÉTENDU)
SELECT 'BRONZE Layer' AS layer, table_name, record_count FROM (
    SELECT 'customer_demographics' AS table_name, COUNT(*) AS record_count FROM BRONZE.customer_demographics
    UNION ALL SELECT 'financial_transactions', COUNT(*) FROM BRONZE.financial_transactions
    UNION ALL SELECT 'promotions_data', COUNT(*) FROM BRONZE.promotions_data
    UNION ALL SELECT 'marketing_campaigns', COUNT(*) FROM BRONZE.marketing_campaigns
    UNION ALL SELECT 'logistics_and_shipping', COUNT(*) FROM BRONZE.logistics_and_shipping
    UNION ALL SELECT 'supplier_information', COUNT(*) FROM BRONZE.supplier_information
    UNION ALL SELECT 'employee_records', COUNT(*) FROM BRONZE.employee_records
    UNION ALL SELECT 'product_reviews', COUNT(*) FROM BRONZE.product_reviews
    UNION ALL SELECT 'inventory_staging', COUNT(*) FROM BRONZE.inventory_staging
    UNION ALL SELECT 'store_locations_staging', COUNT(*) FROM BRONZE.store_locations_staging
)
UNION ALL
SELECT 'SILVER Layer' AS layer, table_name, record_count FROM (
    SELECT 'financial_transactions_clean', COUNT(*) FROM SILVER.financial_transactions_clean
    UNION ALL SELECT 'promotions_clean', COUNT(*) FROM SILVER.promotions_clean
    UNION ALL SELECT 'customer_demographics_clean', COUNT(*) FROM SILVER.customer_demographics_clean
    UNION ALL SELECT 'employee_records_clean', COUNT(*) FROM SILVER.employee_records_clean
    UNION ALL SELECT 'supplier_information_clean', COUNT(*) FROM SILVER.supplier_information_clean
    UNION ALL SELECT 'logistics_and_shipping_clean', COUNT(*) FROM SILVER.logistics_and_shipping_clean
    UNION ALL SELECT 'customer_service_interactions_clean', COUNT(*) FROM SILVER.customer_service_interactions_clean
    UNION ALL SELECT 'marketing_campaigns_clean', COUNT(*) FROM SILVER.marketing_campaigns_clean
    UNION ALL SELECT 'product_reviews_clean', COUNT(*) FROM SILVER.product_reviews_clean
    UNION ALL SELECT 'inventory_clean', COUNT(*) FROM SILVER.inventory_clean
    UNION ALL SELECT 'store_locations_clean', COUNT(*) FROM SILVER.store_locations_clean
)
ORDER BY layer, table_name;

-- 6.2 Statistiques de qualité des données
SELECT 
    table_name,
    total_records,
    records_with_null,
    ROUND(records_with_null * 100.0 / total_records, 2) AS null_percentage,
    unique_records
FROM (
    SELECT 
        'financial_transactions_clean' AS table_name,
        COUNT(*) AS total_records,
        SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS records_with_null,
        COUNT(DISTINCT transaction_id) AS unique_records
    FROM SILVER.financial_transactions_clean
    UNION ALL
    SELECT 
        'customer_demographics_clean',
        COUNT(*),
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
        COUNT(DISTINCT customer_id)
    FROM SILVER.customer_demographics_clean
    UNION ALL
    SELECT 
        'employee_records_clean',
        COUNT(*),
        SUM(CASE WHEN employee_id IS NULL THEN 1 ELSE 0 END),
        COUNT(DISTINCT employee_id)
    FROM SILVER.employee_records_clean
) ORDER BY null_percentage DESC;

-- 6.3 Validation des plages de valeurs
SELECT 
    'SALARY' AS metric,
    MIN(salary) AS min_value,
    MAX(salary) AS max_value,
    AVG(salary) AS avg_value,
    STDDEV(salary) AS std_dev
FROM SILVER.employee_records_clean
UNION ALL
SELECT 
    'ANNUAL_INCOME',
    MIN(annual_income),
    MAX(annual_income),
    AVG(annual_income),
    STDDEV(annual_income)
FROM SILVER.customer_demographics_clean
UNION ALL
SELECT 
    'TRANSACTION_AMOUNT',
    MIN(amount),
    MAX(amount),
    AVG(amount),
    STDDEV(amount)
FROM SILVER.financial_transactions_clean;

-- 6.4 Vérification de l'intégrité temporelle
SELECT 
    'Promotions' AS dataset,
    COUNT(*) AS total,
    SUM(CASE WHEN start_date > CURRENT_DATE() THEN 1 ELSE 0 END) AS future_start,
    SUM(CASE WHEN end_date < CURRENT_DATE() THEN 1 ELSE 0 END) AS expired
FROM SILVER.promotions_clean
UNION ALL
SELECT 
    'Campagnes marketing',
    COUNT(*),
    SUM(CASE WHEN start_date > CURRENT_DATE() THEN 1 ELSE 0 END),
    SUM(CASE WHEN end_date < CURRENT_DATE() THEN 1 ELSE 0 END)
FROM SILVER.marketing_campaigns_clean;

