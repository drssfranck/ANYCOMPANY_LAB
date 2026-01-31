-- ============================================================================
-- DATA PRODUCT ANALYTIQUE - PHASE 3
-- ============================================================================
-- FICHIER 3 : CUSTOMERS_ENRICHED & MARKETING_PERFORMANCE
-- Description : Tables pour segmentation client et performance marketing
-- Usage : CRM, personnalisation, optimisation budget, analyse ROI campagnes
-- ============================================================================

USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- TABLE : customers_enriched (Clients enrichis)
-- Description : Base clients avec comportements d'achat et segmentation
-- Granularité : 1 ligne = 1 client
-- Clé primaire : customer_id
-- ============================================================================

-- D'abord, vérifions quelles colonnes sont disponibles
-- SELECT * FROM SILVER.customer_service_interactions_clean LIMIT 5;

CREATE OR REPLACE TABLE ANALYTICS.customers_enriched 
CLUSTER BY (customer_region, customer_segment, income_segment)
COMMENT = 'Base clients enrichie avec données démographiques et comportementales pour segmentation et CRM'
AS
SELECT
    -- ========================================================================
    -- IDENTIFIANTS ET DIMENSIONS CLIENT
    -- ========================================================================
    cd.customer_id,
    cd.name AS customer_name,
    cd.date_of_birth,
    cd.gender,
    cd.region AS customer_region,
    cd.country AS customer_country,
    cd.city AS customer_city,
    cd.marital_status,
    cd.annual_income,
    
    -- ========================================================================
    -- FEATURES DÉMOGRAPHIQUES CALCULÉES
    -- ========================================================================
    DATEDIFF('year', cd.date_of_birth, CURRENT_DATE()) AS age,
    
    CASE 
        WHEN age < 25 THEN '18-24'
        WHEN age < 35 THEN '25-34'
        WHEN age < 45 THEN '35-44'
        WHEN age < 55 THEN '45-54'
        WHEN age < 65 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    
    CASE 
        WHEN cd.annual_income < 30000 THEN 'Low Income'
        WHEN cd.annual_income < 60000 THEN 'Lower Middle Income'
        WHEN cd.annual_income < 100000 THEN 'Upper Middle Income'
        WHEN cd.annual_income < 150000 THEN 'High Income'
        ELSE 'Very High Income'
    END AS income_segment,
    
    -- ========================================================================
    -- MÉTRIQUES COMPORTEMENTALES (depuis les ventes)
    -- ========================================================================
    COALESCE(s.total_spent, 0) AS lifetime_value,
    COALESCE(s.transaction_count, 0) AS total_transactions,
    COALESCE(s.avg_transaction, 0) AS avg_transaction_value,
    COALESCE(s.first_purchase_date, CURRENT_DATE()) AS first_purchase_date,
    COALESCE(s.last_purchase_date, CURRENT_DATE()) AS last_purchase_date,
    COALESCE(s.days_since_last_purchase, 999) AS days_since_last_purchase,
    
    -- ========================================================================
    -- MÉTRIQUES D'ENGAGEMENT (SI disponible - commenté car customer_id manquant)
    -- ========================================================================
    -- COALESCE(csi.interaction_count, 0) AS service_interaction_count,
    -- COALESCE(csi.avg_satisfaction, 0) AS avg_satisfaction_score,
    -- COALESCE(csi.complaint_count, 0) AS complaint_count,
    -- COALESCE(csi.last_interaction_date, NULL) AS last_contact_date,
    
    0 AS service_interaction_count,  -- Valeur par défaut
    0 AS avg_satisfaction_score,     -- Valeur par défaut
    0 AS complaint_count,            -- Valeur par défaut
    NULL AS last_contact_date,       -- Valeur par défaut
    
    -- ========================================================================
    -- SEGMENTATION CLIENT (RFM adaptatif)
    -- ========================================================================
    CASE 
        WHEN COALESCE(s.days_since_last_purchase, 999) <= 30 THEN 'Recent'
        WHEN COALESCE(s.days_since_last_purchase, 999) <= 90 THEN 'Active'
        WHEN COALESCE(s.days_since_last_purchase, 999) <= 180 THEN 'At Risk'
        ELSE 'Churned'
    END AS recency_segment,
    
    CASE 
        WHEN COALESCE(s.total_spent, 0) >= 10000 THEN 'VIP'
        WHEN COALESCE(s.total_spent, 0) >= 5000 THEN 'High Value'
        WHEN COALESCE(s.total_spent, 0) >= 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS monetary_segment,
    
    CASE 
        WHEN COALESCE(s.transaction_count, 0) >= 20 THEN 'Frequent'
        WHEN COALESCE(s.transaction_count, 0) >= 10 THEN 'Regular'
        WHEN COALESCE(s.transaction_count, 0) >= 3 THEN 'Occasional'
        ELSE 'Rare'
    END AS frequency_segment,
    
    -- Segment composite
    CASE 
        WHEN recency_segment = 'Recent' AND monetary_segment IN ('VIP', 'High Value') THEN 'Champions'
        WHEN recency_segment = 'Recent' AND frequency_segment = 'Frequent' THEN 'Loyal Customers'
        WHEN recency_segment = 'At Risk' AND monetary_segment IN ('VIP', 'High Value') THEN 'At Risk High Value'
        WHEN recency_segment = 'Churned' AND monetary_segment IN ('VIP', 'High Value') THEN 'Lost Champions'
        WHEN recency_segment = 'Recent' THEN 'New/Recent Customers'
        WHEN monetary_segment = 'Low Value' THEN 'Price Sensitive'
        ELSE 'Other'
    END AS customer_segment,
    
    -- ========================================================================
    -- SCORES ET INDICATEURS DE RISQUE/FIDÉLITÉ
    -- ========================================================================
    CASE 
        WHEN recency_segment = 'Churned' THEN 5
        WHEN recency_segment = 'At Risk' THEN 3
        ELSE 1
    END AS churn_risk_score,
    
    CASE 
        WHEN monetary_segment IN ('VIP', 'High Value') AND recency_segment IN ('Recent', 'Active')
        THEN 5
        WHEN monetary_segment IN ('Medium Value', 'Low Value') AND recency_segment IN ('Recent', 'Active')
        THEN 3
        ELSE 1
    END AS loyalty_score,
    
    -- ========================================================================
    -- MÉTADONNÉES TECHNIQUES
    -- ========================================================================
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_USER() AS created_by,
    'v1.0' AS data_version,
    'CUSTOMERS_ENRICHED' AS data_source

FROM SILVER.customer_demographics_clean cd

-- Jointure avec les statistiques de ventes
LEFT JOIN (
    SELECT 
        TRY_CAST(merchant_entity AS NUMBER) AS customer_id_num,
        SUM(sale_amount) AS total_spent,
        COUNT(*) AS transaction_count,
        AVG(sale_amount) AS avg_transaction,
        MIN(sale_date) AS first_purchase_date,
        MAX(sale_date) AS last_purchase_date,
        DATEDIFF('day', MAX(sale_date), CURRENT_DATE()) AS days_since_last_purchase
    FROM ANALYTICS.sales_enriched
    WHERE TRY_CAST(merchant_entity AS NUMBER) IS NOT NULL
    GROUP BY TRY_CAST(merchant_entity AS NUMBER)
) s ON cd.customer_id = s.customer_id_num

-- NOTE: La jointure avec customer_service_interactions_clean est commentée car
-- cette table n'a pas de customer_id dans votre schéma actuel
-- LEFT JOIN ( ... ) csi ON cd.customer_id = csi.customer_id
;

-- ============================================================================
-- VÉRIFICATION ET STATISTIQUES
-- ============================================================================

-- Vérifier que la table a été créée
SELECT 
    'CUSTOMERS_ENRICHED' AS table_name,
    COUNT(*) AS total_customers,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT customer_region) AS regions_covered,
    ROUND(AVG(lifetime_value), 2) AS avg_lifetime_value,
    ROUND(AVG(age), 1) AS avg_age,
    COUNT(CASE WHEN customer_segment = 'Champions' THEN 1 END) AS champion_count,
    COUNT(CASE WHEN customer_segment = 'At Risk High Value' THEN 1 END) AS at_risk_vip_count,
    COUNT(CASE WHEN customer_segment = 'Churned' THEN 1 END) AS churned_count
FROM ANALYTICS.customers_enriched;

-- ============================================================================
-- DOCUMENTATION DE LA TABLE customers_enriched
-- ============================================================================

COMMENT ON TABLE ANALYTICS.customers_enriched IS 
'Base clients enrichie avec segmentation RFM adaptative, métriques comportementales et scores de fidélité.';

-- ============================================================================
-- TABLE : marketing_performance (Performance campagnes marketing)
-- Description : Vue consolidée des performances marketing par campagne
-- Granularité : 1 ligne = 1 campagne
-- Clé primaire : campaign_id
-- ============================================================================

CREATE OR REPLACE TABLE ANALYTICS.marketing_performance 
CLUSTER BY (start_date, region, campaign_type)
COMMENT = 'Performance détaillée des campagnes marketing avec ROI et métriques d''efficacité'
AS
SELECT
    -- ========================================================================
    -- DIMENSIONS CAMPAGNE
    -- ========================================================================
    m.campaign_id,
    m.campaign_name,
    m.campaign_type,
    m.product_category,
    m.target_audience,
    m.start_date,
    m.end_date,
    m.region,
    
    -- ========================================================================
    -- FEATURES TEMPORELLES
    -- ========================================================================
    DATEDIFF(DAY, m.start_date, m.end_date) AS campaign_duration_days,
    EXTRACT(MONTH FROM m.start_date) AS start_month,
    EXTRACT(QUARTER FROM m.start_date) AS start_quarter,
    EXTRACT(YEAR FROM m.start_date) AS start_year,
    
    -- ========================================================================
    -- MÉTRIQUES DE BASE
    -- ========================================================================
    m.budget AS campaign_budget,
    m.reach AS estimated_reach,
    m.conversion_rate AS target_conversion_rate,
    
    -- ========================================================================
    -- MÉTRIQUES DE PERFORMANCE (depuis sales_enriched)
    -- ========================================================================
    COALESCE(s.actual_sales, 0) AS actual_sales,
    COALESCE(s.generated_revenue, 0) AS generated_revenue,
    COALESCE(s.unique_customers, 0) AS unique_customers_acquired,
    COALESCE(s.avg_transaction, 0) AS avg_transaction_value,
    
    -- ========================================================================
    -- CALCULS D'EFFICACITÉ ET ROI
    -- ========================================================================
    -- ROI
    CASE 
        WHEN m.budget > 0 
        THEN ROUND((COALESCE(s.generated_revenue, 0) - m.budget) * 100.0 / m.budget, 2)
        ELSE 0 
    END AS roi_percentage,
    
    -- Revenu par euro dépensé
    CASE 
        WHEN m.budget > 0 
        THEN ROUND(COALESCE(s.generated_revenue, 0) / m.budget, 2)
        ELSE 0 
    END AS revenue_per_euro_spent,
    
    -- Taux de conversion réel
    CASE 
        WHEN m.reach > 0 
        THEN ROUND(COALESCE(s.actual_sales, 0) * 100.0 / m.reach, 4)
        ELSE 0 
    END AS actual_conversion_rate,
    
    -- Coût par acquisition (CPA)
    CASE 
        WHEN COALESCE(s.actual_sales, 0) > 0 
        THEN ROUND(m.budget / COALESCE(s.actual_sales, 0), 2)
        ELSE NULL 
    END AS cost_per_acquisition,
    
    -- Coût par client unique
    CASE 
        WHEN COALESCE(s.unique_customers, 0) > 0 
        THEN ROUND(m.budget / COALESCE(s.unique_customers, 0), 2)
        ELSE NULL 
    END AS cost_per_unique_customer,
    
    -- Valeur vie client moyenne générée
    CASE 
        WHEN COALESCE(s.unique_customers, 0) > 0 
        THEN ROUND(COALESCE(s.generated_revenue, 0) / COALESCE(s.unique_customers, 0), 2)
        ELSE 0 
    END AS avg_customer_lifetime_value,
    
    -- ========================================================================
    -- ÉVALUATION PERFORMANCE
    -- ========================================================================
    CASE 
        WHEN roi_percentage > 200 THEN 'Excellent'
        WHEN roi_percentage > 100 THEN 'Good'
        WHEN roi_percentage > 50 THEN 'Acceptable'
        WHEN roi_percentage > 0 THEN 'Poor'
        ELSE 'Negative'
    END AS performance_rating,
    
    -- Indicateur d'efficacité
    CASE 
        WHEN actual_conversion_rate > target_conversion_rate * 1.2 THEN 'Exceeds Target'
        WHEN actual_conversion_rate >= target_conversion_rate THEN 'Meets Target'
        WHEN actual_conversion_rate > target_conversion_rate * 0.8 THEN 'Below Target'
        ELSE 'Underperforming'
    END AS conversion_performance,
    
    -- ========================================================================
    -- MÉTADONNÉES TECHNIQUES
    -- ========================================================================
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_USER() AS created_by,
    'v1.0' AS data_version,
    'MARKETING_PERFORMANCE' AS data_source

FROM SILVER.marketing_campaigns_clean m

-- Agréger les performances depuis sales_enriched
LEFT JOIN (
    SELECT 
        campaign_id,
        COUNT(*) AS actual_sales,
        SUM(sale_amount) AS generated_revenue,
        COUNT(DISTINCT merchant_entity) AS unique_customers,
        AVG(sale_amount) AS avg_transaction
    FROM ANALYTICS.sales_enriched
    WHERE campaign_id IS NOT NULL
    GROUP BY campaign_id
) s ON m.campaign_id = s.campaign_id
;

-- ============================================================================
-- VÉRIFICATION MARKETING_PERFORMANCE
-- ============================================================================

SELECT 
    'MARKETING_PERFORMANCE' AS table_name,
    COUNT(*) AS total_campaigns,
    COUNT(DISTINCT campaign_type) AS campaign_types,
    ROUND(AVG(roi_percentage), 2) AS avg_roi,
    ROUND(SUM(generated_revenue), 2) AS total_generated_revenue,
    ROUND(SUM(campaign_budget), 2) AS total_budget_spent,
    ROUND(AVG(actual_conversion_rate), 4) AS avg_conversion_rate,
    COUNT(CASE WHEN performance_rating = 'Excellent' THEN 1 END) AS excellent_campaigns,
    COUNT(CASE WHEN performance_rating = 'Negative' THEN 1 END) AS negative_roi_campaigns
FROM ANALYTICS.marketing_performance;

-- ============================================================================
-- VUE : v_top_performing_campaigns
-- ============================================================================

CREATE OR REPLACE VIEW ANALYTICS.v_top_performing_campaigns AS
SELECT 
    campaign_name,
    campaign_type,
    region,
    start_date,
    end_date,
    campaign_budget,
    generated_revenue,
    roi_percentage,
    cost_per_acquisition,
    actual_conversion_rate,
    performance_rating,
    conversion_performance
FROM ANALYTICS.marketing_performance
WHERE roi_percentage > 0
ORDER BY roi_percentage DESC, generated_revenue DESC
LIMIT 20
;

COMMENT ON VIEW ANALYTICS.v_top_performing_campaigns IS 
'Top 20 des campagnes marketing les plus performantes par ROI et revenu généré.';

-- ============================================================================
-- TESTS DE QUALITÉ SPÉCIFIQUES
-- ============================================================================

-- Test 1 : Vérifier ROI des campagnes réalistes
SELECT 
    'Test 1: ROI anormal (> 1000%)' AS test_name,
    COUNT(*) AS failed_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL - ' || COUNT(*) || ' ROI anormaux détectés'
    END AS test_result
FROM ANALYTICS.marketing_performance
WHERE roi_percentage > 1000;

-- Test 2 : Vérifier les budgets cohérents
SELECT 
    'Test 2: Budgets négatifs' AS test_name,
    COUNT(*) AS failed_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL - ' || COUNT(*) || ' budgets négatifs détectés'
    END AS test_result
FROM ANALYTICS.marketing_performance
WHERE campaign_budget < 0;

-- Test 3 : Vérifier les taux de conversion cohérents
SELECT 
    'Test 3: Conversion rate > 100%' AS test_name,
    COUNT(*) AS failed_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL - ' || COUNT(*) || ' taux de conversion > 100% détectés'
    END AS test_result
FROM ANALYTICS.marketing_performance
WHERE actual_conversion_rate > 100 OR target_conversion_rate > 100;

-- ============================================================================
-- STATISTIQUES SYNTHÈSE
-- ============================================================================

SELECT 
    'CUSTOMERS_ENRICHED' AS dataset,
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(AVG(lifetime_value), 2) AS avg_lifetime_value
FROM ANALYTICS.customers_enriched
UNION ALL
SELECT 
    'MARKETING_PERFORMANCE',
    COUNT(*),
    COUNT(DISTINCT campaign_id),
    ROUND(AVG(roi_percentage), 2)
FROM ANALYTICS.marketing_performance;

-- ============================================================================
-- EXEMPLES DE REQUÊTES UTILES
-- ============================================================================

-- Top 10 clients par valeur
SELECT 
    customer_name,
    customer_segment,
    lifetime_value,
    total_transactions,
    days_since_last_purchase,
    churn_risk_score
FROM ANALYTICS.customers_enriched
ORDER BY lifetime_value DESC
LIMIT 10;

-- Analyse des campagnes par type
SELECT 
    campaign_type,
    COUNT(*) AS campaign_count,
    ROUND(AVG(roi_percentage), 2) AS avg_roi,
    ROUND(SUM(generated_revenue), 2) AS total_revenue,
    ROUND(AVG(cost_per_acquisition), 2) AS avg_cpa
FROM ANALYTICS.marketing_performance
GROUP BY campaign_type
ORDER BY avg_roi DESC;

-- Distribution des segments clients
SELECT 
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(lifetime_value), 2) AS avg_lifetime_value,
    ROUND(AVG(age), 1) AS avg_age,
    ROUND(AVG(annual_income), 2) AS avg_income,
    COUNT(CASE WHEN churn_risk_score >= 3 THEN 1 END) AS at_risk_count
FROM ANALYTICS.customers_enriched
GROUP BY customer_segment
ORDER BY avg_lifetime_value DESC;