-- ============================================================================
-- DATA PRODUCT ANALYTIQUE - PHASE 3
-- ============================================================================
-- FICHIER 2 : PROMOTIONS_ACTIVE (Promotions actives et performances)
-- Description : Catalogue des promotions avec métriques de performance
-- Granularité : 1 ligne = 1 promotion
-- Clé primaire : promotion_id
-- Usage : Évaluation ROI, planification promotions, optimisation stratégique
-- ============================================================================

USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- TABLE : promotions_active
-- ============================================================================
CREATE OR REPLACE TABLE ANALYTICS.promotions_active 
CLUSTER BY (start_date, region, promotion_type)
COMMENT = 'Catalogue des promotions actives et historiques avec métriques de performance'
AS
SELECT
    -- ========================================================================
    -- DIMENSIONS PROMOTION
    -- ========================================================================
    p.promotion_id,
    p.product_category,
    p.promotion_type,
    p.discount_percentage,
    p.start_date,
    p.end_date,
    p.region,
    
    -- ========================================================================
    -- FEATURES TEMPORELLES
    -- ========================================================================
    DATEDIFF(DAY, p.start_date, p.end_date) AS duration_days,
    EXTRACT(MONTH FROM p.start_date) AS start_month,
    EXTRACT(QUARTER FROM p.start_date) AS start_quarter,
    EXTRACT(YEAR FROM p.start_date) AS start_year,
    
    -- ========================================================================
    -- STATUT DE LA PROMOTION (calculé dynamiquement)
    -- ========================================================================
    CASE 
        WHEN CURRENT_DATE() < p.start_date THEN 'Scheduled'
        WHEN CURRENT_DATE() BETWEEN p.start_date AND p.end_date THEN 'Active'
        WHEN CURRENT_DATE() > p.end_date THEN 'Completed'
        ELSE 'Unknown'
    END AS promotion_status,
    
    -- ========================================================================
    -- MÉTRIQUES DE PERFORMANCE (agrégées depuis sales_enriched)
    -- ========================================================================
    COALESCE(s.sale_count, 0) AS total_sales,
    COALESCE(s.total_revenue, 0) AS total_gross_revenue,
    COALESCE(s.avg_transaction, 0) AS avg_transaction_amount,
    COALESCE(s.unique_customers_reached, 0) AS unique_customers_reached,
    
    -- ========================================================================
    -- CALCULS DE ROI ET EFFICACITÉ
    -- ========================================================================
    -- Coût des remises
    COALESCE(s.total_discount_given, 0) AS total_discount_cost,
    
    -- Revenu net après remises
    COALESCE(s.total_net_revenue, 0) AS total_net_revenue,
    
    -- ROI de la promotion
    CASE 
        WHEN COALESCE(s.total_discount_given, 0) > 0
        THEN ROUND(
            (COALESCE(s.total_net_revenue, 0) - COALESCE(s.total_discount_given, 0)) * 100.0 / 
            COALESCE(s.total_discount_given, 0), 2
        )
        ELSE 0 
    END AS roi_percentage,
    
    -- Revenu par euro de remise
    CASE 
        WHEN COALESCE(s.total_discount_given, 0) > 0
        THEN ROUND(COALESCE(s.total_net_revenue, 0) / COALESCE(s.total_discount_given, 0), 2)
        ELSE NULL 
    END AS revenue_per_discount_euro,
    
    -- Taux de pénétration (à calculer séparément)
    0 AS market_share_pct,  -- Placeholder - à calculer dans une étape ultérieure
    
    -- ========================================================================
    -- MÉTADONNÉES TECHNIQUES
    -- ========================================================================
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_USER() AS created_by,
    'v1.0' AS data_version,
    'PROMOTIONS_ACTIVE' AS data_source

FROM SILVER.promotions_clean p

-- Agréger les ventes liées à chaque promotion avec plus de détails
LEFT JOIN (
    SELECT 
        promotion_id,
        COUNT(*) AS sale_count,
        SUM(sale_amount) AS total_revenue,
        SUM(net_amount) AS total_net_revenue,
        AVG(sale_amount) AS avg_transaction,
        COUNT(DISTINCT merchant_entity) AS unique_customers_reached,  -- CORRECTION: Nom correct
        SUM(sale_amount - net_amount) AS total_discount_given
    FROM ANALYTICS.sales_enriched
    WHERE promotion_id IS NOT NULL
    GROUP BY promotion_id
) s ON p.promotion_id = s.promotion_id
;

-- ============================================================================
-- MISE À JOUR : Calculer la part de marché après création de la table
-- ============================================================================

-- Créer une vue pour les ventes totales par région et jour
CREATE OR REPLACE VIEW ANALYTICS.v_daily_region_sales AS
SELECT 
    sale_region,
    sale_date,
    COUNT(*) AS daily_sales_count,
    SUM(sale_amount) AS daily_total_revenue
FROM ANALYTICS.sales_enriched
GROUP BY sale_region, sale_date;

-- Mettre à jour la part de marché dans promotions_active
CREATE OR REPLACE TABLE ANALYTICS.promotions_active_enhanced AS
SELECT 
    pa.*,
    CASE 
        WHEN market.total_sales_in_period > 0
        THEN ROUND(pa.total_sales * 100.0 / market.total_sales_in_period, 2)
        ELSE 0 
    END AS market_share_pct_calculated,
    COALESCE(market.total_sales_in_period, 0) AS total_market_sales
FROM ANALYTICS.promotions_active pa
LEFT JOIN (
    SELECT 
        p.promotion_id,
        SUM(drs.daily_sales_count) AS total_sales_in_period
    FROM SILVER.promotions_clean p
    LEFT JOIN ANALYTICS.v_daily_region_sales drs 
        ON p.region = drs.sale_region
        AND drs.sale_date BETWEEN p.start_date AND p.end_date
    GROUP BY p.promotion_id
) market ON pa.promotion_id = market.promotion_id;

-- Remplacer la table originale par la version améliorée
DROP TABLE ANALYTICS.promotions_active;
ALTER TABLE ANALYTICS.promotions_active_enhanced RENAME TO promotions_active;

-- ============================================================================
-- DOCUMENTATION DE LA TABLE
-- ============================================================================

COMMENT ON TABLE ANALYTICS.promotions_active IS 
'Catalogue complet des promotions avec métriques détaillées de performance.
Inclut ROI, pénétration marché, et efficacité par type de promotion.';

COMMENT ON COLUMN ANALYTICS.promotions_active.roi_percentage IS 
'Return on Investment de la promotion. Calcul: (Revenu net - Coût remises) * 100 / Coût remises.';

COMMENT ON COLUMN ANALYTICS.promotions_active.market_share_pct IS 
'Part de marché capturée par la promotion pendant sa période d''activité.';

-- ============================================================================
-- VUE : v_current_promotions (Promotions actuellement actives)
-- ============================================================================

CREATE OR REPLACE VIEW ANALYTICS.v_current_promotions AS
SELECT 
    promotion_id,
    product_category,
    promotion_type,
    discount_percentage,
    start_date,
    end_date,
    region,
    duration_days,
    total_sales,
    total_gross_revenue,
    total_discount_cost,
    roi_percentage,
    revenue_per_discount_euro,
    market_share_pct,
    DATEDIFF(DAY, CURRENT_DATE(), end_date) AS days_remaining
FROM ANALYTICS.promotions_active
WHERE promotion_status = 'Active'
ORDER BY end_date ASC, roi_percentage DESC
;

COMMENT ON VIEW ANALYTICS.v_current_promotions IS 
'Vue des promotions actuellement actives avec jours restants et métriques de performance.';

-- ============================================================================
-- TESTS DE QUALITÉ SPÉCIFIQUES À LA TABLE promotions_active
-- ============================================================================

-- Test 1 : Vérifier les durées de promotion cohérentes
SELECT 
    'Test 1: Durations négatives' AS test_name,
    COUNT(*) AS failed_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL - ' || COUNT(*) || ' durées négatives détectées'
    END AS test_result
FROM ANALYTICS.promotions_active
WHERE duration_days < 0;

-- Test 2 : Vérifier les taux de discount cohérents
SELECT 
    'Test 2: Discount hors limites' AS test_name,
    COUNT(*) AS failed_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL - ' || COUNT(*) || ' discounts hors limites détectés'
    END AS test_result
FROM ANALYTICS.promotions_active
WHERE discount_percentage < 0 OR discount_percentage > 100;

-- Test 3 : Vérifier les dates de promotion
SELECT 
    'Test 3: Dates incohérentes' AS test_name,
    COUNT(*) AS failed_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL - ' || COUNT(*) || ' dates incohérentes détectées'
    END AS test_result
FROM ANALYTICS.promotions_active
WHERE start_date > end_date;

-- Test 4 : Vérifier le ROI cohérent
SELECT 
    'Test 4: ROI extrêmes' AS test_name,
    COUNT(*) AS failed_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL - ' || COUNT(*) || ' ROI extrêmes détectés'
    END AS test_result
FROM ANALYTICS.promotions_active
WHERE roi_percentage > 10000 OR roi_percentage < -10000;

-- ============================================================================
-- STATISTIQUES DESCRIPTIVES
-- ============================================================================

SELECT 
    'PROMOTIONS_ACTIVE' AS table_name,
    COUNT(*) AS total_promotions,
    COUNT(DISTINCT product_category) AS unique_categories,
    COUNT(DISTINCT promotion_type) AS unique_promotion_types,
    COUNT(DISTINCT region) AS unique_regions,
    MIN(start_date) AS earliest_promotion,
    MAX(end_date) AS latest_promotion,
    ROUND(AVG(discount_percentage), 2) AS avg_discount_rate,
    ROUND(AVG(roi_percentage), 2) AS avg_roi,
    SUM(total_gross_revenue) AS total_generated_revenue,
    SUM(total_discount_cost) AS total_discount_costs,
    ROUND(AVG(market_share_pct), 2) AS avg_market_share
FROM ANALYTICS.promotions_active;

-- ============================================================================
-- ANALYSE DE PERFORMANCE PAR TYPE DE PROMOTION
-- ============================================================================

SELECT 
    promotion_type,
    COUNT(*) AS promotion_count,
    ROUND(AVG(discount_percentage), 2) AS avg_discount,
    SUM(total_sales) AS total_transactions,
    SUM(total_gross_revenue) AS total_revenue,
    ROUND(AVG(roi_percentage), 2) AS avg_roi,
    ROUND(AVG(revenue_per_discount_euro), 2) AS avg_revenue_per_discount,
    ROUND(AVG(market_share_pct), 2) AS avg_market_share,
    CASE 
        WHEN AVG(roi_percentage) > 100 THEN 'High Performing'
        WHEN AVG(roi_percentage) > 50 THEN 'Medium Performing'
        WHEN AVG(roi_percentage) > 0 THEN 'Low Performing'
        ELSE 'Negative ROI'
    END AS performance_category
FROM ANALYTICS.promotions_active
GROUP BY promotion_type
ORDER BY avg_roi DESC;

-- ============================================================================
-- VUE : v_promotion_performance_summary (Résumé des performances)
-- ============================================================================

CREATE OR REPLACE VIEW ANALYTICS.v_promotion_performance_summary AS
SELECT 
    promotion_status,
    COUNT(*) AS promotion_count,
    ROUND(AVG(discount_percentage), 2) AS avg_discount_pct,
    SUM(total_sales) AS total_transactions,
    SUM(total_gross_revenue) AS total_revenue_generated,
    ROUND(AVG(roi_percentage), 2) AS avg_roi,
    ROUND(AVG(market_share_pct), 2) AS avg_market_share
FROM ANALYTICS.promotions_active
GROUP BY promotion_status
ORDER BY 
    CASE promotion_status
        WHEN 'Active' THEN 1
        WHEN 'Scheduled' THEN 2
        WHEN 'Completed' THEN 3
        ELSE 4
    END;

COMMENT ON VIEW ANALYTICS.v_promotion_performance_summary IS 
'Résumé des performances des promotions par statut.';

-- ============================================================================
-- VÉRIFICATION FINALE
-- ============================================================================

SELECT 
    '✅ PROMOTIONS_ACTIVE créée avec succès' AS message,
    COUNT(*) AS total_promotions,
    COUNT(CASE WHEN promotion_status = 'Active' THEN 1 END) AS active_promotions,
    COUNT(CASE WHEN promotion_status = 'Scheduled' THEN 1 END) AS scheduled_promotions,
    COUNT(CASE WHEN promotion_status = 'Completed' THEN 1 END) AS completed_promotions,
    ROUND(AVG(roi_percentage), 2) AS average_roi,
    ROUND(SUM(total_gross_revenue), 2) AS total_revenue_generated
FROM ANALYTICS.promotions_active;