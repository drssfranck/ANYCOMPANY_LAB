-- ============================================================================
-- DATA PRODUCT ANALYTIQUE - PHASE 3
-- ============================================================================
-- FICHIER 1 : SALES_ENRICHED (Ventes enrichies)
-- Description : Table centrale combinant transactions, promotions et campagnes
-- Granularité : 1 ligne = 1 transaction de vente
-- Clé primaire : transaction_id
-- Usage : Analyses des ventes, segmentation, ML (prédiction revenus)
-- ============================================================================

-- ============================================================================
-- TABLE : sales_enriched
-- ============================================================================
CREATE OR REPLACE TABLE ANALYTICS.sales_enriched 
CLUSTER BY (sale_date, sale_region)
COMMENT = 'Table centrale des ventes enrichies avec promotions et campagnes marketing actives'
AS
SELECT
    -- ========================================================================
    -- IDENTIFIANTS ET DIMENSIONS TRANSACTION
    -- ========================================================================
    ft.transaction_id                 AS sale_id,
    ft.transaction_date               AS sale_date,
    ft.amount                         AS sale_amount,
    ft.payment_method,
    ft.entity                         AS merchant_entity,
    ft.region                         AS sale_region,
    ft.account_code,
    
    -- ========================================================================
    -- FEATURES TEMPORELLES (optimisées pour ML et analyse)
    -- ========================================================================
    EXTRACT(YEAR FROM ft.transaction_date)     AS sale_year,
    EXTRACT(MONTH FROM ft.transaction_date)    AS sale_month,
    EXTRACT(QUARTER FROM ft.transaction_date)  AS sale_quarter,
    EXTRACT(DAYOFWEEK FROM ft.transaction_date) AS sale_day_of_week,
    DATE_TRUNC('WEEK', ft.transaction_date)    AS sale_week_start,
    
    -- ========================================================================
    -- DIMENSIONS PROMOTIONNELLES (jointure temporelle)
    -- ========================================================================
    p.promotion_id                    AS promotion_id,
    p.product_category                AS promo_product_category,
    p.promotion_type,
    p.discount_percentage,
    p.start_date                      AS promo_start_date,
    p.end_date                        AS promo_end_date,
    
    -- Indicateurs binaires pour ML
    CASE 
        WHEN p.promotion_id IS NOT NULL THEN 1 
        ELSE 0 
    END                               AS has_promotion,
    
    COALESCE(p.discount_percentage, 0) AS discount_rate,
    
    -- ========================================================================
    -- DIMENSIONS MARKETING (jointure temporelle et géographique)
    -- ========================================================================
    m.campaign_id,
    m.campaign_name,
    m.campaign_type,
    m.product_category                AS campaign_product_category,
    m.target_audience                 AS campaign_target,
    m.budget                          AS campaign_budget,
    m.reach                           AS campaign_reach,
    m.conversion_rate                 AS campaign_conversion_rate,
    
    -- Indicateurs binaires pour ML
    CASE 
        WHEN m.campaign_id IS NOT NULL THEN 1 
        ELSE 0 
    END                               AS has_campaign,
    
    -- ========================================================================
    -- MÉTRIQUES CALCULÉES & BUSINESS LOGIC
    -- ========================================================================
    -- Montant net après application des promotions
    ft.amount * (1 - COALESCE(p.discount_percentage, 0) / 100.0) AS net_amount,
    
    -- Impact estimé de la campagne sur cette transaction
    CASE 
        WHEN m.campaign_id IS NOT NULL AND m.conversion_rate > 0
        THEN ft.amount * m.conversion_rate 
        ELSE 0 
    END                               AS estimated_campaign_impact,
    
    -- Catégorisation du montant pour segmentation et analyse
    CASE 
        WHEN ft.amount < 1000     THEN 'Low'
        WHEN ft.amount < 5000     THEN 'Medium'
        WHEN ft.amount < 10000    THEN 'High'
        ELSE 'Very High'
    END                               AS amount_category,
    
    -- Indicateur de saisonnalité (week-end vs semaine)
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM ft.transaction_date) IN (1, 7) 
        THEN 'Weekend'
        ELSE 'Weekday'
    END                               AS day_type,
    
    -- ========================================================================
    -- MÉTADONNÉES TECHNIQUES
    -- ========================================================================
    CURRENT_TIMESTAMP()               AS created_at,
    CURRENT_USER()                    AS created_by,
    'v1.0'                            AS data_version,
    'SALES_ENRICHED'                  AS data_source

FROM SILVER.financial_transactions_clean ft

-- Jointure avec les promotions actives au moment de la transaction
LEFT JOIN SILVER.promotions_clean p
    ON ft.region = p.region
    AND ft.transaction_date BETWEEN p.start_date AND p.end_date

-- Jointure avec les campagnes marketing actives au moment de la transaction
LEFT JOIN SILVER.marketing_campaigns_clean m
    ON ft.region = m.region
    AND ft.transaction_date BETWEEN m.start_date AND m.end_date

-- Filtrage : ne conserver que les transactions de type "vente"
WHERE ft.transaction_type = 'Sale'
;

-- ============================================================================
-- DOCUMENTATION DE LA TABLE
-- ============================================================================

COMMENT ON TABLE ANALYTICS.sales_enriched IS 
'Table analytique centrale des ventes enrichie avec contextes promotionnel et marketing. 
Sert de source unique pour analyses commerciales et modèles prédictifs.';

-- ============================================================================
-- VUE : daily_sales_summary (Résumé quotidien des ventes)
-- ============================================================================
-- Description : Agrégation quotidienne pour dashboards et reporting
-- Granularité : 1 ligne = 1 jour × 1 région
-- Usage : Dashboards BI, monitoring quotidien
-- ============================================================================

CREATE OR REPLACE VIEW ANALYTICS.daily_sales_summary AS
SELECT
    sale_date,
    sale_region,
    
    -- Métriques de vente
    COUNT(*) AS total_transactions,
    SUM(sale_amount) AS total_revenue,
    AVG(sale_amount) AS avg_transaction_value,
    
    -- Métriques promotionnelles
    COUNT(DISTINCT promotion_id) AS active_promotions,
    SUM(has_promotion) AS transactions_with_promo,
    ROUND(SUM(has_promotion) * 100.0 / NULLIF(COUNT(*), 0), 2) AS promo_penetration_rate,
    
    -- Métriques campagnes
    COUNT(DISTINCT campaign_id) AS active_campaigns,
    SUM(has_campaign) AS transactions_with_campaign,
    
    -- Métriques de discount
    AVG(discount_rate) AS avg_discount_rate,
    SUM(sale_amount - net_amount) AS total_discount_given,
    
    -- Breakdown par méthode de paiement
    SUM(CASE WHEN payment_method = 'Credit Card' THEN 1 ELSE 0 END) AS cc_transactions,
    SUM(CASE WHEN payment_method = 'Cash' THEN 1 ELSE 0 END) AS cash_transactions,
    SUM(CASE WHEN payment_method = 'Bank Transfer' THEN 1 ELSE 0 END) AS transfer_transactions
    
FROM ANALYTICS.sales_enriched
GROUP BY sale_date, sale_region
;

COMMENT ON VIEW ANALYTICS.daily_sales_summary IS 
'Résumé quotidien des ventes par région. Usage: Dashboards, monitoring quotidien, reporting opérationnel.';

-- ============================================================================
-- TESTS DE QUALITÉ SPÉCIFIQUES À LA TABLE sales_enriched
-- ============================================================================

-- Test 1 : Vérifier l'intégrité des montants
SELECT 
    'Test 1: Montants négatifs' AS test_name,
    COUNT(*) AS failed_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL - ' || COUNT(*) || ' montants négatifs détectés'
    END AS test_result
FROM ANALYTICS.sales_enriched
WHERE sale_amount < 0;

-- Test 2 : Vérifier les taux de discount cohérents
SELECT 
    'Test 2: Discount > 100%' AS test_name,
    COUNT(*) AS failed_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL - ' || COUNT(*) || ' discounts > 100% détectés'
    END AS test_result
FROM ANALYTICS.sales_enriched
WHERE discount_rate > 100;

-- Test 3 : Vérifier que net_amount <= sale_amount
SELECT 
    'Test 3: Net amount > Sale amount' AS test_name,
    COUNT(*) AS failed_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL - ' || COUNT(*) || ' incohérences de montants détectées'
    END AS test_result
FROM ANALYTICS.sales_enriched
WHERE net_amount > sale_amount;

-- Test 4 : Vérifier les dates de ventes cohérentes
SELECT 
    'Test 4: Dates futures' AS test_name,
    COUNT(*) AS failed_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL - ' || COUNT(*) || ' dates futures détectées'
    END AS test_result
FROM ANALYTICS.sales_enriched
WHERE sale_date > CURRENT_DATE();

-- ============================================================================
-- STATISTIQUES DESCRIPTIVES
-- ============================================================================

SELECT 
    'SALES_ENRICHED' AS table_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT sale_id) AS unique_sales,
    COUNT(DISTINCT sale_region) AS unique_regions,
    MIN(sale_date) AS earliest_date,
    MAX(sale_date) AS latest_date,
    ROUND(SUM(sale_amount), 2) AS total_revenue,
    ROUND(AVG(sale_amount), 2) AS avg_transaction,
    ROUND(SUM(has_promotion) * 100.0 / COUNT(*), 2) AS promo_coverage_pct,
    ROUND(SUM(has_campaign) * 100.0 / COUNT(*), 2) AS campaign_coverage_pct
FROM ANALYTICS.sales_enriched;