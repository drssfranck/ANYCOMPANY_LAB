---------------------------------------------------------------
-- ANYCOMPANY DATA PIPELINE - PHASE 2: DATA EXPLORATION & ANALYSIS
-- Script d'exploration des données et analyses business
-- Date: 31/01/2025
-- Auteur: Franck MBE
---------------------------------------------------------------

USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;

---------------------------------------------------------------
-- PARTIE 2.1 – COMPRÉHENSION DES JEUX DE DONNÉES
---------------------------------------------------------------

-- 2.1.1 Vue d'ensemble des tables SILVER
SELECT 
    TABLE_NAME,
    COUNT(*) AS column_count,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c2 
     WHERE c2.TABLE_NAME = c1.TABLE_NAME 
     AND c2.IS_NULLABLE = 'YES') AS nullable_columns,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c2 
     WHERE c2.TABLE_NAME = c1.TABLE_NAME 
     AND c2.DATA_TYPE IN ('NUMBER', 'DECIMAL', 'FLOAT')) AS numeric_columns
FROM INFORMATION_SCHEMA.COLUMNS c1
WHERE TABLE_SCHEMA = 'SILVER'
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;

-- 2.1.2 Périmètre métier et volumes par table
WITH table_stats AS (
    SELECT 
        'financial_transactions_clean' AS table_name,
        'Transactions financières' AS business_domain,
        COUNT(*) AS record_count,
        MIN(transaction_date) AS min_date,
        MAX(transaction_date) AS max_date,
        'transaction_id' AS primary_key,
        COUNT(DISTINCT transaction_id) AS distinct_keys
    FROM financial_transactions_clean
    UNION ALL
    SELECT 
        'customer_demographics_clean',
        'Démographie clients',
        COUNT(*),
        MIN(date_of_birth),
        MAX(date_of_birth),
        'customer_id',
        COUNT(DISTINCT customer_id)
    FROM customer_demographics_clean
    UNION ALL
    SELECT 
        'promotions_clean',
        'Promotions commerciales',
        COUNT(*),
        MIN(start_date),
        MAX(end_date),
        'promotion_id',
        COUNT(DISTINCT promotion_id)
    FROM promotions_clean
    UNION ALL
    SELECT 
        'marketing_campaigns_clean',
        'Campagnes marketing',
        COUNT(*),
        MIN(start_date),
        MAX(end_date),
        'campaign_id',
        COUNT(DISTINCT campaign_id)
    FROM marketing_campaigns_clean
    UNION ALL
    SELECT 
        'inventory_clean',
        'Gestion stocks',
        COUNT(*),
        MIN(last_restock_date),
        MAX(last_restock_date),
        'product_id',
        COUNT(DISTINCT product_id)
    FROM inventory_clean
    UNION ALL
    SELECT 
        'employee_records_clean',
        'Ressources humaines',
        COUNT(*),
        MIN(hire_date),
        MAX(hire_date),
        'employee_id',
        COUNT(DISTINCT employee_id)
    FROM employee_records_clean
    UNION ALL
    SELECT 
        'supplier_information_clean',
        'Gestion fournisseurs',
        COUNT(*),
        NULL,
        NULL,
        'supplier_id',
        COUNT(DISTINCT supplier_id)
    FROM supplier_information_clean
    UNION ALL
    SELECT 
        'logistics_and_shipping_clean',
        'Logistique',
        COUNT(*),
        MIN(ship_date),
        MAX(estimated_delivery),
        'shipment_id',
        COUNT(DISTINCT shipment_id)
    FROM logistics_and_shipping_clean
    UNION ALL
    SELECT 
        'customer_service_interactions_clean',
        'Service client',
        COUNT(*),
        MIN(interaction_date),
        MAX(interaction_date),
        'interaction_id',
        COUNT(DISTINCT interaction_id)
    FROM customer_service_interactions_clean
    UNION ALL
    SELECT 
        'product_reviews_clean',
        'Avis clients',
        COUNT(*),
        MIN(review_date),
        MAX(review_date),
        'reviewer_id',
        COUNT(DISTINCT reviewer_id)
    FROM product_reviews_clean
    UNION ALL
    SELECT 
        'store_locations_clean',
        'Points de vente',
        COUNT(*),
        NULL,
        NULL,
        'store_id',
        COUNT(DISTINCT store_id)
    FROM store_locations_clean
)
SELECT 
    table_name,
    business_domain,
    record_count,
    min_date,
    max_date,
    primary_key,
    distinct_keys,
    CASE 
        WHEN record_count = distinct_keys THEN 'OK'
        ELSE 'DOUBLONS POTENTIELS'
    END AS data_quality
FROM table_stats
ORDER BY record_count DESC;

-- 2.1.3 Analyse des valeurs manquantes par table clé
SELECT 
    'financial_transactions_clean' AS table_name,
    COUNT(*) AS total_records,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS missing_amount,
    SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) AS missing_date,
    SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS missing_region,
    ROUND(SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_missing_amount
FROM financial_transactions_clean
UNION ALL
SELECT 
    'customer_demographics_clean',
    COUNT(*),
    SUM(CASE WHEN annual_income IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN date_of_birth IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN annual_income IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM customer_demographics_clean
UNION ALL
SELECT 
    'customer_service_interactions_clean',
    COUNT(*),
    SUM(CASE WHEN customer_satisfaction IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN interaction_date IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN duration_minutes IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN customer_satisfaction IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM customer_service_interactions_clean;

-- 2.1.4 Distribution des données numériques clés
WITH numeric_stats AS (
    SELECT 
        'Montant transaction' AS metric,
        MIN(amount) AS min_value,
        MAX(amount) AS max_value,
        AVG(amount) AS avg_value,
        MEDIAN(amount) AS median_value,
        STDDEV(amount) AS std_dev,
        COUNT(*) AS sample_size
    FROM financial_transactions_clean
    UNION ALL
    SELECT 
        'Revenu annuel client',
        MIN(annual_income),
        MAX(annual_income),
        AVG(annual_income),
        MEDIAN(annual_income),
        STDDEV(annual_income),
        COUNT(*)
    FROM customer_demographics_clean
    UNION ALL
    SELECT 
        'Salaire employé',
        MIN(salary),
        MAX(salary),
        AVG(salary),
        MEDIAN(salary),
        STDDEV(salary),
        COUNT(*)
    FROM employee_records_clean
    UNION ALL
    SELECT 
        'Stock actuel',
        MIN(current_stock),
        MAX(current_stock),
        AVG(current_stock),
        MEDIAN(current_stock),
        STDDEV(current_stock),
        COUNT(*)
    FROM inventory_clean
)
SELECT 
    metric,
    min_value,
    max_value,
    avg_value,
    median_value,
    std_dev,
    sample_size,
    CASE 
        WHEN std_dev > avg_value THEN 'FORTE DISPERSION'
        WHEN std_dev > avg_value * 0.5 THEN 'DISPERSION MODÉRÉE'
        ELSE 'FAIBLE DISPERSION'
    END AS variability_assessment
FROM numeric_stats;

---------------------------------------------------------------
-- PARTIE 2.2 – ANALYSES EXPLORATOIRES DESCRIPTIVES
---------------------------------------------------------------

-- 2.2.1 Évolution des ventes dans le temps (mensuelle)
SELECT 
    DATE_TRUNC('month', transaction_date) AS month,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_transaction_value,
    COUNT(DISTINCT entity) AS unique_entities
FROM financial_transactions_clean
WHERE transaction_type = 'Sale'  
GROUP BY DATE_TRUNC('month', transaction_date)
ORDER BY month;


-- 2.2.2 Évolution des ventes avec tendance (trimestrielle)
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('quarter', transaction_date) AS quarter,
        SUM(amount) AS total_revenue,
        COUNT(*) AS transaction_count
    FROM financial_transactions_clean
    WHERE transaction_type = 'Sale'
    GROUP BY DATE_TRUNC('quarter', transaction_date)
)
SELECT 
    quarter,
    total_revenue,
    transaction_count,
    total_revenue / transaction_count AS avg_ticket,
    LAG(total_revenue) OVER (ORDER BY quarter) AS previous_quarter_revenue,
    ROUND((total_revenue - LAG(total_revenue) OVER (ORDER BY quarter)) * 100.0 / 
          LAG(total_revenue) OVER (ORDER BY quarter), 2) AS growth_rate_pct
FROM monthly_sales
ORDER BY quarter;

-- 2.2.3 Performance par région
SELECT 
    region,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_transaction_value,
    COUNT(DISTINCT entity) AS unique_entities,
    ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS revenue_share_pct
FROM financial_transactions_clean
WHERE transaction_type = 'Sale'
GROUP BY region
ORDER BY total_revenue DESC;

-- 2.2.4 Top 10 des entités (clients/magasins) par chiffre d'affaires
SELECT 
    entity,
    region,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_transaction_value,
    MAX(amount) AS max_transaction,
    MIN(amount) AS min_transaction
FROM financial_transactions_clean
WHERE transaction_type = 'Sale'
GROUP BY entity, region
ORDER BY total_revenue DESC
LIMIT 10;

-- 2.2.5 Répartition des clients par segments démographiques
SELECT 
    CASE 
        WHEN age < 25 THEN '18-24'
        WHEN age < 35 THEN '25-34'
        WHEN age < 45 THEN '35-44'
        WHEN age < 55 THEN '45-54'
        WHEN age < 65 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    gender,
    marital_status,
    region,
    COUNT(*) AS customer_count,
    AVG(annual_income) AS avg_income,
    MIN(annual_income) AS min_income,
    MAX(annual_income) AS max_income
FROM customer_demographics_clean
GROUP BY 
    CASE 
        WHEN age < 25 THEN '18-24'
        WHEN age < 35 THEN '25-34'
        WHEN age < 45 THEN '35-44'
        WHEN age < 55 THEN '45-54'
        WHEN age < 65 THEN '55-64'
        ELSE '65+'
    END,
    gender,
    marital_status,
    region
ORDER BY age_group, gender, customer_count DESC;

-- 2.2.6 Analyse du panier moyen par segment client
WITH customer_segments AS (
    SELECT 
        cd.customer_id,
        CASE 
            WHEN TRY_TO_NUMBER(cd.annual_income) < 30000 THEN 'Faible revenu'
            WHEN TRY_TO_NUMBER(cd.annual_income) < 60000 THEN 'Revenu moyen'
            WHEN TRY_TO_NUMBER(cd.annual_income) < 100000 THEN 'Revenu élevé'
            ELSE 'Très haut revenu'
        END AS income_segment,
        cd.region,
        cd.age,
        TRY_TO_NUMBER(ft.amount) AS amount
    FROM customer_demographics_clean cd
    JOIN financial_transactions_clean ft 
        ON cd.customer_id = ft.entity
    WHERE ft.transaction_type = 'Sale'
      AND TRY_TO_NUMBER(cd.annual_income) IS NOT NULL
      AND TRY_TO_NUMBER(ft.amount) IS NOT NULL
)
SELECT 
    income_segment,
    region,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_transaction_value,
    SUM(amount) / COUNT(DISTINCT customer_id) AS revenue_per_customer
FROM customer_segments
GROUP BY income_segment, region
ORDER BY revenue_per_customer DESC;





---------------------------------------------------------------
-- PARTIE 2.3 – ANALYSES BUSINESS TRANSVERSES
---------------------------------------------------------------

-- 2.3.1 VENTES ET PROMOTIONS
-- Comparaison ventes avec/sans promotion (par catégorie)
WITH promotion_periods AS (
    SELECT 
        product_category,
        start_date,
        end_date
    FROM promotions_clean
    WHERE CURRENT_DATE() BETWEEN start_date AND end_date
),
sales_with_promotion AS (
    SELECT 
        p.product_category,
        'Avec promotion' AS promotion_status,
        COUNT(*) AS transaction_count,
        SUM(ft.amount) AS total_revenue,
        AVG(ft.amount) AS avg_transaction_value
    FROM financial_transactions_clean ft
    JOIN promotion_periods p ON ft.transaction_date BETWEEN p.start_date AND p.end_date
    WHERE ft.transaction_type = 'Sale'
    GROUP BY p.product_category
),
sales_without_promotion AS (
    SELECT 
        'Sans promotion' AS promotion_status,
        COUNT(*) AS transaction_count,
        SUM(ft.amount) AS total_revenue,
        AVG(ft.amount) AS avg_transaction_value
    FROM financial_transactions_clean ft
    WHERE ft.transaction_type = 'Sale'
    AND NOT EXISTS (
        SELECT 1 FROM promotion_periods p 
        WHERE ft.transaction_date BETWEEN p.start_date AND p.end_date
    )
)
SELECT * FROM sales_with_promotion
UNION ALL
SELECT 'Toutes catégories', promotion_status, transaction_count, total_revenue, avg_transaction_value 
FROM sales_without_promotion
ORDER BY product_category, promotion_status;

-- 2.3.2 Sensibilité des catégories aux promotions
WITH promotion_impact AS (
    SELECT 
        p.product_category,
        p.discount_percentage,
        COUNT(DISTINCT p.promotion_id) AS promotion_count,
        COUNT(ft.transaction_id) AS sales_during_promotion,
        SUM(ft.amount) AS revenue_during_promotion,
        AVG(ft.amount) AS avg_sale_during_promotion
    FROM promotions_clean p
    LEFT JOIN financial_transactions_clean ft 
        ON ft.transaction_date BETWEEN p.start_date AND p.end_date
        AND ft.transaction_type = 'Sale'
    GROUP BY p.product_category, p.discount_percentage
),
category_baseline AS (
    SELECT 
        REGEXP_SUBSTR(entity, '([A-Za-z]+)') AS inferred_category,
        COUNT(*) AS total_sales,
        AVG(amount) AS avg_sale_baseline
    FROM financial_transactions_clean
    WHERE transaction_type = 'Sale'
    GROUP BY REGEXP_SUBSTR(entity, '([A-Za-z]+)')
)
SELECT 
    pi.product_category,
    pi.discount_percentage,
    pi.promotion_count,
    pi.sales_during_promotion,
    pi.revenue_during_promotion,
    cb.total_sales,
    cb.avg_sale_baseline,
    pi.avg_sale_during_promotion,
    ROUND((pi.avg_sale_during_promotion - cb.avg_sale_baseline) * 100.0 / cb.avg_sale_baseline, 2) AS lift_percentage
FROM promotion_impact pi
LEFT JOIN category_baseline cb ON pi.product_category = cb.inferred_category
ORDER BY lift_percentage DESC;

-- 2.3.3 MARKETING ET PERFORMANCE COMMERCIALE
-- Lien campagnes ↔ ventes
SELECT 
    mc.campaign_name,
    mc.campaign_type,
    mc.start_date,
    mc.end_date,
    mc.budget,
    mc.reach,
    mc.conversion_rate,
    COUNT(ft.transaction_id) AS resulting_sales,
    SUM(ft.amount) AS campaign_revenue,
    ROUND(mc.budget * 100.0 / NULLIF(SUM(ft.amount), 0), 2) AS roi_percentage,
    ROUND(SUM(ft.amount) / NULLIF(mc.budget, 0), 2) AS revenue_per_budget_unit
FROM marketing_campaigns_clean mc
LEFT JOIN financial_transactions_clean ft 
    ON ft.transaction_date BETWEEN mc.start_date AND mc.end_date
    AND ft.transaction_type = 'Sale'
    AND ft.region = mc.region
GROUP BY 
    mc.campaign_name, mc.campaign_type, mc.start_date, mc.end_date, 
    mc.budget, mc.reach, mc.conversion_rate
ORDER BY roi_percentage DESC;

-- 2.3.4 Identification des campagnes les plus efficaces
WITH campaign_metrics AS (
    SELECT 
        mc.campaign_id,
        mc.campaign_name,
        mc.campaign_type,
        mc.region,
        mc.budget,
        mc.conversion_rate,
        COUNT(ft.transaction_id) AS sales_count,
        SUM(ft.amount) AS generated_revenue,
        ROUND(SUM(ft.amount) / NULLIF(mc.budget, 0), 2) AS revenue_per_euro
    FROM marketing_campaigns_clean mc
    LEFT JOIN financial_transactions_clean ft 
        ON ft.transaction_date BETWEEN mc.start_date AND mc.end_date
        AND ft.transaction_type = 'Sale'
        AND ft.region = mc.region
    GROUP BY mc.campaign_id, mc.campaign_name, mc.campaign_type, mc.region, mc.budget, mc.conversion_rate
)
SELECT 
    campaign_name,
    campaign_type,
    region,
    budget,
    conversion_rate,
    sales_count,
    generated_revenue,
    revenue_per_euro,
    CASE 
        WHEN revenue_per_euro > 10 THEN 'TRÈS EFFICACE'
        WHEN revenue_per_euro > 5 THEN 'EFFICACE'
        WHEN revenue_per_euro > 2 THEN 'MOYENNE'
        ELSE 'PEU EFFICACE'
    END AS efficiency_rating,
    RANK() OVER (ORDER BY revenue_per_euro DESC) AS efficiency_rank
FROM campaign_metrics
WHERE budget > 0
ORDER BY efficiency_rank;

-- 2.3.5 EXPÉRIENCE CLIENT
-- Impact des avis produits sur les ventes (analyse corrélation)
WITH review_metrics AS (
    SELECT 
        pr.product_category,
        pr.sentiment,
        COUNT(DISTINCT pr.reviewer_id) AS reviewer_count,
        AVG(pr.rating) AS avg_rating,
        COUNT(pr.reviewer_id) AS review_count
    FROM product_reviews_clean pr
    GROUP BY pr.product_category, pr.sentiment
),
sales_metrics AS (
    SELECT 
        REGEXP_SUBSTR(ft.entity, '([A-Za-z]+)') AS inferred_category,
        COUNT(*) AS sales_count,
        SUM(ft.amount) AS sales_revenue,
        AVG(ft.amount) AS avg_sale_value
    FROM financial_transactions_clean ft
    WHERE ft.transaction_type = 'Sale'
    GROUP BY REGEXP_SUBSTR(ft.entity, '([A-Za-z]+)')
)
SELECT 
    rm.product_category,
    rm.sentiment,
    rm.avg_rating,
    rm.review_count,
    sm.sales_count,
    sm.sales_revenue,
    sm.avg_sale_value,
    ROUND(sm.sales_revenue / NULLIF(rm.review_count, 0), 2) AS revenue_per_review
FROM review_metrics rm
LEFT JOIN sales_metrics sm ON rm.product_category = sm.inferred_category
WHERE rm.review_count > 10
ORDER BY revenue_per_review DESC;

-- 2.3.6 Influence des interactions service client sur la satisfaction
SELECT 
    csi.resolution_status,
    csi.satisfaction_category,
    COUNT(*) AS interaction_count,
    AVG(csi.duration_minutes) AS avg_duration,
    SUM(CASE WHEN csi.follow_up_required = 'Yes' THEN 1 ELSE 0 END) AS follow_up_count,
    ROUND(AVG(cd.annual_income), 2) AS avg_customer_income,
    COUNT(DISTINCT csi.interaction_id) AS unique_interactions
FROM customer_service_interactions_clean csi
LEFT JOIN customer_demographics_clean cd ON csi.customer_id = cd.customer_id
GROUP BY csi.resolution_status, csi.satisfaction_category
ORDER BY interaction_count DESC;

-- 2.3.7 OPÉRATIONS ET LOGISTIQUE
-- Analyse des ruptures de stock
SELECT 
    i.product_category,
    i.region,
    i.country,
    i.warehouse,
    COUNT(*) AS product_count,
    SUM(CASE WHEN i.current_stock <= i.reorder_point THEN 1 ELSE 0 END) AS critical_stock_items,
    SUM(CASE WHEN i.current_stock = 0 THEN 1 ELSE 0 END) AS out_of_stock_items,
    ROUND(AVG(i.days_since_restock), 1) AS avg_days_since_restock,
    ROUND(AVG(i.lead_time), 1) AS avg_lead_time
FROM inventory_clean i
GROUP BY i.product_category, i.region, i.country, i.warehouse
HAVING SUM(CASE WHEN i.current_stock <= i.reorder_point THEN 1 ELSE 0 END) > 0
ORDER BY critical_stock_items DESC;

-- 2.3.8 Impact des délais de livraison sur la satisfaction client
WITH delivery_analysis AS (
    SELECT 
        ls.shipment_id,
        ls.order_id,
        ls.shipping_method,
        ls.carrier,
        ls.estimated_delivery_days,
        ls.delivery_status,
        csi.customer_satisfaction,
        csi.satisfaction_category
    FROM logistics_and_shipping_clean ls
    LEFT JOIN customer_service_interactions_clean csi 
        ON ls.order_id = csi.order_id
    WHERE csi.issue_category like '%DELIVERY%'
)
SELECT 
    shipping_method,
    carrier,
    delivery_status,
    AVG(estimated_delivery_days) AS avg_delivery_days,
    AVG(customer_satisfaction) AS avg_satisfaction_score,
    COUNT(*) AS shipment_count,
    SUM(CASE WHEN satisfaction_category = 'Très satisfait' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS high_satisfaction_pct
FROM delivery_analysis
GROUP BY shipping_method, carrier, delivery_status
ORDER BY avg_satisfaction_score DESC;

-- 2.3.9 Analyse croisée: Stock vs Ventes vs Satisfaction
WITH inventory_sales AS (
    SELECT 
        i.product_category,
        i.region,
        i.stock_level,
        COUNT(DISTINCT i.product_id) AS product_count,
        AVG(i.current_stock) AS avg_stock_level,
        SUM(ft.amount) AS total_sales,
        COUNT(ft.transaction_id) AS sales_count,
        AVG(pr.rating) AS avg_product_rating
    FROM inventory_clean i
    LEFT JOIN financial_transactions_clean ft 
        ON REGEXP_SUBSTR(ft.entity, '([A-Za-z]+)') = i.product_category
        AND ft.region = i.region
        AND ft.transaction_type = 'Sale'
    LEFT JOIN product_reviews_clean pr 
        ON pr.product_category = i.product_category
    GROUP BY i.product_category, i.region, i.stock_level
)
SELECT 
    product_category,
    region,
    stock_level,
    product_count,
    avg_stock_level,
    total_sales,
    sales_count,
    avg_product_rating,
    ROUND(total_sales / NULLIF(product_count, 0), 2) AS sales_per_product,
    CASE 
        WHEN stock_level = 'Critique' AND avg_product_rating > 4 THEN 'ALERTE: Bon produit mais stock critique'
        WHEN stock_level = 'Élevé' AND total_sales IS NULL THEN 'ALERTE: Surstock produit non vendu'
        WHEN stock_level IN ('Normal', 'Bas') AND avg_product_rating > 4.5 THEN 'OPPORTUNITÉ: Augmenter stock produit populaire'
        ELSE 'Situation normale'
    END AS business_insight
FROM inventory_sales
ORDER BY total_sales DESC NULLS LAST;

---------------------------------------------------------------
-- PARTIE 2.4 – SYNTHÈSE ET RECOMMANDATIONS
---------------------------------------------------------------

-- 2.4.1 Tableau de bord synthétique des performances
CREATE OR REPLACE VIEW SILVER.business_dashboard AS
WITH kpis AS (
    -- Chiffre d'affaires total
    SELECT 'CA Total' AS metric, SUM(amount) AS value FROM financial_transactions_clean WHERE transaction_type = 'Sale'
    UNION ALL
    -- Nombre de transactions
    SELECT 'Nombre transactions', COUNT(*) FROM financial_transactions_clean WHERE transaction_type = 'Sale'
    UNION ALL
    -- Panier moyen
    SELECT 'Panier moyen', AVG(amount) FROM financial_transactions_clean WHERE transaction_type = 'Sale'
    UNION ALL
    -- Clients uniques
    SELECT 'Clients uniques', COUNT(DISTINCT entity) FROM financial_transactions_clean WHERE transaction_type = 'Sale'
    UNION ALL
    -- Taux de conversion marketing
    SELECT 'Taux conversion moyen', AVG(conversion_rate) * 100 FROM marketing_campaigns_clean
    UNION ALL
    -- Satisfaction client moyenne
    SELECT 'Satisfaction client', AVG(customer_satisfaction) FROM customer_service_interactions_clean
    UNION ALL
    -- Produits en rupture
    SELECT 'Produits stock critique', COUNT(*) FROM inventory_clean WHERE stock_level = 'Critique'
    UNION ALL
    -- Retards livraison
    SELECT 'Livraisons en retard', COUNT(*) FROM logistics_and_shipping_clean WHERE delivery_status = 'En retard'
    UNION ALL
    -- ROI marketing
    SELECT 'ROI marketing moyen', 
        AVG(CASE WHEN budget > 0 THEN generated_revenue / budget ELSE NULL END) * 100
    FROM (
        SELECT mc.budget, SUM(ft.amount) AS generated_revenue
        FROM marketing_campaigns_clean mc
        LEFT JOIN financial_transactions_clean ft 
            ON ft.transaction_date BETWEEN mc.start_date AND mc.end_date
            AND ft.region = mc.region
        GROUP BY mc.campaign_id, mc.budget
    ) campaign_revenue
)
SELECT 
    metric,
    ROUND(value, 2) AS value,
    CASE 
        WHEN metric LIKE '%moyen%' THEN '€'
        WHEN metric LIKE '%taux%' OR metric LIKE '%satisfaction%' OR metric LIKE '%ROI%' THEN '%'
        ELSE 'unité'
    END AS unit,
    CASE 
        WHEN metric = 'Satisfaction client' AND value >= 4 THEN '🟢 Excellent'
        WHEN metric = 'Satisfaction client' AND value >= 3 THEN '🟡 Satisfaisant'
        WHEN metric = 'Satisfaction client' AND value < 3 THEN '🔴 À améliorer'
        WHEN metric = 'Produits stock critique' AND value > 10 THEN '🔴 Critique'
        WHEN metric = 'Produits stock critique' AND value > 5 THEN '🟡 Attention'
        WHEN metric = 'Produits stock critique' AND value <= 5 THEN '🟢 Bon'
        WHEN metric = 'Livraisons en retard' AND value > 20 THEN '🔴 Critique'
        WHEN metric = 'Livraisons en retard' AND value > 10 THEN '🟡 À surveiller'
        WHEN metric = 'Livraisons en retard' AND value <= 10 THEN '🟢 Bon'
        ELSE '➖ Normal'
    END AS status
FROM kpis;

-- 2.4.2 Affichage du tableau de bord
SELECT * FROM SILVER.business_dashboard;

-- 2.4.3 Top 5 recommandations business
SELECT 
    rank() OVER (ORDER BY priority DESC) as recommendation_rank,
    recommendation,
    business_impact,
    estimated_effort,
    responsible_department
FROM (
    SELECT 
        1 AS priority,
        'Augmenter le stock des produits avec avis >4.5 et stock bas' AS recommendation,
        'Augmentation CA de 15-20%' AS business_impact,
        'Faible' AS estimated_effort,
        'Logistique' AS responsible_department
    UNION ALL SELECT 2, 'Cibler les campagnes sur les régions à fort panier moyen', '+10% ROI marketing', 'Moyenne', 'Marketing'
    UNION ALL SELECT 3, 'Améliorer le taux de résolution des tickets service client', '+20% satisfaction client', 'Moyenne', 'Service Client'
    UNION ALL SELECT 4, 'Négocier les délais avec les fournisseurs fiables mais lents', '-30% ruptures stock', 'Élevée', 'Achats'
    UNION ALL SELECT 5, 'Créer promotions ciblées pour clients revenu élevé', '+25% fidélisation', 'Faible', 'Marketing'
) recommendations
ORDER BY recommendation_rank;

---------------------------------------------------------------
-- PARTIE 2.5 – EXPORT POUR VISUALISATION
---------------------------------------------------------------

-- 2.5.1 Préparation des données pour Tableau/PowerBI
CREATE OR REPLACE VIEW SILVER.sales_analysis_export AS
SELECT 
    ft.transaction_date,
    ft.entity,
    ft.region,
    ft.amount,
    ft.transaction_type,
    cd.customer_id,
    cd.age,
    cd.gender,
    cd.annual_income,
    cd.marital_status,
    mc.campaign_name,
    mc.campaign_type,
    p.promotion_type,
    p.discount_percentage,
    i.product_category,
    i.stock_level,
    i.current_stock,
    i.reorder_point,
    ls.shipping_method,
    ls.delivery_status,
    csi.satisfaction_category,
    pr.sentiment,
    pr.avg_rating
FROM financial_transactions_clean ft
LEFT JOIN customer_demographics_clean cd ON ft.entity = cd.customer_id
LEFT JOIN marketing_campaigns_clean mc 
    ON ft.transaction_date BETWEEN mc.start_date AND mc.end_date
    AND ft.region = mc.region
LEFT JOIN promotions_clean p 
    ON ft.transaction_date BETWEEN p.start_date AND p.end_date
LEFT JOIN inventory_clean i ON ft.region = i.region
LEFT JOIN logistics_and_shipping_clean ls ON ft.entity = ls.order_id
LEFT JOIN customer_service_interactions_clean csi ON ft.entity = csi.customer_id
LEFT JOIN (
    SELECT product_category, AVG(rating) as avg_rating, sentiment
    FROM product_reviews_clean
    GROUP BY product_category, sentiment
) pr ON i.product_category = pr.product_category
WHERE ft.transaction_type = 'Sale';

-- 2.5.2 Statistiques pour rapport mensuel
CREATE OR REPLACE VIEW SILVER.monthly_report_data AS
SELECT 
    DATE_TRUNC('month', transaction_date) AS report_month,
    region,
    COUNT(DISTINCT entity) AS active_customers,
    COUNT(*) AS total_transactions,
    SUM(amount) AS monthly_revenue,
    AVG(amount) AS avg_ticket,
    MAX(amount) AS max_transaction,
    COUNT(DISTINCT CASE WHEN transaction_type = 'REFUND' THEN transaction_id END) AS refund_count,
    SUM(CASE WHEN transaction_type = 'REFUND' THEN amount ELSE 0 END) AS total_refunds
FROM financial_transactions_clean
GROUP BY DATE_TRUNC('month', transaction_date), region;