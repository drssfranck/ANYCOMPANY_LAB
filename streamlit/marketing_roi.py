# marketing_roi.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session

# Configuration de la page
st.set_page_config(
    page_title="Performance Marketing - AnyCompany",
    page_icon="💰",
    layout="wide"
)

# Initialisation de la session Snowflake
@st.cache_resource
def get_snowflake_session():
    try:
        return get_active_session()
    except:
        st.error("❌ Impossible de se connecter à Snowflake")
        return None

session = get_snowflake_session()

# Titre principal
st.title("💰 Performance Marketing - AnyCompany")
st.markdown("Analyse ROI et efficacité des campagnes marketing")
st.markdown("---")

# Sidebar avec filtres
with st.sidebar:
    st.header("🔍 Filtres d'Analyse")
    
    # Type de campagne
    if session:
        try:
            campaign_types = session.sql("""
                SELECT DISTINCT campaign_type 
                FROM ANALYTICS.MARKETING_PERFORMANCE 
                WHERE campaign_type IS NOT NULL
                ORDER BY campaign_type
            """).to_pandas()
            selected_campaign_types = st.multiselect(
                "Types de campagne",
                options=campaign_types['CAMPAIGN_TYPE'].tolist(),
                default=campaign_types['CAMPAIGN_TYPE'].tolist()[:3] if len(campaign_types) > 0 else []
            )
        except:
            selected_campaign_types = []
    
    # Performance rating
    rating_options = ["EXCELLENT", "GOOD", "AVERAGE", "POOR", "ALL"]
    selected_rating = st.selectbox("Rating Performance", rating_options, index=4)
    
    # ROI minimum
    min_roi = st.slider("ROI minimum (%)", -100, 500, 0)
    
    # Période
    year_options = ["Toutes années", "2023", "2024", "2025"]
    selected_year = st.selectbox("Année", year_options, index=0)
    
    st.markdown("---")
    st.info("💡 ROI = (Revenu - Budget) × 100 / Budget")

# Fonctions de chargement des données
@st.cache_data(ttl=300)
def load_marketing_kpis():
    """Charger les KPI marketing globaux"""
    query = """
    SELECT 
        COUNT(*) as total_campaigns,
        SUM(campaign_budget) as total_budget,
        SUM(generated_revenue) as total_revenue,
        AVG(roi_percentage) as avg_roi,
        AVG(actual_conversion_rate) * 100 as avg_conversion_rate,
        SUM(unique_customers_acquired) as total_customers_acquired,
        AVG(cost_per_acquisition) as avg_cpa,
        AVG(revenue_per_euro_spent) as avg_revenue_per_euro
    FROM ANALYTICS.MARKETING_PERFORMANCE
    WHERE campaign_budget > 0
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_campaign_details():
    """Charger le détail des campagnes"""
    query = """
    SELECT 
        campaign_id,
        campaign_name,
        campaign_type,
        product_category,
        target_audience,
        start_date,
        end_date,
        region,
        campaign_duration_days,
        campaign_budget,
        estimated_reach,
        target_conversion_rate * 100 as target_conversion_pct,
        actual_sales,
        generated_revenue,
        unique_customers_acquired,
        avg_transaction_value,
        roi_percentage,
        revenue_per_euro_spent,
        actual_conversion_rate * 100 as actual_conversion_pct,
        cost_per_acquisition,
        cost_per_unique_customer,
        avg_customer_lifetime_value,
        performance_rating,
        conversion_performance
    FROM ANALYTICS.MARKETING_PERFORMANCE
    WHERE campaign_budget > 0
    ORDER BY start_date DESC, roi_percentage DESC
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_campaign_by_type():
    """Charger les performances par type de campagne"""
    query = """
    SELECT 
        campaign_type,
        COUNT(*) as campaign_count,
        SUM(campaign_budget) as total_budget,
        SUM(generated_revenue) as total_revenue,
        AVG(roi_percentage) as avg_roi,
        AVG(actual_conversion_rate) * 100 as avg_conversion_rate,
        AVG(revenue_per_euro_spent) as avg_revenue_per_euro,
        SUM(unique_customers_acquired) as total_customers,
        AVG(cost_per_acquisition) as avg_cpa
    FROM ANALYTICS.MARKETING_PERFORMANCE
    WHERE campaign_type IS NOT NULL
    GROUP BY campaign_type
    ORDER BY avg_roi DESC
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_campaign_by_region():
    """Charger les campagnes par région"""
    query = """
    SELECT 
        region,
        COUNT(*) as campaign_count,
        SUM(campaign_budget) as total_budget,
        SUM(generated_revenue) as total_revenue,
        AVG(roi_percentage) as avg_roi,
        AVG(actual_conversion_rate) * 100 as avg_conversion_rate,
        SUM(unique_customers_acquired) as total_customers,
        AVG(cost_per_unique_customer) as avg_customer_cost
    FROM ANALYTICS.MARKETING_PERFORMANCE
    WHERE region IS NOT NULL
    GROUP BY region
    ORDER BY avg_roi DESC
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_campaign_by_category():
    """Charger les campagnes par catégorie produit"""
    query = """
    SELECT 
        product_category,
        COUNT(*) as campaign_count,
        SUM(campaign_budget) as total_budget,
        SUM(generated_revenue) as total_revenue,
        AVG(roi_percentage) as avg_roi,
        AVG(actual_conversion_rate) * 100 as avg_conversion_rate,
        SUM(actual_sales) as total_sales,
        AVG(avg_transaction_value) as avg_ticket
    FROM ANALYTICS.MARKETING_PERFORMANCE
    WHERE product_category IS NOT NULL
    GROUP BY product_category
    ORDER BY avg_roi DESC
    LIMIT 15
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_time_analysis():
    """Analyse temporelle des campagnes"""
    query = """
    SELECT 
        start_year,
        start_quarter,
        start_month,
        COUNT(*) as campaign_count,
        SUM(campaign_budget) as total_budget,
        SUM(generated_revenue) as total_revenue,
        AVG(roi_percentage) as avg_roi,
        SUM(unique_customers_acquired) as total_customers
    FROM ANALYTICS.MARKETING_PERFORMANCE
    WHERE start_year IS NOT NULL
    GROUP BY start_year, start_quarter, start_month
    ORDER BY start_year DESC, start_quarter DESC, start_month DESC
    LIMIT 12
    """
    return session.sql(query).to_pandas()

# Chargement et affichage des données
if session:
    try:
        # Charger les données
        kpis_df = load_marketing_kpis()
        details_df = load_campaign_details()
        type_df = load_campaign_by_type()
        region_df = load_campaign_by_region()
        category_df = load_campaign_by_category()
        time_df = load_time_analysis()
        
        # Section 1: KPI Marketing Globaux
        st.subheader("📈 KPI Marketing Globaux")
        
        if not kpis_df.empty:
            kpis = kpis_df.iloc[0]
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    label="Budget Total",
                    value=f"€{kpis['TOTAL_BUDGET']:,.0f}",
                    delta=f"{kpis['TOTAL_CAMPAIGNS']} campagnes"
                )
            
            with col2:
                net_profit = kpis['TOTAL_REVENUE'] - kpis['TOTAL_BUDGET']
                st.metric(
                    label="ROI Moyen",
                    value=f"{kpis['AVG_ROI']:.1f}%",
                    delta=f"€{net_profit:,.0f} net"
                )
            
            with col3:
                st.metric(
                    label="Taux Conversion",
                    value=f"{kpis['AVG_CONVERSION_RATE']:.1f}%",
                    delta=f"CPA: €{kpis['AVG_CPA']:.2f}"
                )
            
            with col4:
                st.metric(
                    label="Clients Acquis",
                    value=f"{kpis['TOTAL_CUSTOMERS_ACQUIRED']:,}",
                    delta=f"€/{kpis['AVG_REVENUE_PER_EURO']:.2f}/€"
                )
        
        st.markdown("---")
        
        # Section 2: Détail des Campagnes
        st.subheader("📋 Portefeuille des Campagnes")
        
        if not details_df.empty:
            # Appliquer les filtres
            filtered_df = details_df.copy()
            
            if selected_rating != "ALL":
                filtered_df = filtered_df[filtered_df['PERFORMANCE_RATING'] == selected_rating]
            
            if selected_campaign_types:
                filtered_df = filtered_df[filtered_df['CAMPAIGN_TYPE'].isin(selected_campaign_types)]
            
            if selected_year != "Toutes années":
                filtered_df = filtered_df[filtered_df['START_DATE'].dt.year == int(selected_year)]
            
            filtered_df = filtered_df[filtered_df['ROI_PERCENTAGE'] >= min_roi]
            
            # Afficher le tableau
            st.dataframe(
                filtered_df,
                column_config={
                    "CAMPAIGN_NAME": "Nom Campagne",
                    "CAMPAIGN_TYPE": "Type",
                    "PRODUCT_CATEGORY": "Catégorie",
                    "TARGET_AUDIENCE": "Cible",
                    "START_DATE": st.column_config.DateColumn("Début"),
                    "END_DATE": st.column_config.DateColumn("Fin"),
                    "REGION": "Région",
                    "CAMPAIGN_DURATION_DAYS": "Durée (jours)",
                    "CAMPAIGN_BUDGET": st.column_config.NumberColumn("Budget (€)", format="€%.2f"),
                    "GENERATED_REVENUE": st.column_config.NumberColumn("Revenu (€)", format="€%.2f"),
                    "ROI_PERCENTAGE": st.column_config.NumberColumn("ROI %", format="%.1f%%"),
                    "REVENUE_PER_EURO_SPENT": st.column_config.NumberColumn("€/€ Budget", format="€%.2f"),
                    "ACTUAL_CONVERSION_PCT": st.column_config.NumberColumn("Conversion %", format="%.1f%%"),
                    "COST_PER_ACQUISITION": st.column_config.NumberColumn("CPA (€)", format="€%.2f"),
                    "UNIQUE_CUSTOMERS_ACQUIRED": "Clients Acquis",
                    "PERFORMANCE_RATING": "Rating",
                    "CONVERSION_PERFORMANCE": "Perf. Conversion"
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Statistiques du tableau filtré
            col1, col2, col3 = st.columns(3)
            
            with col1:
                avg_roi_filtered = filtered_df['ROI_PERCENTAGE'].mean()
                st.metric("ROI Moyen Filtre", f"{avg_roi_filtered:.1f}%")
            
            with col2:
                total_budget_filtered = filtered_df['CAMPAIGN_BUDGET'].sum()
                st.metric("Budget Total Filtre", f"€{total_budget_filtered:,.0f}")
            
            with col3:
                conversion_rate_filtered = filtered_df['ACTUAL_CONVERSION_PCT'].mean()
                st.metric("Conversion Moyenne", f"{conversion_rate_filtered:.1f}%")
        
        st.markdown("---")
        
        # Section 3: Analyse par Type de Campagne
        st.subheader("🎯 Performance par Type de Campagne")
        
        if not type_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Tableau des types
                st.dataframe(
                    type_df,
                    column_config={
                        "CAMPAIGN_TYPE": "Type Campagne",
                        "CAMPAIGN_COUNT": "Nombre",
                        "TOTAL_BUDGET": st.column_config.NumberColumn("Budget Total (€)", format="€%.2f"),
                        "AVG_ROI": st.column_config.NumberColumn("ROI Moyen %", format="%.1f%%"),
                        "AVG_REVENUE_PER_EURO": st.column_config.NumberColumn("€/€ Budget", format="€%.2f"),
                        "AVG_CONVERSION_RATE": st.column_config.NumberColumn("Conversion %", format="%.1f%%"),
                        "TOTAL_CUSTOMERS": "Clients Acquis",
                        "AVG_CPA": st.column_config.NumberColumn("CPA Moyen (€)", format="€%.2f")
                    },
                    hide_index=True
                )
            
            with col2:
                # Graphique ROI par type
                st.bar_chart(
                    type_df,
                    x='CAMPAIGN_TYPE',
                    y='AVG_ROI'
                )
                
                # Graphique Budget vs Revenu
                st.write("📊 Budget vs Revenu par Type")
                budget_vs_revenue = type_df[['CAMPAIGN_TYPE', 'TOTAL_BUDGET', 'TOTAL_REVENUE']].copy()
                budget_vs_revenue.columns = ['Type', 'Budget (€)', 'Revenu (€)']
                st.dataframe(budget_vs_revenue, hide_index=True)
        
        st.markdown("---")
        
        # Section 4: Analyse Géographique
        st.subheader("🌍 Performance par Région")
        
        if not region_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Tableau des régions
                st.dataframe(
                    region_df,
                    column_config={
                        "REGION": "Région",
                        "CAMPAIGN_COUNT": "Campagnes",
                        "TOTAL_BUDGET": st.column_config.NumberColumn("Budget Total (€)", format="€%.2f"),
                        "AVG_ROI": st.column_config.NumberColumn("ROI Moyen %", format="%.1f%%"),
                        "AVG_CONVERSION_RATE": st.column_config.NumberColumn("Conversion %", format="%.1f%%"),
                        "TOTAL_CUSTOMERS": "Clients Acquis",
                        "AVG_CUSTOMER_COST": st.column_config.NumberColumn("Coût/Client (€)", format="€%.2f")
                    },
                    hide_index=True
                )
            
            with col2:
                # Top 5 régions par ROI
                st.write("🏆 Top 5 Régions par ROI")
                top_regions = region_df.nlargest(5, 'AVG_ROI')
                
                for idx, row in top_regions.iterrows():
                    with st.container(border=True):
                        cols = st.columns([3, 2, 2])
                        with cols[0]:
                            st.write(f"**{row['REGION']}**")
                        with cols[1]:
                            st.write(f"ROI: {row['AVG_ROI']:.1f}%")
                        with cols[2]:
                            st.write(f"Conversion: {row['AVG_CONVERSION_RATE']:.1f}%")
        
        st.markdown("---")
        
        # Section 5: Analyse par Catégorie Produit
        st.subheader("📦 Performance par Catégorie Produit")
        
        if not category_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Tableau des catégories
                st.dataframe(
                    category_df,
                    column_config={
                        "PRODUCT_CATEGORY": "Catégorie",
                        "CAMPAIGN_COUNT": "Campagnes",
                        "TOTAL_BUDGET": st.column_config.NumberColumn("Budget Total (€)", format="€%.2f"),
                        "AVG_ROI": st.column_config.NumberColumn("ROI Moyen %", format="%.1f%%"),
                        "AVG_CONVERSION_RATE": st.column_config.NumberColumn("Conversion %", format="%.1f%%"),
                        "TOTAL_SALES": "Ventes",
                        "AVG_TICKET": st.column_config.NumberColumn("Panier Moyen (€)", format="€%.2f")
                    },
                    hide_index=True
                )
            
            with col2:
                # Graphique ROI par catégorie
                st.bar_chart(
                    category_df.head(8),
                    x='PRODUCT_CATEGORY',
                    y='AVG_ROI'
                )
        
        # Section 6: Analyse de l'Efficacité
        st.markdown("---")
        st.subheader("📊 Analyse d'Efficacité")
        
        if not details_df.empty:
            col1, col2, col3 = st.columns(3)
            
            # Calculer les ratios d'efficacité
            with col1:
                # ROI vs Conversion
                high_roi_high_conv = len(details_df[
                    (details_df['ROI_PERCENTAGE'] > 100) & 
                    (details_df['ACTUAL_CONVERSION_PCT'] > 5)
                ])
                st.metric("Campagnes Excellentes", f"{high_roi_high_conv}")
                st.caption("ROI > 100% & Conversion > 5%")
            
            with col2:
                # Budget Efficiency
                efficient_campaigns = len(details_df[
                    details_df['REVENUE_PER_EURO_SPENT'] > 3
                ])
                st.metric("Campagnes Efficaces", f"{efficient_campaigns}")
                st.caption("Revenu > 3€ par € dépensé")
            
            with col3:
                # Customer Acquisition
                low_cpa_campaigns = len(details_df[
                    details_df['COST_PER_ACQUISITION'] < 50
                ])
                st.metric("Acquisition Rentable", f"{low_cpa_campaigns}")
                st.caption("CPA < 50€")
        
        # Section 7: Insights et Recommandations
        st.markdown("---")
        st.subheader("💡 Insights et Recommandations")
        
        if not type_df.empty and not region_df.empty and not category_df.empty:
            # Meilleur type de campagne
            best_type = type_df.loc[type_df['AVG_ROI'].idxmax()]
            # Meilleure région
            best_region = region_df.loc[region_df['AVG_ROI'].idxmax()]
            # Meilleure catégorie
            best_category = category_df.loc[category_df['AVG_ROI'].idxmax()]
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                with st.container(border=True):
                    st.markdown("#### 🏆 Meilleur Type")
                    st.success(f"**{best_type['CAMPAIGN_TYPE']}**")
                    st.write(f"ROI: {best_type['AVG_ROI']:.1f}%")
                    st.write(f"Conversion: {best_type['AVG_CONVERSION_RATE']:.1f}%")
                    st.write(f"Campagnes: {best_type['CAMPAIGN_COUNT']}")
            
            with col2:
                with st.container(border=True):
                    st.markdown("#### 🌍 Meilleure Région")
                    st.info(f"**{best_region['REGION']}**")
                    st.write(f"ROI: {best_region['AVG_ROI']:.1f}%")
                    st.write(f"Conversion: {best_region['AVG_CONVERSION_RATE']:.1f}%")
                    st.write(f"Campagnes: {best_region['CAMPAIGN_COUNT']}")
            
            with col3:
                with st.container(border=True):
                    st.markdown("#### 📦 Meilleure Catégorie")
                    st.warning(f"**{best_category['PRODUCT_CATEGORY']}**")
                    st.write(f"ROI: {best_category['AVG_ROI']:.1f}%")
                    st.write(f"Conversion: {best_category['AVG_CONVERSION_RATE']:.1f}%")
                    st.write(f"Campagnes: {best_category['CAMPAIGN_COUNT']}")
            
            # Recommandations stratégiques
            st.markdown("---")
            st.subheader("🎯 Recommandations Stratégiques")
            
            reco_col1, reco_col2 = st.columns(2)
            
            with reco_col1:
                st.write("**🚀 Augmenter l'investissement dans:**")
                if best_type['AVG_ROI'] > 150:
                    st.success(f"• Type: {best_type['CAMPAIGN_TYPE']}")
                if best_region['AVG_ROI'] > 150:
                    st.success(f"• Région: {best_region['REGION']}")
                if best_category['AVG_ROI'] > 150:
                    st.success(f"• Catégorie: {best_category['PRODUCT_CATEGORY']}")
            
            with reco_col2:
                st.write("**⚡ Optimisations prioritaires:**")
                # Identifier les types sous-performants
                if len(type_df) > 3:
                    worst_type = type_df.loc[type_df['AVG_ROI'].idxmin()]
                    if worst_type['AVG_ROI'] < 50:
                        st.warning(f"• Réviser: {worst_type['CAMPAIGN_TYPE']} (ROI: {worst_type['AVG_ROI']:.1f}%)")
                
                # Identifier les régions sous-performantes
                if len(region_df) > 3:
                    worst_region = region_df.loc[region_df['AVG_ROI'].idxmin()]
                    if worst_region['AVG_ROI'] < 50:
                        st.warning(f"• Réviser: {worst_region['REGION']} (ROI: {worst_region['AVG_ROI']:.1f}%)")
        
        # Section 8: Export des données
        st.markdown("---")
        if st.button("📊 Générer Rapport Complet"):
            with st.spinner("Préparation du rapport..."):
                # Combiner les données
                report_data = pd.concat([
                    details_df,
                    type_df,
                    region_df,
                    category_df
                ], ignore_index=True)
                
                csv_data = report_data.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Télécharger Rapport (CSV)",
                    data=csv_data,
                    file_name=f"rapport_marketing_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
        
        # Informations sur les données
        st.markdown("---")
        with st.expander("ℹ️ Informations sur les données"):
            if not details_df.empty:
                excellent_count = len(details_df[details_df['PERFORMANCE_RATING'] == 'EXCELLENT'])
                good_count = len(details_df[details_df['PERFORMANCE_RATING'] == 'GOOD'])
                average_count = len(details_df[details_df['PERFORMANCE_RATING'] == 'AVERAGE'])
                poor_count = len(details_df[details_df['PERFORMANCE_RATING'] == 'POOR'])
                
                st.write(f"**Distribution des ratings:**")
                st.write(f"• Excellent: {excellent_count}")
                st.write(f"• Bon: {good_count}")
                st.write(f"• Moyen: {average_count}")
                st.write(f"• Faible: {poor_count}")
                
                st.write(f"**Période couverte:** {details_df['START_DATE'].min()} au {details_df['END_DATE'].max()}")
                st.write(f"**Budget total analysé:** €{details_df['CAMPAIGN_BUDGET'].sum():,.0f}")
                st.write(f"**Dernière mise à jour:** {datetime.now().strftime('%d/%m/%Y %H:%M')}")
        
    except Exception as e:
        st.error(f"Erreur lors du chargement des données: {str(e)}")
        st.info("Vérifiez que la table ANALYTICS.MARKETING_PERFORMANCE existe dans Snowflake.")
else:
    st.warning("⏳ En attente de connexion à Snowflake...")

# Footer
st.markdown("---")
st.caption("© 2024 AnyCompany - Performance Marketing - Dernière mise à jour: " + datetime.now().strftime("%d/%m/%Y %H:%M"))