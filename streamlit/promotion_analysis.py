# promotion_analysis.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session

# Configuration de la page
st.set_page_config(
    page_title="Analyse des Promotions - AnyCompany",
    page_icon="🎯",
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
st.title("🎯 Analyse des Promotions - AnyCompany")
st.markdown("Analyse complète des performances promotionnelles avec ROI et part de marché")
st.markdown("---")

# Sidebar avec filtres
with st.sidebar:
    st.header("🔍 Filtres d'Analyse")
    
    # Statut de promotion
    status_options = ["ACTIVE", "UPCOMING", "EXPIRED", "ALL"]
    selected_status = st.selectbox("Statut Promotion", status_options, index=0)
    
    # Type de promotion
    if session:
        try:
            promo_types = session.sql("""
                SELECT DISTINCT promotion_type 
                FROM ANALYTICS.PROMOTIONS_ACTIVE 
                WHERE promotion_type IS NOT NULL
                ORDER BY promotion_type
            """).to_pandas()
            selected_promo_types = st.multiselect(
                "Types de promotion",
                options=promo_types['PROMOTION_TYPE'].tolist(),
                default=promo_types['PROMOTION_TYPE'].tolist()[:3] if len(promo_types) > 0 else []
            )
        except:
            selected_promo_types = []
    
    # Plage de réduction
    discount_range = st.slider(
        "Plage de réduction (%)",
        min_value=0,
        max_value=100,
        value=(10, 50)
    )
    
    # ROI minimum
    min_roi = st.slider("ROI minimum (%)", -100, 500, 0)
    
    st.markdown("---")
    st.info("💡 ROI = (Revenu Net - Coût Remises) × 100 / Coût Remises")

# Fonctions de chargement des données
@st.cache_data(ttl=300)
def load_promotion_kpis():
    """Charger les KPI globaux des promotions"""
    query = """
    SELECT 
        COUNT(*) as total_promotions,
        SUM(CASE WHEN promotion_status = 'ACTIVE' THEN 1 ELSE 0 END) as active_promotions,
        SUM(CASE WHEN promotion_status = 'UPCOMING' THEN 1 ELSE 0 END) as upcoming_promotions,
        SUM(total_gross_revenue) as total_gross_revenue,
        SUM(total_discount_cost) as total_discount_cost,
        AVG(roi_percentage) as avg_roi,
        AVG(revenue_per_discount_euro) as avg_revenue_per_discount,
        SUM(total_sales) as total_transactions,
        SUM(unique_customers_reached) as total_customers_reached
    FROM ANALYTICS.PROMOTIONS_ACTIVE
    WHERE promotion_status != 'EXPIRED' OR promotion_status IS NULL
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_promotion_details():
    """Charger le détail des promotions"""
    query = """
    SELECT 
        promotion_id,
        product_category,
        promotion_type,
        discount_percentage,
        start_date,
        end_date,
        region,
        duration_days,
        promotion_status,
        total_sales,
        total_gross_revenue,
        total_discount_cost,
        total_net_revenue,
        roi_percentage,
        revenue_per_discount_euro,
        unique_customers_reached,
        market_share_pct,
        avg_transaction_amount
    FROM ANALYTICS.PROMOTIONS_ACTIVE
    ORDER BY start_date DESC, roi_percentage DESC
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_promotion_by_type():
    """Charger les performances par type de promotion"""
    query = """
    SELECT 
        promotion_type,
        COUNT(*) as promotion_count,
        SUM(total_gross_revenue) as total_revenue,
        SUM(total_discount_cost) as total_discount,
        AVG(discount_percentage) as avg_discount_pct,
        AVG(roi_percentage) as avg_roi,
        AVG(revenue_per_discount_euro) as avg_revenue_per_euro,
        SUM(total_sales) as total_transactions,
        SUM(unique_customers_reached) as total_customers
    FROM ANALYTICS.PROMOTIONS_ACTIVE
    WHERE promotion_type IS NOT NULL
    GROUP BY promotion_type
    ORDER BY total_revenue DESC
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_promotion_by_region():
    """Charger les promotions par région"""
    query = """
    SELECT 
        region,
        COUNT(*) as promotion_count,
        SUM(total_gross_revenue) as total_revenue,
        SUM(total_discount_cost) as total_discount,
        AVG(roi_percentage) as avg_roi,
        SUM(total_sales) as total_transactions,
        SUM(unique_customers_reached) as total_customers,
        AVG(market_share_pct) as avg_market_share
    FROM ANALYTICS.PROMOTIONS_ACTIVE
    WHERE region IS NOT NULL
    GROUP BY region
    ORDER BY total_revenue DESC
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_promotion_by_category():
    """Charger les promotions par catégorie produit"""
    query = """
    SELECT 
        product_category,
        COUNT(*) as promotion_count,
        SUM(total_gross_revenue) as total_revenue,
        SUM(total_discount_cost) as total_discount,
        AVG(discount_percentage) as avg_discount_pct,
        AVG(roi_percentage) as avg_roi,
        SUM(total_sales) as total_transactions,
        AVG(avg_transaction_amount) as avg_ticket
    FROM ANALYTICS.PROMOTIONS_ACTIVE
    WHERE product_category IS NOT NULL
    GROUP BY product_category
    ORDER BY total_revenue DESC
    LIMIT 15
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_time_analysis():
    """Analyse temporelle des promotions"""
    query = """
    SELECT 
        start_year,
        start_quarter,
        start_month,
        promotion_status,
        COUNT(*) as promotion_count,
        SUM(total_gross_revenue) as total_revenue,
        SUM(total_discount_cost) as total_discount,
        AVG(roi_percentage) as avg_roi
    FROM ANALYTICS.PROMOTIONS_ACTIVE
    WHERE start_year IS NOT NULL
    GROUP BY start_year, start_quarter, start_month, promotion_status
    ORDER BY start_year DESC, start_quarter DESC, start_month DESC
    """
    return session.sql(query).to_pandas()

# Chargement et affichage des données
if session:
    try:
        # Charger les données
        kpis_df = load_promotion_kpis()
        details_df = load_promotion_details()
        type_df = load_promotion_by_type()
        region_df = load_promotion_by_region()
        category_df = load_promotion_by_category()
        time_df = load_time_analysis()
        
        # Section 1: KPI Globaux
        st.subheader("📊 KPI Globaux des Promotions")
        
        if not kpis_df.empty:
            kpis = kpis_df.iloc[0]
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    label="Promotions Actives",
                    value=f"{kpis['ACTIVE_PROMOTIONS']:,}",
                    delta=f"{kpis['TOTAL_PROMOTIONS']:,} total"
                )
            
            with col2:
                st.metric(
                    label="CA Brut Promotions",
                    value=f"€{kpis['TOTAL_GROSS_REVENUE']:,.0f}",
                    delta=None
                )
            
            with col3:
                st.metric(
                    label="ROI Moyen",
                    value=f"{kpis['AVG_ROI']:.1f}%",
                    delta=None
                )
            
            with col4:
                st.metric(
                    label="Clients Touchés",
                    value=f"{kpis['TOTAL_CUSTOMERS_REACHED']:,}",
                    delta=f"{kpis['TOTAL_TRANSACTIONS']:,} transactions"
                )
        
        st.markdown("---")
        
        # Section 2: Détail des Promotions
        st.subheader("📋 Catalogue des Promotions")
        
        if not details_df.empty:
            # Appliquer les filtres
            filtered_df = details_df.copy()
            
            if selected_status != "ALL":
                filtered_df = filtered_df[filtered_df['PROMOTION_STATUS'] == selected_status]
            
            if selected_promo_types:
                filtered_df = filtered_df[filtered_df['PROMOTION_TYPE'].isin(selected_promo_types)]
            
            filtered_df = filtered_df[
                (filtered_df['DISCOUNT_PERCENTAGE'] >= discount_range[0]) &
                (filtered_df['DISCOUNT_PERCENTAGE'] <= discount_range[1]) &
                (filtered_df['ROI_PERCENTAGE'] >= min_roi)
            ]
            
            # Afficher le tableau
            st.dataframe(
                filtered_df,
                column_config={
                    "PROMOTION_ID": "ID Promotion",
                    "PRODUCT_CATEGORY": "Catégorie",
                    "PROMOTION_TYPE": "Type",
                    "DISCOUNT_PERCENTAGE": st.column_config.NumberColumn("Réduction %", format="%.1f%%"),
                    "START_DATE": st.column_config.DateColumn("Début"),
                    "END_DATE": st.column_config.DateColumn("Fin"),
                    "REGION": "Région",
                    "DURATION_DAYS": "Durée (jours)",
                    "PROMOTION_STATUS": "Statut",
                    "TOTAL_GROSS_REVENUE": st.column_config.NumberColumn("CA Brut (€)", format="€%.2f"),
                    "TOTAL_DISCOUNT_COST": st.column_config.NumberColumn("Coût Remises (€)", format="€%.2f"),
                    "ROI_PERCENTAGE": st.column_config.NumberColumn("ROI %", format="%.1f%%"),
                    "REVENUE_PER_DISCOUNT_EURO": st.column_config.NumberColumn("€/€ Réduction", format="€%.2f"),
                    "MARKET_SHARE_PCT": st.column_config.NumberColumn("Part Marché %", format="%.1f%%")
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
                total_revenue_filtered = filtered_df['TOTAL_GROSS_REVENUE'].sum()
                st.metric("CA Total Filtre", f"€{total_revenue_filtered:,.0f}")
            
            with col3:
                avg_discount_filtered = filtered_df['DISCOUNT_PERCENTAGE'].mean()
                st.metric("Réduction Moyenne", f"{avg_discount_filtered:.1f}%")
        
        st.markdown("---")
        
        # Section 3: Analyse par Type de Promotion
        st.subheader("🏷️ Performance par Type de Promotion")
        
        if not type_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Tableau des types
                st.dataframe(
                    type_df,
                    column_config={
                        "PROMOTION_TYPE": "Type Promotion",
                        "PROMOTION_COUNT": "Nombre",
                        "TOTAL_REVENUE": st.column_config.NumberColumn("CA Total (€)", format="€%.2f"),
                        "AVG_ROI": st.column_config.NumberColumn("ROI Moyen %", format="%.1f%%"),
                        "AVG_REVENUE_PER_EURO": st.column_config.NumberColumn("€/€ Réduction", format="€%.2f"),
                        "TOTAL_TRANSACTIONS": "Transactions",
                        "TOTAL_CUSTOMERS": "Clients"
                    },
                    hide_index=True
                )
            
            with col2:
                # Graphique ROI par type
                st.bar_chart(
                    type_df,
                    x='PROMOTION_TYPE',
                    y='AVG_ROI'
                )
        
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
                        "PROMOTION_COUNT": "Promotions",
                        "TOTAL_REVENUE": st.column_config.NumberColumn("CA Total (€)", format="€%.2f"),
                        "AVG_ROI": st.column_config.NumberColumn("ROI Moyen %", format="%.1f%%"),
                        "AVG_MARKET_SHARE": st.column_config.NumberColumn("Part Marché %", format="%.1f%%"),
                        "TOTAL_TRANSACTIONS": "Transactions",
                        "TOTAL_CUSTOMERS": "Clients"
                    },
                    hide_index=True
                )
            
            with col2:
                # Carte thermique des régions
                st.write("📊 Top 5 Régions par ROI")
                top_regions = region_df.nlargest(5, 'AVG_ROI')
                
                for idx, row in top_regions.iterrows():
                    with st.container(border=True):
                        cols = st.columns([3, 2, 2])
                        with cols[0]:
                            st.write(f"**{row['REGION']}**")
                        with cols[1]:
                            st.write(f"ROI: {row['AVG_ROI']:.1f}%")
                        with cols[2]:
                            st.write(f"Part marché: {row['AVG_MARKET_SHARE']:.1f}%")
        
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
                        "PROMOTION_COUNT": "Promotions",
                        "TOTAL_REVENUE": st.column_config.NumberColumn("CA Total (€)", format="€%.2f"),
                        "AVG_ROI": st.column_config.NumberColumn("ROI Moyen %", format="%.1f%%"),
                        "AVG_DISCOUNT_PCT": st.column_config.NumberColumn("Réduction %", format="%.1f%%"),
                        "TOTAL_TRANSACTIONS": "Transactions",
                        "AVG_TICKET": st.column_config.NumberColumn("Panier Moyen (€)", format="€%.2f")
                    },
                    hide_index=True
                )
            
            with col2:
                # Graphique ROI vs Réduction
                scatter_data = category_df.copy()
                scatter_data = scatter_data[['PRODUCT_CATEGORY', 'AVG_DISCOUNT_PCT', 'AVG_ROI', 'TOTAL_REVENUE']]
                scatter_data.columns = ['Catégorie', 'Réduction %', 'ROI %', 'CA Total']
                st.write("📈 ROI vs Réduction par Catégorie")
                st.dataframe(scatter_data, hide_index=True)
        
        # Section 6: Insights et Recommandations
        st.markdown("---")
        st.subheader("💡 Insights et Recommandations")
        
        if not type_df.empty and not region_df.empty:
            # Meilleur type de promotion
            best_type = type_df.loc[type_df['AVG_ROI'].idxmax()]
            # Meilleure région
            best_region = region_df.loc[region_df['AVG_ROI'].idxmax()]
            # Meilleure catégorie
            best_category = category_df.loc[category_df['AVG_ROI'].idxmax()] if not category_df.empty else None
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                with st.container(border=True):
                    st.markdown("#### 🏆 Meilleur Type")
                    st.success(f"**{best_type['PROMOTION_TYPE']}**")
                    st.write(f"ROI: {best_type['AVG_ROI']:.1f}%")
                    st.write(f"€/€: {best_type['AVG_REVENUE_PER_EURO']:.2f}")
                    st.write(f"Promotions: {best_type['PROMOTION_COUNT']}")
            
            with col2:
                with st.container(border=True):
                    st.markdown("#### 🌍 Meilleure Région")
                    st.info(f"**{best_region['REGION']}**")
                    st.write(f"ROI: {best_region['AVG_ROI']:.1f}%")
                    st.write(f"Part marché: {best_region['AVG_MARKET_SHARE']:.1f}%")
                    st.write(f"Promotions: {best_region['PROMOTION_COUNT']}")
            
            with col3:
                if best_category is not None:
                    with st.container(border=True):
                        st.markdown("#### 📦 Meilleure Catégorie")
                        st.warning(f"**{best_category['PRODUCT_CATEGORY']}**")
                        st.write(f"ROI: {best_category['AVG_ROI']:.1f}%")
                        st.write(f"Réduction: {best_category['AVG_DISCOUNT_PCT']:.1f}%")
                        st.write(f"Promotions: {best_category['PROMOTION_COUNT']}")
        
        # Section 7: Export des données
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
                    file_name=f"rapport_promotions_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
        
        # Informations sur les données
        st.markdown("---")
        with st.expander("ℹ️ Informations sur les données"):
            if not details_df.empty:
                active_count = len(details_df[details_df['PROMOTION_STATUS'] == 'ACTIVE'])
                upcoming_count = len(details_df[details_df['PROMOTION_STATUS'] == 'UPCOMING'])
                expired_count = len(details_df[details_df['PROMOTION_STATUS'] == 'EXPIRED'])
                
                st.write(f"**Statut des promotions:**")
                st.write(f"• Actives: {active_count}")
                st.write(f"• À venir: {upcoming_count}")
                st.write(f"• Expirées: {expired_count}")
                
                st.write(f"**Période couverte:** {details_df['START_DATE'].min()} au {details_df['END_DATE'].max()}")
                st.write(f"**Dernière mise à jour:** {datetime.now().strftime('%d/%m/%Y %H:%M')}")
        
    except Exception as e:
        st.error(f"Erreur lors du chargement des données: {str(e)}")
        st.info("Vérifiez que la table ANALYTICS.PROMOTIONS_ACTIVE existe dans Snowflake.")
else:
    st.warning("⏳ En attente de connexion à Snowflake...")

# Footer
st.markdown("---")
st.caption("© 2024 AnyCompany - Analyse Promotions - Dernière mise à jour: " + datetime.now().strftime("%d/%m/%Y %H:%M"))