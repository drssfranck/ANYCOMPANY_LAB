# sales_dashboard.py
import streamlit as st
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session

# Configuration minimale
st.set_page_config(page_title="Ventes", layout="wide")

# Session Snowflake
def get_session():
    try:
        return get_active_session()
    except:
        st.error("Connexion Snowflake échouée")
        return None

session = get_session()

st.title("📊 Tableau de Bord Ventes")

if session:
    try:
        # KPI de base
        kpi_query = """
        SELECT 
            COUNT(*) as transactions,
            SUM(sale_amount) as revenue,
            AVG(sale_amount) as avg_ticket,
            MIN(sale_date) as first_date,
            MAX(sale_date) as last_date
        FROM ANALYTICS.SALES_ENRICHED
        WHERE sale_amount > 0
        """
        
        kpi_data = session.sql(kpi_query).to_pandas()
        
        if not kpi_data.empty:
            # Afficher KPI
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Transactions", f"{kpi_data['TRANSACTIONS'].iloc[0]:,}")
            
            with col2:
                st.metric("CA Total", f"€{kpi_data['REVENUE'].iloc[0]:,.0f}")
            
            with col3:
                st.metric("Panier Moyen", f"€{kpi_data['AVG_TICKET'].iloc[0]:,.2f}")
            
            # Ventes quotidiennes
            daily_query = """
            SELECT 
                sale_date,
                SUM(sale_amount) as daily_revenue,
                COUNT(*) as daily_transactions
            FROM ANALYTICS.SALES_ENRICHED
            WHERE sale_amount > 0
            GROUP BY sale_date
            ORDER BY sale_date DESC
            LIMIT 30
            """
            
            daily_data = session.sql(daily_query).to_pandas()
            
            if not daily_data.empty:
                st.subheader("Évolution des Ventes")
                daily_chart = daily_data.sort_values('SALE_DATE')
                st.line_chart(daily_chart, x='SALE_DATE', y='DAILY_REVENUE')
            
            # Régions
            region_query = """
            SELECT 
                sale_region,
                COUNT(*) as transactions,
                SUM(sale_amount) as revenue
            FROM ANALYTICS.SALES_ENRICHED
            WHERE sale_amount > 0
              AND sale_region IS NOT NULL
            GROUP BY sale_region
            ORDER BY revenue DESC
            LIMIT 10
            """
            
            region_data = session.sql(region_query).to_pandas()
            
            if not region_data.empty:
                st.subheader("Top Régions")
                st.dataframe(region_data)
            
            # Dernières ventes
            recent_query = """
            SELECT 
                sale_date,
                sale_id,
                sale_region,
                sale_amount,
                payment_method,
                CASE WHEN has_promotion = 1 THEN 'Oui' ELSE 'Non' END as promotion
            FROM ANALYTICS.SALES_ENRICHED
            WHERE sale_amount > 0
            ORDER BY sale_date DESC
            LIMIT 20
            """
            
            recent_data = session.sql(recent_query).to_pandas()
            
            if not recent_data.empty:
                st.subheader("Dernières Transactions")
                st.dataframe(recent_data)
                
        else:
            st.warning("Aucune donnée disponible")
            
    except Exception as e:
        st.error(f"Erreur: {str(e)}")
else:
    st.warning("En attente de connexion...")