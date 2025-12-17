import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
import time

# ==========================================
# 1. POSTGRES CONFIGURATION
# ==========================================
# If running Streamlit on your HOST machine, use "localhost".
# If running Streamlit inside Docker, use "postgres".
DB_HOST = "postgres" 
DB_NAME = "streaming"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_PORT = "5432"

# Function to get data
def query_postgres(query):
    """
    Connects to Postgres, runs the query, and returns a Pandas DataFrame.
    Using a fresh connection per query prevents 'connection closed' errors.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        return pd.DataFrame()

# ==========================================
# 2. DASHBOARD LAYOUT
# ==========================================
st.set_page_config(page_title="Stock Trading Dashboard", layout="wide")
st.title("📈 Real-Time Stock Trading Dashboard")

# Auto-refresh logic
if 'refresh_count' not in st.session_state:
    st.session_state.refresh_count = 0

# Sidebar
st.sidebar.header("Settings")
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 5, 60, 10)
auto_refresh = st.sidebar.checkbox("Auto Refresh", value=True)

# ---------------------------------------------------------
# METRICS ROW
# ---------------------------------------------------------
recent_query = """
SELECT 
    COUNT(*) as total_trades,
    SUM(trade_value) as total_value,
    AVG(price) as avg_price,
    COUNT(DISTINCT sector) as active_sectors
FROM enriched_trades
WHERE event_timestamp > NOW() - INTERVAL '5 minutes'
"""
metrics_df = query_postgres(recent_query)

col1, col2, col3, col4 = st.columns(4)

if not metrics_df.empty and metrics_df['total_trades'].iloc[0] is not None:
    with col1:
        st.metric("Total Trades (5m)", f"{metrics_df['total_trades'].iloc[0]:,.0f}")
    with col2:
        val = metrics_df['total_value'].iloc[0] or 0
        st.metric("Total Value (5m)", f"${val:,.0f}")
    with col3:
        avg = metrics_df['avg_price'].iloc[0] or 0
        st.metric("Avg Price", f"${avg:.2f}")
    with col4:
        st.metric("Active Sectors", f"{metrics_df['active_sectors'].iloc[0]:.0f}")
else:
    st.warning("No data found in the last 5 minutes.")

# ---------------------------------------------------------
# TIME SERIES CHART (Sector Value)
# ---------------------------------------------------------
st.subheader("💰 Sector Trading Volume Over Time")

sector_query = """
SELECT DISTINCT ON (window_start, sector)
    window_start,
    sector,
    total_value,
    trade_count
FROM sector_metrics
WHERE window_start > NOW() - INTERVAL '1 hour'
ORDER BY window_start, sector, ingestion_time DESC
"""
sector_df = query_postgres(sector_query)

if not sector_df.empty:
    fig_line = px.line(
        sector_df,
        x='window_start',
        y='total_value',
        color='sector',
        title='Trading Value by Sector (Last Hour)',
        labels={'window_start': 'Time', 'total_value': 'Total Value ($)'}
    )
    fig_line.update_layout(height=400)
    st.plotly_chart(fig_line, use_container_width=True)

# ---------------------------------------------------------
# SPLIT COLUMNS
# ---------------------------------------------------------
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📊 Trade Count by Sector")
    
    trade_count_query = """
    SELECT 
        sector,
        SUM(trade_count) as total_trades
    FROM sector_metrics
    WHERE window_start > NOW() - INTERVAL '1 hour'
    GROUP BY sector
    ORDER BY total_trades DESC
    """
    trade_count_df = query_postgres(trade_count_query)
    
    if not trade_count_df.empty:
        fig_bar = px.bar(
            trade_count_df,
            x='sector',
            y='total_trades',
            color='sector',
            title='Total Trades by Sector'
        )
        st.plotly_chart(fig_bar, use_container_width=True)

with col_right:
    st.subheader("🏆 Top Traded Stocks")
    
    top_stocks_query = """
    SELECT 
        symbol,
        company,
        sector,
        COUNT(*) as trade_count,
        SUM(trade_value) as total_value
    FROM enriched_trades
    WHERE event_timestamp > NOW() - INTERVAL '1 hour'
    GROUP BY symbol, company, sector
    ORDER BY total_value DESC
    LIMIT 10
    """
    top_stocks_df = query_postgres(top_stocks_query)
    
    if not top_stocks_df.empty:
        fig_pie = px.pie(
            top_stocks_df,
            values='total_value',
            names='symbol',
            title='Top 10 Stocks by Value',
            hole=0.4
        )
        st.plotly_chart(fig_pie, use_container_width=True)

# ---------------------------------------------------------
# RAW DATA TABLE
# ---------------------------------------------------------
st.subheader("📋 Recent Trades")

# Note: Changed 'timestamp' to 'event_timestamp'
recent_trades_query = """
SELECT 
    event_timestamp as timestamp,
    symbol,
    company,
    sector,
    price,
    volume,
    trade_value
FROM enriched_trades
ORDER BY event_timestamp DESC
LIMIT 20
"""
recent_trades_df = query_postgres(recent_trades_query)
st.dataframe(recent_trades_df, use_container_width=True)

# ---------------------------------------------------------
# REFRESH LOGIC
# ---------------------------------------------------------
if auto_refresh:
    time.sleep(refresh_rate)
    st.session_state.refresh_count += 1
    st.rerun()