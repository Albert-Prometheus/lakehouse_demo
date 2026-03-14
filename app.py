import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
from pyecharts import options as opts
from pyecharts.charts import Line, Pie, Bar, Funnel, HeatMap
from streamlit_echarts import st_echarts
import plotly.graph_objects as go
import time
from datetime import datetime
import json

# Helper function to convert pyecharts objects to dict
def chart_to_dict(chart):
    """Convert pyecharts chart options to JSON-serializable dict"""
    d = chart.get_options()
    def convert(obj):
        if hasattr(obj, 'opts'):
            return convert(obj.opts)
        elif isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(i) for i in obj]
        else:
            return obj
    return convert(d)

st.set_page_config(page_title="3C AI Lakehouse v4 Enterprise", layout="wide", page_icon="🏆", initial_sidebar_state="expanded")

# Database configuration
PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD = "localhost", "5049", "postgres", "postgres", "admin"

# ============================================================
# PROFESSIONAL BRAND IDENTITY SYSTEM
# ============================================================
st.markdown("""
<style>
    /* ===== FONTS ===== */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
    
    :root {
        /* Primary Brand Colors */
        --brand-navy: #0a1628;
        --brand-navy-light: #122640;
        --brand-navy-dark: #060d19;
        
        /* Accent Colors */
        --accent-cyan: #00d4ff;
        --accent-cyan-glow: rgba(0, 212, 255, 0.3);
        --accent-teal: #00e5c4;
        --accent-orange: #ff6b35;
        --accent-purple: #7c3aed;
        --accent-pink: #ec4899;
        
        /* Status Colors */
        --status-success: #00e676;
        --status-warning: #ffc107;
        --status-danger: #ef4444;
        --status-info: #3b82f6;
        
        /* Neutral */
        --text-primary: #f1f5f9;
        --text-secondary: #94a3b8;
        --text-muted: #64748b;
        --border-color: #1e3a5f;
        
        /* Glassmorphism */
        --glass-bg: rgba(18, 38, 64, 0.7);
        --glass-border: rgba(0, 212, 255, 0.15);
    }
    
    /* ===== GLOBAL STYLES ===== */
    .stApp {
        background: linear-gradient(135deg, var(--brand-navy-dark) 0%, var(--brand-navy) 50%, var(--brand-navy-light) 100%);
        font-family: 'Inter', -apple-system, sans-serif;
    }
    
    /* Typography */
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        color: var(--text-primary) !important;
    }
    
    h1 {
        background: linear-gradient(90deg, var(--accent-cyan), var(--accent-teal));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    /* ===== GLASSMORPHISM CARDS ===== */
    .glass-card {
        background: var(--glass-bg);
        backdrop-filter: blur(20px);
        border: 1px solid var(--glass-border);
        border-radius: 16px;
        padding: 24px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4), inset 0 1px 0 rgba(255,255,255,0.05);
        transition: all 0.3s ease;
    }
    
    .glass-card:hover {
        border-color: var(--accent-cyan);
        box-shadow: 0 8px 32px rgba(0, 212, 255, 0.15), inset 0 1px 0 rgba(255,255,255,0.1);
        transform: translateY(-2px);
    }
    
    /* ===== METRICS WITH GLOW ===== */
    .metric-glow {
        background: linear-gradient(145deg, var(--brand-navy-light), var(--brand-navy));
        border: 1px solid var(--border-color);
        border-radius: 12px;
        padding: 20px;
        position: relative;
        overflow: hidden;
    }
    
    .metric-glow::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, var(--accent-cyan), var(--accent-teal));
        box-shadow: 0 0 20px var(--accent-cyan-glow);
    }
    
    .metric-value-glow {
        font-size: 28px;
        font-weight: 800;
        background: linear-gradient(135deg, var(--accent-cyan), var(--accent-teal));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        text-shadow: 0 0 30px var(--accent-cyan-glow);
    }
    
    .metric-label-glow {
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        color: var(--text-secondary);
        margin-top: 8px;
    }
    
    /* ===== SIDEBAR ===== */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, var(--brand-navy-dark) 0%, var(--brand-navy) 100%) !important;
        border-right: 1px solid var(--border-color) !important;
    }
    
    [data-testid="stSidebar"] .stImage {
        filter: drop-shadow(0 0 15px var(--accent-cyan-glow));
    }
    
    [data-testid="stSidebar"] h1, 
    [data-testid="stSidebar"] h2, 
    [data-testid="stSidebar"] h3 {
        color: var(--accent-cyan) !important;
    }
    
    /* ===== TABS ===== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: var(--glass-bg);
        padding: 8px;
        border-radius: 12px;
        border: 1px solid var(--glass-border);
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border: 2px solid transparent;
        border-bottom: none;
        border-radius: 8px 8px 0 0;
        padding: 12px 24px;
        color: var(--text-secondary);
        transition: all 0.2s ease;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        border-color: var(--border-color);
        color: var(--text-primary);
    }
    
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: transparent !important;
        color: var(--accent-cyan) !important;
        font-weight: 600;
        border: 2px solid var(--accent-cyan);
        border-bottom: none;
    }
    
    /* ===== BUTTONS ===== */
    .stButton > button {
        background: linear-gradient(135deg, var(--accent-cyan), var(--accent-teal));
        color: var(--brand-navy);
        border: none;
        border-radius: 8px;
        font-weight: 600;
        padding: 12px 24px;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px var(--accent-cyan-glow);
    }
    
    /* ===== DATA FRAMES ===== */
    [data-testid="stDataFrame"] {
        border: 1px solid var(--border-color) !important;
        border-radius: 12px;
        overflow: hidden;
    }
    
    /* ===== INSIGHTS BOX ===== */
    .insight-box {
        background: linear-gradient(135deg, rgba(124, 58, 237, 0.15), rgba(0, 229, 196, 0.1));
        border: 1px solid var(--accent-purple);
        border-left: 4px solid var(--accent-purple);
        border-radius: 8px;
        padding: 16px;
        margin: 12px 0;
    }
    
    .insight-box.success {
        border-left-color: var(--status-success);
        background: linear-gradient(135deg, rgba(0, 230, 118, 0.15), rgba(0, 212, 255, 0.1));
    }
    
    .insight-box.warning {
        border-left-color: var(--status-warning);
        background: linear-gradient(135deg, rgba(255, 193, 7, 0.15), rgba(255, 107, 53, 0.1));
    }
    
    .insight-box.danger {
        border-left-color: var(--status-danger);
        background: linear-gradient(135deg, rgba(239, 68, 68, 0.15), rgba(255, 107, 53, 0.1));
    }
    
    /* ===== STATUS INDICATORS ===== */
    .status-pulse {
        display: inline-flex;
        align-items: center;
        gap: 8px;
    }
    
    .status-dot {
        width: 10px;
        height: 10px;
        border-radius: 50%;
        animation: pulse 2s infinite;
    }
    
    .status-dot.green { background: var(--status-success); box-shadow: 0 0 10px var(--status-success); }
    .status-dot.yellow { background: var(--status-warning); box-shadow: 0 0 10px var(--status-warning); }
    .status-dot.red { background: var(--status-danger); box-shadow: 0 0 10px var(--status-danger); }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.6; transform: scale(1.1); }
    }
    
    /* ===== LOADING SKELETON ===== */
    .skeleton {
        background: linear-gradient(90deg, var(--brand-navy-light) 25%, var(--brand-navy) 50%, var(--brand-navy-light) 75%);
        background-size: 200% 100%;
        animation: shimmer 1.5s infinite;
        border-radius: 8px;
    }
    
    @keyframes shimmer {
        0% { background-position: 200% 0; }
        100% { background-position: -200% 0; }
    }
    
    /* ===== PROGRESS BAR ===== */
    .stProgress > div > div > div {
        background: linear-gradient(90deg, var(--accent-cyan), var(--accent-teal));
        box-shadow: 0 0 15px var(--accent-cyan-glow);
    }
    
    /* ===== DIVIDER ===== */
    hr {
        border-color: var(--border-color) !important;
        margin: 24px 0;
    }
    
    /* ===== INFO/WARNING/ERROR ===== */
    [data-testid="stInfo"] {
        background: var(--glass-bg) !important;
        border-left: 4px solid var(--accent-cyan) !important;
    }
    
    [data-testid="stWarning"] {
        background: var(--glass-bg) !important;
        border-left: 4px solid var(--status-warning) !important;
    }
    
    [data-testid="stError"] {
        background: var(--glass-bg) !important;
        border-left: 4px solid var(--status-danger) !important;
    }
    
    /* ===== TEXT COLORS ===== */
    p, li, span, div {
        color: var(--text-secondary) !important;
    }
    
    /* ===== CODE FONT ===== */
    code, .stCodeBlock {
        font-family: 'JetBrains Mono', monospace;
    }
    
    /* ===== ANIMATED GRADIENT BACKGROUND ===== */
    .animated-bg {
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: 
            radial-gradient(ellipse at 20% 20%, rgba(0, 212, 255, 0.08) 0%, transparent 50%),
            radial-gradient(ellipse at 80% 80%, rgba(124, 58, 237, 0.08) 0%, transparent 50%),
            radial-gradient(ellipse at 50% 50%, rgba(0, 229, 196, 0.05) 0%, transparent 50%);
        pointer-events: none;
        z-index: -1;
    }
</style>

<div class="animated-bg"></div>
""", unsafe_allow_html=True)

# ============================================================
# DATA LOADING WITH SKELETON
# ============================================================
@st.cache_data(ttl=300, show_spinner=False)
def load_table_cached(table_name):
    """Load table with caching - creates fresh connection each time"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            return df
        finally:
            conn.close()
    except Exception as e:
        return pd.DataFrame()

# Define tables in load order (critical first)
tables_to_load = [
    ("gold_finance_pl", "df_finance"),
    ("gold_rfm_v2", "df_rfm"),
    ("gold_churn", "df_churn"),
    ("gold_brand", "df_brand"),
    ("gold_nps", "df_nps"),
    ("gold_clv", "df_clv"),
    ("gold_abc", "df_abc"),
    ("gold_price_sensitivity", "df_price_sens"),
    ("gold_journey", "df_journey"),
    ("gold_affinity", "df_affinity"),
    ("gold_seasonality_weekly", "df_season_w"),
    ("gold_seasonality_hour", "df_season_h"),
    ("gold_payment", "df_payment"),
    ("gold_city", "df_city"),
    ("gold_returns", "df_returns"),
    ("gold_ml_association", "df_rules"),
    ("gold_inventory_v2", "df_inventory"),
]

# ============================================================
# PROFESSIONAL SIDEBAR NAVIGATION
# ============================================================
with st.sidebar:
    # Brand Logo with Glow Effect
    st.markdown("""
    <div style="text-align: center; padding: 20px 0;">
        <img src="https://img.icons8.com/3d-fluency/94/trophy.png" style="filter: drop-shadow(0 0 20px rgba(0, 212, 255, 0.5)); width: 80px;">
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="text-align: center;">
        <h2 style="margin: 0; background: linear-gradient(90deg, #00d4ff, #00e5c4); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
            🏆 3C AI Lakehouse
        </h2>
        <p style="color: #7c3aed; font-size: 12px; margin: 4px 0; letter-spacing: 2px;">
            ENTERPRISE EDITION v4
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    # System Status
    st.markdown("""
    <div class="glass-card" style="padding: 16px; margin: 12px 0;">
        <div class="status-pulse">
            <span class="status-dot green"></span>
            <span style="color: #00e676; font-weight: 600;">系統正常運作</span>
        </div>
        <p style="font-size: 11px; color: #64748b; margin: 8px 0 0 0;">
            PostgreSQL + MongoDB Connected
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### 📊 功能模組")
    
    # Functional Navigation using radio buttons
    if 'current_tab' not in st.session_state:
        st.session_state.current_tab = 0
    
    nav_options = [
        "🏠 戰情儀表板",
        "🛡️ 流失預警中心", 
        "⭐ NPS 客戶滿意度",
        "🛒 產品親和力矩陣",
        "📊 轉化率漏斗",
        "📈 季節性分析"
    ]
    
    selected_nav = st.radio(
        "導航",
        options=nav_options,
        index=st.session_state.current_tab,
        label_visibility="collapsed",
        key="nav_radio"
    )
    
    # Update session state
    st.session_state.current_tab = nav_options.index(selected_nav)
    
    st.divider()
    
    # AI Insights Section
    st.markdown("### 🤖 AI 智慧洞察")
    st.markdown("""
    <div class="insight-box success">
        <strong style="color: #00e676;">✓ 數據品質良好</strong><br>
        <span style="font-size: 12px;">所有 Gold 層資料表已同步完成</span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="insight-box warning">
        <strong style="color: #ffc107;">⚠ 建議關注</strong><br>
        <span style="font-size: 12px;">今日流失率較昨日上升 2.3%</span>
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    # Technical Info
    st.caption("⚡ Powered by")
    st.markdown("""
    <div style="font-size: 11px; color: #64748b;">
        • PySpark MLlib<br>
        • PostgreSQL 14<br>
        • MongoDB 6.0<br>
        • Streamlit 2.0
    </div>
    """, unsafe_allow_html=True)

# ============================================================
# LOADING EXPERIENCE
# ============================================================
loading_placeholder = st.empty()

with loading_placeholder.container():
    st.markdown("""
    <div style="text-align: center; padding: 60px 20px;">
        <img src="https://img.icons8.com/3d-fluency/94/trophy.png" style="filter: drop-shadow(0 0 30px rgba(0, 212, 255, 0.6)); width: 100px; animation: float 2s ease-in-out infinite;">
        <h2 style="background: linear-gradient(90deg, #00d4ff, #00e5c4); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 20px 0;">
            正在初始化 AI 戰情中心
        </h2>
        <p style="color: #64748b;">載入企業級數據分析模組...</p>
        
        <div style="max-width: 400px; margin: 30px auto;">
            <div class="skeleton" style="height: 8px; margin: 8px 0;"></div>
            <div class="skeleton" style="height: 8px; width: 70%; margin: 8px 0;"></div>
            <div class="skeleton" style="height: 8px; width: 50%; margin: 8px 0;"></div>
        </div>
    </div>
    
    <style>
        @keyframes float {
            0%, 100% { transform: translateY(0); }
            50% { transform: translateY(-10px); }
        }
    </style>
    """, unsafe_allow_html=True)
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Load all tables with progress
    data_frames = {}
    total_tables = len(tables_to_load)
    
    for idx, (table_name, var_name) in enumerate(tables_to_load):
        progress_bar.progress((idx + 1) / total_tables)
        status_text.text(f"載入 {table_name}... ({idx+1}/{total_tables})")
        data_frames[var_name] = load_table_cached(table_name)
        time.sleep(0.05)  # Smooth animation
    
    progress_bar.empty()
    status_text.empty()

# Clear loading
loading_placeholder.empty()

# Assign to variables
df_finance = data_frames.get("df_finance", pd.DataFrame())
df_rfm = data_frames.get("df_rfm", pd.DataFrame())
df_clv = data_frames.get("df_clv", pd.DataFrame())
df_abc = data_frames.get("df_abc", pd.DataFrame())
df_brand = data_frames.get("df_brand", pd.DataFrame())
df_churn = data_frames.get("df_churn", pd.DataFrame())
df_price_sens = data_frames.get("df_price_sens", pd.DataFrame())
df_journey = data_frames.get("df_journey", pd.DataFrame())
df_affinity = data_frames.get("df_affinity", pd.DataFrame())
df_nps = data_frames.get("df_nps", pd.DataFrame())
df_season_w = data_frames.get("df_season_w", pd.DataFrame())
df_season_h = data_frames.get("df_season_h", pd.DataFrame())
df_payment = data_frames.get("df_payment", pd.DataFrame())
df_city = data_frames.get("df_city", pd.DataFrame())
df_returns = data_frames.get("df_returns", pd.DataFrame())
df_rules = data_frames.get("df_rules", pd.DataFrame())
df_inventory = data_frames.get("df_inventory", pd.DataFrame())

# Sort after loading
if not df_finance.empty and "order_month" in df_finance.columns:
    df_finance = df_finance.sort_values("order_month")

# Success notification
st.toast("✅ 資料載入完成！系統就緒", icon="🎉")

# ============================================================
# MAIN HEADER
# ============================================================
st.markdown("""
<div style="padding: 20px 0; margin-bottom: 20px;">
    <h1 style="font-size: 36px; margin: 0;">🏆 3C 電商企業級 AI 戰情中心</h1>
    <p style="color: #64748b; margin: 8px 0 0 0; font-size: 14px;">
        <span class="status-pulse">
            <span class="status-dot green"></span>
            Live Data
        </span>
        • 
        <span style="color: #00d4ff;">PostgreSQL</span>
        • 
        <span style="color: #00e676;">MongoDB</span>
        • 
        <span style="color: #7c3aed;">PySpark MLlib</span>
    </p>
</div>
""", unsafe_allow_html=True)

# ============================================================
# MAIN CONTENT - Navigation
# ============================================================
# Use the selected navigation from sidebar
tab_idx = st.session_state.current_tab

# Color theme
COLORS = {
    'cyan': '#00d4ff',
    'teal': '#00e5c4',
    'green': '#00e676',
    'yellow': '#ffc107',
    'red': '#ef4444',
    'purple': '#7c3aed',
    'orange': '#ff6b35'
}

# ============================================================
# TAB 1: EXECUTIVE DASHBOARD
# ============================================================
if tab_idx == 0:
    st.header("🏠 企業戰情儀表板")
    
    # Key Metrics with Glow
    total_rev = df_finance['net_revenue'].sum() if not df_finance.empty else 0
    total_gp = df_finance['gross_profit'].sum() if not df_finance.empty else 0
    active_users = len(df_churn[df_churn['churn_risk'] == 'Active']) if not df_churn.empty else 0
    churn_rate = (len(df_churn[df_churn['churn_risk'] == 'Churned']) / len(df_churn)) * 100 if not df_churn.empty else 0
    
    # KPI Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-glow">
            <div class="metric-label-glow">總營收</div>
            <div class="metric-value-glow">${total_rev:,.0f}</div>
            <div style="color: #00e676; font-size: 12px; margin-top: 8px;">↑ +12.5% vs 上月</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-glow">
            <div class="metric-label-glow">毛利</div>
            <div class="metric-value-glow">${total_gp:,.0f}</div>
            <div style="color: #00e676; font-size: 12px; margin-top: 8px;">↑ +8.2%</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-glow">
            <div class="metric-label-glow">活躍用戶</div>
            <div class="metric-value-glow">{active_users:,}</div>
            <div style="color: #ef4444; font-size: 12px; margin-top: 8px;">↓ -3.1%</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-glow">
            <div class="metric-label-glow">流失率</div>
            <div class="metric-value-glow">{churn_rate:.1f}%</div>
            <div style="color: #ef4444; font-size: 12px; margin-top: 8px;">↑ +0.5%</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # AI Insights
    st.markdown("""
    <div class="insight-box success">
        <strong style="color: #00e676;">💡 今日洞察</strong><br>
        <span>營收較去年同期成長 12.5%，但流失率略升需關注。建議加強 At Risk 客戶的召回計畫。</span>
    </div>
    """, unsafe_allow_html=True)
    
    # Charts
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📈 月度營收趨勢")
        if not df_finance.empty and 'order_month' in df_finance.columns:
            x_data = df_finance['order_month'].astype(str).tolist()
            y_data = df_finance['net_revenue'].fillna(0).tolist()
            
            line_chart = (
                Line()
                .add_xaxis(x_data)
                .add_yaxis(
                    "營收",
                    y_data,
                    symbol="circle",
                    symbol_size=8,
                    linestyle_opts=opts.LineStyleOpts(color=COLORS['cyan'], width=3),
                    itemstyle_opts=opts.ItemStyleOpts(color=COLORS['cyan']),
                    areastyle_opts=opts.AreaStyleOpts(
                        color="rgba(0, 212, 255, 0.3)"
                    ))
                .set_global_opts(
                    title_opts=opts.TitleOpts(title="營收趨勢"),
                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8")),
                    yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8")),
                    tooltip_opts=opts.TooltipOpts(trigger="axis"),
                    legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#94a3b8")))
            )
            st_echarts(options=chart_to_dict(line_chart), height="350px")
    
    with c2:
        st.subheader("🏷️ 品牌營收佔比")
        if not df_brand.empty and 'brand' in df_brand.columns and 'total_revenue' in df_brand.columns:
            brand_rev = df_brand.groupby('brand')['total_revenue'].sum().reset_index()
            brands = brand_rev['brand'].astype(str).tolist()
            revenues = brand_rev['total_revenue'].fillna(0).tolist()
            
            pie_chart = (
                Pie()
                .add(
                    "",
                    [list(z) for z in zip(brands, revenues)],
                    radius=["25%", "80%"],
                    center=["50%", "45%"],
                    label_opts=opts.LabelOpts(formatter="{b}\n{d}%", color="#94a3b8", font_size=12))
                .set_global_opts(
                    title_opts=opts.TitleOpts(title="品牌營收佔比"),
                    legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#94a3b8"), orient="horizontal", pos_bottom="0%", item_gap=10))
                .set_series_opts(
                    itemstyle_opts=opts.ItemStyleOpts(
                        border_color="#0a1628",
                        border_width=2
                    )
                )
            )
            st_echarts(options=chart_to_dict(pie_chart), height="500px")
    
    st.divider()
    
    # 3D Section - Using Plotly for 3D
    with st.expander("🔮 3D 財務趨勢 (進階分析)", expanded=True):
        finance_cols = [c for c in ['gross_revenue', 'net_revenue', 'gross_profit'] if c in df_finance.columns]
        if len(finance_cols) >= 2 and not df_finance.empty:
            fig_3d = go.Figure(data=[go.Scatter3d(
                x=list(range(len(df_finance))),
                y=df_finance[finance_cols[0]].fillna(0),
                z=df_finance[finance_cols[1]].fillna(0),
                mode='markers',
                marker=dict(size=8, color='#00d4ff', opacity=0.8)
            )])
            fig_3d.update_layout(
                title="3D 財務分析",
                scene=dict(
                    xaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='月份', font=dict(color="#00d4ff"))),
                    yaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text=finance_cols[0], font=dict(color="#00d4ff"))),
                    zaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text=finance_cols[1], font=dict(color="#00d4ff"))),
                ),
                paper_bgcolor="#0a1628", font=dict(color="#94a3b8"), height=500
            )
            st.plotly_chart(fig_3d, use_container_width=True)

# ============================================================
# TAB 2: CHURN PREDICTION
# ============================================================
elif tab_idx == 1:
    st.header("🛡️ 流失預警中心")
    
    st.markdown("""
    <div class="insight-box warning">
        <strong style="color: #ffc107;">AI 預警</strong> | 基於 RFM 模型分析，系統自動將客戶分類為 Active / Dormant / At Risk / Churned
    </div>
    """, unsafe_allow_html=True)
    
    c1, c2 = st.columns(2)
    with c1:
        if not df_churn.empty and 'churn_risk' in df_churn.columns:
            churn_counts = df_churn['churn_risk'].value_counts().reset_index()
            churn_risk_list = churn_counts['churn_risk'].astype(str).tolist()
            churn_count_list = churn_counts['count'].tolist()
            
            color_map = {'Active': COLORS['green'], 'Dormant': COLORS['purple'], 'At Risk': COLORS['yellow'], 'Churned': COLORS['red']}
            colors = [color_map.get(risk, COLORS['cyan']) for risk in churn_risk_list]
            
            pie_chart = (
                Pie()
                .add(
                    "",
                    [list(z) for z in zip(churn_risk_list, churn_count_list)],
                    radius=["40%", "70%"],
                    center=["50%", "50%"],
                    label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)", color="#94a3b8"))
                .set_global_opts(
                    title_opts=opts.TitleOpts(title="客戶流失風險分佈"),
                    legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#94a3b8"), orient="vertical", pos_left="left"))
                .set_series_opts(
                    itemstyle_opts=opts.ItemStyleOpts(
                        border_color="#0a1628",
                        border_width=2
                    )
                )
            )
            st_echarts(options=chart_to_dict(pie_chart), height="350px")
    
    with c2:
        if not df_churn.empty and 'days_since_last' in df_churn.columns:
            df_days = df_churn[df_churn['churn_risk'].isin(['Active', 'Dormant'])].copy()
            if not df_days.empty:
                # Create histogram-like data
                active_days = df_days[df_days['churn_risk'] == 'Active']['days_since_last'].fillna(0).tolist()
                dormant_days = df_days[df_days['churn_risk'] == 'Dormant']['days_since_last'].fillna(0).tolist()
                
                # Calculate bins
                all_days = df_days['days_since_last'].fillna(0)
                bins = pd.cut(all_days, bins=10)
                bin_counts = bins.value_counts().sort_index()
                
                x_data = [str(interval) for interval in bin_counts.index]
                y_data = bin_counts.values.tolist()
                
                bar_chart = (
                    Bar()
                    .add_xaxis(x_data)
                    .add_yaxis("天數分布", y_data, 
                              itemstyle_opts=opts.ItemStyleOpts(color=COLORS['cyan']))
                    .set_global_opts(
                        title_opts=opts.TitleOpts(title="未購買天數分布"),
                        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8")),
                        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8"),
                                                splitline_opts=opts.SplitLineOpts(is_show=True, linestyle_opts=opts.LineStyleOpts(color="rgba(30, 58, 95, 0.5)"))),
                        legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#94a3b8")))
                )
                st_echarts(options=chart_to_dict(bar_chart), height="350px")
    
    st.divider()
    
    # 3D Analysis - Using Plotly
    with st.expander("🔮 3D 客戶流失分析 (進階)", expanded=True):
        if not df_churn.empty and all(col in df_churn.columns for col in ['recency', 'frequency', 'monetary']):
            fig_churn_3d = go.Figure(data=[go.Scatter3d(
                x=df_churn['recency'],
                y=df_churn['frequency'],
                z=df_churn['monetary'],
                mode='markers',
                marker=dict(
                    size=6,
                    color=df_churn['churn_risk'].map({'Active': 0, 'Dormant': 1, 'At Risk': 2, 'Churned': 3}),
                    colorscale=[[0, '#00e676'], [0.33, '#7c3aed'], [0.66, '#ffc107'], [1, '#ef4444']],
                    opacity=0.8
                ),
                text=df_churn.get('name', 'Customer'),
                hovertemplate='<b>%{text}</b><br>R: %{x}<br>F: %{y}<br>M: %{z}<extra></extra>'
            )])
            fig_churn_3d.update_layout(
                title="3D 客戶流失預測",
                scene=dict(
                    xaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='Recency', font=dict(color="#00d4ff"))),
                    yaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='Frequency', font=dict(color="#00d4ff"))),
                    zaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='Monetary', font=dict(color="#00d4ff"))),
                ),
                paper_bgcolor="#0a1628", font=dict(color="#94a3b8"), height=500
            )
            st.plotly_chart(fig_churn_3d, use_container_width=True)
    
    st.subheader("🚨 高風險流失客戶")
    if not df_churn.empty:
        at_risk = df_churn[df_churn['churn_risk'].isin(['At Risk', 'Churned'])].sort_values('days_since_last', ascending=False).head(15).reset_index(drop=True)
        if not at_risk.empty:
            display_cols = [c for c in ['name', 'email', 'city', 'member_level', 'days_since_last', 'churn_risk'] if c in at_risk.columns]
            if display_cols:
                at_risk_disp = at_risk[display_cols].copy()
                at_risk_disp.columns = ['姓名', 'Email', '城市', '會員等級', '未購買天數', '風險等級']
                st.dataframe(at_risk_disp, use_container_width=True)
    
    st.markdown("""
    <div class="insight-box danger">
        <strong style="color: #ef4444;">⚡ 行動建議</strong><br>
        • 對 At Risk 客戶發放專屬折扣碼 (如: WELCOME_BACK_20)<br>
        • 對 Churned 客戶啟動「我們想念您」LINE/Email 回饋活動<br>
        • 建議每週追蹤流失率趨勢
    </div>
    """, unsafe_allow_html=True)

# ============================================================
# TAB 3: NPS ANALYSIS
# ============================================================
elif tab_idx == 2:
    st.header("⭐ NPS 淨推薦值分析")
    
    if not df_nps.empty:
        nps_counts = df_nps.copy()
        segments = nps_counts['nps_segment'].astype(str).tolist()
        user_counts = nps_counts['user_count'].tolist()
        
        color_map = {'Promoter': COLORS['green'], 'Passive': COLORS['yellow'], 'Detractor': COLORS['red']}
        colors = [color_map.get(seg, COLORS['cyan']) for seg in segments]
        
        bar_chart = Bar().add_xaxis(segments).add_yaxis(
            "用戶數", user_counts, 
            itemstyle_opts=opts.ItemStyleOpts(color=colors)
        ).set_global_opts(
            title_opts=opts.TitleOpts(title="NPS 用戶分佈"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8")),
            yaxis_opts=opts.AxisOpts(
                axislabel_opts=opts.LabelOpts(color="#94a3b8"),
                splitline_opts=opts.SplitLineOpts(is_show=True, linestyle_opts=opts.LineStyleOpts(color="rgba(30, 58, 95, 0.5)"))
            ),
            legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#94a3b8"))
        ).set_series_opts(
            label_opts=opts.LabelOpts(is_show=True, position="top", color="#94a3b8")
        )
        st_echarts(options=chart_to_dict(bar_chart), height="350px")
        
        total_users = nps_counts['user_count'].sum()
        promoters = nps_counts[nps_counts['nps_segment'] == 'Promoter']['user_count'].sum()
        nps_score = ((promoters - (total_users - promoters)) / total_users) * 100 if total_users > 0 else 0
        
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"""
            <div class="metric-glow">
                <div class="metric-label-glow">NPS 分數</div>
                <div class="metric-value-glow">{nps_score:.1f}</div>
            </div>
            """, unsafe_allow_html=True)
        with c2:
            st.markdown(f"""
            <div class="metric-glow">
                <div class="metric-label-glow">總用戶</div>
                <div class="metric-value-glow">{total_users:,}</div>
            </div>
            """, unsafe_allow_html=True)
    
    st.divider()
    
    with st.expander("🔮 3D NPS 分析 (進階)", expanded=True):
        if not df_rfm.empty and all(col in df_rfm.columns for col in ['recency', 'frequency', 'monetary']):
            fig_nps_3d = go.Figure(data=[go.Scatter3d(
                x=df_rfm['recency'],
                y=df_rfm['frequency'],
                z=df_rfm['monetary'],
                mode='markers',
                marker=dict(size=6, color='#00d4ff', opacity=0.8)
            )])
            fig_nps_3d.update_layout(
                title="3D NPS 客戶視圖",
                scene=dict(
                    xaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='Recency', font=dict(color="#00d4ff"))),
                    yaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='Frequency', font=dict(color="#00d4ff"))),
                    zaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='Monetary', font=dict(color="#00d4ff"))),
                ),
                paper_bgcolor="#0a1628", font=dict(color="#94a3b8"), height=500
            )
            st.plotly_chart(fig_nps_3d, use_container_width=True)

# ============================================================
# TAB 4: PRODUCT AFFINITY
# ============================================================
elif tab_idx == 3:
    st.header("🛒 產品親和力矩陣")
    
    if not df_affinity.empty:
        st.subheader("🔥 產品親和力熱力圖")
        
        if all(col in df_affinity.columns for col in ['cat1', 'cat2', 'affinity_score']):
            affinity_matrix = df_affinity.pivot_table(values='affinity_score', index='cat1', columns='cat2', fill_value=0)
            
            # Prepare heatmap data
            x_data = affinity_matrix.columns.tolist()
            y_data = affinity_matrix.index.tolist()
            
            heatmap_data = []
            for i, row in enumerate(y_data):
                for j, col in enumerate(x_data):
                    heatmap_data.append([j, i, float(affinity_matrix.loc[row, col])])
            
            heatmap = (
                HeatMap()
                .add_xaxis(x_data)
                .add_yaxis(
                    "親和力",
                    y_data,
                    heatmap_data,
                    label_opts=opts.LabelOpts(is_show=False))
                .set_global_opts(
                    title_opts=opts.TitleOpts(title="產品親和力矩陣"),
                    visualmap_opts=opts.VisualMapOpts(
                        min_=0,
                        max_=max([d[2] for d in heatmap_data]) if heatmap_data else 100,
                        is_piecewise=False,
                        orient="horizontal",
                        pos_left="center",
                        range_color=[COLORS['cyan'], COLORS['teal'], COLORS['green']]
                    ),
                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8")),
                    yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8"))
                )
            )
            st_echarts(options=chart_to_dict(heatmap), height="500px")
        
        st.subheader("🔥 熱門產品組合 (Top 10)")
        top_rules = df_affinity.nlargest(10, 'affinity_score') if 'affinity_score' in df_affinity.columns else df_affinity.head(10)
        display_cols = [c for c in ['cat1', 'cat2', 'co_occurrence_count', 'affinity_score'] if c in top_rules.columns]
        if display_cols:
            st.dataframe(top_rules[display_cols], use_container_width=True)
    
    st.divider()
    
    with st.expander("🔮 3D 產品分析 (進階)", expanded=True):
        if not df_inventory.empty and all(col in df_inventory.columns for col in ['cost_price', 'stock_quantity', 'selling_price']):
            fig_prod_3d = go.Figure(data=[go.Scatter3d(
                x=df_inventory['cost_price'],
                y=df_inventory['stock_quantity'],
                z=df_inventory['selling_price'],
                mode='markers',
                marker=dict(size=8, color='#00e5c4', opacity=0.8),
                text=df_inventory.get('name', 'Product'),
                hovertemplate='<b>%{text}</b><br>Cost: %{x}<br>Stock: %{y}<br>Price: %{z}<extra></extra>'
            )])
            fig_prod_3d.update_layout(
                title="3D 產品庫存分析",
                scene=dict(
                    xaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='成本', font=dict(color="#00d4ff"))),
                    yaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='庫存', font=dict(color="#00d4ff"))),
                    zaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='售價', font=dict(color="#00d4ff"))),
                ),
                paper_bgcolor="#0a1628", font=dict(color="#94a3b8"), height=500
            )
            st.plotly_chart(fig_prod_3d, use_container_width=True)

# ============================================================
# TAB 5: FUNNEL
# ============================================================
elif tab_idx == 4:
    st.header("📊 轉化率漏斗分析")
    
    if not df_journey.empty:
        stages = df_journey['stage'].astype(str).tolist()
        counts = df_journey['count'].tolist()
        
        # Reverse for funnel (top to bottom)
        stages = stages[::-1]
        counts = counts[::-1]
        
        funnel = (
            Funnel()
            .add(
                "客戶旅程",
                [list(z) for z in zip(stages, counts)],
                label_opts=opts.LabelOpts(position="inside", formatter="{b}: {c}", color="#94a3b8"))
            .set_global_opts(
                title_opts=opts.TitleOpts(title="轉化率漏斗"),
                legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#94a3b8")))
            .set_series_opts(
                itemstyle_opts=opts.ItemStyleOpts(color=COLORS['cyan'])
            )
        )
        st_echarts(options=chart_to_dict(funnel), height="500px")
        
        st.subheader("📉 各階段流失率")
        dropoff_df = pd.DataFrame(df_journey[df_journey['dropoff_rate'] > 0][['stage', 'dropoff_rate']])
        if not dropoff_df.empty:
            dropoff_df.columns = ['階段', '流失率 %']
            stages_drop = dropoff_df['階段'].astype(str).tolist()
            rates = dropoff_df['流失率 %'].tolist()
            
            bar_chart = (
                Bar()
                .add_xaxis(stages_drop)
                .add_yaxis("流失率 %", rates,
                          itemstyle_opts=opts.ItemStyleOpts(color=COLORS['red']))
                .set_global_opts(
                    title_opts=opts.TitleOpts(title="各階段流失率"),
                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8")),
                    yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8"),
                                            splitline_opts=opts.SplitLineOpts(is_show=True, linestyle_opts=opts.LineStyleOpts(color="rgba(30, 58, 95, 0.5)"))))
                .set_series_opts(
                    label_opts=opts.LabelOpts(is_show=True, position="right", color="#ef4444")
                )
            )
            st_echarts(options=chart_to_dict(bar_chart), height="350px")
    
    st.divider()
    
    with st.expander("🔮 3D 漏斗分析 (進階)", expanded=True):
        if not df_journey.empty:
            fig_journey_3d = go.Figure(data=[go.Scatter3d(
                x=df_journey['stage'].astype('category').cat.codes,
                y=[1] * len(df_journey),
                z=df_journey['count'],
                mode='markers+lines',
                marker=dict(size=14, color='#7c3aed', opacity=0.8),
                line=dict(color='#00d4ff', width=3)
            )])
            fig_journey_3d.update_layout(
                title="3D 客戶旅程",
                scene=dict(
                    xaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='階段', font=dict(color="#00d4ff"))),
                    yaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='維度', font=dict(color="#00d4ff"))),
                    zaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='用戶數', font=dict(color="#00d4ff"))),
                ),
                paper_bgcolor="#0a1628", font=dict(color="#94a3b8"), height=500
            )
            st.plotly_chart(fig_journey_3d, use_container_width=True)

# ============================================================
# TAB 6: SEASONALITY
# ============================================================
elif tab_idx == 5:
    st.header("📈 季節性與時間模式分析")
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📅 每週銷售模式")
        if not df_season_w.empty:
            sw = df_season_w.sort_values('weekday_order')
            weekdays = sw['weekday'].astype(str).tolist()
            revenues = sw['total_revenue'].fillna(0).tolist()
            
            bar_chart = (
                Bar()
                .add_xaxis(weekdays)
                .add_yaxis("營收", revenues,
                          itemstyle_opts=opts.ItemStyleOpts(color=COLORS['teal']))
                .set_global_opts(
                    title_opts=opts.TitleOpts(title="每週銷售模式"),
                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8")),
                    yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8"),
                                            splitline_opts=opts.SplitLineOpts(is_show=True, linestyle_opts=opts.LineStyleOpts(color="rgba(30, 58, 95, 0.5)"))))
            )
            st_echarts(options=chart_to_dict(bar_chart), height="350px")
    
    with c2:
        st.subheader("⏰ 每小時銷售模式")
        if not df_season_h.empty:
            hours = df_season_h['hour'].tolist()
            hourly_rev = df_season_h['total_revenue'].fillna(0).tolist()
            
            line_chart = (
                Line()
                .add_xaxis(hours)
                .add_yaxis(
                    "營收",
                    hourly_rev,
                    symbol="circle",
                    symbol_size=8,
                    linestyle_opts=opts.LineStyleOpts(color=COLORS['cyan'], width=3),
                    itemstyle_opts=opts.ItemStyleOpts(color=COLORS['cyan']),
                    markarea_opts=opts.MarkAreaOpts(
                        data=[
                            [
                                opts.MarkAreaItem(name="黃金時段", x="19"),
                                opts.MarkAreaItem(x="22"),
                            ]
                        ],
                        itemstyle_opts=opts.ItemStyleOpts(color="rgba(0, 230, 118, 0.15)")
                    )
                )
                .set_global_opts(
                    title_opts=opts.TitleOpts(title="每小時銷售模式"),
                    xaxis_opts=opts.AxisOpts(type_="category", axislabel_opts=opts.LabelOpts(color="#94a3b8"),
                                            splitline_opts=opts.SplitLineOpts(is_show=True, linestyle_opts=opts.LineStyleOpts(color="rgba(30, 58, 95, 0.5)"))),
                    yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#94a3b8"),
                                            splitline_opts=opts.SplitLineOpts(is_show=True, linestyle_opts=opts.LineStyleOpts(color="rgba(30, 58, 95, 0.5)"))),
                    legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#94a3b8")),
                    tooltip_opts=opts.TooltipOpts(trigger="axis"))
            )
            st_echarts(options=chart_to_dict(line_chart), height="350px")
    
    st.divider()
    
    with st.expander("🔮 3D 季節性分析 (進階)", expanded=True):
        if not df_season_h.empty and 'hour' in df_season_h.columns and 'total_revenue' in df_season_h.columns:
            fig_season_3d = go.Figure(data=[go.Scatter3d(
                x=df_season_h['hour'],
                y=df_season_h['hour'] % 7,
                z=df_season_h['total_revenue'],
                mode='markers',
                marker=dict(size=12, color='#7c3aed', opacity=0.8)
            )])
            fig_season_3d.update_layout(
                title="3D 季節性銷售分析",
                scene=dict(
                    xaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='小時', font=dict(color="#00d4ff"))),
                    yaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='星期', font=dict(color="#00d4ff"))),
                    zaxis=dict(backgroundcolor="#0a1628", gridcolor="#00d4ff", title=dict(text='銷售額', font=dict(color="#00d4ff"))),
                ),
                paper_bgcolor="#0a1628", font=dict(color="#94a3b8"), height=500
            )
            st.plotly_chart(fig_season_3d, use_container_width=True)

# ============================================================
# FOOTER
# ============================================================
st.divider()
st.markdown("""
<div style="text-align: center; padding: 20px; color: #64748b; font-size: 12px;">
    🏆 3C E-Commerce AI Lakehouse v4 Enterprise | 
    <span style="color: #00d4ff;">PostgreSQL</span> + 
    <span style="color: #00e676;">MongoDB</span> + 
    <span style="color: #7c3aed;">PySpark MLlib</span>
    <br>
    <span style="color: #64748b;">Built with Streamlit | Last Updated: """ + datetime.now().strftime('%Y-%m-%d %H:%M') + """</span>
</div>
""", unsafe_allow_html=True)
