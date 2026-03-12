import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(page_title="3C AI Lakehouse v4 Enterprise", layout="wide", page_icon="🏆")

st.markdown("""
<style>
    .stApp { background-color: #0E1117; }
    .metric-card { 
        background: linear-gradient(145deg, #1a1f2e 0%, #252b3b 100%); 
        border-radius: 12px; 
        padding: 20px; 
        text-align: center; 
        border: 1px solid #3d4663;
        box-shadow: 0 4px 20px rgba(0,0,0,0.4);
    }
    .metric-value { 
        font-size: 32px; 
        font-weight: 800; 
        background: linear-gradient(90deg, #00D4FF, #00FF88);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .metric-label { 
        font-size: 11px; 
        color: #8892b0 !important; 
        text-transform: uppercase; 
        letter-spacing: 2px; 
        margin-top: 5px;
    }
    [data-testid="stMetricValue"] { color: #00FF88 !important; }
    [data-testid="stMetricDelta"] { color: #FF6B6B !important; }
    .insight-box { 
        background: linear-gradient(135deg, #1a1f2e 0%, #2d1b4e 50%, #1a1f2e 100%); 
        border: 1px solid #7c3aed;
        color: #e2e8f0;
        padding: 16px;
        border-radius: 8px;
        margin: 10px 0;
    }
    h1, h2, h3 { color: #00D4FF !important; }
    [data-testid="stSidebar"] { background-color: #0B0F19 !important; border-right: 1px solid #3d4663; }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 { color: #00D4FF !important; }
    [data-testid="stDataFrame"] { border: 1px solid #3d4663 !important; }
    hr { border-color: #3d4663 !important; }
    [data-testid="stInfo"] { background-color: #1a1f2e !important; border-left: 4px solid #00D4FF !important; }
    [data-testid="stWarning"] { background-color: #1a1f2e !important; border-left: 4px solid #FFD700 !important; }
    p, li, span { color: #cbd5e1 !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] { 
        background: #1a1f2e; 
        border-radius: 10px 10px 0 0; 
        padding: 10px 20px;
    }
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD = "localhost", "5049", "postgres", "postgres", "admin"

def load_table(table_name):
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

with st.sidebar:
    st.image("https://img.icons8.com/3d-fluency/94/trophy.png", width=70)
    st.title("🏆 企業級數據湖倉 v4")
    st.caption("Advanced AI Analytics Edition")
    st.divider()
    st.markdown("### 💡 AI 戰情中心")
    st.info("""
    **v4 新功能預覽:**
    - 🔴 流失預警系統
    - ⭐ NPS 淨推薦值
    - 🛒 產品親和力矩陣  
    - 📊 轉化率漏斗
    """)
    st.divider()
    st.caption("Powered by PySpark MLlib + Streamlit")

try:
    df_finance = load_table("gold_finance_pl").sort_values("order_month")
    df_rfm = load_table("gold_rfm_v2")
    df_clv = load_table("gold_clv")
    df_abc = load_table("gold_abc")
    df_brand = load_table("gold_brand")
    df_churn = load_table("gold_churn")
    df_price_sens = load_table("gold_price_sensitivity")
    df_journey = load_table("gold_journey")
    df_affinity = load_table("gold_affinity")
    df_nps = load_table("gold_nps")
    df_season_w = load_table("gold_seasonality_weekly")
    df_season_h = load_table("gold_seasonality_hour")
    df_payment = load_table("gold_payment")
    df_city = load_table("gold_city")
    df_returns = load_table("gold_returns")
    df_rules = load_table("gold_ml_association")
    df_inventory = load_table("gold_inventory_v2")
except Exception as e:
    st.error(f"資料載入失敗: {e}")
    st.stop()

st.title("🏆 3C 電商企業級 AI 戰情中心")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏠 戰情儀表板", "🛡️ 流失預警中心", "⭐ NPS 客戶滿意度", 
    "🛒 產品親和力矩陣", "📊 轉化率漏斗", "📈 季節性分析"
])

with tab1:
    st.header("🏠 企業戰情儀表板 (Executive Dashboard)")
    
    total_rev = df_finance['net_revenue'].sum()
    total_gp = df_finance['gross_profit'].sum()
    active_users = len(df_churn[df_churn['churn_risk'] == 'Active'])
    churn_rate = (len(df_churn[df_churn['churn_risk'] == 'Churned']) / len(df_churn)) * 100
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("總營收", f"${total_rev:,.0f}", delta="+12.5% vs 上月", delta_color="normal")
    c2.metric("毛利", f"${total_gp:,.0f}", delta="+8.2%", delta_color="normal")
    c3.metric("活躍用戶", f"{active_users}", delta="-3.1%", delta_color="inverse")
    c4.metric("流失率", f"{churn_rate:.1f}%", delta="+0.5%", delta_color="inverse")
    
    st.divider()
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📈 月度營收趨勢")
        fig_rev = px.area(df_finance, x='order_month', y='net_revenue', title="Revenue Trend", markers=True)
        fig_rev.update_layout(height=350, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#ccc')
        st.plotly_chart(fig_rev, use_container_width=True)
    with c2:
        st.subheader("🏷️ 品牌營收佔比")
        brand_rev = df_brand.groupby('brand')['total_revenue'].sum().reset_index()
        fig_pie = px.pie(brand_rev, values='total_revenue', names='brand', hole=0.4, title="Brand Revenue")
        fig_pie.update_layout(height=350, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#ccc')
        st.plotly_chart(fig_pie, use_container_width=True)

with tab2:
    st.header("🛡️ 流失預警中心 (Churn Prediction)")
    st.markdown("基於客戶最後購買距今天數，AI 自動分類為 **Active (活躍)** / **Dormant (沉睡)** / **At Risk (危險)** / **Churned (已流失)**。")
    
    c1, c2 = st.columns([1, 1])
    with c1:
        churn_counts = df_churn['churn_risk'].value_counts().reset_index()
        fig_churn = px.pie(churn_counts, values='count', names='churn_risk', hole=0.5,
                          color='churn_risk', color_discrete_map={'Active': '#00CC96', 'Dormant': '#AB63FA', 'At Risk': '#FFA15A', 'Churned': '#EF553B'})
        fig_churn.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#ccc')
        st.plotly_chart(fig_churn, use_container_width=True)
    with c2:
        if 'days_since_last' in df_churn.columns:
            df_days = df_churn[df_churn['churn_risk'].isin(['Active', 'Dormant'])].copy()
            fig_days = px.histogram(df_days, x='days_since_last', color='churn_risk', barmode='overlay',
                                  color_discrete_map={'Active': '#00CC96', 'Dormant': '#AB63FA'})
            fig_days.update_layout(title="最後購買距今天數分布", plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#ccc')
            st.plotly_chart(fig_days, use_container_width=True)
    
    st.subheader("🚨 高風險流失客戶名單 (At Risk + Churned)")
    at_risk = df_churn[df_churn['churn_risk'].isin(['At Risk', 'Churned'])].sort_values('days_since_last', ascending=False).head(15).reset_index(drop=True)
    at_risk_disp = at_risk[['name', 'email', 'city', 'member_level', 'days_since_last', 'churn_risk']]
    at_risk_disp.columns = ['姓名', 'Email', '城市', '會員等級', '未購買天數', '風險等級']
    st.dataframe(at_risk_disp, use_container_width=True)
    
    st.info("💡 **行動建議**: 對 At Risk 客戶發放專屬折扣碼；對 Churned 客戶啟動「我們想念您」LINE/Email 回饋活動。")

with tab3:
    st.header("⭐ NPS 淨推薦值分析 (Net Promoter Score)")
    st.markdown("根據客戶的消費頻率與總金額，模擬 NPS 等級分布。**Promoter (推薦者)** 為最有可能向他人推薦品牌的忠誠客戶。")
    
    nps_counts = df_nps.copy()
    fig_nps = px.bar(nps_counts, x='nps_segment', y='user_count', text='user_count',
                     color='nps_segment', 
                     color_discrete_map={'Promoter': '#00CC96', 'Passive': '#FFA15A', 'Detractor': '#EF553B'})
    fig_nps.update_layout(title="NPS 等級分布", plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#ccc')
    st.plotly_chart(fig_nps, use_container_width=True)
    
    total_users = nps_counts['user_count'].sum()
    promoters = nps_counts[nps_counts['nps_segment'] == 'Promoter']['user_count'].sum()
    nps_score = ((promoters - (total_users - promoters)) / total_users) * 100 if total_users > 0 else 0
    
    c1, c2 = st.columns(2)
    with c1:
        st.metric("NPS 分數", f"{nps_score:.1f}")
    with c2:
        st.metric("總用戶", f"{total_users:,}")

with tab4:
    st.header("🛒 產品親和力矩陣 (Product Affinity)")
    st.markdown("透過關聯規則 (Association Rules) 發現哪些產品經常被一起購買。")
    
    if not df_affinity.empty:
        st.subheader("🔥 熱門產品組合 (Top 10)")
        top_rules = df_rules.nlargest(10, 'confidence') if 'confidence' in df_rules.columns else df_rules.head(10)
        
        display_cols = [c for c in ['antecedent', 'consequent', 'support', 'confidence', 'lift'] if c in top_rules.columns]
        if display_cols:
            st.dataframe(top_rules[display_cols], use_container_width=True)
        
        st.subheader("📊 產品親和力熱力圖")
        if 'antecedent' in df_affinity.columns and 'consequent' in df_affinity.columns:
            affinity_matrix = df_affinity.pivot_table(values='lift', index='antecedent', columns='consequent', fill_value=0)
            fig_heat = px.imshow(affinity_matrix, title="產品親和力矩陣", color_continuous_scale='Blues')
            fig_heat.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#ccc')
            st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.info("親和力數據載入中...")

with tab5:
    st.header("📊 轉化率漏斗分析")
    st.markdown("追蹤客戶從「瀏覽」到「購買」的完整轉化路徑。")
    
    if not df_journey.empty:
        fig_funnel = px.funnel(df_journey, x='count', y='stage', title="客戶旅程漏斗")
        fig_funnel.update_layout(height=500, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#ccc')
        st.plotly_chart(fig_funnel, use_container_width=True)
        
        st.subheader("📉 各階段流失率")
        dropoff_df = pd.DataFrame(df_journey[df_journey['dropoff_rate'] > 0][['stage', 'dropoff_rate']])
        dropoff_df.columns = ['階段', '流失率 %']
        fig_drop = px.bar(dropoff_df, x='流失率 %', y='階段', orientation='h', color='流失率 %', color_continuous_scale='Reds')
        st.plotly_chart(fig_drop, use_container_width=True)

with tab6:
    st.header("📈 季節性與時間模式分析")
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📅 每週銷售模式 (Weekday Pattern)")
        sw = df_season_w.sort_values('weekday_order')
        fig_sw = px.bar(sw, x='weekday', y='total_revenue', color='total_revenue', color_continuous_scale='Blues')
        st.plotly_chart(fig_sw, use_container_width=True)
    with c2:
        st.subheader("⏰ 每小時銷售模式 (Hourly Pattern)")
        fig_sh = px.line(df_season_h, x='hour', y='total_revenue', markers=True)
        fig_sh.add_vrect(x0=19, x1=22, fillcolor="green", opacity=0.1, annotation_text="黃金時段", annotation_position="top left")
        st.plotly_chart(fig_sh, use_container_width=True)

st.divider()
st.caption("🏆 3C E-Commerce AI Lakehouse v4 Enterprise | PostgreSQL + MongoDB + PySpark MLlib | Built with Streamlit")
