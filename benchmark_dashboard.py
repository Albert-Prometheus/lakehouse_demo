import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pymongo import MongoClient
import time
from datetime import datetime, timedelta
import random

st.set_page_config(page_title="⚙️ Performance Benchmark", layout="wide", page_icon="⚙️")

st.markdown("""
<style>
    .stApp { background-color: #0a0a0f; }
    h1 { 
        background: linear-gradient(90deg, #00d4ff, #ff00ff); 
        -webkit-background-clip: text; 
        -webkit-text-fill-color: transparent;
        font-weight: 900;
    }
    .stMarkdown { color: #e0e0e0; }
    
    /* Professional Gauge */
    .gauge-wrapper {
        background: linear-gradient(180deg, #151520 0%, #0d0d15 100%);
        border: 1px solid #2a2a3a;
        border-radius: 20px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 10px 40px rgba(0,0,0,0.5), inset 0 1px 0 rgba(255,255,255,0.05);
    }
    .gauge-title {
        color: #888;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin-bottom: 10px;
    }
    .gauge-value {
        font-size: 48px;
        font-weight: 900;
        background: linear-gradient(180deg, #00ff88, #00d4ff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .gauge-unit {
        color: #666;
        font-size: 14px;
    }
    
    /* Metric Card */
    .metric-card {
        background: linear-gradient(145deg, #12121a 0%, #1a1a28 100%);
        border: 1px solid #2d2d3d;
        border-radius: 15px;
        padding: 20px;
        box-shadow: 0 5px 20px rgba(0,0,0,0.3);
    }
    .metric-card-title {
        color: #666;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .metric-card-value {
        color: #fff;
        font-size: 28px;
        font-weight: 700;
        margin: 5px 0;
    }
    .metric-card-delta {
        font-size: 12px;
    }
    
    /* Status Indicator */
    .status-dot {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        margin-right: 8px;
        animation: pulse 2s infinite;
    }
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.5; }
        100% { opacity: 1; }
    }
    
    /* Table Styling */
    .dataframe {
        font-size: 12px !important;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        background: #151520;
        border-radius: 10px 10px 0 0;
        padding: 10px 20px;
    }
</style>
""", unsafe_allow_html=True)

PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD = "localhost", "5049", "postgres", "postgres", "admin"
MONGO_URI = "mongodb://mongo:admin@localhost:27017/?authSource=admin"

@st.cache_data(ttl=5)
def get_postgres_data():
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
    
    # Get gold table metrics
    gold_tables = [
        'gold_finance_pl', 'gold_rfm_v2', 'gold_churn', 'gold_nps',
        'gold_cohort', 'gold_brand', 'gold_inventory_v2', 'gold_abc',
        'gold_affinity', 'gold_seasonality_weekly', 'gold_funnel_v2'
    ]
    
    table_stats = []
    for table in gold_tables:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            cur.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{table}'))")
            size = cur.fetchone()[0]
            cur.close()
            table_stats.append({'table': table, 'rows': count, 'size': size})
        except:
            table_stats.append({'table': table, 'rows': 0, 'size': '0 B'})
    
    # Get order metrics
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM orders")
    total_orders = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM orders WHERE order_date > NOW() - INTERVAL '1 hour'")
    hourly_orders = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM orders WHERE order_date > NOW() - INTERVAL '5 minutes'")
    min5_orders = cur.fetchone()[0]
    
    # Calculate TPS
    cur.execute("""
        SELECT 
            MAX(order_date) as last_order,
            COUNT(*) as total
        FROM orders 
        WHERE order_date > NOW() - INTERVAL '1 hour'
    """)
    hour_data = cur.fetchone()
    
    # Get revenue
    cur.execute("SELECT COALESCE(SUM(shipping_fee + discount_amount), 0) FROM orders WHERE order_date > NOW() - INTERVAL '24 hours'")
    daily_revenue = cur.fetchone()[0]
    
    # Get unique customers
    cur.execute("SELECT COUNT(DISTINCT user_id) FROM orders")
    unique_customers = cur.fetchone()[0]
    
    # Get avg order value
    cur.execute("SELECT COALESCE(AVG(shipping_fee + discount_amount), 0) FROM orders")
    avg_order_value = cur.fetchone()[0]
    
    # Query performance test
    query_times = {}
    test_queries = [
        ("SELECT * FROM gold_finance_pl ORDER BY order_month DESC LIMIT 100", "Gold Finance"),
        ("SELECT * FROM gold_rfm_v2 LIMIT 100", "RFM Data"),
        ("SELECT * FROM orders JOIN order_items ON orders.order_id = order_items.order_id LIMIT 100", "Join Query"),
    ]
    
    for q, name in test_queries:
        start = time.time()
        try:
            cur.execute(q)
            cur.fetchall()
            query_times[name] = (time.time() - start) * 1000
        except:
            query_times[name] = 0
    
    cur.close()
    conn.close()
    
    return {
        'total_orders': total_orders,
        'hourly_orders': hourly_orders,
        'min5_orders': min5_orders,
        'daily_revenue': daily_revenue,
        'unique_customers': unique_customers,
        'avg_order_value': avg_order_value,
        'table_stats': table_stats,
        'query_times': query_times
    }

@st.cache_data(ttl=10)
def get_mongo_data():
    try:
        mongo = MongoClient(MONGO_URI)
        db = mongo['ecommerce_stream']
        
        # Clickstream stats
        clickstream_count = db.clickstream.count_documents({})
        
        # Recent events (last 5 min)
        recent_events = list(db.clickstream_stream.find(
            {'timestamp': {'$gte': datetime.now() - timedelta(minutes=5)}}
        ).sort('_id', -1).limit(100))
        
        # Event types
        event_types = {}
        for e in recent_events:
            et = e.get('event_type', 'unknown')
            event_types[et] = event_types.get(et, 0) + 1
        
        # Collection stats
        collections = {}
        for coll_name in db.list_collection_names():
            collections[coll_name] = db[coll_name].count_documents({})
        
        mongo.close()
        
        return {
            'clickstream_count': clickstream_count,
            'recent_events': len(recent_events),
            'event_types': event_types,
            'collections': collections
        }
    except Exception as e:
        return {'error': str(e)}

# ============================================================
# HEADER
# ============================================================
col_header1, col_header2 = st.columns([3, 1])

with col_header1:
    st.title("⚙️ Performance Benchmark Center")
    st.markdown("**Real-Time System Monitoring | Query Performance | Data Lakehouse Metrics**")

with col_header2:
    st.markdown(f"""
    <div style="text-align: right; padding: 20px;">
        <span class="status-dot" style="background: #00ff88;"></span>
        <span style="color: #00ff88;">LIVE</span>
        <br>
        <span style="color: #666; font-size: 12px;">{datetime.now().strftime('%H:%M:%S')}</span>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# ============================================================
# MAIN METRICS
# ============================================================
pg_data = get_postgres_data()
mg_data = get_mongo_data()

# KPI Row
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

with kpi1:
    tps = pg_data['min5_orders'] / 300 if pg_data['min5_orders'] > 0 else 0
    st.markdown(f"""
    <div class="gauge-wrapper">
        <div class="gauge-title">Current TPS</div>
        <div class="gauge-value">{tps:.1f}</div>
        <div class="gauge-unit">tx/sec</div>
    </div>
    """, unsafe_allow_html=True)

with kpi2:
    st.markdown(f"""
    <div class="gauge-wrapper">
        <div class="gauge-title">Hourly Orders</div>
        <div class="gauge-value">{pg_data['hourly_orders']:,}</div>
        <div class="gauge-unit">orders</div>
    </div>
    """, unsafe_allow_html=True)

with kpi3:
    st.markdown(f"""
    <div class="gauge-wrapper">
        <div class="gauge-title">Daily Revenue</div>
        <div class="gauge-value">${pg_data['daily_revenue']:,.0f}</div>
        <div class="gauge-unit">TWD</div>
    </div>
    """, unsafe_allow_html=True)

with kpi4:
    st.markdown(f"""
    <div class="gauge-wrapper">
        <div class="gauge-title">Unique Customers</div>
        <div class="gauge-value">{pg_data['unique_customers']:,}</div>
        <div class="gauge-unit">users</div>
    </div>
    """, unsafe_allow_html=True)

with kpi5:
    st.markdown(f"""
    <div class="gauge-wrapper">
        <div class="gauge-title">Avg Order</div>
        <div class="gauge-value">${pg_data['avg_order_value']:,.0f}</div>
        <div class="gauge-unit">TWD</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# ============================================================
# TABS
# ============================================================
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Throughput", "📈 Gold Tables", "⚡ Query Perf", "🍃 MongoDB", "🔄 Live Data"])

# ============================================================
# TAB 1: THROUGHPUT
# ============================================================
with tab1:
    col_t1_1, col_t1_2 = st.columns(2)
    
    with col_t1_1:
        st.subheader("📈 Orders Per Minute (Real-Time)")
        
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
        orders_min = pd.read_sql("""
            SELECT date_trunc('minute', order_date) as minute, 
                   COUNT(*) as orders
            FROM orders 
            WHERE order_date > NOW() - INTERVAL '1 hour'
            GROUP BY 1
            ORDER BY 1
        """, conn)
        conn.close()
        
        if not orders_min.empty:
            orders_min['minute'] = pd.to_datetime(orders_min['minute'])
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=orders_min['minute'], y=orders_min['orders'],
                fill='tozeroy',
                mode='lines',
                line=dict(color='#00d4ff', width=3),
                fillcolor='rgba(0, 212, 255, 0.2)',
                name='Orders'
            ))
            fig.add_trace(go.Scatter(
                x=orders_min['minute'], y=orders_min['orders'].rolling(5).mean(),
                mode='lines',
                line=dict(color='#ff00ff', width=2, dash='dash'),
                name='Moving Avg'
            ))
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#ccc',
                height=300,
                legend=dict(orientation='h', y=1.1)
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col_t1_2:
        st.subheader("💰 Revenue Trend (Today)")
        
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
        revenue_hour = pd.read_sql("""
            SELECT date_trunc('hour', order_date) as hour, 
                   SUM(shipping_fee + discount_amount) as revenue
            FROM orders 
            WHERE order_date > NOW() - INTERVAL '24 hours'
            GROUP BY 1
            ORDER BY 1
        """, conn)
        conn.close()
        
        if not revenue_hour.empty:
            revenue_hour['hour'] = pd.to_datetime(revenue_hour['hour'])
            
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=revenue_hour['hour'], y=revenue_hour['revenue'],
                marker=dict(color=revenue_hour['revenue'], colorscale='Viridis'),
                name='Revenue'
            ))
            fig2.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#ccc',
                height=300
            )
            st.plotly_chart(fig2, use_container_width=True)

# ============================================================
# TAB 2: GOLD TABLES
# ============================================================
with tab2:
    st.subheader("🏆 Gold Layer Tables (Data Lakehouse)")
    
    if pg_data['table_stats']:
        df_tables = pd.DataFrame(pg_data['table_stats'])
        
        col_gt1, col_gt2 = st.columns([2, 1])
        
        with col_gt1:
            fig = px.bar(
                df_tables, x='table', y='rows',
                color='rows', color_continuous_scale='Blues',
                title="Gold Table Row Counts"
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#ccc',
                height=350,
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col_gt2:
            st.markdown("**Table Sizes:**")
            for _, row in df_tables.iterrows():
                st.markdown(f"- {row['table']}: `{row['size']}`")

# ============================================================
# TAB 3: QUERY PERFORMANCE
# ============================================================
with tab3:
    st.subheader("⚡ Query Performance Test")
    
    if pg_data['query_times']:
        df_queries = pd.DataFrame([
            {'Query': k, 'Time (ms)': v} for k, v in pg_data['query_times'].items()
        ])
        
        col_q1, col_q2 = st.columns([2, 1])
        
        with col_q1:
            fig = px.bar(
                df_queries, x='Query', y='Time (ms)',
                color='Time (ms)', color_continuous_scale='Reds',
                title="Query Execution Time"
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#ccc',
                height=300
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col_q2:
            st.markdown("**Performance Status:**")
            for q, t in pg_data['query_times'].items():
                status = "🟢" if t < 100 else "🟡" if t < 500 else "🔴"
                st.markdown(f"{status} {q}: **{t:.1f}ms**")

# ============================================================
# TAB 4: MONGODB
# ============================================================
with tab4:
    st.subheader("🍃 MongoDB Streaming Metrics")
    
    col_m1, col_m2 = st.columns(2)
    
    with col_m1:
        if 'error' not in mg_data:
            # Event type distribution
            if mg_data.get('event_types'):
                et_df = pd.DataFrame([
                    {'Event Type': k, 'Count': v} for k, v in mg_data['event_types'].items()
                ])
                
                fig = px.pie(
                    et_df, values='Count', names='Event Type',
                    hole=0.5,
                    color_discrete_sequence=px.colors.qualitative.Bold
                )
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#ccc',
                    height=300
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No recent streaming events")
        else:
            st.error(f"MongoDB Error: {mg_data['error']}")
    
    with col_m2:
        if 'collections' in mg_data:
            st.markdown("**Collection Counts:**")
            for coll, count in mg_data['collections'].items():
                st.metric(coll, f"{count:,}")

# ============================================================
# TAB 5: LIVE DATA
# ============================================================
with tab5:
    st.subheader("🔄 Live Data from Enterprise Dashboard")
    
    col_l1, col_l2 = st.columns(2)
    
    with col_l1:
        st.markdown("**Recent Orders (Live)**")
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
        recent_orders = pd.read_sql("""
            SELECT o.order_id, o.order_date, u.name, o.status, 
                   o.shipping_fee + o.discount_amount as total
            FROM orders o
            LEFT JOIN users u ON o.user_id = u.user_id
            ORDER BY o.order_date DESC
            LIMIT 10
        """, conn)
        conn.close()
        
        if not recent_orders.empty:
            recent_orders['order_date'] = pd.to_datetime(recent_orders['order_date']).dt.strftime('%H:%M:%S')
            st.dataframe(recent_orders, use_container_width=True, hide_index=True)
    
    with col_l2:
        st.markdown("**Order Status Distribution**")
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
        status_dist = pd.read_sql("""
            SELECT status, COUNT(*) as count
            FROM orders
            GROUP BY status
        """, conn)
        conn.close()
        
        if not status_dist.empty:
            fig = px.bar(
                status_dist, x='status', y='count',
                color='count', color_continuous_scale='Greens'
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#ccc',
                height=250
            )
            st.plotly_chart(fig, use_container_width=True)

st.divider()

# ============================================================
# AUTO REFRESH
# ============================================================
col_refresh, col_time = st.columns([1, 3])

with col_refresh:
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.rerun()

with col_time:
    st.caption(f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Auto-refresh: 5s")

st.markdown("""
<script>
    setTimeout(function(){
        window.location.reload();
    }, 5000);
</script>
""", unsafe_allow_html=True)

st.caption("⚙️ 3C E-Commerce Performance Benchmark | PostgreSQL + MongoDB | Real-Time Monitoring")
