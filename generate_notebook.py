import nbformat as nbf

nb = nbf.v4.new_notebook()

# Notebook Metadata / Setup
cells = [
    nbf.v4.new_markdown_cell("""# PySpark ETL: Lakehouse Analytics
這個 Notebook 會演示如何使用 Apache Spark (PySpark) 從不同的資料來源 (PostgreSQL 與 MongoDB) 提取資料、進行清洗與融合轉換 (Silver Layer)，最後計算出業務所需的聚合報表並寫回 PostgreSQL (Gold Layer)，以供 Streamlit Dashboard 呈現。

---
### 1. 環境設定與套件安裝
首先，我們需要在 Jupyter 內部安裝 JDBC Driver 與 MongoDB Spark Connector，這樣 PySpark 才能與外部資料庫溝通。"""),
    
    nbf.v4.new_code_cell("""!pip install psycopg2-binary
# 注意：在啟動 PySpark Session 之前，我們必須在 spark.jars.packages 裡面指定 driver 包！"""),
    
    nbf.v4.new_markdown_cell("""### 2. 初始化 Spark Session (掛載 Connectors)
這裡是最關鍵的一步：我們要告訴 Spark 啟動時下載 `mongo-spark-connector` 以及 `postgresql-jdbc` 兩個 jar 檔。"""),
    
    nbf.v4.new_code_cell("""from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 指定 Spark 版本相應的 Connector 版本 (這裡以 Spark 3.5.0 為例)
spark = SparkSession.builder \\
    .appName("Lakehouse_ETL_Job") \\
    .config("spark.jars.packages", 
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1," # MongoDB Connector
            "org.postgresql:postgresql:42.6.0") \\
    .config("spark.mongodb.input.uri", "mongodb://mongo:admin@mongodb:27017/ecommerce_lake.clickstream") \\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark Session with JDBC & Mongo Connectors initialized!")"""),
    
    nbf.v4.new_markdown_cell("""### 3. Bronze Layer: 從異質資料庫讀取原始資料 (Extract)
我們將同時從 PostgreSQL 和 MongoDB 把原始資料抓出來放入 Spark DataFrame 中。"""),
    
    nbf.v4.new_code_cell("""# === 3.1 讀取 PostgreSQL (Users & Orders) ===
pg_url = "jdbc:postgresql://pgvector:5432/postgres"
pg_properties = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# 讀取 Users 表
users_df = spark.read.jdbc(url=pg_url, table="users", properties=pg_properties)
# 讀取 Orders 表
orders_df = spark.read.jdbc(url=pg_url, table="orders", properties=pg_properties)

print("--- PostgreSQL Users DataFrame Schema ---")
users_df.printSchema()"""),

    nbf.v4.new_code_cell("""# === 3.2 讀取 MongoDB (Clickstream 日誌) ===
# 由於 MongoDB 是半結構化，Spark 讀進來時 metadata 欄位會是一個 Struct (巢狀 JSON)
clickstream_df = spark.read.format("mongo").load()

print("--- MongoDB Clickstream DataFrame Schema ---")
clickstream_df.printSchema()"""),
    
    nbf.v4.new_markdown_cell("""### 4. Silver Layer: 資料清洗與融合 (Transform)
我們需要：
1. **處理 Clickstream**：把 `metadata` 裡面的 URL 和 停留時間 攤平 (Flatten) 變成獨立欄位。
2. **JOIN 兩個資料源**：把 Orders 資訊與 Clickstream 的 User_ID 進行關聯，這樣我們就能分析用戶行為與最終購買的關係。"""),
    
    nbf.v4.new_code_cell("""# 攤平 MongoDB 巢狀 JSON 結構
clean_clicks_df = clickstream_df \\
    .withColumn("url", col("metadata.url")) \\
    .withColumn("duration_sec", col("metadata.session_duration_sec").cast("int")) \\
    .drop("metadata", "_id") # 丟棄原始巢狀欄位與 Mongo 預設的 _id

# 與 Users 進行 JOIN，獲取地區資訊 (舉例)
# 我們想知道不同地區使用者的平均停留時間
user_clicks_df = clean_clicks_df.join(users_df, on="user_id", how="left")
user_clicks_df.show(5)"""),
    
    nbf.v4.new_markdown_cell("""### 5. Gold Layer: 業務聚合計算
這裡我們模擬出兩個 Dashboard 需要的關鍵圖表數據：
1. **各商品分類的總銷售額與訂單數**。
2. **每日的網站事件統計 (Page views vs Add to cart)**。"""),
    
    nbf.v4.new_code_cell("""# 報表 1: 銷售表現 (Sales Performance)
sales_gold_df = orders_df \\
    .filter(col("status") == "Completed") \\
    .groupBy("product_category") \\
    .agg(
        sum("amount").alias("total_revenue"),
        count("order_id").alias("total_orders")
    ) \\
    .orderBy(desc("total_revenue"))

print("--- Gold Layer: Sales Performance ---")
sales_gold_df.show()

# 報表 2: 網站行為漏斗 (Behavior Funnel by Date)
funnel_gold_df = clean_clicks_df \\
    .withColumn("event_date", to_date(col("timestamp"))) \\
    .groupBy("event_date", "event_type") \\
    .agg(count("event_id").alias("event_count")) \\
    .orderBy("event_date", "event_type")

print("--- Gold Layer: Daily Events Funnel ---")
funnel_gold_df.show(5)"""),
    
    nbf.v4.new_markdown_cell("""### 6. 寫回 PostgreSQL 供 Streamlit 讀取 (Load)
計算完畢後，我們要把它存回 PostgreSQL 裡面 (可以建立一個新的 Schema 或加上後綴標記為 Gold Tables)，這樣 Dashboard 就可以用極快的速度呈現。"""),
    
    nbf.v4.new_code_cell("""# 將結果寫回 PostgreSQL
sales_gold_df.write \\
    .jdbc(url=pg_url, table="gold_sales_performance", mode="overwrite", properties=pg_properties)

funnel_gold_df.write \\
    .jdbc(url=pg_url, table="gold_daily_events", mode="overwrite", properties=pg_properties)

print("ETL Pipeline Completed! Data saved to 'gold_sales_performance' and 'gold_daily_events' tables in PostgreSQL.")""")
]

nb['cells'] = cells

with open('C:\\Users\\Albert Yang\\Documents\\PySpark\\lakehouse_demo\\ETL_Job.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb, f)
