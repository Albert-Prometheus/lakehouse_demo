import nbformat as nbf

nb = nbf.v4.new_notebook()

cells = [
    nbf.v4.new_markdown_cell("# 🚀 ETL v4: 超進階企業分析\n流失預警、價格敏感度、產品親和力矩陣、客戶旅程漏斗、NPS 淨推薦值"),

    nbf.v4.new_code_cell("""from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \\
    .appName("3C_ETL_v4_Advanced") \\
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

pg_url = "jdbc:postgresql://pgvector:5432/postgres"
pg_props = {"user": "postgres", "password": "admin", "driver": "org.postgresql.Driver"}

users = spark.read.jdbc(url=pg_url, table="users", properties=pg_props)
products = spark.read.jdbc(url=pg_url, table="products", properties=pg_props)
orders = spark.read.jdbc(url=pg_url, table="orders", properties=pg_props)
order_items = spark.read.jdbc(url=pg_url, table="order_items", properties=pg_props)
returns = spark.read.jdbc(url=pg_url, table="returns", properties=pg_props)

sales = order_items.join(orders, "order_id") \\
    .join(products, "product_id") \\
    .filter(col("status") == "Completed") \\
    .withColumn("line_revenue", col("quantity") * col("unit_price")) \\
    .withColumn("line_cost", col("quantity") * col("cost_price")) \\
    .withColumn("line_profit", col("line_revenue") - col("line_cost")) \\
    .withColumn("order_month", date_format("order_date", "yyyy-MM"))

print("Data loaded.")"""),

    # ---- Churn Prediction ----
    nbf.v4.new_markdown_cell("### 1. 流失預警 (Churn Prediction)"),
    nbf.v4.new_code_cell("""current_date = sales.agg(max("order_date")).collect()[0][0]
days_threshold = 60  # 60天沒買視為流失

# 計算每個客戶的最後購買日
last_purchase = sales.groupBy("user_id") \\
    .agg(max("order_date").alias("last_order_date"))

gold_churn = users.join(last_purchase, "user_id", "left") \\
    .withColumn("days_since_last", datediff(lit(current_date), col("last_order_date"))) \\
    .withColumn("churn_risk",
        when(col("days_since_last").isNull(), "New - No Purchase")
        .when(col("days_since_last") > days_threshold * 2, "Churned")
        .when(col("days_since_last") > days_threshold, "At Risk")
        .when(col("days_since_last") > 30, "Dormant")
        .otherwise("Active")) \\
    .fillna(days_threshold * 3, subset=["days_since_last"])

churn_summary = gold_churn.groupBy("churn_risk").agg(
    count("user_id").alias("user_count"),
    round(avg("days_since_last"), 1).alias("avg_days_inactive")
).orderBy(desc("user_count"))

print("Churn Prediction ready.")
churn_summary.show()"""),

    # ---- Price Sensitivity ----
    nbf.v4.new_markdown_cell("### 2. 價格敏感度分析"),
    nbf.v4.new_code_cell("""# 計算每個客戶的平均訂單金額與購買頻率
customer_value = sales.groupBy("user_id").agg(
    avg("line_revenue").alias("avg_order_value"),
    count("order_id").alias("total_orders"),
    sum("quantity").alias("total_items")
)

# 找出高頻低價 vs 低頻高價客戶
gold_price_sensitivity = customer_value.withColumn("sensitivity_segment",
    when((col("avg_order_value") < 1000) & (col("total_orders") > 5), "Price Sensitive - Frequent")
    .when((col("avg_order_value") > 3000) & (col("total_orders") <= 3), "Quality Focused - Occasional")
    .when((col("avg_order_value") > 5000) & (col("total_orders") > 5), "Premium Loyal")
    .otherwise("Average Shopper"))

seg_stats = gold_price_sensitivity.groupBy("sensitivity_segment").agg(
    count("user_id").alias("count"),
    round(avg("avg_order_value"), 2).alias("avg_order_value"),
    round(avg("total_orders"), 2).alias("avg_orders")
)

print("Price Sensitivity ready.")
seg_stats.show()"""),

    # ---- Customer Journey Funnel ----
    nbf.v4.new_markdown_cell("### 3. 客戶旅程漏斗 (Journey Funnel)"),
    nbf.v4.new_code_cell("""# 簡化的客戶旅程漏斗數據
journey_data = [
    ("Visit (瀏覽)", 8000), ("View Product (查看商品)", 4200), 
    ("Add to Cart (加入購物車)", 1800), ("Checkout (開始結帳)", 950), ("Purchase (購買完成)", 680)
]
journey_df = spark.createDataFrame(journey_data, ["stage", "count"])

# 計算轉化率 (手動計算)
journey_pdf = journey_df.toPandas()
journey_pdf['conversion_rate'] = (journey_pdf['count'] / journey_pdf['count'].shift(-1) * 100).round(1)
journey_pdf['dropoff_rate'] = (100 - journey_pdf['conversion_rate']).round(1)
gold_journey = spark.createDataFrame(journey_pdf)

print("Customer Journey ready.")
gold_journey.show()"""),

    # ---- Product Affinity Matrix ----
    nbf.v4.new_markdown_cell("### 4. 產品親和力矩陣 (共現矩陣)"),
    nbf.v4.new_code_cell("""# 找出同一次訂單中同時出現的產品組合
order_products = sales.groupBy("order_id").agg(collect_set("category").alias("categories"))

# 只取多品類訂單
multi_cat = order_products.filter(size(col("categories")) > 1)

# 計算共現次數
from pyspark.sql.functions import explode
pairs = multi_cat.select(explode(col("categories")).alias("cat1"), "order_id") \\
    .join(multi_cat.select(explode(col("categories")).alias("cat2"), "order_id"), "order_id") \\
    .filter(col("cat1") < col("cat2"))

gold_affinity = pairs.groupBy("cat1", "cat2").agg(
    count("order_id").alias("co_occurrence_count")
).withColumn("affinity_score", 
    round(col("co_occurrence_count") / lit(multi_cat.count()) * 100, 2)) \\
    .orderBy(desc("affinity_score"))

print("Product Affinity ready.")
gold_affinity.show(10)"""),

    # ---- NPS (Net Promoter Score) ----
    nbf.v4.new_markdown_cell("### 5. NPS 淨推薦值分析"),
    nbf.v4.new_code_cell("""# 根據 purchase frequency 推估 NPS 等級 (9-10 = Promoter, 7-8 = Passive, 0-6 = Detractor)
# 實際應用中應該讀取客服滿意度數據

nps_data = sales.groupBy("user_id").agg(
    count("order_id").alias("order_count"),
    sum("line_revenue").alias("total_spent")
).withColumn("nps_segment",
    when((col("order_count") >= 8) & (col("total_spent") > 20000), "Promoter (9-10)")
    .when((col("order_count") >= 4) & (col("total_spent") > 8000), "Passive (7-8)")
    .otherwise("Detractor (0-6)"))

nps_summary = nps_data.groupBy("nps_segment").agg(
    count("user_id").alias("user_count"),
    sum("total_spent").alias("total_revenue")
).withColumn("nps_contribution", 
    round(col("total_revenue") / sum("total_revenue").over(Window.partitionBy()) * 100, 1))

print("NPS Analysis ready.")
nps_summary.show()"""),

    # ---- Seasonality ----
    nbf.v4.new_markdown_cell("### 6. 季節性分析 (週期性)"),
    nbf.v4.new_code_cell("""# 計算每週、每月、每週天的銷售模式
seasonality_weekly = sales.withColumn("weekday", date_format("order_date", "EEEE")) \\
    .groupBy("weekday").agg(sum("line_revenue").alias("total_revenue")) \\
    .withColumn("weekday_order", 
        when(col("weekday")=="Monday",1).when(col("weekday")=="Tuesday",2).when(col("weekday")=="Wednesday",3)
        .when(col("weekday")=="Thursday",4).when(col("weekday")=="Friday",5).when(col("weekday")=="Saturday",6).otherwise(7)) \\
    .orderBy("weekday_order")

seasonality_hour = sales.withColumn("hour", hour("order_date")) \\
    .groupBy("hour").agg(sum("line_revenue").alias("total_revenue")).orderBy("hour")

print("Seasonality ready.")
seasonality_weekly.show()"""),

    # ---- Write All ----
    nbf.v4.new_markdown_cell("### 寫入所有 v4 Gold Tables"),
    nbf.v4.new_code_cell("""tables = {
    "gold_churn": gold_churn.select("user_id", "name", "email", "city", "member_level", "last_order_date", "days_since_last", "churn_risk"),
    "gold_price_sensitivity": gold_price_sensitivity,
    "gold_journey": gold_journey,
    "gold_affinity": gold_affinity,
    "gold_nps": nps_summary,
    "gold_seasonality_weekly": seasonality_weekly,
    "gold_seasonality_hour": seasonality_hour,
}
for name, df in tables.items():
    df.write.jdbc(url=pg_url, table=name, mode="overwrite", properties=pg_props)
    print(f"  [OK] {name}")
print("\\n=== ETL v4 Complete! 7 new advanced Gold Tables written. ===")""")
]

nb['cells'] = cells
with open('C:\\Users\\Albert Yang\\Documents\\PySpark\\lakehouse_demo\\ETL_v4_advanced.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb, f)
print("ETL_v4_advanced.ipynb created.")
