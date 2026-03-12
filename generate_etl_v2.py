import nbformat as nbf

nb = nbf.v4.new_notebook()

cells = [
    nbf.v4.new_markdown_cell("# 🏭 3C E-Commerce Advanced ETL Pipeline\n完整的企業級 ETL：財務損益、進銷存、會員 RFM、同期群、退貨分析、供應商績效、銷售預測、流量歸因。"),

    # ---- Cell 1: Spark Session ----
    nbf.v4.new_code_cell("""from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.fpm import FPGrowth

spark = SparkSession.builder \\
    .appName("3C_Advanced_ETL_v2") \\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.6.0") \\
    .config("spark.mongodb.input.uri", "mongodb://mongo:admin@mongodb:27017/ecommerce_lake.clickstream?authSource=admin") \\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

pg_url = "jdbc:postgresql://pgvector:5432/postgres"
pg_props = {"user": "postgres", "password": "admin", "driver": "org.postgresql.Driver"}
print("Spark Session initialized.")"""),

    # ---- Cell 2: Load Bronze ----
    nbf.v4.new_code_cell("""users = spark.read.jdbc(url=pg_url, table="users", properties=pg_props)
products = spark.read.jdbc(url=pg_url, table="products", properties=pg_props)
orders = spark.read.jdbc(url=pg_url, table="orders", properties=pg_props)
order_items = spark.read.jdbc(url=pg_url, table="order_items", properties=pg_props)
returns = spark.read.jdbc(url=pg_url, table="returns", properties=pg_props)
suppliers = spark.read.jdbc(url=pg_url, table="suppliers", properties=pg_props)
purchase_orders = spark.read.jdbc(url=pg_url, table="purchase_orders", properties=pg_props)
clicks = spark.read.format("mongo").load()

# Read support tickets - must use .option() to override collection
tickets = spark.read.format("mongo") \\
    .option("uri", "mongodb://mongo:admin@mongodb:27017/ecommerce_lake.support_tickets?authSource=admin") \\
    .load()
print("All Bronze data loaded.")"""),

    # ---- Cell 3: Silver - Enriched Sales ----
    nbf.v4.new_code_cell("""# Silver: Enriched order-level sales fact table
sales = order_items.join(orders, "order_id") \\
    .join(products, "product_id") \\
    .filter(col("status") == "Completed") \\
    .withColumn("line_revenue", col("quantity") * col("unit_price")) \\
    .withColumn("line_cost", col("quantity") * col("cost_price")) \\
    .withColumn("line_profit", col("line_revenue") - col("line_cost")) \\
    .withColumn("order_month", date_format("order_date", "yyyy-MM")) \\
    .withColumn("order_week", date_format("order_date", "yyyy-'W'ww"))
print("Silver enriched sales ready. Rows:", sales.count())"""),

    # ---- Cell 4: Gold 1 - Finance P&L ----
    nbf.v4.new_markdown_cell("### Gold 1: 財務損益表 (P&L by Month)"),
    nbf.v4.new_code_cell("""# Monthly P&L with shipping and discounts
order_level = sales.groupBy("order_id", "order_month", "order_date") \\
    .agg(
        sum("line_revenue").alias("order_revenue"),
        sum("line_cost").alias("order_cost"),
        first("shipping_fee").alias("shipping_fee"),
        first("discount_amount").alias("discount_amount")
    )

# Join with returns for refund deductions
monthly_refunds = returns.withColumn("return_month", date_format("return_date", "yyyy-MM")) \\
    .groupBy("return_month").agg(sum("refund_amount").alias("total_refunds"))

gold_finance = order_level.groupBy("order_month") \\
    .agg(
        sum("order_revenue").alias("gross_revenue"),
        sum("order_cost").alias("total_cogs"),
        sum("shipping_fee").alias("total_shipping"),
        sum("discount_amount").alias("total_discounts"),
        count("order_id").alias("total_orders")
    ) \\
    .join(monthly_refunds, order_level["order_month"] == monthly_refunds["return_month"], "left") \\
    .fillna(0, subset=["total_refunds"]) \\
    .withColumn("net_revenue", col("gross_revenue") - col("total_discounts") - col("total_refunds")) \\
    .withColumn("gross_profit", col("net_revenue") - col("total_cogs")) \\
    .withColumn("gross_margin_pct", round(col("gross_profit") / col("net_revenue") * 100, 1)) \\
    .select("order_month", "gross_revenue", "total_cogs", "total_discounts", "total_refunds",
            "total_shipping", "net_revenue", "gross_profit", "gross_margin_pct", "total_orders") \\
    .orderBy("order_month")

print("Gold Finance P&L ready.")
gold_finance.show(5)"""),

    # ---- Cell 5: Gold 2 - Category Performance ----
    nbf.v4.new_markdown_cell("### Gold 2: 品類利潤率分析"),
    nbf.v4.new_code_cell("""gold_category = sales.groupBy("category") \\
    .agg(
        sum("line_revenue").alias("total_revenue"),
        sum("line_cost").alias("total_cost"),
        sum("line_profit").alias("total_profit"),
        sum("quantity").alias("total_units_sold"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("product_id").alias("product_count")
    ) \\
    .withColumn("profit_margin_pct", round(col("total_profit") / col("total_revenue") * 100, 1)) \\
    .withColumn("avg_order_value", round(col("total_revenue") / col("total_orders"), 2)) \\
    .orderBy(desc("total_revenue"))

print("Gold Category Performance ready.")
gold_category.show()"""),

    # ---- Cell 6: Gold 3 - Inventory Health ----
    nbf.v4.new_markdown_cell("### Gold 3: 進銷存與庫存健康度"),
    nbf.v4.new_code_cell("""# Calculate total sold per product
sold_qty = sales.groupBy("product_id").agg(sum("quantity").alias("total_sold"))

gold_inventory = products.join(sold_qty, "product_id", "left") \\
    .join(suppliers.select("supplier_id", col("name").alias("supplier_name")), "supplier_id", "left") \\
    .fillna(0, subset=["total_sold"]) \\
    .withColumn("days_of_stock", 
        when(col("total_sold") > 0, round(col("stock_quantity") / (col("total_sold") / 365), 0))
        .otherwise(lit(9999))) \\
    .withColumn("inventory_status",
        when(col("stock_quantity") <= col("reorder_point"), "Critical - Reorder Now")
        .when(col("stock_quantity") <= col("reorder_point") * 2, "Low Stock")
        .when(col("days_of_stock") > 365, "Overstock")
        .otherwise("Healthy")) \\
    .withColumn("inventory_value", col("stock_quantity") * col("cost_price")) \\
    .select("product_id", "sku", "brand", "name", "category", "cost_price", "selling_price",
            "stock_quantity", "reorder_point", "total_sold", "days_of_stock",
            "inventory_status", "inventory_value", "supplier_name") \\
    .orderBy("days_of_stock")

print("Gold Inventory ready.")"""),

    # ---- Cell 7: Gold 4 - RFM + KMeans ----
    nbf.v4.new_markdown_cell("### Gold 4: RFM K-Means 客戶分群"),
    nbf.v4.new_code_cell("""current_date = sales.agg(max("order_date")).collect()[0][0]

rfm = sales.groupBy("user_id").agg(
    datediff(lit(current_date), max("order_date")).alias("recency"),
    countDistinct("order_id").alias("frequency"),
    sum("line_revenue").alias("monetary")
)

assembler = VectorAssembler(inputCols=["recency", "frequency", "monetary"], outputCol="features")
rfm_vec = assembler.transform(rfm)
scaler = StandardScaler(inputCol="features", outputCol="scaled", withStd=True, withMean=True)
rfm_scaled = scaler.fit(rfm_vec).transform(rfm_vec)

kmeans = KMeans(featuresCol="scaled", k=5, seed=42)
rfm_pred = kmeans.fit(rfm_scaled).transform(rfm_scaled)

# Get cluster stats to assign meaningful labels
cluster_stats = rfm_pred.groupBy("prediction").agg(
    avg("recency").alias("avg_r"), avg("frequency").alias("avg_f"), avg("monetary").alias("avg_m")
).orderBy("avg_m")
cluster_stats.show()

# Join with user info
gold_rfm = rfm_pred.select("user_id", "recency", "frequency", "monetary", "prediction") \\
    .join(users.select("user_id", "name", "email", "city", "member_level", "signup_date"), "user_id") \\
    .withColumn("customer_segment",
        when(col("prediction") == 0, "Hibernating")
        .when(col("prediction") == 1, "At Risk")
        .when(col("prediction") == 2, "Loyal")
        .when(col("prediction") == 3, "Champions")
        .otherwise("New / Potential")
    )

print("Gold RFM ready.")"""),

    # ---- Cell 8: Gold 5 - Cohort Analysis ----
    nbf.v4.new_markdown_cell("### Gold 5: 同期群分析 (Cohort Retention)"),
    nbf.v4.new_code_cell("""# Cohort = month of first purchase
first_purchase = sales.groupBy("user_id").agg(
    min(date_format("order_date", "yyyy-MM")).alias("cohort_month")
)

cohort_sales = sales.join(first_purchase, "user_id") \\
    .withColumn("order_month_str", date_format("order_date", "yyyy-MM")) \\
    .withColumn("months_since_first",
        months_between(to_date(col("order_month_str"), "yyyy-MM"), to_date(col("cohort_month"), "yyyy-MM")).cast("int"))

gold_cohort = cohort_sales.groupBy("cohort_month", "months_since_first") \\
    .agg(countDistinct("user_id").alias("active_users")) \\
    .orderBy("cohort_month", "months_since_first")

print("Gold Cohort ready.")
gold_cohort.show(10)"""),

    # ---- Cell 9: Gold 6 - Returns Analysis ----
    nbf.v4.new_markdown_cell("### Gold 6: 退貨退款分析"),
    nbf.v4.new_code_cell("""gold_returns = returns.join(products.select("product_id", "brand", "name", "category"), "product_id") \\
    .withColumn("return_month", date_format("return_date", "yyyy-MM")) \\
    .groupBy("category", "brand", "reason") \\
    .agg(
        count("return_id").alias("return_count"),
        sum("refund_amount").alias("total_refund")
    ) \\
    .orderBy(desc("return_count"))

# Return rate by category
total_sold = sales.groupBy("category").agg(countDistinct("order_id").alias("sold_orders"))
returned = returns.join(products.select("product_id", "category"), "product_id") \\
    .groupBy("category").agg(count("return_id").alias("return_orders"))

gold_return_rate = total_sold.join(returned, "category", "left") \\
    .fillna(0, subset=["return_orders"]) \\
    .withColumn("return_rate_pct", round(col("return_orders") / col("sold_orders") * 100, 2)) \\
    .orderBy(desc("return_rate_pct"))

print("Gold Returns ready.")
gold_return_rate.show()"""),

    # ---- Cell 10: Gold 7 - Supplier Scorecard ----
    nbf.v4.new_markdown_cell("### Gold 7: 供應商績效記分卡"),
    nbf.v4.new_code_cell("""# Delivery performance
po_received = purchase_orders.filter(col("status") == "Received")
po_perf = po_received.withColumn("days_late", datediff("received_date", "expected_date")) \\
    .withColumn("is_on_time", when(col("days_late") <= 0, 1).otherwise(0))

gold_supplier = po_perf.join(suppliers, "supplier_id") \\
    .groupBy("supplier_id", suppliers["name"].alias("supplier_name"), "city", "rating") \\
    .agg(
        count("po_id").alias("total_pos"),
        sum("is_on_time").alias("on_time_count"),
        round(avg("days_late"), 1).alias("avg_days_late"),
        sum(col("quantity") * col("unit_cost")).alias("total_procurement_value")
    ) \\
    .withColumn("on_time_pct", round(col("on_time_count") / col("total_pos") * 100, 1)) \\
    .orderBy(desc("total_procurement_value"))

print("Gold Supplier Scorecard ready.")
gold_supplier.show()"""),

    # ---- Cell 11: Gold 8 - Sales Trend (for forecasting) ----
    nbf.v4.new_markdown_cell("### Gold 8: 每週銷售趨勢 (供預測用)"),
    nbf.v4.new_code_cell("""gold_weekly_sales = sales \\
    .withColumn("week_start", date_trunc("week", col("order_date"))) \\
    .groupBy("week_start") \\
    .agg(
        sum("line_revenue").alias("weekly_revenue"),
        sum("line_profit").alias("weekly_profit"),
        sum("quantity").alias("weekly_units"),
        countDistinct("order_id").alias("weekly_orders"),
        countDistinct("user_id").alias("weekly_active_users")
    ) \\
    .orderBy("week_start")

print("Gold Weekly Sales ready.")
gold_weekly_sales.show(5)"""),

    # ---- Cell 12: Gold 9 - Clickstream Funnel ----
    nbf.v4.new_markdown_cell("### Gold 9: 流量分析與轉換漏斗"),
    nbf.v4.new_code_cell("""gold_funnel = clicks \\
    .withColumn("event_date", to_date(col("timestamp"))) \\
    .groupBy("event_date", "event_type") \\
    .agg(count("event_id").alias("event_count")) \\
    .orderBy("event_date")

# Referrer / Channel Attribution
gold_channel = clicks \\
    .groupBy("referrer") \\
    .agg(
        count("event_id").alias("total_events"),
        countDistinct("user_id").alias("unique_users")
    ) \\
    .orderBy(desc("total_events"))

# Device breakdown
gold_device = clicks \\
    .groupBy("device", "os") \\
    .agg(count("event_id").alias("event_count")) \\
    .orderBy(desc("event_count"))

print("Gold Funnel / Channel / Device ready.")"""),

    # ---- Cell 13: Gold 10 - Support Tickets ----
    nbf.v4.new_markdown_cell("### Gold 10: 客服工單分析"),
    nbf.v4.new_code_cell("""gold_support = tickets \\
    .groupBy("type", "priority") \\
    .agg(
        count("ticket_id").alias("ticket_count"),
        avg("satisfaction_score").alias("avg_satisfaction")
    ) \\
    .orderBy(desc("ticket_count"))

print("Gold Support ready.")
gold_support.show()"""),

    # ---- Cell 14: Write All Gold Tables ----
    nbf.v4.new_markdown_cell("### 寫入所有 Gold Tables 到 PostgreSQL"),
    nbf.v4.new_code_cell("""tables = {
    "gold_finance_pl": gold_finance,
    "gold_category": gold_category,
    "gold_inventory_v2": gold_inventory,
    "gold_rfm_v2": gold_rfm,
    "gold_cohort": gold_cohort,
    "gold_returns": gold_returns,
    "gold_return_rate": gold_return_rate,
    "gold_supplier": gold_supplier,
    "gold_weekly_sales": gold_weekly_sales,
    "gold_funnel_v2": gold_funnel,
    "gold_channel": gold_channel,
    "gold_device": gold_device,
    "gold_support": gold_support,
}

for name, df in tables.items():
    df.write.jdbc(url=pg_url, table=name, mode="overwrite", properties=pg_props)
    print(f"  [OK] {name} written.")

print("\\n=== All 13 Gold Tables Written to PostgreSQL! ===")""")
]

nb['cells'] = cells
with open('C:\\Users\\Albert Yang\\Documents\\PySpark\\lakehouse_demo\\ETL_Full_v2.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb, f)

print("ETL_Full_v2.ipynb created.")
