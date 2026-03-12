import nbformat as nbf

nb = nbf.v4.new_notebook()

cells = [
    nbf.v4.new_markdown_cell("# 🔬 Advanced Analytics ETL v3\nABC Pareto 分類、品牌績效、折扣 ROI、時間熱力圖、付款方式、地區分析、CLV 預測"),

    nbf.v4.new_code_cell("""from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \\
    .appName("3C_ETL_v3_extras") \\
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
    .withColumn("line_profit", col("line_revenue") - col("line_cost"))

print("Data loaded. Sales rows:", sales.count())"""),

    # ---- ABC Pareto Analysis ----
    nbf.v4.new_markdown_cell("### 1. ABC Pareto 分類 (80/20 法則)"),
    nbf.v4.new_code_cell("""product_rev = sales.groupBy("product_id", "brand", "name", "category") \\
    .agg(sum("line_revenue").alias("total_revenue"), sum("quantity").alias("total_units"))

total = product_rev.agg(sum("total_revenue")).collect()[0][0]

w = Window.orderBy(desc("total_revenue"))
gold_abc = product_rev \\
    .withColumn("revenue_rank", row_number().over(w)) \\
    .withColumn("cumulative_revenue", sum("total_revenue").over(w.rowsBetween(Window.unboundedPreceding, 0))) \\
    .withColumn("cumulative_pct", round(col("cumulative_revenue") / lit(total) * 100, 2)) \\
    .withColumn("abc_class",
        when(col("cumulative_pct") <= 80, "A")
        .when(col("cumulative_pct") <= 95, "B")
        .otherwise("C")) \\
    .withColumn("revenue_pct", round(col("total_revenue") / lit(total) * 100, 2))

print("ABC Analysis ready. Class distribution:")
gold_abc.groupBy("abc_class").agg(count("*").alias("products"), sum("total_revenue").alias("rev")).show()"""),

    # ---- Brand Performance ----
    nbf.v4.new_markdown_cell("### 2. 品牌績效排行"),
    nbf.v4.new_code_cell("""brand_returns = returns.join(products.select("product_id", "brand"), "product_id") \\
    .groupBy("brand").agg(count("return_id").alias("return_count"), sum("refund_amount").alias("refund_total"))

brand_sold = sales.groupBy("brand").agg(countDistinct("order_id").alias("sold_orders"))

gold_brand = sales.groupBy("brand") \\
    .agg(
        sum("line_revenue").alias("total_revenue"),
        sum("line_profit").alias("total_profit"),
        sum("quantity").alias("total_units"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("user_id").alias("unique_customers")
    ) \\
    .join(brand_returns, "brand", "left") \\
    .join(brand_sold, "brand", "left") \\
    .fillna(0) \\
    .withColumn("profit_margin_pct", round(col("total_profit") / col("total_revenue") * 100, 1)) \\
    .withColumn("return_rate_pct", round(col("return_count") / col("sold_orders") * 100, 1)) \\
    .withColumn("avg_revenue_per_customer", round(col("total_revenue") / col("unique_customers"), 2)) \\
    .orderBy(desc("total_revenue"))

print("Brand Performance ready.")
gold_brand.show(5)"""),

    # ---- Discount ROI ----
    nbf.v4.new_markdown_cell("### 3. 折扣效果 ROI 分析"),
    nbf.v4.new_code_cell("""order_totals = sales.groupBy("order_id", "discount_amount") \\
    .agg(sum("line_revenue").alias("order_revenue"), sum("line_profit").alias("order_profit"),
         sum("quantity").alias("order_items"))

gold_discount = order_totals \\
    .withColumn("discount_band",
        when(col("discount_amount") == 0, "No Discount")
        .when(col("discount_amount") <= 50, "$1-50")
        .when(col("discount_amount") <= 100, "$51-100")
        .when(col("discount_amount") <= 150, "$101-150")
        .otherwise("$150+")) \\
    .groupBy("discount_band") \\
    .agg(
        count("order_id").alias("order_count"),
        round(avg("order_revenue"), 2).alias("avg_order_value"),
        round(avg("order_items"), 1).alias("avg_items_per_order"),
        round(sum("order_revenue"), 2).alias("total_revenue"),
        round(sum("order_profit"), 2).alias("total_profit"),
        round(sum("discount_amount"), 2).alias("total_discount_given")
    ) \\
    .withColumn("discount_roi", round(col("total_revenue") / greatest(col("total_discount_given"), lit(1)), 2))

print("Discount ROI ready.")
gold_discount.show()"""),

    # ---- Hourly Heatmap ----
    nbf.v4.new_markdown_cell("### 4. 時間模式熱力圖 (星期 x 時段)"),
    nbf.v4.new_code_cell("""gold_hourly = sales \\
    .withColumn("hour_of_day", hour("order_date")) \\
    .withColumn("day_of_week", date_format("order_date", "EEEE")) \\
    .groupBy("day_of_week", "hour_of_day") \\
    .agg(
        count("order_id").alias("order_count"),
        sum("line_revenue").alias("total_revenue")
    ) \\
    .orderBy("day_of_week", "hour_of_day")

print("Hourly Heatmap ready.")"""),

    # ---- Payment Analysis ----
    nbf.v4.new_markdown_cell("### 5. 付款方式分析"),
    nbf.v4.new_code_cell("""gold_payment = sales.groupBy("payment_method") \\
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("line_revenue").alias("total_revenue"),
        round(avg("line_revenue"), 2).alias("avg_line_value"),
        countDistinct("user_id").alias("unique_users")
    ) \\
    .withColumn("revenue_share_pct",
        round(col("total_revenue") / sum("total_revenue").over(Window.partitionBy()), 2) * 100) \\
    .orderBy(desc("total_revenue"))

print("Payment Analysis ready.")
gold_payment.show()"""),

    # ---- City / Region Analysis ----
    nbf.v4.new_markdown_cell("### 6. 地區銷售分析"),
    nbf.v4.new_code_cell("""gold_city = sales.join(users.select("user_id", "city"), "user_id") \\
    .groupBy("city") \\
    .agg(
        countDistinct("user_id").alias("total_customers"),
        countDistinct("order_id").alias("total_orders"),
        sum("line_revenue").alias("total_revenue"),
        sum("line_profit").alias("total_profit"),
        round(avg("line_revenue"), 2).alias("avg_item_value")
    ) \\
    .withColumn("revenue_per_customer", round(col("total_revenue") / col("total_customers"), 2)) \\
    .orderBy(desc("total_revenue"))

print("City Analysis ready.")
gold_city.show()"""),

    # ---- CLV Prediction ----
    nbf.v4.new_markdown_cell("### 7. 客戶生命週期價值 (CLV) 預測"),
    nbf.v4.new_code_cell("""current_date_val = sales.agg(max("order_date")).collect()[0][0]

gold_clv = sales.join(users.select("user_id", col("name").alias("user_name"), "city", "member_level", "signup_date"), "user_id") \\
    .groupBy("user_id", "user_name", "city", "member_level", "signup_date") \\
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("line_revenue").alias("total_revenue"),
        sum("line_profit").alias("total_profit"),
        min("order_date").alias("first_purchase"),
        max("order_date").alias("last_purchase"),
        avg("line_revenue").alias("avg_item_value")
    ) \\
    .withColumn("customer_age_days", datediff(lit(current_date_val), col("signup_date"))) \\
    .withColumn("active_days", datediff(col("last_purchase"), col("first_purchase"))) \\
    .withColumn("purchase_frequency", round(col("total_orders") / greatest(col("customer_age_days") / 30, lit(1)), 2)) \\
    .withColumn("projected_annual_value",
        round(col("purchase_frequency") * 12 * (col("total_revenue") / greatest(col("total_orders"), lit(1))), 2)) \\
    .withColumn("clv_score",
        round(col("projected_annual_value") * least(col("active_days") / 365 + 1, lit(3)), 2)) \\
    .withColumn("clv_tier",
        when(col("clv_score") >= 50000, "Platinum")
        .when(col("clv_score") >= 20000, "Gold")
        .when(col("clv_score") >= 5000, "Silver")
        .otherwise("Bronze")) \\
    .orderBy(desc("clv_score"))

print("CLV Prediction ready.")
gold_clv.show(5)"""),

    # ---- Write All ----
    nbf.v4.new_markdown_cell("### 寫入所有新 Gold Tables"),
    nbf.v4.new_code_cell("""tables = {
    "gold_abc": gold_abc, "gold_brand": gold_brand, "gold_discount": gold_discount,
    "gold_hourly": gold_hourly, "gold_payment": gold_payment, "gold_city": gold_city, "gold_clv": gold_clv,
}
for name, df in tables.items():
    df.write.jdbc(url=pg_url, table=name, mode="overwrite", properties=pg_props)
    print(f"  [OK] {name}")
print("\\n=== ETL v3 Complete! 7 new Gold Tables written. ===")""")
]

nb['cells'] = cells
with open('C:\\Users\\Albert Yang\\Documents\\PySpark\\lakehouse_demo\\ETL_v3_extras.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb, f)
print("ETL_v3_extras.ipynb created.")
