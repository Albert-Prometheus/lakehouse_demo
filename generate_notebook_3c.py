import nbformat as nbf

nb = nbf.v4.new_notebook()

cells = [
    nbf.v4.new_markdown_cell("# 3C E-commerce PySpark ETL (Silver -> Gold)"),
    nbf.v4.new_code_cell("""from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \\
    .appName("3C_Lakehouse_ETL") \\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.6.0") \\
    .config("spark.mongodb.input.uri", "mongodb://mongo:admin@mongodb:27017/ecommerce_lake.clickstream?authSource=admin") \\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Spark Session Initialized!")"""),

    nbf.v4.new_markdown_cell("### Extract: Bronze Layer"),
    nbf.v4.new_code_cell("""pg_url = "jdbc:postgresql://pgvector:5432/postgres"
pg_props = {"user": "postgres", "password": "admin", "driver": "org.postgresql.Driver"}

users_df = spark.read.jdbc(url=pg_url, table="users", properties=pg_props)
products_df = spark.read.jdbc(url=pg_url, table="products", properties=pg_props)
orders_df = spark.read.jdbc(url=pg_url, table="orders", properties=pg_props)
clicks_df = spark.read.format("mongo").load()

print("Bronze data loaded.")"""),

    nbf.v4.new_markdown_cell("### Transform: Silver & Gold Layers"),
    nbf.v4.new_code_cell("""# --- 1. Finance & Inventory (Gold) ---
# Join orders with products to calculate Revenue and Cost
completed_orders = orders_df.filter(col("status") == "Completed")
sales_with_products = completed_orders.join(products_df, "product_id") \\
    .withColumn("order_revenue", col("quantity") * col("selling_price")) \\
    .withColumn("order_cost", col("quantity") * col("cost_price"))

# Finance Table (Daily)
gold_finance = sales_with_products \\
    .withColumn("order_date_only", to_date("order_date")) \\
    .groupBy("order_date_only") \\
    .agg(
        sum("order_revenue").alias("daily_revenue"),
        sum("order_cost").alias("daily_cost"),
        (sum("order_revenue") - sum("order_cost")).alias("daily_gross_profit"),
        count("order_id").alias("daily_orders")
    ).orderBy("order_date_only")

# Inventory / Product Performance Table
gold_inventory = sales_with_products \\
    .groupBy("product_id", "name", "category", "stock_quantity") \\
    .agg(
        sum("quantity").alias("total_sold_quantity"),
        sum("order_revenue").alias("total_revenue")
    ) \\
    .withColumn("inventory_status", 
                when(col("stock_quantity") < 50, "Low Stock")
                .when(col("stock_quantity") < 150, "Medium")
                .otherwise("Healthy")) \\
    .orderBy(desc("total_revenue"))

# --- 2. Member & CRM (Gold) ---
# Member Lifetime Value and Segmentation
gold_members = sales_with_products.join(users_df, "user_id") \\
    .groupBy("member_level") \\
    .agg(
        countDistinct("user_id").alias("total_customers"),
        sum("order_revenue").alias("total_spent"),
        count("order_id").alias("total_orders")
    ) \\
    .withColumn("avg_spent_per_user", col("total_spent") / col("total_customers"))

# --- 3. Clickstream Funnel (Gold) ---
gold_funnel = clicks_df \\
    .withColumn("event_date", to_date(col("timestamp"))) \\
    .groupBy("event_date", "event_type") \\
    .agg(count("event_id").alias("event_count")) \\
    .orderBy("event_date", "event_type")

print("Gold Tables calculated.")"""),

    nbf.v4.new_markdown_cell("### Load: Write to PostgreSQL"),
    nbf.v4.new_code_cell("""gold_finance.write.jdbc(url=pg_url, table="gold_finance", mode="overwrite", properties=pg_props)
gold_inventory.write.jdbc(url=pg_url, table="gold_inventory", mode="overwrite", properties=pg_props)
gold_members.write.jdbc(url=pg_url, table="gold_members", mode="overwrite", properties=pg_props)
gold_funnel.write.jdbc(url=pg_url, table="gold_funnel", mode="overwrite", properties=pg_props)
print("3C E-Commerce ETL Pipeline Completed!")""")
]

nb['cells'] = cells
with open('C:\\Users\\Albert Yang\\Documents\\PySpark\\lakehouse_demo\\ETL_Job_3C.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb, f)
