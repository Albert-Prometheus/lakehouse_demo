import nbformat as nbf

nb = nbf.v4.new_notebook()

cells = [
    nbf.v4.new_markdown_cell("# 🚀 Advanced PySpark ML: E-Commerce AI Analytics\n這份 Notebook 將展示如何使用 PySpark MLlib 進行高階商業分析：RFM 客戶分群 (K-Means) 與 購物籃關聯分析 (FP-Growth)。"),
    nbf.v4.new_code_cell("""from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.fpm import FPGrowth

spark = SparkSession.builder \\
    .appName("3C_Advanced_ML") \\
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

pg_url = "jdbc:postgresql://pgvector:5432/postgres"
pg_props = {"user": "postgres", "password": "admin", "driver": "org.postgresql.Driver"}

# 讀取資料
users_df = spark.read.jdbc(url=pg_url, table="users", properties=pg_props)
products_df = spark.read.jdbc(url=pg_url, table="products", properties=pg_props)
orders_df = spark.read.jdbc(url=pg_url, table="orders", properties=pg_props)
print("Data loaded for ML.")"""),

    nbf.v4.new_markdown_cell("### 1. RFM 模型與 K-Means 客戶分群"),
    nbf.v4.new_code_cell("""# 計算 RFM 指標
sales = orders_df.filter(col("status") == "Completed").join(products_df, "product_id") \\
    .withColumn("revenue", col("quantity") * col("selling_price"))

current_date = sales.agg(max("order_date")).collect()[0][0]

rfm_df = sales.groupBy("user_id").agg(
    datediff(lit(current_date), max("order_date")).alias("recency_days"),
    count("order_id").alias("frequency"),
    sum("revenue").alias("monetary_value")
)

# 特徵工程：組裝特徵向量並標準化 (因為金額與天數的尺度差異很大)
assembler = VectorAssembler(inputCols=["recency_days", "frequency", "monetary_value"], outputCol="features")
rfm_features = assembler.transform(rfm_df)

scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
scalerModel = scaler.fit(rfm_features)
rfm_scaled = scalerModel.transform(rfm_features)

# 訓練 K-Means 模型 (分為 4 群)
kmeans = KMeans(featuresCol="scaledFeatures", k=4, seed=42)
model = kmeans.fit(rfm_scaled)
predictions = model.transform(rfm_scaled)

# 整理最終結果並關聯使用者名稱
rfm_gold = predictions.select("user_id", "recency_days", "frequency", "monetary_value", "prediction") \\
    .join(users_df.select("user_id", "name", "email"), "user_id")

# 賦予群集商業意義 (這裡使用簡單的邏輯命名)
rfm_gold = rfm_gold.withColumn("customer_segment",
    when(col("prediction") == 0, "潛力新客 (Potential)")
    .when(col("prediction") == 1, "流失危機客 (At Risk)")
    .when(col("prediction") == 2, "忠誠大戶 (Champions)")
    .otherwise("一般客戶 (Standard)")
)

print("K-Means Clustering Completed.")"""),

    nbf.v4.new_markdown_cell("### 2. 購物籃分析 (FP-Growth 關聯規則)"),
    nbf.v4.new_code_cell("""# 準備資料：找出每個用戶買過的所有商品名稱清單
basket_df = sales.groupBy("user_id").agg(collect_set("name").alias("items"))

# 訓練 FP-Growth 模型
# minSupport: 至少要有 1% 的人買過這個組合
# minConfidence: 買了 A 之後，至少有 10% 的機率買 B
fpGrowth = FPGrowth(itemsCol="items", minSupport=0.01, minConfidence=0.1)
model_fp = fpGrowth.fit(basket_df)

# 獲取關聯規則 (Association Rules)
rules_df = model_fp.associationRules

# 轉換陣列為字串，以便存入關聯式資料庫
rules_gold = rules_df.withColumn("antecedent", concat_ws(", ", col("antecedent"))) \\
                     .withColumn("consequent", concat_ws(", ", col("consequent"))) \\
                     .orderBy(desc("confidence"))

print("FP-Growth Association Rules generated.")"""),

    nbf.v4.new_markdown_cell("### 3. 寫入 PostgreSQL (Gold ML Tables)"),
    nbf.v4.new_code_cell("""rfm_gold.drop("prediction").write.jdbc(url=pg_url, table="gold_ml_rfm", mode="overwrite", properties=pg_props)
rules_gold.write.jdbc(url=pg_url, table="gold_ml_association", mode="overwrite", properties=pg_props)
print("🚀 Advanced ML ETL Pipeline Completed! Data saved to 'gold_ml_rfm' and 'gold_ml_association'.")""")
]

nb['cells'] = cells
with open('C:\\Users\\Albert Yang\\Documents\\PySpark\\lakehouse_demo\\ETL_Advanced_ML.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb, f)
