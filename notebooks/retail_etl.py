# --------------------------------------------
# Configure Azure Storage Access
# --------------------------------------------

spark.conf.set(
"fs.azure.account.key.retaildatalake225.blob.core.windows.net",
"<STORAGE_ACCOUNT_KEY>"
)

# --------------------------------------------
# Read Bronze Layer
# --------------------------------------------

df_transactions = spark.read.parquet(
"wasbs://retail@retaildatalake225.blob.core.windows.net/bronze/transaction/"
)

df_products = spark.read.parquet(
"wasbs://retail@retaildatalake225.blob.core.windows.net/bronze/product/"
)

df_stores = spark.read.parquet(
"wasbs://retail@retaildatalake225.blob.core.windows.net/bronze/store/"
)

df_customers = spark.read.parquet(
"wasbs://retail@retaildatalake225.blob.core.windows.net/bronze/customer/"
)

# --------------------------------------------
# Data Cleaning (Silver Layer)
# --------------------------------------------

from pyspark.sql.functions import col

df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customers = df_customers.select(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "city",
    "registration_date"
).dropDuplicates(["customer_id"])

# --------------------------------------------
# Join Data → Silver Dataset
# --------------------------------------------

df_silver = (
    df_transactions
    .join(df_customers, "customer_id")
    .join(df_products, "product_id")
    .join(df_stores, "store_id")
    .withColumn("total_amount", col("quantity") * col("price"))
)

# --------------------------------------------
# Save Silver Layer
# --------------------------------------------

silver_path = "wasbs://retail@retaildatalake225.blob.core.windows.net/silver/"

df_silver.write.mode("overwrite").format("delta").save(silver_path)

# --------------------------------------------
# Read Silver Layer
# --------------------------------------------

silver_df = spark.read.format("delta").load(silver_path)

# --------------------------------------------
# Gold Layer Aggregations
# --------------------------------------------

from pyspark.sql.functions import sum, countDistinct, avg

gold_df = silver_df.groupBy(
    "transaction_date",
    "product_id",
    "product_name",
    "category",
    "store_id",
    "store_name",
    "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)

# --------------------------------------------
# Save Gold Layer
# --------------------------------------------

gold_path = "wasbs://retail@retaildatalake225.blob.core.windows.net/gold/"

gold_df.write.mode("overwrite").format("delta").save(gold_path)
