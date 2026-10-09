# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Project Utilities and initialize Notebook widgets

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog",'fmgc','Catalog')
dbutils.widgets.text("data_source","customers","Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"s3://databricks-project-child-company-s3-712389990106-eu-north-1-an/{data_source}/*.csv"

base_path


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*","_metadata.file_name","_metadata.file_size")
      )


# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Drop Duplicate Records

# COMMAND ----------

df_duplicates = df_bronze.groupBy("customer_id").count().filter("count > 1")
display(df_duplicates)

# COMMAND ----------

print("Rows with duplicates: ", df_bronze.count())

df_silver = df_bronze.dropDuplicates(["customer_id"])
print("Rows after removing duplicates: ", df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Trim customer names

# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim("customer_name"))
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name", F.trim(F.col("customer_name"))
)
    
# Check again
display(df_silver.filter(F.col("customer_name") != F.trim("customer_name")))

# COMMAND ----------

# MAGIC %md
# MAGIC 3. City Typos

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',

    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}

allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
         .when(F.col("city").isin(allowed), F.col("city"))
         .otherwise(None)
    )
)

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Title casing issue

# COMMAND ----------

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

#Start with cap follwed by small letter
df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
     .otherwise(F.initcap("customer_name"))
)

# COMMAND ----------

#sanity check
display(df_silver.filter(F.col("customer_name") != F.initcap("customer_name")))

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Missing Values of cities

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

# find them in records
null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

# Discuss with business team
customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")   # Replace null with fixed city
    )
    .drop("fixed_city")
)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Convert customer_id to String 

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC Standardize Customer Attributes to Match Parent Company DataModel

# COMMAND ----------

df_silver = (
    df_silver
    # Build final customer column: "CustomerName-City" or "CustomerName-Unknown"
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )
    
    # Static attributes aligned with parent data model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

# COMMAND ----------

display(df_silver.limit(6))

# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")


#  req cols only
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.child_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Marging with Parents

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmgc.gold.child_dim_customers").select(
    F.col("customer_id").alias("customer_code"), #to match with parent
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table = DeltaTable.forName(spark, f"{catalog}.gold.dim_customers")

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

