# Spark Basics Assignment - CSI Celebal Internship
# Data Cleaning, Transformation & Analysis using PySpark

# Step 1: Understanding Basics
# MapReduce is slow because it reads and writes everything to disk after each step.
# Spark is much faster because it processes data in memory (RAM) instead of disk.
# Spark also gives us DataFrames which are like tables - easy to work with.
# It uses lazy evaluation meaning it waits and optimizes before actually running anything.

# Step 2: Setting up Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("CSI_Celebal_Spark_Basics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark session started")
print(f"Version: {spark.version}")


# Step 3: Loading the data
print("\n--- STEP 3: Loading Data ---")

df = spark.read.csv("data/dataset.csv", header=True, inferSchema=True)

print("First 10 rows:")
df.show(10, truncate=False)

print(f"Columns: {df.columns}")
print("\nSchema:")
df.printSchema()
print(f"Total rows: {df.count()}")


# Step 4: Data Cleaning
print("\n--- STEP 4: Data Cleaning ---")

# removing duplicates
total_before = df.count()
df_dedup = df.dropDuplicates()
total_after_dedup = df_dedup.count()
print(f"Rows before: {total_before}, After removing duplicates: {total_after_dedup}")
print(f"Duplicates found: {total_before - total_after_dedup}")

# checking for null values
print("\nNull values in each column:")
for col_name in df_dedup.columns:
    null_count = df_dedup.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        print(f"  {col_name}: {null_count} nulls")

# filling missing values with mean for numeric cols
mean_salary = df_dedup.select(F.mean("salary")).first()[0]
mean_age = df_dedup.select(F.mean("age")).first()[0]
mean_rating = df_dedup.select(F.mean("rating")).first()[0]

df_clean = df_dedup.fillna({
    "salary": round(mean_salary, 2),
    "age": int(round(mean_age)),
    "rating": round(mean_rating, 2),
    "region": "Unknown"
})

# verifying cleaning worked
print("\nAfter cleaning - checking nulls again:")
for col_name in df_clean.columns:
    null_count = df_clean.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        print(f"  {col_name}: {null_count} nulls")
    else:
        print(f"  {col_name}: OK")

print(f"Clean data has {df_clean.count()} rows")


# Step 5: Filtering Data
print("\n--- STEP 5: Filtering ---")

# filter by age
print("\nEmployees older than 35:")
df_clean.filter(F.col("age") > 35).show(truncate=False)

# filter by department
print("Engineering department employees:")
df_clean.filter(F.col("department") == "Engineering").show(truncate=False)

# filter by region
print("North region employees:")
df_clean.filter(F.col("region") == "North").show(truncate=False)

# combining multiple filters
print("Engineering employees earning more than 80k:")
df_clean.filter(
    (F.col("department") == "Engineering") & (F.col("salary") > 80000)
).show(truncate=False)


# Step 6: Transforming Data
print("\n--- STEP 6: Transformations ---")

# renaming some columns to make them clearer
df_transformed = df_clean \
    .withColumnRenamed("name", "employee_name") \
    .withColumnRenamed("rating", "performance_rating")

# converting data types
df_transformed = df_transformed \
    .withColumn("salary", F.col("salary").cast(IntegerType())) \
    .withColumn("performance_rating", F.col("performance_rating").cast(DoubleType()))

# adding new columns based on existing data
df_transformed = df_transformed \
    .withColumn("salary_band",
                F.when(F.col("salary") < 60000, "Junior")
                 .when(F.col("salary") < 80000, "Mid-Level")
                 .when(F.col("salary") < 90000, "Senior")
                 .otherwise("Lead")) \
    .withColumn("experience_level",
                F.when(F.col("age") < 30, "Early Career")
                 .when(F.col("age") < 40, "Mid Career")
                 .otherwise("Senior Career"))

print("Transformed data:")
df_transformed.show(10, truncate=False)
df_transformed.printSchema()


# Step 7: Aggregations
print("\n--- STEP 7: Aggregations ---")

print(f"Total records: {df_transformed.count()}")

avg_sal = df_transformed.select(F.mean("salary")).first()[0]
min_sal = df_transformed.select(F.min("salary")).first()[0]
max_sal = df_transformed.select(F.max("salary")).first()[0]
avg_rating = df_transformed.select(F.mean("performance_rating")).first()[0]

print(f"Average salary: {avg_sal:,.2f}")
print(f"Min salary: {min_sal:,}")
print(f"Max salary: {max_sal:,}")
print(f"Average rating: {avg_rating:.2f}")

print("\nSummary statistics:")
df_transformed.select("age", "salary", "performance_rating").describe().show()


# Step 8: GroupBy operations
print("\n--- STEP 8: GroupBy ---")

# grouping by department
print("Department-wise breakdown:")
df_transformed.groupBy("department").agg(
    F.count("*").alias("count"),
    F.round(F.avg("salary"), 2).alias("avg_salary"),
    F.round(F.avg("performance_rating"), 2).alias("avg_rating"),
    F.sum("salary").alias("total_salary"),
    F.min("salary").alias("min_salary"),
    F.max("salary").alias("max_salary")
).orderBy("department").show(truncate=False)

# grouping by region
print("Region-wise breakdown:")
df_transformed.groupBy("region").agg(
    F.count("*").alias("count"),
    F.round(F.avg("salary"), 2).alias("avg_salary"),
    F.round(F.avg("performance_rating"), 2).alias("avg_rating")
).orderBy("region").show(truncate=False)

# salary band distribution
print("Salary band distribution:")
df_transformed.groupBy("salary_band").agg(
    F.count("*").alias("count"),
    F.round(F.avg("age"), 1).alias("avg_age"),
    F.round(F.avg("performance_rating"), 2).alias("avg_rating")
).orderBy("salary_band").show(truncate=False)

# department x region cross analysis
print("Department and Region cross analysis:")
df_transformed.groupBy("department", "region").agg(
    F.count("*").alias("count"),
    F.round(F.avg("salary"), 2).alias("avg_salary")
).orderBy("department", "region").show(30, truncate=False)

# Step 10: Building a complete pipeline
print("\n--- STEP 10: Complete Pipeline ---")

def run_pipeline(input_path, output_path):
    """Simple end-to-end pipeline: load -> clean -> filter -> transform -> aggregate -> save"""

    # 1. Load
    print("\n1. Loading data...")
    raw_df = spark.read.csv(input_path, header=True, inferSchema=True)
    print(f"   Loaded {raw_df.count()} rows")

    # 2. Clean
    print("2. Cleaning...")
    clean_df = raw_df.dropDuplicates()
    for col_name, col_type in clean_df.dtypes:
        if col_type in ("int", "double", "bigint"):
            mean_val = clean_df.select(F.mean(col_name)).first()[0]
            if mean_val is not None:
                clean_df = clean_df.fillna({col_name: round(mean_val, 2)})
        elif col_type == "string":
            clean_df = clean_df.fillna({col_name: "Unknown"})
    print(f"   After cleaning: {clean_df.count()} rows")

    # 3. Filter
    print("3. Filtering (salary > 50k, age 25-50)...")
    filtered_df = clean_df.filter(
        (F.col("salary") > 50000) & (F.col("age").between(25, 50))
    )
    print(f"   After filtering: {filtered_df.count()} rows")

    # 4. Transform
    print("4. Transforming...")
    transformed_df = filtered_df \
        .withColumnRenamed("name", "employee_name") \
        .withColumnRenamed("rating", "performance_rating") \
        .withColumn("salary", F.col("salary").cast(IntegerType())) \
        .withColumn("salary_band",
                     F.when(F.col("salary") < 60000, "Junior")
                      .when(F.col("salary") < 80000, "Mid-Level")
                      .when(F.col("salary") < 90000, "Senior")
                      .otherwise("Lead")) \
        .withColumn("annual_bonus",
                     F.round(F.col("salary") * F.col("performance_rating") / 100, 2))

    # 5. Aggregate
    print("5. Aggregating by department...")
    result = transformed_df.groupBy("department").agg(
        F.count("*").alias("employee_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.sum("salary"), 2).alias("total_salary"),
        F.round(F.avg("performance_rating"), 2).alias("avg_rating"),
        F.round(F.sum("annual_bonus"), 2).alias("total_bonus"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary")
    ).orderBy(F.desc("avg_salary"))

    print("\nPipeline results:")
    result.show(truncate=False)

    # 6. Save
    print("6. Saving output...")
    result.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
    print(f"   Saved to {output_path}")

    detail_path = output_path.replace("results", "detailed_results")
    transformed_df.coalesce(1).write.csv(detail_path, header=True, mode="overwrite")
    print(f"   Detailed data saved to {detail_path}")

    return result

pipeline_result = run_pipeline("data/dataset.csv", "output/results")

spark.stop()
print("Spark session stopped.")
