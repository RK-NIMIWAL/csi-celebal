"""
===============================================================================
Assignment 6 — Apache Spark: Architecture & Efficient Data Processing
===============================================================================
Author  : Junior Data Engineer Intern
Date    : 2026-06-29
Objective:
    Understand Spark architecture and perform efficient data processing using
    transformations, filtering, schema handling, and optimized file formats.
===============================================================================
"""

# ── 0. Imports ───────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, DateType
)
from pyspark.sql.functions import (
    col, when, lit, upper, round as spark_round,
    avg, count, sum as spark_sum, max as spark_max,
    min as spark_min, year, current_date, months_between
)
import os, time

# ── 1. Spark Session ────────────────────────────────────────────────────────
print("=" * 80)
print("STEP 1: Creating Spark Session")
print("=" * 80)

spark = SparkSession.builder \
    .appName("Assignment6_SparkDataProcessing") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"Spark Version  : {spark.version}")
print(f"App Name       : {spark.sparkContext.appName}")
print(f"Master         : {spark.sparkContext.master}")
print(f"Default Parallelism : {spark.sparkContext.defaultParallelism}")

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION A — SPARK ARCHITECTURE OVERVIEW (printed insights)             ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("\n" + "=" * 80)
print("SECTION A: Spark Architecture — Key Concepts")
print("=" * 80)

architecture_notes = """
┌──────────────────────────────────────────────────────────────────────────┐
│                    APACHE SPARK ARCHITECTURE                            │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. DRIVER PROGRAM                                                       │
│     • The main process that runs the user's application code.            │
│     • Creates the SparkContext / SparkSession.                           │
│     • Converts user code into a DAG (Directed Acyclic Graph) of stages. │
│     • Schedules tasks on executors via the Cluster Manager.              │
│                                                                          │
│  2. CLUSTER MANAGER                                                      │
│     • Allocates resources (CPU, memory) across the cluster.              │
│     • Types: Standalone, YARN, Mesos, Kubernetes.                        │
│     • Acts as a mediator between Driver and Worker nodes.                │
│                                                                          │
│  3. EXECUTORS                                                            │
│     • JVM processes running on worker nodes.                             │
│     • Execute tasks assigned by the Driver.                              │
│     • Store intermediate data in memory/disk for caching.                │
│     • Each executor has dedicated cores and memory.                      │
│                                                                          │
│  EXECUTION MODES                                                         │
│     • Local Mode   : Driver + Executors on one machine (development).    │
│     • Client Mode  : Driver on client machine, Executors on cluster.     │
│     • Cluster Mode : Driver + Executors both run on the cluster.         │
│                                                                          │
│  LAZY EVALUATION & DAG                                                   │
│     • Transformations (map, filter, select) are lazy — not executed      │
│       immediately. Spark builds a logical plan (lineage graph).          │
│     • Actions (show, count, collect, write) trigger actual execution.    │
│     • The DAG Scheduler optimizes the plan, combining stages where       │
│       possible and minimizing shuffles.                                  │
│     • Benefits: avoids unnecessary computation, enables whole-stage      │
│       code generation, and allows predicate pushdown optimisation.       │
│                                                                          │
│  TRANSFORMATIONS VS ACTIONS                                              │
│     Narrow Transformations  : map, filter, select (no shuffle)           │
│     Wide Transformations    : groupBy, join, repartition (shuffle)       │
│     Actions                 : show, count, collect, write                │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
"""
print(architecture_notes)


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION B — READING DATA WITH SCHEMA HANDLING                         ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("=" * 80)
print("STEP 2: Reading CSV data with explicit schema")
print("=" * 80)

# --- 2a. Define explicit schema (best practice for production) ---------------
employee_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),          # nullable for demo
    StructField("joining_date", StringType(), True),     # read as string, cast later
    StructField("city", StringType(), True),
    StructField("experience_years", IntegerType(), True),
    StructField("rating", DoubleType(), True),           # nullable for demo
])

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

csv_path = os.path.join(DATA_DIR, "employees.csv")

# --- 2b. Read CSV with explicit schema --------------------------------------
df = spark.read.csv(
    csv_path,
    header=True,
    schema=employee_schema,
    nullValue=""              # treat empty strings as null
)

print("\n✔ DataFrame loaded successfully from CSV")
print(f"  Rows    : {df.count()}")
print(f"  Columns : {len(df.columns)}")

print("\n── Schema ──")
df.printSchema()

print("\n── First 10 rows (using show — best practice for large data) ──")
df.show(10, truncate=False)


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION C — LAZY EVALUATION DEMONSTRATION                             ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("\n" + "=" * 80)
print("STEP 3: Demonstrating Lazy Evaluation")
print("=" * 80)

# These are TRANSFORMATIONS — Spark builds the DAG but does NOT execute yet.
print("\n[Transformation] Filtering Engineering department ...")
eng_df = df.filter(col("department") == "Engineering")

print("[Transformation] Selecting name and salary ...")
eng_selected = eng_df.select("name", "salary")

print("[Transformation] Adding bonus column ...")
eng_with_bonus = eng_selected.withColumn("bonus", col("salary") * 0.15)

print("\n⏳ No computation happened yet — all transformations are lazy!")
print("   Spark has built a DAG (lineage graph) for these operations.\n")

# This is an ACTION — triggers actual execution of the DAG.
print("[Action] Calling show() — this triggers execution of the DAG:\n")
eng_with_bonus.show(truncate=False)

# Explain the execution plan (logical + physical)
print("── Execution Plan (Logical → Physical) ──")
eng_with_bonus.explain(True)


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION D — FILTERING & COLUMN SELECTION                              ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("\n" + "=" * 80)
print("STEP 4: Filtering and Column Selection")
print("=" * 80)

# --- 4a. Single condition filter ─────────────────────────────────────────────
print("\n── 4a. Employees with salary > 80,000 ──")
high_salary_df = df.filter(col("salary") > 80000)
high_salary_df.select("name", "department", "salary").show(truncate=False)

# --- 4b. Multiple conditions ────────────────────────────────────────────────
print("── 4b. Engineering employees in Mumbai or Pune with 5+ years experience ──")
filtered_df = df.filter(
    (col("department") == "Engineering") &
    (col("city").isin("Mumbai", "Pune")) &
    (col("experience_years") >= 5)
)
filtered_df.select("name", "city", "experience_years", "salary").show(truncate=False)

# --- 4c. Using SQL-style filtering ──────────────────────────────────────────
print("── 4c. SQL-style filter: rating >= 4.5 ──")
top_rated = df.filter("rating >= 4.5")
top_rated.select("name", "department", "rating").show(truncate=False)

# --- 4d. Selecting specific columns ─────────────────────────────────────────
print("── 4d. Selecting specific columns ──")
subset_df = df.select("emp_id", "name", "department", "salary")
subset_df.show(5, truncate=False)


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION E — MODIFYING DATAFRAMES                                      ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("\n" + "=" * 80)
print("STEP 5: Modifying DataFrames (rename, cast, add columns)")
print("=" * 80)

# --- 5a. Rename columns ─────────────────────────────────────────────────────
print("\n── 5a. Renaming columns ──")
renamed_df = df.withColumnRenamed("name", "employee_name") \
               .withColumnRenamed("city", "work_location")
renamed_df.select("emp_id", "employee_name", "work_location").show(5, truncate=False)

# --- 5b. Cast data types ────────────────────────────────────────────────────
print("── 5b. Casting joining_date from String → Date ──")
from pyspark.sql.functions import to_date

casted_df = df.withColumn("joining_date", to_date(col("joining_date"), "yyyy-MM-dd"))
casted_df.printSchema()
casted_df.select("name", "joining_date").show(5, truncate=False)

# --- 5c. Add new computed columns ────────────────────────────────────────────
print("── 5c. Adding computed columns ──")
enriched_df = casted_df \
    .withColumn("annual_bonus",
                spark_round(col("salary") * 0.15, 2)) \
    .withColumn("salary_band",
                when(col("salary") >= 100000, "Senior")
                .when(col("salary") >= 75000, "Mid")
                .otherwise("Junior")) \
    .withColumn("tenure_months",
                spark_round(months_between(current_date(), col("joining_date")), 0).cast(IntegerType()))

print("Schema after adding new columns:")
enriched_df.printSchema()
enriched_df.select(
    "name", "salary", "annual_bonus", "salary_band", "tenure_months"
).show(10, truncate=False)


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION F — HANDLING NULL VALUES                                       ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("\n" + "=" * 80)
print("STEP 6: Handling Null Values")
print("=" * 80)

# --- 6a. Identify nulls ─────────────────────────────────────────────────────
print("\n── 6a. Null count per column ──")
from pyspark.sql.functions import sum as spark_sum_fn

null_counts = df.select([
    spark_sum_fn(col(c).isNull().cast("int")).alias(c) for c in df.columns
])
null_counts.show(truncate=False)

# --- 6b. Filter rows with null salary ───────────────────────────────────────
print("── 6b. Rows with NULL salary ──")
df.filter(col("salary").isNull()).show(truncate=False)

# --- 6c. Fill nulls with defaults ───────────────────────────────────────────
print("── 6c. Filling nulls (salary → median, experience → 0, rating → 3.0) ──")
# Calculate median salary for imputation
median_salary = df.approxQuantile("salary", [0.5], 0.01)[0]
print(f"   Median salary for imputation: {median_salary}")

filled_df = df.fillna({
    "salary": median_salary,
    "experience_years": 0,
    "rating": 3.0
})
filled_df.filter(
    (col("name") == "Arjun Desai") |
    (col("name") == "Swati Kulkarni") |
    (col("name") == "Divya Joshi") |
    (col("name") == "Amit Kumar") |
    (col("name") == "Sanjay Tiwari")
).show(truncate=False)

# --- 6d. Drop rows with any null ────────────────────────────────────────────
print("── 6d. Dropping rows with ANY null ──")
clean_df = df.dropna()
print(f"   Rows before: {df.count()}, Rows after dropna: {clean_df.count()}")


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION G — WIDE TRANSFORMATIONS & PERFORMANCE                        ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("\n" + "=" * 80)
print("STEP 7: Wide Transformations, Shuffle & Performance Concepts")
print("=" * 80)

performance_notes = """
┌──────────────────────────────────────────────────────────────────────────┐
│                     PERFORMANCE CONCEPTS                                │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  NARROW vs WIDE TRANSFORMATIONS                                          │
│  ─────────────────────────────────────────────────────────────────────    │
│  Narrow : Each input partition contributes to ONE output partition.       │
│           Examples: select, filter, map, withColumn                      │
│           ✅ No data movement across partitions (fast)                   │
│                                                                          │
│  Wide   : Input partitions contribute to MULTIPLE output partitions.     │
│           Examples: groupBy, join, repartition, orderBy                  │
│           ⚠️  Requires SHUFFLE — data is redistributed across nodes.     │
│           Shuffle is the most expensive operation in Spark.              │
│                                                                          │
│  SHUFFLE                                                                 │
│  ─────────────────────────────────────────────────────────────────────    │
│  • Data is serialized, written to disk, sent over the network, and       │
│    deserialized — very costly in terms of I/O and network.               │
│  • Minimise shuffles by: using broadcast joins for small tables,         │
│    pre-partitioning data, and reducing the number of groupBy operations. │
│  • spark.sql.shuffle.partitions controls the number of output partitions │
│    after a shuffle (default=200, reduced to 4 for this local demo).      │
│                                                                          │
│  PREDICATE PUSHDOWN                                                      │
│  ─────────────────────────────────────────────────────────────────────    │
│  • Spark pushes filter conditions down to the data source layer.         │
│  • With Parquet (columnar format), only relevant row groups and          │
│    columns are read from disk → significant I/O savings.                 │
│  • CSV does not support predicate pushdown — all data must be read.      │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
"""
print(performance_notes)

# --- 7a. GroupBy — wide transformation (triggers shuffle) ───────────────────
print("── 7a. Aggregation by Department (wide transformation → shuffle) ──")
dept_stats = filled_df.groupBy("department").agg(
    count("*").alias("employee_count"),
    spark_round(avg("salary"), 2).alias("avg_salary"),
    spark_max("salary").alias("max_salary"),
    spark_min("salary").alias("min_salary"),
    spark_round(avg("rating"), 2).alias("avg_rating")
).orderBy(col("avg_salary").desc())

dept_stats.show(truncate=False)

# --- 7b. Aggregation by City ────────────────────────────────────────────────
print("── 7b. Aggregation by City ──")
city_stats = filled_df.groupBy("city").agg(
    count("*").alias("headcount"),
    spark_round(avg("salary"), 2).alias("avg_salary"),
    spark_round(avg("experience_years"), 1).alias("avg_experience")
).orderBy(col("headcount").desc())

city_stats.show(truncate=False)


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION H — FILE FORMATS: CSV vs PARQUET                              ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("\n" + "=" * 80)
print("STEP 8: File Formats — CSV vs Parquet (Performance Comparison)")
print("=" * 80)

csv_output = os.path.join(OUTPUT_DIR, "employees_csv")
parquet_output = os.path.join(OUTPUT_DIR, "employees_parquet")

# --- 8a. Write to CSV ───────────────────────────────────────────────────────
start = time.time()
filled_df.write.mode("overwrite").option("header", True).csv(csv_output)
csv_write_time = time.time() - start
print(f"\n✔ Written to CSV    : {csv_output}  ({csv_write_time:.4f}s)")

# --- 8b. Write to Parquet ───────────────────────────────────────────────────
start = time.time()
filled_df.write.mode("overwrite").parquet(parquet_output)
parquet_write_time = time.time() - start
print(f"✔ Written to Parquet: {parquet_output}  ({parquet_write_time:.4f}s)")

# --- 8c. Read back and compare ──────────────────────────────────────────────
start = time.time()
csv_read_df = spark.read.csv(csv_output, header=True, inferSchema=True)
csv_read_time = time.time() - start

start = time.time()
parquet_read_df = spark.read.parquet(parquet_output)
parquet_read_time = time.time() - start

# File sizes
import pathlib

def get_dir_size(path):
    """Get total size of files in a directory in KB."""
    total = sum(f.stat().st_size for f in pathlib.Path(path).rglob("*") if f.is_file())
    return total / 1024

csv_size = get_dir_size(csv_output)
parquet_size = get_dir_size(parquet_output)

format_comparison = f"""
┌────────────────────────────────────────────────────────────┐
│           CSV vs PARQUET — Performance Comparison           │
├──────────────┬──────────────────┬──────────────────────────┤
│  Metric      │  CSV             │  Parquet                 │
├──────────────┼──────────────────┼──────────────────────────┤
│  Write Time  │  {csv_write_time:.4f}s          │  {parquet_write_time:.4f}s                │
│  Read Time   │  {csv_read_time:.4f}s          │  {parquet_read_time:.4f}s                │
│  File Size   │  {csv_size:.1f} KB          │  {parquet_size:.1f} KB                │
├──────────────┴──────────────────┴──────────────────────────┤
│                                                            │
│  KEY INSIGHTS:                                             │
│  • Parquet is a columnar format — much more compact due    │
│    to efficient encoding and compression.                  │
│  • Parquet supports predicate pushdown: Spark reads only   │
│    required columns and row groups → faster queries.       │
│  • CSV is row-based and human-readable, but larger and     │
│    slower for analytical workloads.                        │
│  • Parquet preserves schema (data types), while CSV        │
│    requires schema inference or explicit specification.    │
│  • For large-scale data pipelines, Parquet is the          │
│    RECOMMENDED format.                                     │
│                                                            │
└────────────────────────────────────────────────────────────┘
"""
print(format_comparison)

# --- 8d. Demonstrate predicate pushdown with Parquet ─────────────────────────
print("── 8d. Predicate Pushdown Demo (Parquet) ──")
print("   Filtering directly from Parquet — Spark pushes the filter to the reader:\n")
parquet_filtered = spark.read.parquet(parquet_output) \
    .filter(col("department") == "Engineering") \
    .select("name", "salary")
parquet_filtered.explain(True)     # show the physical plan
parquet_filtered.show(truncate=False)


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION I — COMPLETE DATA PIPELINE: READ → TRANSFORM → FILTER → WRITE ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("\n" + "=" * 80)
print("STEP 9: Complete Data Pipeline (Read → Transform → Filter → Write)")
print("=" * 80)

pipeline_output_csv = os.path.join(OUTPUT_DIR, "pipeline_result_csv")
pipeline_output_parquet = os.path.join(OUTPUT_DIR, "pipeline_result_parquet")

print("\n── Pipeline Stages ──")

# Stage 1: READ
print("[Stage 1] Reading raw CSV data with explicit schema ...")
raw_df = spark.read.csv(csv_path, header=True, schema=employee_schema, nullValue="")

# Stage 2: CLEAN (handle nulls)
print("[Stage 2] Cleaning — filling null values ...")
cleaned_df = raw_df.fillna({
    "salary": median_salary,
    "experience_years": 0,
    "rating": 3.0
})

# Stage 3: TRANSFORM (add computed columns, rename, cast)
print("[Stage 3] Transforming — adding computed columns ...")
transformed_df = cleaned_df \
    .withColumn("joining_date", to_date(col("joining_date"), "yyyy-MM-dd")) \
    .withColumn("annual_bonus", spark_round(col("salary") * 0.15, 2)) \
    .withColumn("salary_band",
                when(col("salary") >= 100000, "Senior")
                .when(col("salary") >= 75000, "Mid")
                .otherwise("Junior")) \
    .withColumn("tenure_months",
                spark_round(months_between(current_date(), col("joining_date")), 0)
                .cast(IntegerType())) \
    .withColumnRenamed("name", "employee_name") \
    .withColumnRenamed("city", "work_location")

# Stage 4: FILTER (select only relevant records)
print("[Stage 4] Filtering — selecting Mid/Senior band employees ...")
pipeline_df = transformed_df.filter(
    (col("salary_band").isin("Mid", "Senior")) &
    (col("rating") >= 3.5)
).select(
    "emp_id", "employee_name", "department", "salary",
    "annual_bonus", "salary_band", "work_location",
    "experience_years", "rating", "tenure_months"
)

# Stage 5: WRITE (save in both formats)
print("[Stage 5] Writing results to CSV and Parquet ...\n")
pipeline_df.write.mode("overwrite").option("header", True).csv(pipeline_output_csv)
pipeline_df.write.mode("overwrite").parquet(pipeline_output_parquet)

print("✔ Pipeline output (CSV)     :", pipeline_output_csv)
print("✔ Pipeline output (Parquet) :", pipeline_output_parquet)

print("\n── Pipeline Result ──")
pipeline_df.show(truncate=False)
print(f"Total records in pipeline output: {pipeline_df.count()}")


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION J — BEST PRACTICES FOR LARGE DATASETS                         ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("\n" + "=" * 80)
print("STEP 10: Best Practices for Large Datasets")
print("=" * 80)

best_practices = """
┌──────────────────────────────────────────────────────────────────────────┐
│              BEST PRACTICES FOR LARGE-SCALE DATA PROCESSING             │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. AVOID collect() on large datasets                                    │
│     • collect() pulls ALL data to the Driver — can cause OOM errors.     │
│     • Use show(n) or take(n) to preview a subset.                        │
│     • Use write() to persist results instead of collecting.              │
│                                                                          │
│  2. USE PARQUET over CSV                                                 │
│     • Columnar, compressed, schema-preserving.                           │
│     • Supports predicate pushdown and column pruning.                    │
│                                                                          │
│  3. DEFINE SCHEMAS EXPLICITLY                                            │
│     • Avoid inferSchema=True for large files — it requires an extra      │
│       pass over the data.                                                │
│     • Define StructType schemas for predictable, faster reads.           │
│                                                                          │
│  4. PARTITION YOUR DATA wisely                                           │
│     • Use partitionBy("column") when writing to enable partition          │
│       pruning on reads.                                                  │
│     • Adjust spark.sql.shuffle.partitions based on data volume.          │
│                                                                          │
│  5. MINIMISE SHUFFLES                                                    │
│     • Reduce wide transformations (groupBy, join).                       │
│     • Use broadcast joins for small lookup tables.                       │
│     • Cache/persist intermediate DataFrames that are reused.             │
│                                                                          │
│  6. LEVERAGE CATALYST OPTIMIZER                                          │
│     • Use DataFrame/Dataset API over RDDs — Catalyst can optimise.       │
│     • Chain transformations to let Spark build an efficient plan.        │
│                                                                          │
│  7. MONITOR with Spark UI                                                │
│     • Default at http://localhost:4040 during execution.                  │
│     • Inspect stages, tasks, shuffle read/write, and storage.            │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
"""
print(best_practices)

# --- Demonstrating show() vs collect() ──────────────────────────────────────
print("── Demo: show() vs collect() ──")
print("✅ Using show(5) — safe for large datasets, returns nothing to Driver:")
pipeline_df.show(5, truncate=False)

print("⚠️  Using collect() — brings ALL data to Driver (avoid for large data):")
print(f"   collect() returns {len(pipeline_df.collect())} rows as Python list (OK for 30 rows, dangerous for millions)\n")


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION K — PARTITIONED WRITE (BONUS)                                  ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("=" * 80)
print("BONUS: Writing Partitioned Parquet by Department")
print("=" * 80)

partitioned_output = os.path.join(OUTPUT_DIR, "partitioned_by_dept")
filled_df.write.mode("overwrite").partitionBy("department").parquet(partitioned_output)
print(f"\n✔ Partitioned Parquet written to: {partitioned_output}")
print("  Each department is stored in its own subdirectory for partition pruning.\n")

# Show the partition structure
for entry in sorted(pathlib.Path(partitioned_output).iterdir()):
    if entry.is_dir():
        print(f"  📁 {entry.name}")


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  WRAP-UP                                                               ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
print("\n" + "=" * 80)
print("ASSIGNMENT COMPLETE ✔")
print("=" * 80)
print(f"""
Summary of outputs saved to: {OUTPUT_DIR}
  ├── employees_csv/              (raw data saved as CSV)
  ├── employees_parquet/          (raw data saved as Parquet)
  ├── pipeline_result_csv/        (processed pipeline output — CSV)
  ├── pipeline_result_parquet/    (processed pipeline output — Parquet)
  └── partitioned_by_dept/        (department-partitioned Parquet)
""")

# Stop Spark session
spark.stop()
print("Spark session stopped. Goodbye! 🚀")
