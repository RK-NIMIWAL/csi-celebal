# Assignment 6 — Apache Spark: Architecture & Efficient Data Processing

**Author:** Junior Data Engineer Intern  
**Date:** June 2026  

---

## Table of Contents

1. [Spark Architecture Overview](#1-spark-architecture-overview)
2. [Lazy Evaluation & DAG](#2-lazy-evaluation--dag)
3. [Reading Data with Schema Handling](#3-reading-data-with-schema-handling)
4. [Filtering & Column Selection](#4-filtering--column-selection)
5. [Modifying DataFrames](#5-modifying-dataframes)
6. [Transformations vs Actions](#6-transformations-vs-actions)
7. [Wide Transformations & Performance](#7-wide-transformations--performance)
8. [File Formats: CSV vs Parquet](#8-file-formats-csv-vs-parquet)
9. [Null Value Handling](#9-null-value-handling)
10. [Complete Data Pipeline](#10-complete-data-pipeline)
11. [Best Practices](#11-best-practices)

---

## 1. Spark Architecture Overview

Apache Spark follows a **master-worker** architecture with three core components:

### Driver Program
- The **main process** that runs the user's `main()` function.
- Creates the **SparkContext/SparkSession** — the entry point to Spark.
- Converts user code into a **DAG (Directed Acyclic Graph)** of stages and tasks.
- Sends tasks to executors via the Cluster Manager.

### Cluster Manager
- **Resource broker** that allocates CPU and memory across the cluster.
- Types: **Standalone**, **YARN**, **Mesos**, **Kubernetes**.
- Manages the lifecycle of executor processes on worker nodes.

### Executors
- **JVM processes** on worker nodes that execute tasks.
- Each executor has dedicated **cores** (for parallelism) and **memory** (for caching).
- Report task status and results back to the Driver.

### Execution Modes

| Mode | Driver Location | Executor Location | Use Case |
|------|----------------|-------------------|----------|
| **Local** | Local machine | Same machine | Development & testing |
| **Client** | Client machine | Cluster nodes | Interactive analysis (notebooks) |
| **Cluster** | Cluster node | Cluster nodes | Production batch jobs |

```
┌──────────────────────────────────────────────┐
│              DRIVER PROGRAM                  │
│  ┌─────────────┐   ┌──────────────────────┐  │
│  │ SparkContext │──▶│ DAG Scheduler        │  │
│  └─────────────┘   │ Task Scheduler       │  │
│                     └──────────┬───────────┘  │
└────────────────────────────────┼──────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │    CLUSTER MANAGER      │
                    │  (YARN / Standalone /   │
                    │   Mesos / K8s)          │
                    └──┬─────────────────┬────┘
                       │                 │
              ┌────────▼──────┐  ┌───────▼───────┐
              │  EXECUTOR 1   │  │  EXECUTOR 2   │
              │  ┌──────────┐ │  │  ┌──────────┐ │
              │  │ Task 1   │ │  │  │ Task 3   │ │
              │  │ Task 2   │ │  │  │ Task 4   │ │
              │  └──────────┘ │  │  └──────────┘ │
              │  Cache/Memory │  │  Cache/Memory │
              └───────────────┘  └───────────────┘
```

---

## 2. Lazy Evaluation & DAG

### What is Lazy Evaluation?
Spark does **not** execute transformations immediately. Instead, it builds a **logical plan** (lineage graph) of all transformations. Execution is triggered **only** when an **action** is called.

### Why is this powerful?
- **Optimization:** Spark's Catalyst Optimizer can reorganise, combine, and prune operations before executing anything.
- **Fault Tolerance:** The lineage graph enables re-computation of lost partitions without restarting the entire job.
- **Efficiency:** Unnecessary intermediate results are never materialised.

### DAG (Directed Acyclic Graph)
```
read CSV ──▶ filter(dept='Eng') ──▶ select(name, salary) ──▶ withColumn(bonus)
    │                                                              │
    └─── Lazy: plan built ──────────────────────────────────────────┘
                                                                    │
                                                           show() ◀─┘
                                                         [ACTION triggers DAG execution]
```

**Code Example (from script):**
```python
# These transformations are LAZY — no computation yet
eng_df = df.filter(col("department") == "Engineering")
eng_selected = eng_df.select("name", "salary")
eng_with_bonus = eng_selected.withColumn("bonus", col("salary") * 0.15)

# This ACTION triggers the entire DAG
eng_with_bonus.show()
```

---

## 3. Reading Data with Schema Handling

### Best Practice: Explicit Schema
```python
employee_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("joining_date", StringType(), True),
    StructField("city", StringType(), True),
    StructField("experience_years", IntegerType(), True),
    StructField("rating", DoubleType(), True),
])

df = spark.read.csv("employees.csv", header=True, schema=employee_schema, nullValue="")
```

### Why Explicit Schema?

| Approach | Extra Pass? | Type Safety | Speed |
|----------|:-----------:|:-----------:|:-----:|
| `inferSchema=True` | ✅ Yes (reads data twice) | ❌ May guess wrong | Slower |
| Explicit `StructType` | ❌ No | ✅ Guaranteed | Faster |

---

## 4. Filtering & Column Selection

### Filter Strategies
```python
# Single condition
df.filter(col("salary") > 80000)

# Multiple conditions (AND, isin)
df.filter(
    (col("department") == "Engineering") &
    (col("city").isin("Mumbai", "Pune")) &
    (col("experience_years") >= 5)
)

# SQL-style string filter
df.filter("rating >= 4.5")
```

### Column Selection
```python
df.select("emp_id", "name", "department", "salary")
```

> **Performance Note:** Selecting only needed columns early (column pruning) reduces memory usage and speeds up downstream operations, especially with Parquet.

---

## 5. Modifying DataFrames

```python
# Rename columns
df.withColumnRenamed("name", "employee_name")

# Cast data types
df.withColumn("joining_date", to_date(col("joining_date"), "yyyy-MM-dd"))

# Add computed columns
df.withColumn("annual_bonus", round(col("salary") * 0.15, 2))
df.withColumn("salary_band",
    when(col("salary") >= 100000, "Senior")
    .when(col("salary") >= 75000, "Mid")
    .otherwise("Junior")
)
```

---

## 6. Transformations vs Actions

| Category | Examples | Lazy? | Triggers Execution? |
|----------|---------|:-----:|:-------------------:|
| **Narrow Transformation** | `filter`, `select`, `map`, `withColumn` | ✅ | ❌ |
| **Wide Transformation** | `groupBy`, `join`, `repartition`, `orderBy` | ✅ | ❌ |
| **Action** | `show`, `count`, `collect`, `write`, `take` | ❌ | ✅ |

---

## 7. Wide Transformations & Performance

### Shuffle
Wide transformations cause **shuffle** — the most expensive Spark operation:
- Data is serialized → written to disk → sent over network → deserialized.
- **Minimize shuffles** by: using broadcast joins, pre-partitioning, reducing groupBy calls.

### Predicate Pushdown
- Spark pushes filter conditions to the **data source** layer.
- With **Parquet**, only relevant row groups and columns are read from disk.
- **CSV does NOT support predicate pushdown** — all data must be scanned.

```python
# Spark pushes this filter into the Parquet reader
spark.read.parquet("data.parquet").filter(col("department") == "Engineering")
```

---

## 8. File Formats: CSV vs Parquet

| Feature | CSV | Parquet |
|---------|-----|--------|
| **Format** | Row-based, text | Columnar, binary |
| **Compression** | Poor | Excellent (snappy/gzip) |
| **Schema** | Not embedded | Embedded in file |
| **Predicate Pushdown** | ❌ Not supported | ✅ Supported |
| **Column Pruning** | ❌ Must read all | ✅ Read only needed columns |
| **Human Readable** | ✅ Yes | ❌ No |
| **Best For** | Small files, interchange | Analytics, data pipelines |

> **Insight:** For production data pipelines, **always prefer Parquet**. It is smaller, faster to read, preserves schema, and supports predicate pushdown.

---

## 9. Null Value Handling

```python
# Count nulls per column
df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

# Fill nulls with specific defaults
df.fillna({"salary": 78000, "experience_years": 0, "rating": 3.0})

# Drop rows with ANY null
df.dropna()

# Filter rows where a column IS null
df.filter(col("salary").isNull())
```

---

## 10. Complete Data Pipeline

The script implements a full **read → clean → transform → filter → write** pipeline:

```
┌──────────┐    ┌──────────┐    ┌─────────────┐    ┌──────────┐    ┌───────────────┐
│  READ    │───▶│  CLEAN   │───▶│  TRANSFORM  │───▶│  FILTER  │───▶│  WRITE        │
│  (CSV)   │    │  (nulls) │    │  (columns)  │    │  (bands) │    │  (CSV+Parquet)│
└──────────┘    └──────────┘    └─────────────┘    └──────────┘    └───────────────┘
```

| Stage | Operation | Details |
|-------|-----------|---------|
| **Read** | `spark.read.csv()` | Explicit schema, `nullValue=""` |
| **Clean** | `fillna()` | Impute salary with median, defaults for others |
| **Transform** | `withColumn`, `withColumnRenamed` | Add bonus, salary band, tenure; rename columns |
| **Filter** | `filter()` | Mid/Senior band, rating ≥ 3.5 |
| **Write** | `write.csv()`, `write.parquet()` | Both formats for comparison |

---

## 11. Best Practices

1. **Avoid `collect()` on large datasets** — use `show(n)` or `take(n)`.
2. **Use Parquet** for storage — columnar, compressed, schema-aware.
3. **Define schemas explicitly** — avoid `inferSchema=True` for production.
4. **Partition data** — `partitionBy("column")` for efficient reads.
5. **Minimize shuffles** — reduce `groupBy`/`join`; use broadcast joins.
6. **Cache wisely** — `persist()` DataFrames that are reused multiple times.
7. **Monitor via Spark UI** — `http://localhost:4040` during execution.

---

## How to Run

### Prerequisites
- Python 3.8+
- Java 8 or 11 (for Spark)
- PySpark installed (`pip install pyspark`)

### Execution
```bash
cd "d:\csi celebal\assignment 6"
python spark_assignment.py
```

### Output Files
```
output/
├── employees_csv/              # Raw data → CSV
├── employees_parquet/          # Raw data → Parquet
├── pipeline_result_csv/        # Pipeline output → CSV
├── pipeline_result_parquet/    # Pipeline output → Parquet
└── partitioned_by_dept/        # Department-partitioned Parquet
```

---

## Project Structure

```
assignment 6/
├── data/
│   └── employees.csv           # Sample dataset (30 employees)
├── output/                     # Generated output files
├── spark_assignment.py         # Main PySpark script
└── README.md                   # This documentation
```
