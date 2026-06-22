# Apache Spark Basics — Assignment 5

> **CSI Celebal Technologies**

---

## 📋 Table of Contents

- [Objective](#objective)
- [Folder Structure](#folder-structure)
- [Prerequisites](#prerequisites)
- [How to Run](#how-to-run)
- [Steps Performed](#steps-performed)
- [Key Observations](#key-observations)

---

## 🎯 Objective

Understand Apache Spark fundamentals and perform data cleaning, transformation, and aggregation using PySpark DataFrames on a sample employee dataset.

---

## 📁 Folder Structure

```
spark-assignment/
│── data/
│   └── dataset.csv              # Sample employee dataset (30 rows)
│── notebook/
│   └── spark_basics.py          # PySpark script with all 10 steps
│── output/
│   └── results/                 # Aggregated pipeline output (CSV)
│   └── detailed_results/        # Detailed transformed data (CSV)
│── README.md                    # This file
```

---

## ⚙️ Prerequisites

| Tool             | Version    |
| ---------------- | ---------- |
| Python           | 3.8+       |
| Apache Spark     | 3.x        |
| PySpark          | 3.x        |
| Java (JDK)       | 8 or 11    |

### Installation

```bash
# Install PySpark
pip install pyspark

# Verify installation
python -c "import pyspark; print(pyspark.__version__)"
```

> **Note:** Spark requires Java 8 or 11 to be installed and `JAVA_HOME` environment variable set.

---

## 🚀 How to Run

```bash
# Navigate to the project root
cd "assignment 5"

# Run the PySpark script
python notebook/spark_basics.py

# Or use spark-submit (recommended for cluster environments)
spark-submit notebook/spark_basics.py
```

---

## 📝 Steps Performed

### Step 1 — Understand Basics (Theory)

| Feature           | MapReduce                     | Apache Spark                        |
| ----------------- | ----------------------------- | ----------------------------------- |
| Processing        | Disk-based (slow)             | In-memory (up to 100× faster)      |
| API               | Low-level Map/Reduce          | High-level DataFrames & SQL         |
| Ease of use       | Complex Java boilerplate      | Simple Python/Scala/SQL             |
| Iterative jobs    | Very slow (re-reads from disk)| Fast (data cached in memory)        |
| Optimisation      | Manual                        | Catalyst query optimiser            |

### Step 2 — Start Spark

- Created a `SparkSession` with `local[*]` master (uses all available CPU cores).
- SparkSession is the unified entry point for all Spark functionality.

### Step 3 — Load Data

- Loaded `data/dataset.csv` into a Spark DataFrame.
- The dataset contains **30 rows** and **8 columns**: `employee_id`, `name`, `age`, `department`, `salary`, `region`, `join_date`, `rating`.
- Used `inferSchema=True` to automatically detect data types.

### Step 4 — Data Cleaning

- **Duplicates:** Found and removed **1 duplicate** row (employee_id 1 repeated as 21).
- **Missing values detected:**
  - `salary` — 2 nulls → filled with mean salary
  - `age` — 1 null → filled with mean age
  - `rating` — 2 nulls → filled with mean rating
  - `region` — 1 null → filled with "Unknown"
- After cleaning: **29 rows, 0 nulls**.

### Step 5 — Filter Data

Applied various filter conditions:
- **By age:** Employees older than 35 → 8 employees
- **By department:** Engineering employees → 9 employees
- **By region:** North region employees → 7 employees
- **Combined:** Engineering + salary > 80,000 → 4 employees

### Step 6 — Transform Data

- **Renamed columns:** `name` → `employee_name`, `rating` → `performance_rating`
- **Cast types:** salary to `IntegerType`, rating to `DoubleType`
- **Added derived columns:**
  - `salary_band` — Junior / Mid-Level / Senior / Lead
  - `experience_level` — Early Career / Mid Career / Senior Career

### Step 7 — Aggregation

- **Total records:** 30
- **Average salary:** ~73,000
- **Salary range:** 53,000 – 98,000
- **Average rating:** ~4.1
- Used `describe()` for full summary statistics.

### Step 8 — Group Data

- **By Department:** Engineering has the highest avg salary; HR the lowest.
- **By Region:** Balanced distribution across North, South, East, West.
- **By Salary Band:** Lead-level employees have the highest avg rating.
- **Cross analysis:** Department × Region pivot for headcount and salary insights.

### Step 9 — Advanced Concepts

| Concept                 | Description                                                     |
| ----------------------- | --------------------------------------------------------------- |
| **Narrow Transformation** | Each partition produces one output partition (e.g., `filter`, `select`). No data movement. |
| **Wide Transformation**   | Requires data from multiple partitions (e.g., `groupBy`, `join`). Causes **shuffle**. |
| **Shuffle**               | Redistribution of data across partitions — expensive operation (network + disk I/O). |
| **Lazy Evaluation**       | Transformations are not executed until an **action** (`show`, `count`, `collect`) is called. |

### Step 10 — Complete Pipeline

Built an end-to-end pipeline that combines all steps:

```
Load CSV → Deduplicate → Fill Nulls → Filter (salary > 50K, age 25-50)
         → Rename & Cast → Add Derived Columns → GroupBy Aggregation → Save CSV
```

Output saved to `output/results/` and `output/detailed_results/`.

---

## 🔍 Key Observations

1. **Engineering** department has the highest average salary and most employees.
2. **Finance** department has the most experienced (oldest) employees with the highest individual salary (₹98,000).
3. The dataset had **1 exact duplicate** row and **6 missing values** across 4 columns — all handled successfully.
4. **Salary bands** provide a cleaner way to categorise employees than raw salary numbers.
5. **`groupBy()`** is a wide transformation — it triggers a shuffle, which is visible in the Spark UI.
6. **Lazy evaluation** means Spark optimises the entire pipeline before execution, making it more efficient than step-by-step processing.
7. Using **`coalesce(1)`** before writing ensures the output is a single CSV file (useful for small datasets).

---

## 🛠️ Technologies Used

- **Python 3.x** — Programming language
- **Apache Spark (PySpark)** — Distributed data processing
- **Spark SQL / DataFrames** — Structured data API

---

## 📄 License

This project is submitted as part of the CSI Celebal Technologies Summer Internship 2025.
