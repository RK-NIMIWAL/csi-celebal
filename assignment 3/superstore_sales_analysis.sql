-- Week 3 Assignment - Superstore Sales Analysis
-- Using Subqueries, CTEs and Window Functions
-- Dataset: https://www.kaggle.com/datasets/vivek468/superstore-dataset-final

-- ===================
-- STEP 1: Data Setup
-- ===================

CREATE DATABASE IF NOT EXISTS superstore_db;
USE superstore_db;

-- creating the raw table to load CSV data into
DROP TABLE IF EXISTS superstore_raw;

CREATE TABLE superstore_raw (
    row_id        INT PRIMARY KEY,
    order_id      VARCHAR(20),
    order_date    DATE,
    ship_date     DATE,
    ship_mode     VARCHAR(20),
    customer_id   VARCHAR(10),
    customer_name VARCHAR(50),
    segment       VARCHAR(20),
    country       VARCHAR(40),
    city          VARCHAR(40),
    state         VARCHAR(40),
    postal_code   INT,
    region        VARCHAR(20),
    product_id    VARCHAR(20),
    category      VARCHAR(20),
    sub_category  VARCHAR(20),
    product_name  VARCHAR(200),
    sales         DECIMAL(10,2),
    quantity      INT,
    discount      DECIMAL(4,2),
    profit        DECIMAL(10,2)
);

-- Load CSV into the table
-- For MySQL:
--   LOAD DATA INFILE 'path/to/SampleSuperstore.csv'
--   INTO TABLE superstore_raw
--   FIELDS TERMINATED BY ',' ENCLOSED BY '"'
--   LINES TERMINATED BY '\n'
--   IGNORE 1 ROWS;

-- quick check
SELECT COUNT(*) AS total_rows FROM superstore_raw;
SELECT * FROM superstore_raw LIMIT 10;


-- now creating the 3 normalized tables from the raw data

-- customers table
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id   VARCHAR(10) PRIMARY KEY,
    customer_name VARCHAR(50),
    segment       VARCHAR(20),
    country       VARCHAR(40),
    city          VARCHAR(40),
    state         VARCHAR(40),
    postal_code   INT,
    region        VARCHAR(20)
);

INSERT INTO customers
SELECT DISTINCT
    customer_id, customer_name, segment,
    country, city, state, postal_code, region
FROM superstore_raw;

SELECT COUNT(*) AS total_customers FROM customers;
SELECT * FROM customers LIMIT 5;


-- products table
DROP TABLE IF EXISTS products;

CREATE TABLE products (
    product_id   VARCHAR(20) PRIMARY KEY,
    category     VARCHAR(20),
    sub_category VARCHAR(20),
    product_name VARCHAR(200)
);

INSERT INTO products
SELECT DISTINCT
    product_id, category, sub_category, product_name
FROM superstore_raw;

SELECT COUNT(*) AS total_products FROM products;
SELECT * FROM products LIMIT 5;


-- orders table
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    row_id      INT PRIMARY KEY,
    order_id    VARCHAR(20),
    order_date  DATE,
    ship_date   DATE,
    ship_mode   VARCHAR(20),
    customer_id VARCHAR(10),
    product_id  VARCHAR(20),
    sales       DECIMAL(10,2),
    quantity    INT,
    discount    DECIMAL(4,2),
    profit      DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id)  REFERENCES products(product_id)
);

INSERT INTO orders
SELECT row_id, order_id, order_date, ship_date, ship_mode,
       customer_id, product_id, sales, quantity, discount, profit
FROM superstore_raw;

SELECT COUNT(*) AS total_orders FROM orders;
SELECT * FROM orders LIMIT 5;


-- ============================
-- STEP 2: Required Queries
-- ============================

-- Q1: orders where sales > average sales (Subquery)
SELECT
    o.row_id,
    o.order_id,
    c.customer_name,
    p.product_name,
    o.sales
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE o.sales > (SELECT AVG(sales) FROM orders)
ORDER BY o.sales DESC;
-- about 20% of orders are above average but they make up most of the revenue


-- Q2: highest sales order for each customer (Correlated Subquery)
SELECT
    o.order_id,
    c.customer_name,
    o.sales AS highest_sale
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.sales = (
    SELECT MAX(o2.sales)
    FROM orders o2
    WHERE o2.customer_id = o.customer_id
)
ORDER BY o.sales DESC;
-- some customers have max orders over $10k while others barely cross $10


-- Q3: total sales per customer (CTE)
WITH customer_total_sales AS (
    SELECT
        customer_id,
        SUM(sales) AS total_sales,
        COUNT(DISTINCT order_id) AS total_orders
    FROM orders
    GROUP BY customer_id
)
SELECT
    c.customer_name,
    cts.total_sales,
    cts.total_orders
FROM customer_total_sales cts
JOIN customers c ON cts.customer_id = c.customer_id
ORDER BY cts.total_sales DESC;
-- huge variation in customer spending, ranges from under $10 to $25k+


-- Q4: customers with total sales above average (CTE + Subquery)
WITH customer_total_sales AS (
    SELECT customer_id, SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
)
SELECT
    c.customer_name,
    cts.total_sales
FROM customer_total_sales cts
JOIN customers c ON cts.customer_id = c.customer_id
WHERE cts.total_sales > (SELECT AVG(total_sales) FROM customer_total_sales)
ORDER BY cts.total_sales DESC;
-- only around 30-35% of customers are above the average


-- Q5: rank customers by total sales (Window Function - RANK)
WITH customer_total_sales AS (
    SELECT customer_id, SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
)
SELECT
    c.customer_name,
    cts.total_sales,
    RANK() OVER (ORDER BY cts.total_sales DESC) AS sales_rank
FROM customer_total_sales cts
JOIN customers c ON cts.customer_id = c.customer_id
ORDER BY sales_rank;
-- top 10 customers contribute way more than the rest


-- Q6: row numbers for each order within a customer (ROW_NUMBER + PARTITION BY)
SELECT
    c.customer_name,
    o.order_id,
    o.order_date,
    o.sales,
    ROW_NUMBER() OVER (
        PARTITION BY o.customer_id
        ORDER BY o.order_date, o.order_id
    ) AS order_seq
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
ORDER BY c.customer_name, order_seq;
-- most customers have 1-10 orders, helps track repeat buying patterns


-- Q7: top 3 customers by total sales (Window Function - DENSE_RANK)
WITH customer_total_sales AS (
    SELECT customer_id, SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
),
ranked AS (
    SELECT
        customer_id, total_sales,
        DENSE_RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
    FROM customer_total_sales
)
SELECT
    c.customer_name, r.total_sales, r.sales_rank
FROM ranked r
JOIN customers c ON r.customer_id = c.customer_id
WHERE r.sales_rank <= 3
ORDER BY r.sales_rank;
-- these top 3 are really important for the business


-- ================================
-- STEP 3: Final Combined Query
-- JOIN + CTE + Window Function
-- ================================

WITH customer_total_sales AS (
    SELECT
        customer_id,
        SUM(sales) AS total_sales,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(profit) AS total_profit
    FROM orders
    GROUP BY customer_id
),
ranked_customers AS (
    SELECT
        customer_id, total_sales, total_orders, total_profit,
        RANK() OVER (ORDER BY total_sales DESC) AS sales_rank,
        DENSE_RANK() OVER (ORDER BY total_sales DESC) AS dense_rank
    FROM customer_total_sales
)
SELECT
    c.customer_name,
    c.segment,
    c.region,
    rc.total_sales,
    rc.total_orders,
    rc.total_profit,
    rc.sales_rank,
    rc.dense_rank
FROM ranked_customers rc
JOIN customers c ON rc.customer_id = c.customer_id
ORDER BY rc.sales_rank;
-- this gives a full picture - who the customer is, how much they spent, and where they rank


-- ====================================
-- MINI PROJECT: Customer Sales Insights
-- ====================================

-- Q1: Top 5 customers
WITH customer_totals AS (
    SELECT
        o.customer_id, c.customer_name, c.segment,
        SUM(o.sales) AS total_sales,
        SUM(o.profit) AS total_profit,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY o.customer_id, c.customer_name, c.segment
),
ranked AS (
    SELECT *, RANK() OVER (ORDER BY total_sales DESC) AS rnk
    FROM customer_totals
)
SELECT customer_name, segment, total_sales, total_profit, order_count
FROM ranked WHERE rnk <= 5
ORDER BY rnk;
-- losing even one of these customers would hurt quarterly numbers


-- Q2: Bottom 5 customers
WITH customer_totals AS (
    SELECT
        o.customer_id, c.customer_name, c.segment,
        SUM(o.sales) AS total_sales,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY o.customer_id, c.customer_name, c.segment
),
ranked AS (
    SELECT *, RANK() OVER (ORDER BY total_sales ASC) AS rnk
    FROM customer_totals
)
SELECT customer_name, segment, total_sales, order_count
FROM ranked WHERE rnk <= 5
ORDER BY rnk;
-- mostly one-time buyers with very small orders


-- Q3: Customers who made only one order
WITH single_order AS (
    SELECT customer_id, COUNT(DISTINCT order_id) AS order_count
    FROM orders
    GROUP BY customer_id
    HAVING COUNT(DISTINCT order_id) = 1
)
SELECT
    c.customer_name, c.segment, c.region,
    s.order_count, os.total_sales
FROM single_order s
JOIN customers c ON s.customer_id = c.customer_id
JOIN (
    SELECT customer_id, SUM(sales) AS total_sales
    FROM orders GROUP BY customer_id
) os ON s.customer_id = os.customer_id
ORDER BY os.total_sales DESC;
-- these customers are at risk of churning, could use a follow-up campaign


-- Q4: Customers with above-average total sales
WITH customer_totals AS (
    SELECT customer_id, SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
),
avg_sales AS (
    SELECT AVG(total_sales) AS avg_total FROM customer_totals
)
SELECT
    c.customer_name, c.segment, c.region,
    ct.total_sales,
    a.avg_total AS avg_total_sales,
    ROUND(ct.total_sales - a.avg_total, 2) AS above_avg_by
FROM customer_totals ct
JOIN customers c ON ct.customer_id = c.customer_id
CROSS JOIN avg_sales a
WHERE ct.total_sales > a.avg_total
ORDER BY ct.total_sales DESC;
-- these are the growth drivers of the business


-- Q5: Highest order value per customer
WITH ranked_orders AS (
    SELECT
        o.customer_id, o.order_id, o.sales,
        p.product_name, p.category,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.sales DESC) AS rn
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
)
SELECT
    c.customer_name, c.segment,
    ro.order_id,
    ro.sales AS highest_order_value,
    ro.product_name,
    ro.category
FROM ranked_orders ro
JOIN customers c ON ro.customer_id = c.customer_id
WHERE ro.rn = 1
ORDER BY ro.sales DESC;
-- biggest orders are usually from Technology category (copiers, machines etc)


-- ========================
-- Bonus: Customer Dashboard
-- ========================

WITH metrics AS (
    SELECT
        o.customer_id,
        SUM(o.sales) AS total_sales,
        SUM(o.profit) AS total_profit,
        COUNT(DISTINCT o.order_id) AS total_orders,
        AVG(o.sales) AS avg_order_value,
        MAX(o.sales) AS max_single_order,
        MIN(o.order_date) AS first_order,
        MAX(o.order_date) AS last_order
    FROM orders o
    GROUP BY o.customer_id
),
avg_cte AS (
    SELECT AVG(total_sales) AS avg_customer_sales FROM metrics
),
final AS (
    SELECT
        m.*,
        a.avg_customer_sales,
        RANK() OVER (ORDER BY m.total_sales DESC) AS sales_rank,
        NTILE(4) OVER (ORDER BY m.total_sales DESC) AS quartile,
        CASE
            WHEN m.total_sales > a.avg_customer_sales THEN 'Above Average'
            ELSE 'Below Average'
        END AS sales_tier,
        CASE
            WHEN m.total_orders = 1 THEN 'One-Time Buyer'
            WHEN m.total_orders BETWEEN 2 AND 5 THEN 'Occasional'
            WHEN m.total_orders BETWEEN 6 AND 10 THEN 'Regular'
            ELSE 'Loyal'
        END AS buyer_type
    FROM metrics m
    CROSS JOIN avg_cte a
)
SELECT
    c.customer_name, c.segment, c.region,
    ROUND(f.total_sales, 2) AS total_sales,
    ROUND(f.total_profit, 2) AS total_profit,
    f.total_orders,
    ROUND(f.avg_order_value, 2) AS avg_order_value,
    ROUND(f.max_single_order, 2) AS max_single_order,
    f.first_order, f.last_order,
    f.sales_rank, f.quartile,
    f.sales_tier, f.buyer_type
FROM final f
JOIN customers c ON f.customer_id = c.customer_id
ORDER BY f.sales_rank;
-- this combines everything into one view - ranks, tiers, buyer types etc.
