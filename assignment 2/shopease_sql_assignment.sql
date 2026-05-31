-- ============================================================
--  ShopEase E-Commerce Sales Database
--  SQL Assignment -- Celebal Summer Internship 2026, Week 2
--  Author  : Raman Nimiwal
--  Scope   : Schema design, querying, indexing, and transactions
--  Compat  : SQLite / MySQL / PostgreSQL
-- ============================================================


-- ============================================================
-- SECTION 0 -- Schema Definition (DDL)
-- ============================================================
-- We start by setting up the four core tables that model a
-- simplified e-commerce workflow: customers place orders,
-- each order contains one or more line items, and every item
-- maps to a product in the catalog.

CREATE TABLE IF NOT EXISTS customers (
    customer_id  INT           PRIMARY KEY,
    first_name   VARCHAR(50)   NOT NULL,
    last_name    VARCHAR(50)   NOT NULL,
    email        VARCHAR(100)  UNIQUE NOT NULL,
    city         VARCHAR(50)   NOT NULL,
    state        VARCHAR(50)   NOT NULL,
    join_date    DATE          NOT NULL,
    is_premium   BOOLEAN       DEFAULT FALSE
);

-- Indexes on city and state speed up location-based filtering,
-- which is common in regional sales reports.
CREATE INDEX IF NOT EXISTS idx_customers_city  ON customers(city);
CREATE INDEX IF NOT EXISTS idx_customers_state ON customers(state);

CREATE TABLE IF NOT EXISTS products (
    product_id   INT            PRIMARY KEY,
    product_name VARCHAR(100)   NOT NULL,
    category     VARCHAR(50)    NOT NULL,
    brand        VARCHAR(50)    NOT NULL,
    unit_price   DECIMAL(10,2)  NOT NULL CHECK (unit_price > 0),
    stock_qty    INT            NOT NULL DEFAULT 0 CHECK (stock_qty >= 0)
);

CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);

CREATE TABLE IF NOT EXISTS orders (
    order_id     INT            PRIMARY KEY,
    customer_id  INT            NOT NULL,
    order_date   DATE           NOT NULL,
    status       VARCHAR(20)    NOT NULL DEFAULT 'Pending'
                 CHECK (status IN ('Pending','Shipped','Delivered','Cancelled')),
    total_amount DECIMAL(12,2)  NOT NULL CHECK (total_amount >= 0),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Date and status are the two most frequently filtered columns
-- in the orders table, so both get their own index.
CREATE INDEX IF NOT EXISTS idx_orders_date   ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

CREATE TABLE IF NOT EXISTS order_items (
    item_id      INT            PRIMARY KEY,
    order_id     INT            NOT NULL,
    product_id   INT            NOT NULL,
    quantity     INT            NOT NULL CHECK (quantity > 0),
    unit_price   DECIMAL(10,2)  NOT NULL CHECK (unit_price > 0),
    discount_pct DECIMAL(5,2)   DEFAULT 0 CHECK (discount_pct BETWEEN 0 AND 100),
    FOREIGN KEY (order_id)   REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);


-- ============================================================
-- SECTION 0 -- Sample Data
-- ============================================================
-- A small but realistic dataset: 8 customers, 8 products across
-- 3 categories, 10 orders, and 15 individual line items.

INSERT INTO customers VALUES
(101, 'Aarav',  'Sharma', 'aarav.s@email.com',  'Mumbai',    'Maharashtra', '2024-01-15', TRUE),
(102, 'Priya',  'Patel',  'priya.p@email.com',  'Ahmedabad', 'Gujarat',     '2024-02-20', FALSE),
(103, 'Rohan',  'Gupta',  'rohan.g@email.com',  'Delhi',     'Delhi',       '2024-03-10', TRUE),
(104, 'Sneha',  'Reddy',  'sneha.r@email.com',  'Hyderabad', 'Telangana',   '2024-04-05', FALSE),
(105, 'Vikram', 'Singh',  'vikram.s@email.com', 'Jaipur',    'Rajasthan',   '2024-05-12', TRUE),
(106, 'Ananya', 'Iyer',   'ananya.i@email.com', 'Chennai',   'Tamil Nadu',  '2024-06-18', FALSE),
(107, 'Karan',  'Mehta',  'karan.m@email.com',  'Pune',      'Maharashtra', '2024-07-22', TRUE),
(108, 'Divya',  'Nair',   'divya.n@email.com',  'Kochi',     'Kerala',      '2024-08-30', FALSE);

INSERT INTO products VALUES
(201, 'Wireless Earbuds',     'Electronics', 'BoAt',          1499.00, 250),
(202, 'Cotton T-Shirt',       'Clothing',    'Levis',          799.00, 500),
(203, 'Smart Watch',          'Electronics', 'Noise',         2999.00, 150),
(204, 'Running Shoes',        'Clothing',    'Nike',          4599.00, 120),
(205, 'Bluetooth Speaker',    'Electronics', 'JBL',           3499.00, 200),
(206, 'Bedsheet Set',         'Home',        'Spaces',        1299.00, 300),
(207, 'Laptop Stand',         'Electronics', 'AmazonBasics',   899.00, 180),
(208, 'Cushion Covers (Set)', 'Home',        'HomeCenter',     599.00, 400);

INSERT INTO orders VALUES
(1001, 101, '2024-08-01', 'Delivered',  4498.00),
(1002, 102, '2024-08-03', 'Delivered',   799.00),
(1003, 103, '2024-08-05', 'Shipped',    7498.00),
(1004, 101, '2024-08-10', 'Delivered',  3499.00),
(1005, 104, '2024-08-12', 'Cancelled',  2999.00),
(1006, 105, '2024-08-15', 'Delivered',  5898.00),
(1007, 106, '2024-08-18', 'Pending',    1299.00),
(1008, 103, '2024-08-20', 'Delivered',   899.00),
(1009, 107, '2024-08-25', 'Shipped',    6098.00),
(1010, 108, '2024-08-28', 'Delivered',  1598.00);

INSERT INTO order_items VALUES
(5001, 1001, 201, 2, 1499.00, 0),
(5002, 1001, 207, 1,  899.00, 10),
(5003, 1002, 202, 1,  799.00, 0),
(5004, 1003, 203, 1, 2999.00, 0),
(5005, 1003, 204, 1, 4599.00, 5),
(5006, 1004, 205, 1, 3499.00, 0),
(5007, 1005, 203, 1, 2999.00, 0),
(5008, 1006, 201, 1, 1499.00, 10),
(5009, 1006, 204, 1, 4599.00, 5),
(5010, 1007, 206, 1, 1299.00, 0),
(5011, 1008, 207, 1,  899.00, 0),
(5012, 1009, 205, 1, 3499.00, 0),
(5013, 1009, 208, 2,  599.00, 15),
(5014, 1010, 206, 1, 1299.00, 0),
(5015, 1010, 208, 1,  599.00, 0);


-- ============================================================
-- SECTION A -- Fundamentals (SELECT, Constraints, Primary Keys)
-- ============================================================

-- Q1. Full dump of the customers table -- useful as a quick
--     sanity check after the initial data load.
SELECT * FROM customers;

-- Q2. Projecting only the columns we need. In production,
--     fetching fewer columns means less data over the wire.
SELECT first_name, last_name, city
FROM customers;

-- Q3. Distinct categories help us understand the breadth of
--     ShopEase's product catalog at a glance.
SELECT DISTINCT category
FROM products
ORDER BY category;

-- Q4. PRIMARY KEY rationale
--     Each table has a single-column integer primary key:
--       customers   -> customer_id
--       products    -> product_id
--       orders      -> order_id
--       order_items -> item_id
--
--     The PK must be UNIQUE so that every row is individually
--     addressable. It must also be NOT NULL because SQL treats
--     two NULLs as non-equal -- a NULL key could never reliably
--     participate in a join or a foreign-key lookup.

-- Q5. Constraints on the email column
--     Two constraints work together here:
--       UNIQUE  -- guarantees no two customers share an address.
--       NOT NULL -- ensures every record has a valid email.
--
--     If we try to insert a row whose email already exists, the
--     database raises "UNIQUE constraint failed: customers.email"
--     and the insert is rejected outright -- the existing row
--     remains untouched.

-- Q6. Demonstrating the CHECK constraint on unit_price
--     The table definition enforces CHECK (unit_price > 0).
--     If we attempt:
--
--       INSERT INTO products VALUES
--         (209, 'Broken Item', 'Electronics', 'Brand', -50.00, 0);
--
--     the engine immediately raises "CHECK constraint failed:
--     unit_price" and aborts the insert. This means pricing
--     validation lives in the database itself -- the application
--     layer doesn't have to duplicate the rule.


-- ============================================================
-- SECTION B -- Filtering & Optimization (WHERE, Indexes)
-- ============================================================

-- Q7. Filtering by order status to see only completed sales.
SELECT order_id, customer_id, order_date, status, total_amount
FROM orders
WHERE status = 'Delivered';

-- Q8. Finding electronics priced above Rs.2,000 -- handy for
--     identifying high-margin items worth promoting.
SELECT product_id, product_name, category, unit_price
FROM products
WHERE category = 'Electronics'
  AND unit_price > 2000;

-- Q9. Customers who joined in 2024 and are based in Maharashtra.
--     The date comparison uses literal boundaries rather than
--     wrapping the column in YEAR(), keeping the query SARGable
--     so the index on join_date can be used.
SELECT customer_id, first_name, last_name, city, state, join_date
FROM customers
WHERE join_date >= '2024-01-01'
  AND join_date <  '2025-01-01'
  AND state = 'Maharashtra';

-- Q10. Active orders placed between 10-25 August 2024.
--      We exclude cancelled orders since they don't contribute
--      to fulfilled revenue.
SELECT order_id, order_date, status, total_amount
FROM orders
WHERE order_date BETWEEN '2024-08-10' AND '2024-08-25'
  AND status <> 'Cancelled';

-- Q11. How the idx_orders_date index helps
--     Without the index the engine has to scan every row in the
--     orders table and check each date -- a full table scan.
--     The B-Tree index keeps dates in sorted order, so the engine
--     can binary-search to the start of the range and walk forward,
--     touching only the rows it needs. Performance goes from O(n)
--     to roughly O(log n + k) where k is the number of matches.
--
--     The query below directly benefits from this index:
SELECT order_id, customer_id, total_amount
FROM orders
WHERE order_date BETWEEN '2024-08-01' AND '2024-08-15';

-- Q12. SARGability -- keeping queries index-friendly
--     A common mistake is wrapping an indexed column in a function:
--
--       SELECT * FROM customers WHERE YEAR(join_date) = 2024;
--
--     This forces the engine to evaluate YEAR() on every stored
--     value before it can compare, which defeats the index entirely.
--     The equivalent SARGable rewrite compares the raw column against
--     boundary literals, letting the B-Tree do its job:
SELECT customer_id, first_name, last_name, join_date
FROM customers
WHERE join_date >= '2024-01-01'
  AND join_date <  '2025-01-01';


-- ============================================================
-- SECTION C -- Aggregation (GROUP BY, SUM, COUNT, AVG, MIN, MAX)
-- ============================================================

-- Q13. A quick count to confirm how many orders are in the system.
SELECT COUNT(*) AS total_orders
FROM orders;

-- Q14. Revenue from delivered orders only -- this is effectively
--      the confirmed, collected revenue.
SELECT SUM(total_amount) AS delivered_revenue
FROM orders
WHERE status = 'Delivered';

-- Q15. Average unit price by category, sorted highest first.
--      This helps us understand which verticals carry higher-
--      ticket products.
SELECT   category,
         ROUND(AVG(unit_price), 2) AS avg_price
FROM     products
GROUP BY category
ORDER BY avg_price DESC;

-- Q16. Order count and total revenue broken down by status.
--      Useful for tracking pipeline health at a glance.
SELECT   status,
         COUNT(*)                   AS order_count,
         ROUND(SUM(total_amount),2) AS total_revenue
FROM     orders
GROUP BY status
ORDER BY total_revenue DESC;

-- Q17. Price range (max and min) within each category.
--      A wide spread may indicate mixed positioning in that vertical.
SELECT   category,
         MAX(unit_price) AS max_price,
         MIN(unit_price) AS min_price
FROM     products
GROUP BY category;

-- Q18. Categories whose average price exceeds Rs.2,000.
--      HAVING filters on aggregated results, unlike WHERE which
--      filters individual rows before grouping.
SELECT   category,
         ROUND(AVG(unit_price), 2) AS avg_price
FROM     products
GROUP BY category
HAVING   avg_price > 2000;


-- ============================================================
-- SECTION D -- Joins & Relationships
-- ============================================================

-- Q19. INNER JOIN -- pairing each order with its customer's name.
--      Only rows with a match on both sides are returned.
SELECT o.order_id,
       o.order_date,
       c.first_name,
       c.last_name,
       o.total_amount
FROM   orders    o
INNER JOIN customers c ON o.customer_id = c.customer_id
ORDER BY o.order_date;

-- Q20. LEFT JOIN -- every customer appears in the result, even
--      those who haven't placed any orders yet (they show NULLs
--      in the order columns). Handy for finding dormant users.
SELECT c.customer_id,
       c.first_name,
       c.last_name,
       o.order_id,
       o.total_amount
FROM   customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
ORDER BY c.customer_id;

-- Q21. Three-table join: orders -> order_items -> products.
--      This gives us line-level detail including product names,
--      quantities, and any applied discounts.
SELECT oi.order_id,
       p.product_name,
       oi.quantity,
       oi.unit_price,
       oi.discount_pct
FROM   order_items oi
INNER JOIN products p ON oi.product_id = p.product_id
INNER JOIN orders   o ON oi.order_id   = o.order_id
ORDER BY oi.order_id;

-- Q22. Comparing join types:
--
--   LEFT JOIN  -- returns all rows from the left table; unmatched
--     rows on the right side come back as NULLs.
--     Typical use: "Show every customer, whether or not they ordered."
--
--   RIGHT JOIN -- mirrors the LEFT JOIN: all rows from the right
--     table are kept, with NULLs filling in for missing left-side data.
--     Typical use: "Show every order, even if the customer record is missing."
--
--   FULL OUTER JOIN -- combines both directions; NULLs appear on
--     whichever side has no match.
--     Typical use: "Audit for orphan orders AND inactive customers in one pass."
--
-- LEFT JOIN example (same idea as Q20):
SELECT c.customer_id, c.first_name, o.order_id
FROM   customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id;

-- Equivalent result using RIGHT JOIN (tables swapped):
SELECT c.customer_id, c.first_name, o.order_id
FROM   orders o
RIGHT JOIN customers c ON o.customer_id = c.customer_id;

-- Q23. Foreign key relationships in this schema:
--     orders.customer_id     -> customers.customer_id
--     order_items.order_id   -> orders.order_id
--     order_items.product_id -> products.product_id
--
--   If we try to insert an order referencing a customer_id that
--   doesn't exist, the database raises "FOREIGN KEY constraint
--   failed" and rejects the insert. This is referential integrity
--   in action -- it prevents orphan records at the database level.
--   (Note: in SQLite, run "PRAGMA foreign_keys = ON;" first.)


-- ============================================================
-- SECTION E -- Advanced Concepts (CASE, ACID, Transactions)
-- ============================================================

-- Q24. Classifying products into pricing tiers using CASE.
--      This is a clean way to add business logic directly in SQL
--      without needing a lookup table.
SELECT product_name,
       unit_price,
       CASE
           WHEN unit_price <  1000 THEN 'Budget'
           WHEN unit_price <= 3000 THEN 'Mid-Range'
           ELSE                         'Premium'
       END AS price_tier
FROM   products
ORDER BY unit_price;

-- Q25. Pivoting order statuses into a single summary row.
--      CASE inside SUM is a classic technique for producing
--      crosstab-style output without a PIVOT keyword.
SELECT
    SUM(CASE WHEN status = 'Delivered'  THEN 1 ELSE 0 END) AS delivered,
    SUM(CASE WHEN status <> 'Delivered' THEN 1 ELSE 0 END) AS not_delivered
FROM orders;

-- Q26. ACID properties -- the four guarantees every relational
--      database makes about transactions:
--
--   Atomicity   -- A transaction is all-or-nothing. Think of a bank
--     transfer: the debit from one account and the credit to another
--     either both happen, or neither does. Money can't vanish midway.
--
--   Consistency -- The database always moves from one valid state to
--     another. Constraints (NOT NULL, CHECK, FK) are enforced at
--     commit time, so invalid data never becomes permanent.
--
--   Isolation   -- Concurrent transactions don't see each other's
--     in-progress changes. Two simultaneous withdrawals from the
--     same account won't both read the old balance and overdraw it.
--
--   Durability  -- Once a COMMIT is acknowledged, the data survives
--     crashes, power failures, and restarts. The write is on disk
--     (or WAL) before the client gets confirmation.

-- Q27. An atomic transaction that inserts a new order with two
--      line items and decrements stock in a single unit of work.
--      If any statement fails, the ROLLBACK undoes everything.
BEGIN;

    -- Create the order header
    INSERT INTO orders (order_id, customer_id, order_date, status, total_amount)
    VALUES (1011, 102, CURRENT_DATE, 'Pending', 1598.00);

    -- Add two line items
    INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price, discount_pct)
    VALUES (5016, 1011, 206, 1, 999.00, 0);

    INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price, discount_pct)
    VALUES (5017, 1011, 208, 1, 599.00, 0);

    -- Reduce inventory for the purchased products
    UPDATE products
    SET    stock_qty = stock_qty - 1
    WHERE  product_id IN (206, 208);

COMMIT;
-- If any step above fails, replace COMMIT with ROLLBACK to
-- discard all changes and leave the database in its prior state.


-- ============================================================
-- SECTION F -- Data Quality Checks
-- ============================================================
-- These queries act as lightweight validation scripts. In a real
-- pipeline, you'd run them after every ETL load and alert on
-- any non-empty result set.

-- Row counts -- quick confirmation that the load is complete.
SELECT 'customers'   AS tbl, COUNT(*) AS rows FROM customers
UNION ALL
SELECT 'products',            COUNT(*)        FROM products
UNION ALL
SELECT 'orders',              COUNT(*)        FROM orders
UNION ALL
SELECT 'order_items',         COUNT(*)        FROM order_items;

-- Duplicate emails -- the UNIQUE constraint prevents them, but
-- this query is a useful cross-check if data was bulk-loaded
-- with constraints temporarily disabled.
SELECT email, COUNT(*) AS cnt
FROM   customers
GROUP BY email
HAVING cnt > 1;

-- Orphan line items that reference a non-existent order.
SELECT oi.item_id
FROM   order_items oi
LEFT JOIN orders o ON oi.order_id = o.order_id
WHERE  o.order_id IS NULL;

-- Orders that have no associated line items -- could indicate
-- an incomplete data load or a bug in the order-creation flow.
SELECT o.order_id
FROM   orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
WHERE  oi.item_id IS NULL;

-- Products that are out of stock or incorrectly negative.
SELECT product_id, product_name, stock_qty
FROM   products
WHERE  stock_qty <= 0;

-- Revenue discrepancy: the stored total_amount on the order
-- header vs. the sum recomputed from individual line items
-- (factoring in discounts). A mismatch usually means shipping
-- fees or taxes are embedded in the header but not modeled as
-- separate line items.
SELECT o.order_id,
       o.total_amount                                        AS header_total,
       ROUND(SUM(oi.quantity * oi.unit_price
             * (1 - oi.discount_pct / 100.0)), 2)           AS computed_total
FROM   orders      o
JOIN   order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id
HAVING ABS(header_total - computed_total) > 0.01;

-- ============================================================
-- END OF SCRIPT
-- ============================================================
