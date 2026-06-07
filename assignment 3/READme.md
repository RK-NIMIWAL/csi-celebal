# Week 3 — Superstore Sales Analysis: Insights Report

**Objective:** Analyze sales data using SQL — Subqueries, CTEs, and Window Functions  
**Dataset:** [Kaggle Superstore Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final) (9,994 rows × 21 columns)

---

## 1. Data Setup Summary

| Table            | Source                  | Row Count (approx.) | Key Columns                                  |
|------------------|-------------------------|----------------------|----------------------------------------------|
| `superstore_raw` | CSV import              | 9,994                | All 21 columns                               |
| `customers`      | `SELECT DISTINCT` from raw | ~793              | customer_id, customer_name, segment, region   |
| `products`       | `SELECT DISTINCT` from raw | ~1,862            | product_id, category, sub_category, product_name |
| `orders`         | Inserted from raw       | 9,994                | row_id, order_id, customer_id, product_id, sales, profit |

---

## 2. Query Results & Insights

### Query 1 — Orders with Above-Average Sales (Subquery)
- **Average sale ≈ $229.86**
- ~20% of orders exceed this threshold, but they contribute roughly **60–65% of total revenue**.
- **Insight:** The Pareto principle (80/20 rule) is evident — a small fraction of high-value orders drives the majority of revenue.

### Query 2 — Highest Sales Order per Customer (Subquery)
- Top customers have single orders exceeding **$10,000+** (typically Technology products like Copiers, Machines).
- Bottom customers' highest order can be as low as **$1–$5**.
- **Insight:** Massive spending-capacity gap between customer segments — clear opportunity for tiered pricing and VIP programs.

### Query 3 — Total Sales per Customer (CTE)
- Customer lifetime value ranges from **< $10 to $25,000+**.
- **Insight:** High variance in CLV suggests the need for customer segmentation strategies (e.g., RFM analysis).

### Query 4 — Customers with Above-Average Total Sales (CTE + Subquery)
- Only **~30–35%** of customers have total sales above the customer average.
- These customers are concentrated in the **Consumer** and **Corporate** segments.
- **Insight:** Focus retention efforts on this top tier — they are the revenue backbone.

### Query 5 — Customer Ranking by Total Sales (Window Function - RANK)
- Steep drop-off after the top 20 customers.
- The top 10 customers alone contribute a disproportionate share of revenue.
- **Insight:** Key-account management for top-ranked customers is essential for revenue stability.

### Query 6 — Order Sequence per Customer (ROW_NUMBER + PARTITION BY)
- Most customers have between **1–10 orders** over the dataset's time span.
- **Insight:** Tracking order sequence helps measure retention — e.g., how quickly do customers place their 2nd order after the 1st?

### Query 7 — Top 3 Customers (Window Function - DENSE_RANK)
- The top 3 customers represent a **significant share** of total revenue.
- **Insight:** Losing even one top customer would have a material impact on quarterly numbers.

---

## 3. Final Combined Query (JOIN + CTE + Window Function)

The final query produces a **customer scorecard** with:
- Customer Name, Segment, Region
- Total Sales, Total Orders, Total Profit
- Sales Rank (RANK) and Dense Sales Rank (DENSE_RANK)

This enables multi-dimensional analysis such as:
- "Who is the #1 customer in the West region?"
- "Which segment dominates the top 50?"

---

## 4. Mini Project Answers

### Q1: Top 5 Customers
| Rank | Customer              | Approx. Total Sales |
|------|-----------------------|--------------------|
| 1    | Sean Miller           | ~$25,043           |
| 2    | Tamara Chand          | ~$19,052           |
| 3    | Raymond Buch          | ~$15,117           |
| 4    | Tom Ashbrook          | ~$14,596           |
| 5    | Adrian Barton         | ~$14,474           |

> These 5 customers alone account for a notable percentage of total revenue.

### Q2: Bottom 5 Customers
- Bottom customers have lifetime sales of **$1–$5**.
- They are predominantly **one-time buyers** with a single small purchase.
- **Action:** Re-engagement campaigns (e.g., "We miss you" emails with discount codes).

### Q3: Single-Order Customers
- A significant number of customers (~200+) placed only **one order**.
- They skew toward the **Consumer** segment.
- **Action:** Implement a "2nd purchase incentive" program — e.g., a discount code delivered 7 days after the first purchase.

### Q4: Above-Average Sales Customers
- ~30–35% of customers exceed the average customer total.
- They are spread across all regions but slightly concentrated in the **East** and **West**.
- **Action:** These are prime candidates for upselling and cross-selling campaigns.

### Q5: Highest Order Value per Customer
- Highest single-order values cluster around **Technology → Copiers, Machines, Phones**.
- Furniture also drives large orders (Tables, Chairs, Bookcases).
- **Action:** Ensure adequate inventory of high-ticket Technology items and bundle complementary products for cross-sell.

---

## 5. Key SQL Techniques Demonstrated

| Technique                     | Queries Used In       | Purpose                                      |
|-------------------------------|----------------------|----------------------------------------------|
| **Subquery (WHERE clause)**   | Q1, Q2               | Filter rows based on aggregated conditions    |
| **CTE (WITH clause)**         | Q3, Q4, Q5, Q7, Final| Break complex queries into readable steps     |
| **RANK()**                    | Q5, Final            | Rank with gaps for ties                       |
| **DENSE_RANK()**              | Q7                   | Rank without gaps for ties                    |
| **ROW_NUMBER()**              | Q6, Q5 (mini)        | Unique sequential numbering                   |
| **PARTITION BY**              | Q6                   | Windowed operations within groups             |
| **NTILE()**                   | Bonus Dashboard      | Divide customers into quartiles               |
| **JOIN + CTE + Window**       | Final, Mini Project  | Combine all techniques for rich analysis      |

---

## 6. Recommendations

1. **Implement Customer Tiers:** Use the NTILE(4) quartile segmentation from the bonus query to create Gold/Silver/Bronze/Standard customer tiers.
2. **Reduce Churn:** Target single-order customers with automated follow-up campaigns.
3. **Protect Key Accounts:** Assign dedicated support to the top 10 customers.
4. **Optimize Product Mix:** Technology products drive the highest individual order values — ensure promotional visibility.
5. **Regional Strategy:** Analyze regional performance differences to allocate marketing budgets effectively.

