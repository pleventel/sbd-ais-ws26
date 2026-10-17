# Exercise 2 &ndash; Solutions

## Activities 2.1 &ndash; PostgreSQL Analytical Queries (E-commerce)
> Generate the dataset (in another terminal, from the `ecommerce` folder):
> 
> ```bash
> cd ecommerce
> python3 dataset_generator.py
> ```
> 
> This writes `data/orders_1M.csv`, which is available inside the PostgreSQL
> container as `/data/orders_1M.csv`. Its columns are:
> 
> ```
> customer_name, product_category, quantity, price_per_unit, order_date, country
> ```
> 
> Load the generated data into PostgreSQL in a **new table** called `orders`.
> Write the `CREATE TABLE` yourself (choose sensible types for each column) and
> use the same `\COPY` pattern as in section 3.3.

Even though, we can note that the attribute `customer_name` is composite, we will not
need it any form that would require it to be transferred to atomic attributes.

~~~sql
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    id SERIAL PRIMARY KEY, --we do not have a PK generated, so we need to add it manually
    customer_name TEXT,
    product_category TEXT,
    quantity INTEGER,
    price_per_unit FLOAT,
    order_date DATE,
    country TEXT
);
~~~

~~~sql
\COPY orders(customer_name, product_category, quantity, price_per_unit, order_date, country) FROM '/data/orders_1M.csv' DELIMITER ',' CSV HEADER;
~~~

> Using SQL ([list of supported SQL commands](https://www.postgresql.org/docs/current/sql-commands.html)),
> answer the following questions:
> 
> **A.** Which order has the highest `price_per_unit`?
> 
> **B.** What are the top 3 product categories with the highest total quantity
> sold across all orders?
> 
> **C.** What is the total revenue per product category?
> (Revenue = `price_per_unit × quantity`)
> 
> **D.** Who are the top 5 customers by total spending?
> 
> **E.** Look at the spending totals in D — and at how many orders each of those
> customers has. What do you notice? Open `ecommerce/dataset_generator.py` and
> explain *why* the data looks like this.

<details>
<summary><b>A.</b> Which order has the highest <code>price_per_unit</code>?</summary>
<br>

~~~sql
SELECT *
FROM orders
ORDER BY price_per_unit DESC
LIMIT 1;
~~~

Output:
~~~text
 price_per_unit 
----------------
        2000
(1 row)
~~~
</details>


<details>
<summary><b>B.</b> What are the top 3 product categories with the highest total quantity sold across all orders?</summary>
<br>

~~~sql
SELECT product_category, SUM(quantity) AS quantity_sold
FROM orders
GROUP BY product_category
ORDER BY SUM(quantity) DESC
LIMIT 3;
~~~

Output:
~~~text
 product_category | quantity_sold 
------------------+---------------
 Health & Beauty  |        300842
 Electronics      |        300804
 Toys             |        300598
(3 rows)
~~~
</details>

<details>
<summary><b>C.</b> What is the total revenue per product category? (Revenue = <code>price_per_unit × quantity</code>)</summary>
<br>

~~~sql
SELECT product_category,
       ROUND(SUM(price_per_unit*quantity)::numeric, 2) AS revenue
FROM orders
GROUP BY product_category;
~~~

Output:
~~~text
 product_category |   revenue    
------------------+--------------
 Automotive       | 306589798.86
 Books            |  12731976.04
 Electronics      | 241525009.45
 Fashion          |  31566368.22
 Grocery          |  15268355.66
 Health & Beauty  |  46599817.89
 Home & Garden    |  78023780.09
 Office Supplies  |  38276061.64
 Sports           |  61848990.83
 Toys             |  23271039.02
(10 rows)
~~~
</details>

<details>
<summary><b>D.</b> Who are the top 5 customers by total spending?</summary>
<br>

~~~sql
SELECT customer_name,
       ROUND(SUM(price_per_unit*quantity)::numeric, 2) AS revenue
FROM orders
GROUP BY customer_name
ORDER BY SUM(price_per_unit*quantity) DESC
LIMIT 5;
~~~

Output:
~~~text
 customer_name  |  revenue  
----------------+-----------
 Carol Taylor   | 991179.18
 Nina Lopez     | 975444.95
 Daniel Jackson | 959344.48
 Carol Lewis    | 947708.57
 Daniel Young   | 946030.14
(5 rows)
~~~
</details>

<details>
<summary><b>E.</b> Look at the spending totals in D &ndash; and at how many orders each of those customers has. What do you notice? Open <code>ecommerce/dataset_generator.py</code> and explain <i>why</i> the data looks like this.</summary>
<br>

~~~sql
--Look at how customers exist and how evenly do they order
SELECT COUNT(*)                  AS distinct_names,
       ROUND(AVG(orders), 1)     AS avg_orders,
       MIN(orders)               AS min_orders,
       MAX(orders)               AS max_orders
FROM (SELECT customer_name, COUNT(*) AS orders
      FROM orders GROUP BY customer_name) s;
~~~

Output:
~~~text
 distinct_names | avg_orders | min_orders | max_orders 
----------------+------------+------------+------------
           1056 |      947.0 |        837 |       1043
(1 row)
~~~

**Explanation:** the top-5 "customers" look almost identical: each has roughly 950–1030 orders and a total of about 0.95–0.99M. The reason is in `dataset_generator.py`: `random_customer_name()` draws a first name (32 options) and a last name (33 options) uniformly and independently, so there are only $32 \times 33 = 1056$ distinct names, and every one of them receives about $1 000 000 / 1056 \approx 947$ orders. A customer here is therefore not a person but a name that many different orders share, and there is no customer ID in the data. Category, quantity, price, date and country are drawn independently of the name as well, so no customer is systematically richer than another. The ranking in D is mostly noise: a name that randomly collected $\approx 1030$ orders (Carol Taylor, Daniel Jackson) or a few expensive ones ends up on top. $$\text{Revenue} \approx \text{number of orders} \times \text{average order value}$$ (≈ 856 across the table), and both factors are nearly the same for everyone.
</details>


## Activities 2.2 &ndash; Why Is This Self-Join So Slow?

> Users of the system sometimes run naive queries such as:
> 
> ```sql
> SELECT COUNT(*)
> FROM people_big p1
> JOIN people_big p2
>   ON p1.country = p2.country;
> ```
> 
> On the full 1M-row table this takes **more than 10 minutes** and slows down
> the whole database for everyone else. Your job is to find out *why* before
> proposing a fix.
> 
> > **Do not run it on the full table in class** — use the smaller tables below.
> > (If you are curious, run it at home and let it finish.)
> 
> **Step 1 — Measure how it grows.** Create three smaller copies of the table:
> 
> ```sql
> CREATE TABLE people_50k  AS SELECT * FROM people_big WHERE id <= 50000;
> CREATE TABLE people_100k AS SELECT * FROM people_big WHERE id <= 100000;
> CREATE TABLE people_200k AS SELECT * FROM people_big WHERE id <= 200000;
> ```
> 
> Run the self-join (with `\timing on`) on each of the three tables and fill in:
> 
| rows in table | join result (`COUNT(*)`) | time |
|---|---|---|
| 50 000 | **27 501 822** | **1338.130 ms (00:01.338)** |
| 100 000 | **109 946 508**| **8474.542 ms (00:08.475)** |
| 200 000 | **439 395 606**| **43069.826 ms (00:43.070)** |
> 
> When the input **doubles**, by what factor do the result and the time grow?
> Use this to **predict** the result size and the runtime on `people_big` (1M rows).

The number of rows in the joined table basically grow 4 times when doubling the rows in the table. This means that the result grows by quadratic complexity $O(n^2)$.

The runtime increased on average by **5.7 times** (first 6.33 then 5.08).

**Predictions for 1M rows:** we are increasing the row size for 5 times, meaning the number of rows will be 25 times more. The runtime will be increased by $5.7^{\log_2(5)}$ times.
$$\text{join result: } 439\ 395\ 606 \cdot 25 = 10\ 984\ 890\ 150 \text{ rows.}$$
$$\text{time: } 43\ 069.826 \cdot 5.7^{\log_2(5)} = 2\ 450\ 530.887 \text{ ms } (40:50.530).$$

> **Step 2 — Does an index help?** Create an index on `country` of `people_100k`,
> run `ANALYZE people_100k;`, and repeat the query. Did the time change? Use
> `EXPLAIN ANALYZE` to support your answer. Why does (or doesn't) the index help?

~~~sql
CREATE INDEX idx_people_100k_country ON people_100k(country);
ANALYZE people_100k;

EXPLAIN ANALYZE
SELECT COUNT(*)
FROM people_100k p1
JOIN people_100k p2 ON p1.country = p2.country;
~~~

Output:
~~~text
            QUERY PLAN                                                                                            
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Finalize Aggregate  (cost=637025.05..637025.06 rows=1 width=8) (actual time=14058.671..14060.213 rows=1.00 loops=1)
   Buffers: shared hit=186
   ->  Gather  (cost=637024.84..637025.05 rows=2 width=8) (actual time=14058.648..14060.196 rows=2.00 loops=1)
         Workers Planned: 1
         Workers Launched: 1
         Buffers: shared hit=186
         ->  Partial Aggregate  (cost=636024.84..636024.85 rows=1 width=8) (actual time=13967.260..13967.264 rows=1.00 loops=2)
               Buffers: shared hit=186
               ->  Parallel Hash Join  (cost=2332.11..474261.24 rows=64705438 width=0) (actual time=192.541..8691.393 rows=54973254.00 loops=2)
                     Hash Cond: (p1.country = p2.country)
                     Buffers: shared hit=186
                     ->  Parallel Index Only Scan using idx_people_100k_country on people_100k p1  (cost=0.29..1596.52 rows=58824 width=8) (actual time=0.036..7.408 rows=50000.00 loops=2)
                           Heap Fetches: 0
                           Index Searches: 1
                           Buffers: shared hit=93
                     ->  Parallel Hash  (cost=1596.52..1596.52 rows=58824 width=8) (actual time=191.423..191.424 rows=50000.00 loops=2)
                           Buckets: 131072  Batches: 1  Memory Usage: 5248kB
                           Buffers: shared hit=93
                           ->  Parallel Index Only Scan using idx_people_100k_country on people_100k p2  (cost=0.29..1596.52 rows=58824 width=8) (actual time=0.035..3.709 rows=50000.00 loops=2)
                                 Heap Fetches: 0
                                 Index Searches: 1
                                 Buffers: shared hit=93
 Planning Time: 0.387 ms
 JIT:
   Functions: 16
   Options: Inlining true, Optimization true, Expressions true, Deforming true
   Timing: Generation 1.174 ms (Deform 0.266 ms), Inlining 124.845 ms, Optimization 69.897 ms, Emission 66.014 ms, Total 261.931 ms
 Execution Time: 14061.149 ms
(28 rows)
~~~

**Why did it happen?**
- This query is pulling up every single matching pair across the entire table. Using indexing only helps with row lookup.
- All rows are evaluated against each other, so all expansions have to be calculated.

> **Step 3 — Rewrite it.** The query only wants the *number* of matching pairs,
> not the pairs themselves. If a country has *k* people, how many pairs does it
> contribute to the join? Write a query that computes the **same number
> without a join**. Check that it returns exactly the same result as the join on
> `people_100k`, then run it on `people_big` and compare its runtime with your
> prediction from Step 1.

As there're $k$ people from one country, they form $k^2$ number of pairs, meaning the overall sum will be:
$$\sum_{c} k^2_c \text{, where } c \in \text{Countries}.$$

~~~sql
SELECT SUM(k * k) AS total_pairs
FROM (
    SELECT country, COUNT(*) AS k
    FROM people_100k
    GROUP BY country
) sub;
~~~

Output:
~~~text
 total_pairs 
-------------
 10983941260
(1 row)

Time: 243.109 ms
~~~
This ran in **0.2 sec**, so basically 10 000 times faster than the original prediction of more than 40 mins.

> **Step 4 — Discussion (submit in writing).** Considering **scalability** and
> **efficiency**, which approaches and/or optimizations can be applied to improve
> this kind of query in a real system? Discuss at least:
> 
> - what the rewrite in Step 3 tells you about adding more hardware or an index;
> - what you would do if the business actually needed **the pairs themselves**
>   (not just their count) — would a bigger machine or a cluster help, and how
>   much?
> - the limits of an **OLTP database** for this workload, especially in a
>   **large-scale cloud environment**.
> 
> > **Optional:** support your answer with a diagram, SQL or code.


