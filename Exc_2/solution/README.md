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
need it any any form that would require it to be transferred to atomic attributes.

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
SELECT price_per_unit
FROM orders
ORDER BY price_per_unit DESC
LIMIT 1;
~~~

Output:
~~~bash
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
ORDER BY SUM(quantity)
LIMIT 3;
~~~

Output:
~~~bash
 product_category | quantity_sold 
------------------+---------------
 Sports           |        298573
 Automotive       |        299189
 Home & Garden    |        299668
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
~~~bash
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
~~~bash
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
--Look at how many orders these customers made
SELECT customer_name,
       ROUND(SUM(price_per_unit*quantity)::numeric, 2) AS revenue,
       COUNT(id) AS ordered
FROM orders
GROUP BY customer_name
ORDER BY SUM(price_per_unit*quantity) DESC
LIMIT 5;
~~~

Output:
~~~bash
 customer_name  |  revenue  | ordered 
----------------+-----------+---------
 Carol Taylor   | 991179.18 |    1028
 Nina Lopez     | 975444.95 |     980
 Daniel Jackson | 959344.48 |    1033
 Carol Lewis    | 947708.57 |     943
 Daniel Young   | 946030.14 |     973
(5 rows)
~~~

**Explanation:** the python code defines the boundaries of the prices, meaning people will have to order approximately the same amount of products in order to create the same amound of revenue for the company.
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
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM people_100k p1
JOIN people_100k p2 ON p1.country = p2.country;
~~~

This run took **9097.856 ms (00:09.098)**, meaning it was somewhat slower as the original run.

**Why did it happen?**
- This query is pulling up every single matching pair across the entire table. Using indexing only helps with row lookup.
- All rows are evaluated agains each other, so all expansions has to be calculated.

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
    SELECT country, COUNT(*)::bigint AS k
    FROM people_big
    GROUP BY country
) sub;
~~~

Output:
~~~bash
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

