/*
=========================================================================================
        Group customers into three segments based on their spending behaviour:
- VIP: Customers with atleast 12months of history and spending more than $5,000.
- Regular: customers with atleast 12months of history but spend $5,000 or less.
- New: Customers with a lifespan less than 12months.
And find the total numbers of customers in each group.
=========================================================================================
*/


WITH customers_class AS (
-- sub query second step
SELECT
*,
CASE 
     WHEN lifespan >= 12 AND total_spendings > 5000 THEN 'VIP customer'
     WHEN lifespan >= 12 AND total_spendings <= 5000 THEN 'Regular customer'
     ELSE 'New customer'
     END AS customers_classification
FROM (
-- First step
SELECT
        c.customer_key,
        --c.first_name + ' ' + c.last_name AS customers_full_name,
        MIN (order_date) first_order,
        MAX (order_date) AS last_order,
        SUM(sales_amount) AS total_spendings,
        DATEDIFF(Month, MIN (order_date), MAX (order_date)) AS lifespan
FROM gold.dim_customers c 
LEFT JOIN gold.fact_sales f
ON c.customer_key = f.customer_key 
WHERE order_date IS NOT NULL
GROUP BY c.customer_key)t)
-- CTE third step
SELECT
COUNT(customer_key) AS customers_count,
customers_classification
FROM customers_class
GROUP BY customers_classification
ORDER BY customers_count DESC 
