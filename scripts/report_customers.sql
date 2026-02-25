
/*
=====================================================================================================
Customer Report
=====================================================================================================
Purpose: 
    - This reports consolidates key customer metrics and behaviors 

Highlights:
    1. Gathers essential fields such as names, ages, and transaaction details.
    2. Segments customers into categories (VIP, Regular & New) and age groups.
    3. Aggregates customer-level metrics:
        - total orders
        - total sales
        - total quantity purchased 
        - total products
        - lifespan (in months)
    4. Calculates valuable KPIs:
         - recency (months since last order)
         - average order value
         - average monthly spend 
=====================================================================================================
*/

CREATE VIEW gold.report_customers AS 
WITH base_query AS (
/*----------------------------------------------------------------------------------------------------
1) Base Query: Retrieves the core column from the tables
*/----------------------------------------------------------------------------------------------------
SELECT
    f.order_number,
    c.customer_number,
    c.customer_key,
    p.product_key,
    CONCAT (c.first_name,' ', c.last_name) AS customer_name,
    DATEDIFF (Year, c.birth_date, GETDATE()) AS age,
    f.order_date,
    f.sales_amount,
    f.quantity,
    p.product_name
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c 
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p 
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL)

,customer_aggregation AS  (
/*----------------------------------------------------------------------------------------------------
2) Customer Aggregation: Summarizes key metrics at customer level
*/----------------------------------------------------------------------------------------------------
SELECT
    customer_key,
    customer_number,
    customer_name,
    age,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(sales_amount) AS total_sales,
    SUM (quantity) AS quantity_purchased,
    COUNT(DISTINCT product_key) AS total_products,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order,
    DATEDIFF(Month,MIN(order_date), MAX(order_date)) AS life_span
FROM base_query
GROUP BY 
    customer_key,
    customer_name,
    age,
    customer_number)
-- Final step
SELECT
    customer_key,
    customer_number,
    customer_name,
    age,
    CASE 
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 50 THEN '40-50'
        WHEN age > 50 THEN 'Above 50'
        ELSE 'n/a'
    END AS age_group,
    CASE 
        WHEN life_span >= 12 AND total_sales > 5000 THEN 'VIP customer'
        WHEN life_span >= 12 AND total_sales <= 5000 THEN 'Regular customer'
        ELSE 'New customer'
     END AS customers_classification,
last_order,
DATEDIFF(Month, last_order, GETDATE ()) AS recency,
total_orders,
total_sales,
quantity_purchased,
total_products,
life_span,
--- Compute avgere order value
CASE WHEN total_orders = 0 THEN 0 
     ELSE total_sales / total_orders
END AS  avg_order_value,
-- compute average monthly spend
CASE
     WHEN life_span = 0 THEN total_sales
     ELSE total_sales / life_span
END AS monthly_spend
FROM customer_aggregation 
