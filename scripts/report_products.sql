
/*
=====================================================================================================
Product Report
=====================================================================================================
Purpose: 
    - This reports consolidates key product metrics and behaviors 

Highlights:
    1. Gathers essential fields such as product names, category, subcategory and cost.
    2. Segments by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
        - total orders
        - total sales
        - total quantity sold 
        - total consumers (unique)
        - lifespan (in months)
    4. Calculates valuable KPIs:
         - recency (months since last sale)
         - average order revenue
         - average monthly spend 
=====================================================================================================
*/
CREATE VIEW gold.report_products AS 
WITH base_product AS (
/*
---------------------------------------------------------------------------------------------
1) Base query retrieves core columns from facts_sales and dim_products 
---------------------------------------------------------------------------------------------
*/
SELECT 
f.product_key,
f. order_date,
f.shipping_date,
f.sales_amount,
f.quantity,
p.product_name,
p.category,
p.sub_category,
p.cost,
p.product_number,
f.customer_key
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p 
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL )

, second_cte AS (
/*
---------------------------------------------------------------------------------------------
2) Product Aggregation: Summarizes key metrics at product-level
---------------------------------------------------------------------------------------------
*/
SELECT 
product_key,
product_number,
product_name,
category,
sub_category,
cost,
COUNT(order_date) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT customer_key) AS total_consumers,
MIN (order_date) AS first_order,
MAX(order_date) AS last_order
FROM base_product
GROUP BY
product_key,
product_number,
product_name,
category,
sub_category,
cost)
/*
---------------------------------------------------------------------------------------------
3) Final Query: Combines all products results into one output 
---------------------------------------------------------------------------------------------
*/
SELECT
product_key,
product_number,
product_name,
category,
sub_category,
cost, 
last_order,
total_orders,
total_sales,
total_quantity,
total_consumers,
DATEDIFF(Month, first_order,last_order) AS lifespan,
CASE WHEN total_sales <600000 THEN 'Low-Performer'
     WHEN total_sales BETWEEN 600000 AND 900000 THEN 'Mid-Range'
     ELSE 'High-Performer'
END AS sales_segments,
DATEDIFF (Month, last_order, GETDATE()) AS last_sales_in_months,
-- Calculating Order Revenue
CASE WHEN total_orders = 0 THEN 0
     ELSE total_sales/total_orders
END AS avg_order_revenue,
-- Calculating Avg Monthly Revenue
CASE WHEN DATEDIFF(Month, first_order,last_order) = 0 THEN total_sales
ELSE total_sales / DATEDIFF(Month, first_order,last_order) 
END AS avg_monthly_spend
FROM second_cte 
