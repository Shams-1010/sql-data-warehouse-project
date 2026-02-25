--- Generating Key metrics
-- Finding the total sales
SELECT 'total_sales' AS measure_name,
SUM(sales_amount) AS measure_value
FROM gold.fact_sales
UNION ALL
--Finding how many items are sold
SELECT 'total_quantity' AS measure_name,
SUM (quantity) AS measure_value
FROM gold.fact_sales
UNION ALL 
SELECT 'avg_selling_price' AS measure_name,
AVG(sales_amount) AS measure_value
FROM gold.fact_sales
UNION ALL 
---Finding the total number of orders
SELECT 'total_order_number' AS measure_name,
COUNT(order_number) AS measure_value
FROM gold.fact_sales
UNION ALL 
-- Removing duplicate orders
SELECT 'Total_number_of_orders' AS meaure_name,
COUNT(DISTINCT order_number) AS measure_value
FROM gold.fact_sales
UNION ALL 
--- Total number of products 
SELECT 'Total_number_product' AS measure_name,
COUNT(DISTINCT  product_id) AS measure_value
FROM gold.dim_products
UNION ALL 
---- Finding Total number of customers
SELECT 'total_number_customers_with_orders ' AS measure_name,
COUNT(DISTINCT customer_id) AS measure_value
FROM gold.dim_customers
