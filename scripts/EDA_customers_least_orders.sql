--- Top 3 customers with the fewest orders placed
SELECT TOP 3
c.customer_key,
c.first_name + ' ' + c.last_name AS customer_fullname,
COUNT(DISTINCT order_number) AS customers_orders
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c 
ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY customers_orders
