-- Distribution of items sold across countries
SELECT
c.country,
SUM(f.quantity) AS items_sold
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c 
ON f.customer_key = c.customer_key
GROUP BY country
ORDER BY items_sold DESC 
