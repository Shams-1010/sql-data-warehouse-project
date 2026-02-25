-- What top 5 products generate the highest revenue
SELECT TOP 5
p.product_name,
SUM(sales_amount) AS product_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p 
ON f.product_key = p.product_key
GROUP BY product_name 
ORDER BY product_revenue DESC  
