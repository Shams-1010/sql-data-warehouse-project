---- Total revenue generated in each categories
SELECT
p.category,
SUM(f.sales_amount) AS total_revenue_each_category
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p 
ON f.product_key = p.product_key
GROUP BY P.category
ORDER BY total_revenue_each_category DESC 
