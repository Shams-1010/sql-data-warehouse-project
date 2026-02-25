---- Finding average costs in each categories
SELECT category,
AVG(cost) AS avg_cost_by_categories
FROM gold.dim_products
GROUP BY category 
ORDER BY avg_cost_by_categories DESC 
