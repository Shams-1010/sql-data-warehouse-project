---- Finding Total Products by category 
SELECT
category,
COUNT(product_id) AS total_products_by_categories
FROM gold.dim_products
GROUP BY category
ORDER BY total_products_by_categories DESC 
