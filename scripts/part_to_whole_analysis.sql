-- Part to whole analysis 
-- Which categories contribute the most to overall sales
WITH overall_sales AS (
SELECT 
category,
SUM (sales_amount) AS overall_sales
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p 
ON f.product_key = p.product_key
GROUP BY category)
SELECT 
category,
overall_sales,
SUM (overall_sales) OVER () AS total_sales,
CONCAT(ROUND((CAST (overall_sales AS FLOAT) / SUM (overall_sales) OVER ()) * 100, 2),'%')AS percetage_sales 
FROM overall_sales
GROUP BY category, overall_sales
ORDER BY percetage_sales DESC  
