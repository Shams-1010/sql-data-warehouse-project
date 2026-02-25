-- What top 10 products generate the highest revenue using window function
SELECT * 
FROM 
(SELECT
p.product_name,
SUM(sales_amount) AS product_revenue,
ROW_NUMBER() OVER (ORDER BY SUM(sales_amount) DESC) AS row_ranked_products,
RANK() OVER (ORDER BY SUM(sales_amount) DESC) AS ranked_products
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p 
ON f.product_key = p.product_key
GROUP BY product_name)t 
WHERE row_ranked_products <=10
