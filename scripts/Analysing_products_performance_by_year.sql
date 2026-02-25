-- Analyze the yearly performance of products by comparing each prroducts sales to both its average sales performance
-- and the previous year's sales 
WITH yearly_product_sales AS (
SELECT
    p.product_name,
    YEAR(order_date) AS order_year,
    SUM(f.sales_amount) AS current_sales,
    COUNT(f.sales_amount) AS sales_count,
    AVG (f.sales_amount) AS avg_sales
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p 
ON f.product_key = p.product_key
WHERE YEAR(order_date) IS NOT NULL 
GROUP BY YEAR(order_date), p.product_name)
-- Binding with CTE 
SELECT
    product_name,
    order_year,
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name) avg_sales,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS sales_difference,
    CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below average'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above average'
        ELSE 'Average'
        END AS average_difference,
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_yr_change,
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_yr_sales,
    CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase in Sales'
         WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease in Sales'
    ELSE 'No Change in Sales'
    END AS previous_year_change
FROM yearly_product_sales
ORDER BY product_name, order_year 
