/*
Segment production into cost ranges and
count how many products fall into each segment 
*/

WITH segment_cost AS (
SELECT 
product_name,
cost_range,
COUNT(cost_range) OVER (PARTITION BY cost_range ORDER BY product_name) AS cost_count
FROM 
(
SELECT
product_name,
cost,
CASE WHEN cost < 100 THEN 'Below 100'
     WHEN cost BETWEEN 100 AND 500 THEN '100-500'
     WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
     ELSE 'Above 1000 '
END AS cost_range
FROM gold.dim_products)t 
GROUP BY product_name, cost_range)

SELECT 
cost_range,
COUNT (cost_count) AS products_ranges 
FROM segment_cost
GROUP BY cost_range
ORDER BY products_ranges DESC
