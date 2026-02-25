--- EXPLORATORY DATA ANALYSIS 
--Finding how many items are sold
SELECT
SUM (quantity) AS total_quantity
FROM gold.fact_sales;
