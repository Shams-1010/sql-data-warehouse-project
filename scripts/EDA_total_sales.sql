--- EXPLORATORY DATA ANALYSIS  
-- Finding the total sales
SELECT
SUM(sales_amount) AS total_sales_amount
FROM gold.fact_sales;
