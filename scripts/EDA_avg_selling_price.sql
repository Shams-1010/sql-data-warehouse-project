--- EXPLORATORY DATA ANALYSIS  
--- Finding the average selling price
SELECT
AVG(sales_amount) AS avg_selling_price
FROM gold.fact_sales;
