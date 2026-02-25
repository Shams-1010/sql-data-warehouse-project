-- Advance Data Analytics
-- Analysing change over time.
SELECT
YEAR(order_date) AS order_year,
DATENAME(Month,order_date) AS order_month,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATENAME(Month,order_date), YEAR(order_date)
ORDER BY YEAR(order_date), DATENAME(Month,order_date) 
