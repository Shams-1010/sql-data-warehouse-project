---- Calculate the total sales per month and the running total of sales over time.
SELECT 
order_date,
total_sales,
SUM(total_sales) OVER (PARTITION BY DATETRUNC (year,order_date) ORDER BY order_date) AS running_total,
avg_price,
AVG (avg_price) OVER (PARTITION BY order_date ORDER BY order_date) AS moving_avg_price
FROM (
SELECT
DATETRUNC(Month, order_date) AS order_date,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE DATENAME(Month, order_date) IS NOT NULL 
GROUP BY DATETRUNC(Month, order_date))t 
