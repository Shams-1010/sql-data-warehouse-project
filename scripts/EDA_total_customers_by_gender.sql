--- Total customers by gender
SELECT 
gender,
COUNT(customer_id) AS total_customers_by_gender
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers_by_gender DESC 
