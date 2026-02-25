-- Finding Total customers by countries
SELECT
country,
COUNT(customer_id) AS total_country_customers
FROM gold.dim_customers 
GROUP BY country
ORDER BY total_country_customers DESC 
