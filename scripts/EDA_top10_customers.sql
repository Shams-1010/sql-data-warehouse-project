-- Top 10 Customers who have generated highest revenue
SELECT *
FROM 
(SELECT 
c.customer_key,
c.first_name + '' + c.last_name AS customer_fullname,
SUM(f.sales_amount) AS customers_revenue,
RANK () OVER (ORDER BY SUM(f.sales_amount) DESC) AS ranked_revenue
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c 
ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name)t 
WHERE ranked_revenue <=10
ORDER BY ranked_revenue
