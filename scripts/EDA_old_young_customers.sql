--- EXPLORATORY DATA ANALYSIS  
-- Finding the oldest and youngest customers, also finding their age
SELECT 
MIN(birth_date) AS oldest_customer,
MAX(birth_date) AS youngest,
DATEDIFF(year,MIN(birth_date), GETDATE()) oldest_age,
DATEDIFF(year,MAX(birth_date), GETDATE()) youngest_age
FROM gold.dim_customers;
