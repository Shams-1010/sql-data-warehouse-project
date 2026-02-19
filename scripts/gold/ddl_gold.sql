/*
=============================================================================================
DDL Scripts: Create Gold Views

Script Purpose: 
        This script creates views for the Gold layer in the datawarehouse.
        The gold Layer represents the final dimension and facts table (star schema)

        Each view performs transformations and combine data from the silver layer to produce
        a clean, enriched, and business-ready data-sets.

Usage: 
      - These views can be queried directly for analytics and reporting.
=============================================================================================
*/

-- ==========================================================================================
-- Creating View gold.dim_customers
-- ==========================================================================================
IF OBJECT_ID ('gold.dim_customers', 'V') IS NOT NULL 
   DROP VIEW gold.dim_customers;
GO 
CREATE VIEW gold.dim_customers AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
     cnt.cntry AS country,
    ci.cst_maritalstatus AS marital_status,
        CASE WHEN ci.cst_gndr = cr.gen THEN ci.cst_gndr
         WHEN cr.gen = 'n/a' THEN ci.cst_gndr
         WHEN cr.gen = 'n/a' THEN ci.cst_gndr
         WHEN cr.gen IS NULL THEN ci.cst_gndr
         WHEN ci.cst_gndr = 'n/a' THEN cr.gen 
         ELSE ci.cst_gndr
    END AS gender,
    cr.bdate AS birth_date,
    ci.cst_createdate AS creation_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS cr 
ON        ci.cst_key = cr.cid
LEFT JOIN silver.erp_loc_a101 AS cnt 
ON        ci.cst_key = cnt.cid 
GO
-- ==========================================================================================
-- Creating View gold.dim_products
-- ==========================================================================================
IF OBJECT_ID ('gold.dim_products', 'V') IS NOT NULL 
   DROP VIEW gold.dim_products;
GO 
CREATE VIEW gold.dim_products AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY pk.prd_start, pk.prd_key) AS product_key,
    pk.prd_id AS product_id,
    pk.prd_key AS product_number,
    pk.prd_nm AS product_name,
    pk.cat_id AS category_id,
    pi.cat AS category,
    pi.subcat AS sub_category,
    pi.maintanance,
    pk.prd_cost AS cost,
    pk.prd_line AS product_line,
    pk.prd_start AS start_date 
FROM silver.crm_prd_info pk
LEFT JOIN silver.erp_px_cat_g1v2 pi 
ON pk.cat_id = pi.id
WHERE prd_end IS NULL  -- Filtering out old data

GO
-- ==========================================================================================
-- Creating View gold.fact_sales
-- ==========================================================================================
IF OBJECT_ID ('gold.fact_sales', 'V') IS NOT NULL 
   DROP VIEW gold.fact_sales;
GO 
CREATE VIEW gold.fact_sales AS 
SELECT
sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sls_order_dt AS order_date,
sls_ship_dt AS shipping_date,
sls_due_dt AS due_date, 
sls_sales AS sales_amount,
sls_quantity AS quantity,
sls_price AS price 
FROM silver.crm_sales_details sa 
LEFT JOIN gold.dim_products pr 
ON sa.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu 
ON sa.sls_cust_id = cu.customer_id 






