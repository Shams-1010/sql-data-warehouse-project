/*
========================================================================================================
STORED PROCEDURE: Load bronze layer ( From source system to bronze)
========================================================================================================
SCRIPT PURPOSE: This stored procedure loads data into the bronze schema from external CSV files.
It performs the following action:
Truncates table before loading data.
Uses the BULK INSERT command to load data from the csv files to the bronze tables.

Usage example: 
  EXEC bronze.load_bronze;

========================================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN 
    DECLARE @startime DATETIME, @endtime DATETIME, @batchstarttime DATETIME, @batchendtime DATETIME;
    BEGIN TRY 
        SET @batchstarttime = GETDATE();
        PRINT '==================================================================================';
        PRINT 'Loading Silver Layer...';
        PRINT '==================================================================================';

        PRINT '----------------------------------------------------------------------------------';
        PRINT 'Loading CRM Tables...';
        PRINT '----------------------------------------------------------------------------------';
        -- Setting start time for Silver.crm_cust_info
        SET @startime = GETDATE();
        PRINT '>>>Truncating Table : Silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>>>Inserting Data Into: Silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_maritalstatus,
            cst_gndr,
            cst_createdate)

        SELECT 
            cst_id,
            cst_key,
            TRIM (cst_firstname) AS cst_firstname,
            TRIM (cst_lastname) AS cst_lastname, ------Trimming of first name and last name
            CASE WHEN UPPER(TRIM(cst_maritalstatus)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_maritalstatus)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_maritalstatus,-- Normalizing marital status to a readable format

            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,-- Normalizing Gender to a readable format
            cst_createdate   
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_createdate DESC) Flag_last
            FROM bronze.crm_cust_info 
        )t
        WHERE Flag_last = 1 AND cst_id IS NOT NULL; -- Selecting the most recent customers record 
        SET @endtime = GETDATE ();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF (Second, @startime, @endtime) AS NVARCHAR) + ' Seconds';
        PRINT '>>> ----------------';

        SET @startime = GETDATE();
        PRINT '>>>Truncating Table : Silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>>>Inserting Data Into: Silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start,
            prd_end
        )

        SELECT
            prd_id,
            REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extraction of category ID 
            SUBSTRING(prd_key, 7, LEN (prd_key)) AS prd_key, --- Extraction of product key
            prd_nm,
            ISNULL (prd_cost, 0) AS prd_cost, ---- Changing null costs to zero 
        CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'na/a'
        END AS prd_line, -- Mapping product line to readable format
        CAST (prd_start AS DATE) AS prd_start,
        CAST (LEAD (prd_start) OVER (PARTITION BY prd_key ORDER BY prd_start ASC)-1 AS DATE) AS prd_end -- calculating the end date as 1 day before the next start date
        FROM bronze.crm_prd_info;
        SET @endtime = GETDATE ();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF (Second, @startime, @endtime) AS NVARCHAR) + ' Seconds';
        PRINT '>>> ----------------';

        SET @startime = GETDATE();
        PRINT '>>>Truncating Table: Silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>>>Inserting Data Into: Silver.crm_sales_details';
        INSERT INTO  silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price 
        )
        SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN (sls_order_dt) !=8  THEN NULL
            ELSE  CAST(CAST (sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LEN (sls_ship_dt) !=8  THEN NULL
            ELSE  CAST(CAST (sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LEN (sls_due_dt) !=8  THEN NULL
            ELSE  CAST(CAST (sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS (sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales 
        END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <=0 
            THEN sls_sales / NULLIF (sls_quantity,0)
            ELSE sls_price
        END AS sls_price
        FROM bronze.crm_sales_details;
            SET @endtime = GETDATE ();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF (Second, @startime, @endtime) AS NVARCHAR) + ' Seconds';
        PRINT '>>> ----------------';

        SET @startime = GETDATE();
        PRINT '>>>Truncating Table: Silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>>>Inserting Data Into: Silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
        CASE WHEN cid  LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Removing NAS Prefix 
            ELSE cid
        END AS cid,
        CASE WHEN bdate > GETDATE() THEN NULL --- Setting future dates to null 
            ELSE bdate
        END AS bdate,
        CASE
            WHEN gen IS NULL OR LTRIM(RTRIM(gen)) = '' THEN 'n/a'
            WHEN UPPER(LTRIM(RTRIM(gen))) LIKE 'F%' THEN 'Female'
            WHEN UPPER(LTRIM(RTRIM(gen))) LIKE 'M%' THEN 'Male'
            ELSE 'n/a' 
            END AS gen -- Mapping gender to redable format and trimming trailling spaces 
        FROM bronze.erp_cust_az12;
        SET @endtime = GETDATE ();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF (Second, @startime, @endtime) AS NVARCHAR) + ' Seconds';
        PRINT '>>> ----------------';

        SET @startime = GETDATE();
        PRINT '>>>Truncating Table: Silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>>>Inserting Data Into: Silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(cid, cntry)
        SELECT DISTINCT 
        REPLACE(cid, '-', '') cid, -- Removing the cid - sign so as to match the customer key
        CASE WHEN CAST(LTRIM(RTRIM(REPLACE(cntry, CHAR(13),''))) AS VARCHAR(50)) = 'France' THEN 'France'
            WHEN CAST(LTRIM(RTRIM(REPLACE(cntry, CHAR(13),''))) AS VARCHAR(50)) IN ('USA', 'US', 'United States') THEN 'United States'
            WHEN CAST(LTRIM(RTRIM(REPLACE(cntry, CHAR(13),''))) AS VARCHAR(50)) = 'United Kingdom' THEN 'United Kingdom'
            WHEN CAST(LTRIM(RTRIM(REPLACE(cntry, CHAR(13),''))) AS VARCHAR(50)) = 'Canada' THEN 'Canada'
            WHEN CAST(LTRIM(RTRIM(REPLACE(cntry, CHAR(13),''))) AS VARCHAR(50)) IN ('Germany', 'DE') THEN 'Germany'
            WHEN CAST(LTRIM(RTRIM(REPLACE(cntry, CHAR(13),''))) AS VARCHAR(50)) = '' OR cntry IS NULL THEN 'n/a'
            WHEN CAST(LTRIM(RTRIM(REPLACE(cntry, CHAR(13),''))) AS VARCHAR(50)) = 'Australia' THEN 'Australia'
            ELSE CAST(LTRIM(RTRIM(REPLACE(cntry, CHAR(13),''))) AS VARCHAR(50)) -- Repplaced the CHAR 13 invisible character with an empty string then trimmed the left and right trailing spaces
        END AS cntry -- Standardized all country to full name
        FROM bronze.erp_loc_a101;
            SET @endtime = GETDATE ();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF (Second, @startime, @endtime) AS NVARCHAR) + ' Seconds';
        PRINT '>>> ----------------';

        SET @startime = GETDATE();
        PRINT '>>>Truncating Table: Silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>>>Inserting Data Into: Silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintanance)
        SELECT
        id,
        cat,
        subcat,
        LTRIM(RTRIM (REPLACE(maintanance, CHAR(13), ''))) AS maintanance
        FROM bronze.erp_px_cat_g1v2;
        SET @endtime = GETDATE ();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF (Second, @startime, @endtime) AS NVARCHAR) + ' Seconds';
        PRINT '>>> ----------------';
        SET @batchendtime = GETDATE();
        PRINT '>>> Total CRM Tables Load Duration: ' + CAST(DATEDIFF (Second, @batchstarttime, @batchendtime) AS NVARCHAR) + ' Seconds';
        PRINT '>>> ----------------';
    END TRY 
    BEGIN CATCH
        PRINT '=====================================================================';
        PRINT '-------------- AN ERROR OCCURED WHILE LOADING...------------------';
        PRINT 'Error Message ' + ERROR_MESSAGE ();
        PRINT 'Error Message ' + CAST(ERROR_NUMBER () AS NVARCHAR);
        PRINT 'Error Message ' + CAST(ERROR_STATE () AS NVARCHAR);   
        PRINT '=====================================================================';
    END CATCH 
END 
