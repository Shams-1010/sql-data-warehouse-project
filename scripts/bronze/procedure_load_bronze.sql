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

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @Start_time DATETIME, @End_time DATETIME, @Batch_start_time DATETIME, @Batch_end_time DATETIME;
    SET @Batch_start_time = GETDATE();
    BEGIN TRY 

        PRINT '====================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '====================================================';

        PRINT '-----------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-----------------------------------------------------';

    SET @Start_time = GETDATE();  
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/cust_info.csv'
        WITH 
        (
            FIRSTROW = 2,
            FIELDTERMINATOR =',',
            TABLOCK 
        );
    SET @End_time = GETDATE(); 
    PRINT '>> Load Duration : ' + CAST (DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' seconds'
    PRINT '>> ----------------'

        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
    SET @Start_time = GETDATE();
        PRINT '>> Inserting Data: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/backup/prd_info.csv'
        WITH (
            FIRSTROW =2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
    SET @End_time = GETDATE(); 
    PRINT '>> Load Duration : ' + CAST (DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' seconds'
    PRINT '>> ----------------'

        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
    SET @Start_time = GETDATE();
        PRINT '>> Inserting Data: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/sales_details.csv'
        WITH (
            FIRSTROW =2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
    SET @End_time = GETDATE(); 
    PRINT '>> Load Duration : ' + CAST (DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' seconds'
    PRINT '>> ----------------'

        PRINT '-----------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-----------------------------------------------------';

        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
    SET @Start_time = GETDATE();
        PRINT '>> Inserting Data: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/data/CUST_AZ12.csv'
        WITH (
            FIRSTROW =2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
    SET @End_time = GETDATE(); 
    PRINT '>> Load Duration : ' + CAST (DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' seconds'
    PRINT '>> ----------------'


        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
    SET @Start_time = GETDATE();
        PRINT '>> Inserting Data: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/data/LOC_A101.csv'
        WITH (
            FIRSTROW =2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
    SET @End_time = GETDATE(); 
    PRINT '>> Load Duration : ' + CAST (DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' seconds'
    PRINT '>> ----------------'

        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    SET @Start_time = GETDATE();
        PRINT '>> Inserting Data: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/data/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW =2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
    SET @End_time = GETDATE(); 
    PRINT '>> Load Duration : ' + CAST (DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
    PRINT '>> ----------------'
    SET @Batch_end_time = GETDATE()
    PRINT '===================================';
    PRINT 'Loading Bronze Layer is Completed';
    PRINT   '-Total bronze layer duration: ' + CAST (DATEDIFF(second, @Batch_start_time, @Batch_end_time) AS NVARCHAR) + ' seconds';
    PRINT '====================================';
    END TRY
    BEGIN CATCH
        PRINT '=============================================================';
        PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER'
        PRINT 'Error Message ' + ERROR_MESSAGE(); 
        PRINT 'Error Message ' + CAST  (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message ' + CAST  (ERROR_STATE() AS NVARCHAR); 
        PRINT '============================================================='
    END CATCH  
END 
