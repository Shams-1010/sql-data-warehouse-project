/*
===========================================================
Create Database and Schemas
===========================================================
Script Purpose:
    This script creates a new database named "DataWarehouse" after checking if it already exists.
    If the database exists, it drops the old one and creates a new one. Additionally, this script
    sets up 3 schemas within the database named "bronze", "silver" and "gold"

WARNING:
    Running this script will drop the entire 'DataWarehouse' if it already exists. All data in the database will 
    be permanently deleted. Proceed with caution and ensure you have proper backups before rinning this script.
*/


-- Create Database 'Database Warehouse'

USE Master; 
GO

--- Drop and recreate the 'DataWarehouse' database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name= 'DataWarehouse')
BEGIN 
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
    END 
GO 

--- Creating the 'DataWarehouse' database

CREATE DATABASE DataWarehouse;
GO 

USE DataWarehouse;
GO 

--- Creating Schema

CREATE SCHEMA bronze;
GO

CREATE SCHEMA gold;
GO

CREATE SCHEMA silver;
GO   
