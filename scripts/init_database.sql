/* 
================================================================================
 Script Name  : Data Warehouse Drop & Recreate Script
 Description  : 
    This script connects to the 'master' database, checks if a database named
    'DataWareHouse' exists, and if so:
       1. Forces the database into SINGLE_USER mode with ROLLBACK IMMEDIATE,
          terminating all active connections and rolling back uncommitted
          transactions.
       2. Drops the existing 'DataWareHouse' database.
       3. Recreates a fresh 'DataWareHouse' database.
       4. Creates schemas for each data layer: bronze, silver, and gold.

 WARNING     : 
    ⚠️  RUNNING THIS SCRIPT WILL **PERMANENTLY DELETE** THE EXISTING 
       'DataWareHouse' DATABASE AND ALL ITS DATA.
       Ensure you have a valid backup before executing.
       This action cannot be undone once the database is dropped.

 Usage       :
    Intended for development or controlled maintenance scenarios where the 
    data warehouse environment needs to be reset.

 Author      : Pavan Yarlagadda
 Date        : 2025-08-12
================================================================================
*/


-- switch to master database

USE master;

-- Drop and recreate the datawarehouse database

IF EXISTS (SELECT 1 from sys.databases WHERE name = 'DataWareHouse')
	BEGIN
		ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE DataWareHouse;
	END;
GO

--create the datawarehouse data base

CREATE DATABASE DataWareHouse;
GO

-- switch to the created data warehouse DB

USE DataWareHouse;
GO

-- creating a schema for each of the layers in the data a

-- Create the bronze schema

CREATE SCHEMA bronze;
GO

-- Create the Silver schema

CREATE SCHEMA silver;
GO

-- Create the Gold Schema

CREATE SCHEMA gold;
GO
