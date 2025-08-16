/* 
================================================================================
 Script Name  : Silver Layer Table Creation - CRM & ERP Sources
 Description  : 
    This script connects to the 'DataWareHouse' database and creates the 
    required tables in the 'silver' schema for CRM and ERP data sources.

    Steps performed:
      1. Switch to the 'DataWareHouse' database.
      2. For each Silver layer table (CRM and ERP):
         - Checks if the table exists.
         - Drops the table if it exists.
         - Creates a new table with the defined schema.
      3. Adds a [dwh_create_date] column to each table with a default of GETDATE()
         to capture data warehouse load time.

 WARNING     : 
    ⚠️  Running this script will **DROP AND RECREATE** all Silver layer tables 
       listed here. Existing data in these tables will be permanently deleted.
       Ensure backups are taken or that it is safe to overwrite the tables 
       before execution.

 Usage       :
    Intended for initializing or refreshing the Silver layer in the 
    Data Warehouse pipeline. The Silver layer holds cleansed, structured 
    versions of the raw Bronze data to support downstream transformations.

 Author      : Pavan Yarlagadda
 Date        : 2025 - 08 - 16
================================================================================
*/


-- Switch to Datawarehouse DataBase

USE DataWareHouse;
GO

-- Creating the tables for the silver layer : CRM

-- Table exist check, drop if found

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;

-- Creating the CRM data source : cust_info table silver layer

CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Table exist check, drop if found

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;

-- Creating the CRM data source : prd_info table silver layer

CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id VARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Table check,drop if found

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;

-- Creating the CRM data source : sales_details table silver layer

CREATE TABLE silver.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Creating the tables for the silver layer : ERP

-- Table check,drop if found

IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;

-- Creating the ERP data source : CUST_AZ12 table silver layer

CREATE TABLE silver.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Table check,drop if found

IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;

-- Creating the ERP data source : LOC_A101 table silver layer

CREATE TABLE silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Table check,drop if found

IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;

-- Creating the ERP data source : PX_CAT_G1V2 table silver layer

CREATE TABLE silver.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
