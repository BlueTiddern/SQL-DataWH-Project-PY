/* 
================================================================================
 Script Name  : Bronze Layer Table Creation - CRM & ERP Sources
 Description  : 
    This script connects to the 'DataWareHouse' database and creates the 
    required tables in the 'bronze' schema for CRM and ERP data sources.

    Steps performed:
      1. Switches to the 'DataWareHouse' database.
      2. For each table in the CRM and ERP bronze layer:
         - Checks if the table exists.
         - Drops the table if found.
         - Creates a new table with the specified schema definition.

 WARNING     : 
    ⚠️  RUNNING THIS SCRIPT WILL DROP AND RECREATE ALL LISTED TABLES IN 
       THE 'bronze' SCHEMA OF 'DataWareHouse'. 
       This will permanently remove existing data in these tables.
       Ensure backups are taken before execution.

 Usage       :
    Intended for initializing or refreshing the bronze layer structure 
    for staging raw data from CRM and ERP systems.

 Author      : Pavan Yarlagadda
 Date        : 2025-08-14
================================================================================
*/


-- Switch to Datawarehouse DataBase

USE DataWareHouse;
GO

-- Creating the tables for the brozne layer : CRM

-- Table exist check, drop if found

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;

-- Creating the CRM data source : cust_info table bronze layer

CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
GO

-- Table exist check, drop if found

IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

-- Creating the CRM data source : prd_info table bronze layer

CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);
GO

-- Table check,drop if found

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

-- Creating the CRM data source : sales_details table bronze layer

CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
GO

-- Creating the tables for the brozne layer : ERP

-- Table check,drop if found

IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;

-- Creating the ERP data source : CUST_AZ12 table bronze layer

CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);
GO

-- Table check,drop if found

IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;

-- Creating the ERP data source : LOC_A101 table bronze layer

CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);
GO

-- Table check,drop if found

IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;

-- Creating the ERP data source : PX_CAT_G1V2 table bronze layer

CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
GO
