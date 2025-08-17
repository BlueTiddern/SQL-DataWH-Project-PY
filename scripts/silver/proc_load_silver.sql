/*===============================================================================
  Script Name   : silver.proc_load_silver
  Object Type   : Stored Procedure
  Schema        : silver
  Purpose       :
      This stored procedure performs the end-to-end ETL (Extract, Transform, 
      Load) process for populating the SILVER layer tables of the Data Warehouse. 
      It handles the transformation, cleansing, enrichment, and normalization of 
      data sourced from the BRONZE layer.

  Process Flow  :
      1. Logs timestamps for auditing and performance monitoring.
      2. Truncates target SILVER tables before load.
      3. Transforms, cleans, and inserts data into:
         - silver.crm_cust_info       : Customer information (CRM)
         - silver.crm_prd_info        : Product information (CRM)
         - silver.crm_sales_details   : Sales details (CRM)
         - silver.erp_cust_az12       : Customer data (ERP)
         - silver.erp_loc_a101        : Location data (ERP)
         - silver.erp_px_cat_g1v2     : Product category data (ERP)
      4. Applies data quality rules:
         - Deduplication with ROW_NUMBER
         - Normalization of categorical values
         - Null handling and replacements
         - Derived columns and enrichment (e.g. end dates, IDs)
         - Invalid data filtering
      5. Captures total duration and load summary via PRINT statements.
      6. Provides TRY...CATCH error handling with detailed logging.

  Notes         :
      - Truncates SILVER tables before inserting → existing data will be lost.
      - Make sure BRONZE tables contain valid and complete staging data.
      - Designed for repeatable batch runs as part of ETL pipelines.

  Usage Example :
      EXEC silver.proc_load_silver;

  Author        : Pavan Yarlagadda
  Created On    : 2025-08-16

  ===============================================================================
*/

EXEC silver.proc_load_silver;
GO

CREATE OR ALTER PROCEDURE silver.proc_load_silver AS

BEGIN
	
	-- variables for logging time stamps of load operations
	DECLARE @start_time DATETIME, @end_time DATETIME, @silver_start_time DATETIME, @silver_end_time DATETIME;

	BEGIN TRY	
		
		-- Silver data load start time
		SET @silver_start_time = GETDATE();

		PRINT '===================================================';
		PRINT 'SILVER LAYER DATA LOAD';
		PRINT '===================================================';

		-- start time for silver layer load
		SET @silver_start_time = GETDATE();

		PRINT '---------------------------------------------------';
		PRINT 'CRM TABLES DATA LOAD';
		PRINT '---------------------------------------------------';

		--------------------------------------------------------------
		-- Code to truncate, trnasform and insert values into silver.crm_cust_info
		--------------------------------------------------------------

		PRINT '>>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>>> Inserting Data Into: silver.crm_cust_info';

		-- crm_cust_info silver load start time
		SET @start_time = GETDATE();

		INSERT INTO silver.crm_cust_info (
			[cst_id],
			[cst_key],
			[cst_firstname],
			[cst_lastname],
			[cst_marital_status],
			[cst_gndr],
			[cst_create_date]
		)

		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) as first,
			TRIM(cst_lastname) AS last,
			CASE	
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'N/A'
			END AS marital_status, -- Normalize customer marital status to readable format
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'N/A'
			END AS customer_gender, -- Normalize customer gender to readable format
			cst_create_date
		FROM
		(
			SELECT
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as d_rank
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) as s 
		WHERE s.d_rank = 1; -- Select the valid and the most recent records of the customers


		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in cust_info table in:' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';

		---------------------------------------------------------------------
		-- Code to truncate, trnasform and insert values into silver.crm_prd_info
		---------------------------------------------------------------------
		PRINT '>>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		-- start time for crm_prd_info

		SET @start_time = GETDATE();

		PRINT '>>> Inserting Data Into: silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info (

		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt

		)

		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id, -- extract the category ID : Derived columns
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract the product key : Derived columns
			prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost, -- null value handling
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Moutain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'
			END as prd_line, -- Data normalization
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- end date calculations : Data enrichment
		FROM bronze.crm_prd_info

		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in prd_info table in:' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';

		--------------------------------------------------------------
		-- Code to truncate, trnasform and insert values into silver.crm_sales_details
		--------------------------------------------------------------

		PRINT '>>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		-- start time for crm_sales_details
		SET @start_time = GETDATE();

		PRINT '>>> Inserting Data Into: silver.crm_sales_details';

		INSERT INTO silver.crm_sales_details
		(

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
			CASE	
				WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt, -- Handling invalid data and data type transformation
			CASE	
				WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE	
				WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Handling the invalid data by deriving the correct data from other columns
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price <= 0 THEN abs(sls_sales)/NULLIF(sls_quantity,0)
				ELSE sls_price
			END as sls_price -- Handling the invalid data by deriving the correct data from other columns
		FROM bronze.crm_sales_details

		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in sales_details table in:' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';

		PRINT '---------------------------------------------------';
		PRINT 'ERP TABLES DATA LOAD';
		PRINT '---------------------------------------------------';

		--------------------------------------------------------------
		-- Code to truncate, trnasform and insert values into silver.erp_cust_az12
		--------------------------------------------------------------

		PRINT '>>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		-- start load into the erp_cust_az12
		SET @start_time = GETDATE();

		PRINT '>>> Inserting Data Into: silver.erp_cust_az12';

		INSERT INTO silver.erp_cust_az12

		(

		cid,
		bdate,
		gen

		)

		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END as cid, -- Data transformation removing characters  to fit the table
			CASE 
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- filter invalid dates 
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'N/A'
			END as gen -- Data normalization and stadardization
		FROM bronze.erp_cust_az12

		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in erp_cust_az12 table in:' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';


		--------------------------------------------------------------
		-- Code to truncate, trnasform and insert values into silver.erp_loc_a101
		--------------------------------------------------------------

		PRINT '>>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		-- start load into the erp_loc_a101
		SET @start_time = GETDATE();

		PRINT '>>> Inserting Data Into: silver.erp_loc_a101';

		INSERT INTO silver.erp_loc_a101
		(

		cid,
		cntry

		)
		SELECT
			REPLACE(cid, '-', '') AS cid, -- Data tranformation to handle invalid values with '-'
			CASE	
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
				ELSE TRIM(cntry)
			END AS cntry -- Data consistancy transformation: Handled missing or blank cells
		FROM bronze.erp_loc_a101

		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in erp_loc_a101 table in:' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';

		--------------------------------------------------------------
		-- Code to truncate, trnasform and insert values into silver.erp_px_cat_g1v2
		--------------------------------------------------------------

		PRINT '>>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		-- start load into the erp_px_cat_g1v2
		SET @start_time = GETDATE();

		PRINT '>>> Inserting Data Into: silver.erp_px_cat_g1v2';

		INSERT INTO silver.erp_px_cat_g1v2

		(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in erp_px_cat table in:' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';

		SET @silver_end_time = GETDATE();

		PRINT '===================================================';
		PRINT 'SILVER LAYER DATA LOAD ENDS';
		PRINT '     - Total load duration: ' + CAST(DATEDIFF(second, @silver_start_time, @silver_end_time) AS NVARCHAR) + 'seconds';
		PRINT '===================================================';

	END TRY
	BEGIN CATCH
		PRINT '===================================================';
		PRINT '>>> ERROR LOADING DATA INTO THE SILVER LAYER';
		PRINT '>>> ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT '>>> ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '>>> ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================';

	END CATCH
END;
