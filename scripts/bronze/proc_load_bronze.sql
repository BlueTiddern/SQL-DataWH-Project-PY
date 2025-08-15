/* 
================================================================================
 Script Name  : Bronze Layer Bulk Load Stored Procedure
 Description  : 
    Defines and executes the stored procedure [bronze].[load_tables] that 
    automates loading data into the Bronze layer tables of the 'DataWareHouse' 
    database.

    Key operations:
      1. Iterates through all defined CRM and ERP Bronze layer tables.
      2. For each table:
         - Truncates existing data (removes all rows, resets identity if any).
         - Performs a BULK INSERT from a CSV source file located on disk.
         - Logs timing for each load step.
      3. Includes TRY...CATCH error handling for logging basic error details.

 WARNING     : 
    ??  This process will **PERMANENTLY DELETE** all existing data in the 
       target Bronze layer tables before loading fresh data from source files.
       Ensure:
         - You have backups of any data you need.
         - The CSV file paths in BULK INSERT statements are correct and accessible.
         - The script is run only in the intended environment.

 Usage       :
    - For initializing or refreshing Bronze layer staging tables with the 
      latest source system extracts.
    - Should be executed only by authorized personnel with appropriate 
      file system and database permissions.

 Author      : Pavan Yarlagadda
 Date        : 2025-08-14
================================================================================
*/



-- Architecture: Bronze layer
-- First Truncate the table and then bulk import value
-- Bulk import from source database will allow for faster speeds

-- stored procedure to bulk insert the information into the table
EXEC bronze.load_tables;
GO

CREATE OR ALTER PROCEDURE bronze.load_tables AS

BEGIN
	
	DECLARE @start_time DATETIME, @end_time DATETIME, @bronze_start_time DATETIME, @bronze_end_time DATETIME;

	BEGIN TRY

		-- Total execution time
		SET @bronze_start_time = GETDATE();
		
		PRINT '===================================================';
		PRINT 'BRONZE LAYER DATA LOAD';
		PRINT '===================================================';

		PRINT '---------------------------------------------------';
		PRINT 'CRM TABLES DATA LOAD';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
		-- Truncate table schema: Bronze Table: crm_cust_info

		PRINT '>>> Truncate table schema: Bronze Table: crm_cust_info ';

		TRUNCATE TABLE bronze.crm_cust_info;

		-- Bulk insert into the table crm_cust_info

		PRINT '>>> Bulk insert into the table crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\8897p\OneDrive\Desktop\Special_problems_AI_ethics\ProjectFindIt\SQL\SQL_hands_on\SQL_projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK -- OPTIMIZATION FOR SPEED, FOR MINIMAL LOGGING AND STOPPING OTHER OPERATIONS WHILE INSERT
		);

		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in cust_info table in:' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
		-- Truncate table schema: Bronze Table: crm_prd_info

		PRINT '>>> Truncate table schema: Bronze Table: crm_prd_info';

		TRUNCATE TABLE bronze.crm_prd_info;

		-- Bulk insert into the table crm_prd_info

		PRINT '>>> Bulk insert into the table crm_prd_info';

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\8897p\OneDrive\Desktop\Special_problems_AI_ethics\ProjectFindIt\SQL\SQL_hands_on\SQL_projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in cust_info table in:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();

		-- Truncate table schema: Bronze Table: crm_sales_details

		PRINT '>>> Truncate table schema: Bronze Table: crm_sales_details';

		TRUNCATE TABLE bronze.crm_sales_details;

		-- Bulk import into the table crm_sales_details

		PRINT '>>> Bulk import into the table crm_sales_details';

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\8897p\OneDrive\Desktop\Special_problems_AI_ethics\ProjectFindIt\SQL\SQL_hands_on\SQL_projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in cust_info table in:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
		-- Truncate table schema: Bronze Table: erp_cust_az12

		PRINT '>>> Truncate table schema: Bronze Table: erp_cust_az12';

		TRUNCATE TABLE bronze.erp_cust_az12;

		-- Bulk insert into the table erp_cust_az12

		PRINT '>>> Bulk insert into the table erp_cust_az12';

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\8897p\OneDrive\Desktop\Special_problems_AI_ethics\ProjectFindIt\SQL\SQL_hands_on\SQL_projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in cust_info table in:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();

		-- Truncate table schema: Bronze table: erp_loc_a101

		PRINT '>>> Truncate table schema: Bronze table: erp_loc_a101';

		TRUNCATE TABLE bronze.erp_loc_a101;

		-- Bulk import into the table erp_loc_a101

		PRINT '>>> Bulk import into the table erp_loc_a101';

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\8897p\OneDrive\Desktop\Special_problems_AI_ethics\ProjectFindIt\SQL\SQL_hands_on\SQL_projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in cust_info table in:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
		-- Truncate table schema: Bronze table: erp_px_cat_g1v2

		PRINT '>>> Truncate table schema: Bronze table: erp_px_cat_g1v2';

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		-- Bulk import into the table erp_px_cat_g1v2

		PRINT '>>> Bulk import into the table erp_px_cat_g1v2';

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\8897p\OneDrive\Desktop\Special_problems_AI_ethics\ProjectFindIt\SQL\SQL_hands_on\SQL_projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT '>>> Data loaded in cust_info table in:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------';

		SET @bronze_end_time = GETDATE();

		

		PRINT '===================================================';
		PRINT 'BRONZE LAYER DATA LOAD ENDS';
		PRINT '     - Total load duration: ' + CAST(DATEDIFF(second, @bronze_start_time, @bronze_end_time) AS NVARCHAR) + 'seconds';
		PRINT '===================================================';

	END TRY
	BEGIN CATCH
		PRINT '===================================================';
		PRINT'ERROR OCCURED WHILE LOADING DATA INTO BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================';
	END CATCH


END
