/*
==========================================================================================
Stored Procedure: Load Bronze Layer
==========================================================================================
Script Purpose:
This stored Procedure loads data into the 'bronze' schema from external CSV files.
it performs the following actions:
- Truncate the bronze Tables before loading data
- Use the 'BULK INSERT' command to load data from csv files to bronze tables.

parameters:
None.
Usuage Example:
EXEC bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
	DECLARE @start_time DATETIME, @end_time DATETIME;
	PRINT '=====================================';
	PRINT 'Loading Bronze Layer'
	PRINT '=====================================';

	PRINT '-------------------------------------';
	PRINT 'Loading CRM Tables'
	PRINT '-------------------------------------';

	Set @start_time = GETDATE();
	PRINT '>>Truncating Table:bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
	PRINT '>> Inserting Table:bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'F:\SQL Courses\ETL Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
	Set @end_time = GETDATE();
	PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secounds';
	PRINT'----------------------';

	Set @start_time = GETDATE();
	PRINT '>>Truncating Table:bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
	PRINT '>> Inserting Table:bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'F:\SQL Courses\ETL Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		Set @end_time = GETDATE();
	PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secounds';
	PRINT'----------------------';

	Set @start_time = GETDATE();
	PRINT '>>Truncating Table:crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
	PRINT '>> Inserting Table:bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'F:\SQL Courses\ETL Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		Set @end_time = GETDATE();
	PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secounds';
	PRINT'----------------------';

	Set @start_time = GETDATE();
		PRINT '-------------------------------------';
	PRINT 'Loading ERP Tables'
	PRINT '-------------------------------------';
	PRINT '>>Truncating Table:bronze.erb_cust_az12';
		TRUNCATE TABLE bronze.erb_cust_az12;
	PRINT '>> Inserting Table:bronze.erb_cust_az12';
		BULK INSERT bronze.erb_cust_az12
		FROM 'F:\SQL Courses\ETL Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		Set @end_time = GETDATE();
	PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secounds';
	PRINT'----------------------';

	Set @start_time = GETDATE();
	PRINT '>>Truncating Table:bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
	PRINT '>> Inserting Table:bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101 
		FROM 'F:\SQL Courses\ETL Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		Set @end_time = GETDATE();
	PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secounds';
	PRINT'----------------------';

	Set @start_time = GETDATE();
	PRINT '>>Truncating Table:bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	PRINT '>> Inserting Table:bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'F:\SQL Courses\ETL Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		Set @end_time = GETDATE();
	PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'secounds';
	PRINT'----------------------';

	END TRY
	BEGIN CATCH
	PRINT '============================================='
	PRINT 'Error Occured During loading Bronze Layer' 
	PRINT 'Error Message' +CAST( ERROR_MESSAGE() AS NVARCHAR);
	PRINT 'Error Message' +CAST( ERROR_NUMBER()  AS NVARCHAR);
	PRINT '============================================='
	END CATCH
END
