/*
=============================================================================================
Stored procedure: Load bronze Layer (source - bronze)
=============================================================================================
Script : 
        This Stored Procedure loads data into bronze schema from external CSV files.
        - it truncates the bronze tables before loading the data.
        - uses bulk insert command to load the data from csv files to the respective tables.
Parameters : None
This stored procedure does not accept any parameters or return any vales.

Usage Example : Exec bronze.load_bronze;
===============================================================================================
*/


 create or alter procedure bronze.load_bronze as

 begin
   declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
   begin try
     set @batch_start_time = GETDATE();
         print '==========================================';
		 print 'loading crm tables';
		 print '===========================================';


		 -- bulk insert into 1. bronze.crm_cust_info
		 set @start_time = GETDATE();
		 print 'truncating bronze.crm_cust_info'
		 truncate table bronze.crm_cust_info;
		  print 'bulk insert into 1. bronze.crm_cust_info'
		 bulk insert bronze.crm_cust_info
		  from 'C:\Users\bharg\OneDrive\Desktop\sqlservercodes\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		  with (
				  firstrow = 2,
				  fieldterminator = ',',
				  tablock
				);
		set @end_time = GETDATE();
		PRINT '>> LOAD DURATION :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';



		 -- bulk insert into 2.bronze.crm_prd_info
		 set @start_time = GETDATE();
		 print 'truncating  bronze.crm_prd_info'
		  truncate table bronze.crm_prd_info;

		  print 'bulk insert into 2.bronze.crm_prd_info'
		  bulk insert bronze.crm_prd_info
		  from 'C:\Users\bharg\OneDrive\Desktop\sqlservercodes\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		  with (
				  firstrow = 2,
				  fieldterminator = ',',
				  tablock
				);
		set @end_time = GETDATE();
		PRINT '>> LOAD DURATION :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';



		 -- bulk insert into 3.bronze.crm_sales_details
		 set @start_time = GETDATE();
		 print 'truncating  bronze.crm_sales_details'
		  truncate table bronze.crm_sales_details;
		  print 'bulk insert into 3.bronze.crm_sales_details'
		  bulk insert bronze.crm_sales_details
		  from 'C:\Users\bharg\OneDrive\Desktop\sqlservercodes\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		  with (
				  firstrow = 2,
				  fieldterminator = ',',
				  tablock
				);
		set @end_time = GETDATE();
		PRINT '>> LOAD DURATION :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		 print '==========================================';
		 print 'loading erp tables';
		 print '===========================================';


		  -- bulk insert into 4.bronze.erp_cust_az12
		  set @start_time = GETDATE();
		  print 'truncating  bronze.erp_cust_az12'
		  truncate table bronze.erp_cust_az12;
		   print ' bulk insert into 4.bronze.erp_cust_az12'
		  bulk insert bronze.erp_cust_az12
		  from 'C:\Users\bharg\OneDrive\Desktop\sqlservercodes\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		  with (
				  firstrow = 2,
				  fieldterminator = ',',
				  tablock
				);
		set @end_time = GETDATE();
		PRINT '>> LOAD DURATION :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		-- bulk load into 5.bronze.erp_loc_a101
		set @start_time = GETDATE();
		print 'truncating  bronze.erp_loc_a101'
		  truncate table  bronze.erp_loc_a101;
		  print 'bulk load into 5.bronze.erp_loc_a101'
		  bulk insert  bronze.erp_loc_a101
		  from 'C:\Users\bharg\OneDrive\Desktop\sqlservercodes\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		  with (
				  firstrow = 2,
				  fieldterminator = ',',
				  tablock
				);
		   set @end_time = GETDATE();
		   PRINT '>> LOAD DURATION :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';



		 -- bulk load into 6.bronze.erp_px_cat_g1v2
		 set @start_time = GETDATE();
		 print 'truncating  bronze.erp_px_cat_g1v2'
		   truncate table  bronze.erp_px_cat_g1v2;
		print ' bulk load into 6.bronze.erp_px_cat_g1v2'
		  bulk insert  bronze.erp_px_cat_g1v2
		  from 'C:\Users\bharg\OneDrive\Desktop\sqlservercodes\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		  with (
				  firstrow = 2,
				  fieldterminator = ',',
				  tablock
				);
		 set @end_time = GETDATE();
		 PRINT '>> LOAD DURATION :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		set @batch_end_time = GETDATE();
		print '======================================='
		print '-total batchloading time:' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar)+'seconds';
		print '=========================================='
	end try
	begin catch
	  print '====================================='
	  print 'error occured during loading bronze layer'
	  print 'error message' + error_message();
	  print 'error message' + cast(error_number()as nvarchar);
	  print 'error message' + cast(error_state() as nvarchar);
	  print '====================================='
	end catch

 end
 -- executing stored procedure
 exec bronze.load_bronze;
