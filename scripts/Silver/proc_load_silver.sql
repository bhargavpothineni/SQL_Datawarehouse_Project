/*
==================================================================================
Stored Procedure : Load Silver Layer(Bronze -> Silver)
==================================================================================
Script Purpose:
        This Stored Procedure Performs the ETL(Extract, transform, Load) process to
        populate the 'Silver' schema tables from the 'bronze' schema.
Actions Performed:
      -: Truncates Silver tables
      -: Inserts transformed and cleansed data from bronze into silver tables.

Parameters:
   None.
   This Stored Procedure does not accept any parameters or return values.

Usage Example:
      Exec Silver.load_silver;
===========================================================================================
*/

create or ALTER procedure silver.load_silver
as 
begin
  declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
  begin try
   set @batch_start_time = GETDATE();
   print '=====================================';
   print 'loading crm tables......';
   print '=====================================';

   --1. insert into silver.crm_cust_info
   set @start_time = GETDATE();
	 print '>> truncating silver.crm_cust_info';
	 truncate table silver.crm_cust_info;
	 print '>> inserting data into silver.crm_cust_info';
	 
		 insert into silver.crm_cust_info
		   (cst_id,
		   cst_key,
		   cst_firstname,
		   cst_lastname,
		   cst_marital_status,
		   cst_gndr,
			cst_create_date)
		 select 
				cst_id, 
				cst_key,
				trim(cst_firstname) as cst_firstname,
				trim(cst_lastname) as cst_lastname,	
			case 
			  when upper(trim(cst_marital_status)) = 'M' then 'Married'
			  when upper(trim(cst_marital_status)) = 's' then 'Single'
			  else 'n/a'
			end cst_marital_status,
			case 
			  when upper(trim(cst_gndr)) = 'F' then 'Female'
			  when upper(trim(cst_gndr)) = 'M' then 'Male'
			  else 'n/a'
			end cst_gndr,
			cst_create_date
		 from (
		 select *,
		 ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
		 from bronze.crm_cust_info where cst_id is not null)t where flag_last =1
		set @end_time = GETDATE()
		print '>> load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) +' seconds';
  -- 2. insert into silver.crm_prd_info
   set @start_time = GETDATE()
	print '>> truncating silver.crm_prd_info'
	truncate table silver.crm_prd_info;
	print '>> inserting data into silver.crm_prd_info'

		insert into silver.crm_prd_info
			( prd_id,
				 cat_id,
				 prd_key,
				 prd_nm,
				 prd_cost,
				 prd_line,
				 prd_start_dt,
				 prd_end_dt
			)
			select prd_id, replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id ,
			SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
			prd_nm, isnull(prd_cost,0) as prd_cost,
			case 
			  when trim(upper(prd_line)) = 'R' then 'Road'
			  when trim(upper(prd_line)) = 'M' then 'Mountain'
			  when trim(upper(prd_line)) = 'S' then 'Other Sales'
			  when trim(upper(prd_line)) = 'T' then 'Touring'
			  else 'n/a'
			 end prd_line
			, prd_start_dt, dateadd(day,-1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt
			from bronze.crm_prd_info
			set @end_time = GETDATE()
			print '<< load duration:' + cast(datediff(second, @start_time, @end_time)as nvarchar) + ' seconds'

   --3.insert into silver.crm_sales_details
   set @start_time = GETDATE()
	print '>> truncating silver.crm_sales_details'
	truncate table silver.crm_sales_details;
	print '>> inserting data into silver.crm_sales_details'

		   insert into silver.crm_sales_details (
				  sls_ord_num ,
				  sls_prd_key ,
				  sls_cust_id ,
				  sls_order_dt ,
				  sls_ship_date,
				  sls_due_dt,
				  sls_sales,
				  sls_quantity ,
				  sls_price 
				)
			select 
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					case 
					  when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
					  else cast(cast(sls_order_dt as varchar) as date)
					end sls_order_dt,
					case 
					  when sls_ship_date = 0 or len(sls_ship_date) != 8 then null
					  else cast(cast(sls_ship_date as varchar) as date)
					end as sls_ship_date,
					case 
						when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
						else cast(cast(sls_due_dt as varchar) as date)
					end as sls_due_dt,
					case 
						  when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
						  else sls_sales
					  end as sls_sales,
					sls_quantity, 
					case
						 when sls_price = 0 or sls_price is null then sls_sales/nullif(sls_quantity,0)
						 when sls_price < 0 then abs(sls_price)
						 else sls_price
					end sls_price
			from bronze.crm_sales_details
			set @end_time = GETDATE()
			print '<<load duration : ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'

    --4.insert into silver.erp_cust_az12
   set @start_time = GETDATE()
	print '>> truncating silver.erp_cust_az12'
	truncate table silver.erp_cust_az12;
	print '>> inserting data into silver.erp_cust_az12'
 
			insert into silver.erp_cust_az12
			  (
			   cid,
			   bdate,
			   gen
			  )
			 select
			  case 
				  when cid like 'Nas%' then trim(substring(cid, 4, len(cid)))
				  else trim(cid)
			  end as cid,
			   case
						when bdate > getdate() then null
						else bdate
				end as bdate,
			  case 
					 when trim(upper(gen)) in  ('F', 'FEMALE') then 'Female'
					 when trim(upper(gen)) IN ('M', 'MALE') THEN 'Male'
					 else 'n/a'
				end as gen
			from bronze.erp_cust_az12;
		set @end_time = GETDATE()
		print '<<load duration : ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'

    --5.insert into silver.erp_loc_a101
   set @start_time = GETDATE()
	print '>> truncating silver.erp_loc_a101'
	truncate table silver.erp_loc_a101;
	print '>> inserting data into silver.erp_loc_a101'
 
		   insert into silver.erp_loc_a101
									  (
									   cid,
									   cntry
									  )
			select  replace (cid, '-', '') as cid,
				case 
				   when trim(cntry) = 'DE' then 'Germany'
				   when trim(cntry) in ('US', 'USA', 'United States') then 'United States'
				   when trim(cntry) = '' or cntry is null then 'n/a'
				   else trim(cntry)
				end cntry
			from bronze.erp_loc_a101;
		set @end_time = GETDATE()
		print '<<load duration : ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'

		--6.insert into silver.erp_px_cat_g1v2
       set @start_time = GETDATE()
	print '>> truncating silver.erp_px_cat_g1v2'
	truncate table silver.erp_px_cat_g1v2;
	print '>> inserting data into silver.erp_px_cat_g1v2'
 
		 insert into silver.erp_px_cat_g1v2
			(id, cat, subcat, maintenance )
			select id, cat, subcat, maintenance from bronze.erp_px_cat_g1v2
			set @end_time = GETDATE()
	   print '<<load duration : ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

	   set @batch_end_time = GETDATE()
	   print '==========================================================='
	   print 'total batch load time : ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds'
	   print '==========================================================='
	   end try

	   begin catch
	   print 'error occured during loading into silver layer'
	   print 'error message' + error_message();
	   print 'error message' + cast(error_number() as varchar);
	   print 'error message' + cast(error_state() as varchar);
	   end catch


end;
