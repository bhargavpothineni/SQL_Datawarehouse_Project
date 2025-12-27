/*
=================================================================================
DDL Script : Create Silver Tables
=================================================================================
Script Purpose:
     1. This Script creates tables in the 'Silver' Schema, dropping existing tables
        if they already exists
     2. Run this script to re-define the DDL Structure of 'Bronze' Tables
=================================================================================
*/

-- dropping table if exists and creating table structures

if OBJECT_ID('silver.crm_cust_info','u') is not null
   begin
     drop table silver.crm_cust_info
   end

create table silver.crm_cust_info(
     cst_id int,
	 cst_key nvarchar(20),
	 cst_firstname nvarchar(50),
	 cst_lastname nvarchar(50),
	 cst_marital_status nvarchar(10),
	 cst_gndr nvarchar(10),
	 cst_create_date date,
	 dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.crm_prd_info','u') is not null
   begin
     drop table silver.crm_prd_info
   end
create table silver.crm_prd_info(
     prd_id int,
	 cat_id nvarchar(50),
	 prd_key nvarchar(50),
	 prd_nm nvarchar(100),
	 prd_cost int,
	 prd_line nvarchar(50),
	 prd_start_dt date,
	 prd_end_dt date,
	 dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.crm_sales_details','u') is not null
   begin
     drop table silver.crm_sales_details
   end
CREATE TABLE silver.crm_sales_details (
      sls_ord_num  nvarchar(50),
	  sls_prd_key nvarchar(50),
	  sls_cust_id int,
	  sls_order_dt int,
	  sls_ship_date int,
	  sls_due_dt int,
	  sls_sales int,
	  sls_quantity int,
	  sls_price int,
	  dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.erp_cust_az12','u') is not null
   begin
     drop table silver.erp_cust_az12
   end

create table silver.erp_cust_az12 (
    cid nvarchar(50),
	bdate date,
	gen nvarchar(10),
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.erp_loc_a101','u') is not null
   begin
     drop table silver.erp_loc_a101
   end
create table silver.erp_loc_a101
(   
      cid nvarchar(50),
	  cntry nvarchar(50),
	  dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.erp_px_cat_g1v2','u') is not null
   begin
     drop table silver.erp_px_cat_g1v2
   end
create table silver.erp_px_cat_g1v2
 (
   id nvarchar(50),
   cat nvarchar(50),
   subcat nvarchar(50),
   maintenance nvarchar(50),
   dwh_create_date datetime2 default getdate()
 );
