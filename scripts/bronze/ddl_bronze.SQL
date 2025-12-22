/*
==============================================================================
DDL Script : This Script contains syntax for creating and dropping Database if exists and then creating tables for data insertion.
==============================================================================
*/

-- create Database 'datawarehouse'

 USE MASTER;
 CREATE DATABASE Datawarehouse;
 USE Datawarehouse;

-- creating schemas
CREATE SCHEMA bronze;
go
CREATE SCHEMA silver;
go
CREATE SCHEMA gold;
go

use master;
--- drop database if exists
if exists (select name from sys.databases where name = 'Datawarehouse')
begin 
  alter database Datawarehouse set single_user with rollback immediate;
  drop database Datawarehouse
end;
go
-- dropping table if exists and creating table structures

if OBJECT_ID('bronze.crm_cust_info','u') is not null
   begin
     drop table bronze.crm_cust_info
   end

create table bronze.crm_cust_info(
     cst_id int,
	 cst_key nvarchar(20),
	 cst_firstname nvarchar(50),
	 cst_lastname nvarchar(50),
	 cst_marital_status nvarchar(10),
	 cst_gndr nvarchar(10),
	 cst_create_date date
);

if OBJECT_ID('bronze.crm_prd_info','u') is not null
   begin
     drop table bronze.crm_prd_info
   end
create table bronze.crm_prd_info(
     prd_id int,
	 prd_key nvarchar(50),
	 prd_nm nvarchar(100),
	 prd_cost int,
	 prd_line nvarchar(10),
	 prd_start_dt date,
	 prd_end_dt date
);

if OBJECT_ID('bronze.crm_sales_details','u') is not null
   begin
     drop table bronze.crm_sales_details
   end
CREATE TABLE bronze.crm_sales_details (
      sls_ord_num  nvarchar(50),
	  sls_prd_key nvarchar(50),
	  sls_cust_id int,
	  sls_order_dt int,
	  sls_ship_date int,
	  sls_due_dt int,
	  sls_sales int,
	  sls_quantity int,
	  sls_price int
);

if OBJECT_ID('bronze.erp_cust_az12','u') is not null
   begin
     drop table bronze.erp_cust_az12
   end

create table bronze.erp_cust_az12 (
    cid nvarchar(50),
	bdate date,
	gen nvarchar(10)
);

if OBJECT_ID('bronze.erp_loc_a101','u') is not null
   begin
     drop table bronze.erp_loc_a101
   end
create table bronze.erp_loc_a101
(   
      cid nvarchar(50),
	  cntry nvarchar(50)
);
if OBJECT_ID('bronze.erp_px_cat_g1v2','u') is not null
   begin
     drop table bronze.erp_px_cat_g1v2
   end
create table bronze.erp_px_cat_g1v2
 (
   id nvarchar(50),
   cat nvarchar(50),
   subcat nvarchar(50),
   maintenance nvarchar(50)
 );
