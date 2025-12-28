/*
DDL Script: Creating Gold Views
=========================================================================================================================
Script Purpose: 
        : this script creates views for the gold layer in the data warehouse.
        : The gold layer represents the final dimension and fact tables(star schema)
        : each view performs transformations and combines data from the silver layer
          to produce a clean enriched and business ready dataset.

usage:
    - these views can be queried for analytical and reporting
=========================================================================================================================
*/
/*
-- creating customer dimension view 
for getting all the customer information we need to join customer related tables 'silver.crm_cust_info',
 'silver.erp_cust_az12', 'silver.erp_loc_a101' using 'Left Join'
 -- the gender column is updated using data from two tables
 -- the column names have been updated accordingly for better read
 --- the rules for updating column names are :- snake_case (lower letters with underscore)
                                             :- english language
											 :- Avoiding sql reserved words
*/

create view gold.dim_customers as 
select 
 ROW_NUMBER() over(order by ci.cst_id) as customer_key,
 ci.cst_id as customer_id,
 ci.cst_key as customer_number,
 ci.cst_firstname as first_name,
 ci.cst_lastname as last_name,
 cl.cntry as country,
 ci.cst_marital_status as marital_status,
 case
     when ci.cst_gndr != 'n/a' then ci.cst_gndr
	 else coalesce(ca.gen,'n/a')
  end as gender,
  ca.bdate as birthdate,
 ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 cl
on ci.cst_key = cl.cid;
-----------------------------------------------------------------------------------------------------------------------------
-- creating product dimension view

create view gold.dim_products as
select 
    ROW_NUMBER() over(order by pd.prd_start_dt , pd.prd_key) as product_key,
	pd.prd_id as product_id,
	pd.prd_key as product_number,
	pd.prd_nm as product_name,
	pd.cat_id as category_id,
	ct.cat as category,
	ct.subcat as subcategory,
	ct.maintenance,
	pd.prd_cost as cost,
	pd.prd_line as product_line,
	pd.prd_start_dt as start_date	
from silver.crm_prd_info pd
left join silver.erp_px_cat_g1v2 ct
on pd.cat_id = ct.id
where pd.prd_end_dt is null;  -- filter out all historical data 
-----------------------------------------------------------------------------------------------------------------------------------
--creating fact sales view
-- change the product key and customer key columns to surrogate key from the dimension tables to build a relation between the tables

create view gold.fact_sales as 
select 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_date as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id

