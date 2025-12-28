/*
============================================================================================================================
Quality checks:
script purpose:
              this script performs quality checks to validate the integrity, consistency and accuracy of the goldlayer
              these checks ensure the uniqueness of surrogate keys in the dimension tables.
              - referential integrity between fact and dimension tables.
              - validationof relationships in the datamodel for analytical purposes.
============================================================================================================================
*/

select *from gold.dim_customers;


-----------------------------------------------------------------------------------------------------------------------
--- the gender column is updated using both columns from two tables and derived a new column
 
select distinct
 ci.cst_gndr,
 ca.gen,
  case
     when ci.cst_gndr != 'n/a' then ci.cst_gndr
	 else coalesce(ca.gen,'n/a')
  end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 cl
on ci.cst_key = cl.cid
order by 1,2;
----------------------------------------------------------------------------------------------------------------------------
select * from gold.dim_products
select top 5* from silver.erp_px_cat_g1v2
-------------------------------------------------------------------------------------------------------------------------
-- checking foreign key integrity(Dimensions) by joining the fact and dim tables

select * from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = f.customer_key
left join gold.dim_products p
on f.product_key = p.product_key
where p.product_key is null

select * from gold.dim_products;

select * from gold.fact_sales;
