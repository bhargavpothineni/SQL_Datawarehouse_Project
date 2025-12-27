/*
Script Purpose: -This Script includes transformations of data in bronze layer
                -loading the transformed data into silver layer
                -doing quality checks of the loaded data in silver layer.
Transformations include:
                   1. Handling null values in both PK and other columns
                   2. Cleaning unwanted spcaes  
                   3. Data Standardisation and consistency
                   4. Invalid date ranges and orders
                   5. Data consistencies between related fields
                   6. Data Type Casting
                   7. derived colums (new columns from older columns)
                   8. Data Enrichment (adding new values of data)
                   9. Duplicate Values
NOTE : **This script contains  'transformation', 'data loading' and 'quality checks'** Please check the code before running
==========================================================================================================================

*/


-- check for nulls, duplicates in primary key
 select * from bronze.crm_cust_info;

 select cst_id, COUNT(*)
 from bronze.crm_cust_info
 group by cst_id
 having COUNT(*) >1 or cst_id is null;

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
 from bronze.crm_cust_info where cst_id is not null)t where flag_last =1;

 -- data standardisation and consistency

select * from bronze.crm_cust_info where cst_id is null;

select * from silver.crm_cust_info order by cst_id;
=======================================================================================================
select * from 
(select *, ROW_NUMBER() over(partition by prd_id order by prd_id ) as rownum from bronze.crm_prd_info)t where rownum >1;


select prd_id, COUNT(*)
from bronze.crm_prd_info 
group by prd_id
having COUNT(*) >1 or prd_id is null;
----------------------------------------------------------------------------------------------------
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

---------------------------------------------------------------------------------------------------




select * from silver.crm_prd_info;

--data quality checks silver layer
--checking for any null values or negative values in prd_cost column
select prd_cost
from silver.crm_prd_info where prd_cost is null or prd_cost <0;

-- data standardisation and constistency

select distinct prd_line  from silver.crm_prd_info;

-- check for invalid dates
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt
----------------------------------------------------------------------------------------------------------

--------------- cleaning sales details table and inserting into silver layer

select top 10 * from bronze.crm_sales_details ;


----- update this below table select statement with transformed columns for inserting into silver table 
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
		case
	         when sls_price = 0 or sls_price is null then sls_sales/nullif(sls_quantity,0)
	         when sls_price < 0 then abs(sls_price)
	         else sls_price
        end sls_price,
		sls_price
from bronze.crm_sales_details

--- the order date column is in integer format and needs to be transformed
-- check for any missing values in dates or 0's or any random numbers

select sls_order_dt from bronze.crm_sales_details where sls_order_dt <= 0;

--- we got 0's in the date column so it needs to be changed to nulls using " nullif()"

select nullif(sls_order_dt,0) as sls_order_dt from bronze.crm_sales_details  where  nullif(sls_order_dt,0) <= 0;

-- check for other factors like length of the column (its should be 8 if its not it wont work as a date colum)

select nullif(sls_order_dt,0) as sls_order_dt from bronze.crm_sales_details 
where len(nullif(sls_order_dt,0)) != 8 or nullif(sls_order_dt,0) > 20500101
or nullif(sls_order_dt,0) < 19000101

--- if the order date column is 0 or length is not 8 use "case when" to change it to null
-- if the column is correct then change the data type from 'int' to 'varchar' to 'date' using cast

select *, 
case 
    when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
	else cast(cast(sls_order_dt as varchar) as date)
end sls_order_dt
from bronze.crm_sales_details 

--- check for remaining columns "sls_ship_date"

select sls_ship_date  from bronze.crm_sales_details 
where len(sls_ship_date) != 8 or sls_ship_date > 20500101
or sls_ship_date < 19000101 or sls_ship_date <= 0;

/* there are no errors with the ship date column exept casting from int to date
but just incase for future every transformaton like above column is added to this column as well*/

select *, 
case 
    when sls_ship_date = 0 or len(sls_ship_date) != 8 then null
	else cast(cast(sls_ship_date as varchar) as date)
end as sls_ship_date
from bronze.crm_sales_details 
-- add this transformation to the main syntax

-- same with due date column as well

select sls_due_dt  from bronze.crm_sales_details 
where len(sls_due_dt ) != 8 or sls_due_dt  > 20500101
or sls_due_dt  < 19000101 or sls_due_dt  <= 0;

--- we found no error but just incase we will add same transformations to the column

select *, 
case 
    when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
	else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt
from bronze.crm_sales_details 
-- add this transformation to the main syntax
-- make sure to check if the order date is smaller than the shipping date and due date;

select * from bronze.crm_sales_details where sls_order_dt >sls_ship_date or sls_order_dt > sls_due_dt;

--- check for any anaomolies in sales column 
-- we have sales , quantity, price columns 
----- rule is quantity * price = sales
-- negatives, zeros and nulls are not allowed

select sls_sales, sls_quantity, sls_price from bronze.crm_sales_details 
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales,sls_quantity, sls_price;

/* we have nulls, negatives, zeros in both sales and price columns 

  RULES: if negatives, nulls and zeros in sales derive it using quantity and price : (sales = quantity * price)
       : if nulls and zeros in price column calculate using sales and quantity (price = sale/ quantity)
	   : if negatives in price then convert into positive
*/


select distinct sls_sales, sls_quantity, sls_price ,
  case 
       when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
	   else sls_sales
  end  as sls_sales_new ,
  case
	   when sls_price = 0 or sls_price is null then sls_sales/nullif(sls_quantity,0)  -- for future if quantity is null(nullif(sls_quantity,0))
	   when sls_price < 0 then abs(sls_price)
	   else sls_price
 end sls_price_new
from bronze.crm_sales_details 
order by sls_price



---- checking if everything is transformed 
select * from 
(
select sls_sales, sls_quantity, sls_price ,
  case 
       when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
	   else sls_sales
  end  as sls_sales_new ,
  case
	   when sls_price = 0 or sls_price is null then sls_sales/nullif(sls_quantity,0)
	   when sls_price < 0 then abs(sls_price)
	   else sls_price
 end sls_price_new
from bronze.crm_sales_details )t
 where sls_sales_new != sls_quantity * sls_price_new
or sls_sales_new is null or sls_quantity is null or sls_price_new is null
or sls_sales_new <= 0 or sls_quantity <= 0 or sls_price_new <= 0;


/* after all the transformations check the targeted table (silver.crm_sales_details) if the table structure matches with the incoming data like 
    no. of columns data types of the columns etc...
*/

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

/*in the above already created syntax of the targeted table 
  the data types of the date columns are mentioned "int" and needs to be changed to 'Date'
  -- now drop the table and recreate it using correct data types*/



if OBJECT_ID('silver.crm_sales_details','u') is not null
   begin
     drop table silver.crm_sales_details
   end
CREATE TABLE silver.crm_sales_details (
      sls_ord_num  nvarchar(50),
	  sls_prd_key nvarchar(50),
	  sls_cust_id int,
	  sls_order_dt date,
	  sls_ship_date date,
	  sls_due_dt date,
	  sls_sales int,
	  sls_quantity int,
	  sls_price int,
	  dwh_create_date datetime2 default getdate()
);

--- insert the transformed details into the targeted table

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

select * from silver.crm_sales_details

-- after the data is inserted makesure everything is good in the silver table if there are any inconsistencies please make sure to transform them.
------------------------------------------------------------------------------------------------------------------------------------
-- cleaning bronze.erp_cust_az12 and inserting into silver.erp_cust_az12
select * from bronze.erp_cust_az12

/*
the cid column in this table is connected to cst_key in bronze.crm_cust_info upon checking both the columns i have 
figured there is some additional date in cid column for some records like "NASAW00011000" insted of like this "AW00029476"
those extra 3 letters in the front 'NAS' should be removed
*/
-- the "case when"  and "substring "transformation has been added for 
-- remove those extra letters and 'trim' has been added for extra saftey

select
  case 
      when cid like 'Nas%' then trim(substring(cid, 4, len(cid)))
	  else trim(cid)
  end cid1,
  bdate, gen
from bronze.erp_cust_az12 

-- checking for any older or future dates in the bdate column
select bdate from bronze.erp_cust_az12 where bdate > GETDATE() or bdate < '1920-01-01';
--- there are records matching both statements 
select  
       case
            when bdate > getdate() then null
			else bdate
	   end bdate
from bronze.erp_cust_az12

--checking the transformation

select * from (
select  
       case
            when bdate > getdate() then null
			else bdate
	   end bdate
from bronze.erp_cust_az12)t where bdate > getdate()

-- checking the cardinality of the gen column

select distinct gen from bronze.erp_cust_az12;
-- we can find there are nulls, emplystring, m's , f's, male's, female's
-- all should be changed to either male or female

select distinct gen from
(select 
     case 
		 when trim(upper(gen)) in  ('F', 'FEMALE') then 'Female'
		 when trim(upper(gen)) IN ('M', 'MALE') THEN 'Male'
		
		 else 'n/a'
	 end as gen
from bronze.erp_cust_az12)t 

/* update all the transformed values into a single syntax to insert the 
 transformed data into the targeted table and insert into silver.erp_cust_az12 */
 
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
from bronze.erp_cust_az12 

--- after inserting date makesure everything is correct in the new table (data quality)
select * from silver.erp_cust_az12 where bdate > getdate();
select distinct gen from silver.erp_cust_az12;

select * from silver.erp_cust_az12;
------------------------------------------------------------------------------------------
-- transforming 'bronze.erp_loc_a101' data and inserting it into silver.erp_loc_a101

select cid from bronze.erp_loc_a101

select cst_key from silver.crm_cust_info


/* the cid column in thgis tabel is connected to cst_key column in silver.crm_cust_details
but there is -(hypen) in the records of this table like"AW-00011000" insted of "AW00011080"
*/
-- replace func is used for transforming the data 

select replace (cid, '-', '') as cid
 from bronze.erp_loc_a101;

--- transforming cntry column with empty values, irregular names and nulls
-- data standardisation and consistency

select  distinct
    case 
	   when trim(cntry) = 'DE' then 'Germany'
	   when trim(cntry) in ('US', 'USA', 'United States') then 'United States'
	   when trim(cntry) = '' or cntry is null then 'n/a'
	   else trim(cntry)
	end cntry
from bronze.erp_loc_a101;


-- replacing the updated transformed syntax into select statement and inserting the transformed data into targeted silver table

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

--- check the data quality of the silver table 
select distinct cntry from silver.erp_loc_a101;

select * from silver.erp_loc_a101;

----------------------------------------------------------------------------------------------------
--transforming data in bronze.erp_px_cat_g1v2; and inserting into targeted table in silver layer

select * from bronze.erp_px_cat_g1v2 where id not in(
select cat_id from silver.crm_prd_info);

-- checking unwanted spaces
select distinct subcat from bronze.erp_px_cat_g1v2
--- there are no transformations needed for this table so the data is ready to be inserted into targeted table.

insert into silver.erp_px_cat_g1v2
(id, cat, subcat, maintenance )
select id, cat, subcat, maintenance from bronze.erp_px_cat_g1v2

select * from  silver.erp_px_cat_g1v2;


