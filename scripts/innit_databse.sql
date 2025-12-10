/*
==================================================================================================
Create Database and Schemas
==================================================================================================
Script Purpose:
   this script creates a new database names 'Datawarehouse' after checking if it's already exists.
   if the database exists it gets dropped and recreated. additionally the script creates three schemas within the Database
   'bronze', 'silver' and 'gold'.

Warning:
        This script will drop the entire database named "Datawarehouse" if exists. and all the data will be permanently deleted.
*/


USE master;
go


--- drop database if exists
if exists (select name from sys.databases where name = 'Datawarehouse')
begin 
  alter database Datawarehouse set single_user with rollback immediate;
  drop database Datawarehouse
end;
go

-- create Database 'datawarehouse'
CREATE DATABASE Datawarehouse;
go
use Datawarehouse;
go

-- creating schemas

CREATE SCHEMA bronze;
go
CREATE SCHEMA silver;
go
CREATE SCHEMA gold;
go
