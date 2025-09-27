/*
Create DataBase and Schemas

=================================================================

Script Purpose:
		This Script Create Database named 'DataWarehouse' adter checking if it already exist .
		If the database already Exists, it is dropped and recreated. Additionally, The Script 
		sets up three schemas Within The Database:(Bronze,silver and gold).

Warning:
		Runing this Script will drop the Entire Database if it already exists.
		ALL DATA IN THE DATABASE WILL BE PERMANENTLY DELETED SO PROCEED WITH CAUTION.
		and insure you have a backups before running the script
*/

Use master;
GO

-- Drop And Recreate the 'DataWarehouse' database
IF EXISTS(SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET Single_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;

GO


-- CREATE Database 'DataWarehouse'


CREATE Database DataWarehouse;
GO
USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
Go
CREATE SCHEMA silver;
Go
CREATE SCHEMA gold;
GO
