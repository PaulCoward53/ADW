--
-- ------------------------------------------------------------------------------------------------------------------------------------------
-- Set up ETL from a Oracle Server
--
--  1) Add application to ADW_APPLICATION table
--  2) Login into Application Oracle Server as ADWADMIN...
--  3) Modify below SQL to reflect Application ID and DBLink
--  4) Copy results of SQL to ADW worksheet and execute insert
--
--  5) Create DBLink in ADW Instance
--  6) Create a <APP>_BUILD procedure using the sample ADW_SAMPLE_BUILD
--  7) Run <APP>_BUILD procedure in ADW

select 'Insert into ADW_ETL_STAGE (APP_ID,ETL_TYPE,ETL_TYPE_PARM,ETL_SRC_SCHEMA,ETL_SRC_TABLE) 
        values (''<App>'',''DBCOPY'',''<DB LINK>'',''<APP>'','''||table_name||'''); '
 FROM ALL_TABLES WHERE OWNER = '<App Owner>';
	

--
-- ------------------------------------------------------------------------------------------------------------------------------------------
-- Set up ETL from a SQL Server
--

--  1) Add application to ADW_APPLICATION table
--  2) Login into SQL Server as ADWADMIN...
--  3) Modify below SQL to reflect Application ID and Application Connection
--  4) Copy results of SQL to ADW worksheet and execute insert
--
--  5) Create <App Connection> in ADW_CONNECTIONS.ini
--      python ADW_Connections
--
--  6) Run Python program 
--      python ADW_MSSQL_ETL.py -AppID <App Connection>

select 'Insert into ADW_ETL_STAGE (APP_ID,ETL_TYPE,ETL_TYPE_PARM,ETL_SRC_SCHEMA,ETL_SRC_TABLE) 
        values (''<APP ID>'',''<either MSINSERT or BCPTransfer'',''<App Connection>'',''' + TABLE_SCHEMA + ''',''' + table_name + '''); '
 FROM INFORMATION_SCHEMA.tables
      WHERE TABLE_CATALOG = '<APP Database Catalog>' and TABLE_SCHEMA = 'dbo'

