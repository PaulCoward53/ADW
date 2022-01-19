select 'Insert into ADW_ETL_STAGE (APP_ID,ETL_TYPE,ETL_TYPE_PARM,ETL_SRC_SCHEMA,ETL_SRC_TABLE) 
        values (''MSAP'',''MSINSERT'',''MSAP_PROD'',''' + TABLE_SCHEMA + ''',''' + table_name + '''); '
 FROM INFORMATION_SCHEMA.tables
      WHERE TABLE_CATALOG = 'MSAP' and TABLE_SCHEMA = 'dbo'

	  select 'Insert into ADW_ETL_STAGE (APP_ID,ETL_TYPE,ETL_TYPE_PARM,ETL_SRC_SCHEMA,ETL_SRC_TABLE) 
        values (''NW'',''MSINSERT'',''NW_PROD'',''' + TABLE_SCHEMA + ''',''' + table_name + '''); '
 FROM INFORMATION_SCHEMA.tables
      WHERE TABLE_CATALOG = 'Northwind' and TABLE_SCHEMA = 'dbo'