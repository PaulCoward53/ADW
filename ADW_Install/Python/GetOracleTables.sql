select 'Insert into ADW_ETL_STAGE (APP_ID,ETL_TYPE,ETL_TYPE_PARM,ETL_SRC_SCHEMA,ETL_SRC_TABLE) 
        values (''PDM'',''DBCOPY'',''PDM_LINK'',''PDM'','''||table_name||'''); '
 FROM ALL_TABLES WHERE OWNER = 'PDM';
