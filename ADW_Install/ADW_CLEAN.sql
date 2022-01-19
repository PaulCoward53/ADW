CREATE OR REPLACE PROCEDURE ADW_CLEAN (p_App_ID  IN VARCHAR) is
    v_Count    number;
	v_table_name varchar2(128);
  BEGIN

	for tableRec in (SELECT TABLE_NAME FROM USER_TABLES 
	                  WHERE   TABLE_NAME like(p_App_ID||'\_%\_S') escape '\'
					       or TABLE_NAME like(p_App_ID||'\_%\_P') escape '\'
						   or TABLE_NAME like(p_App_ID||'\_%\_B') escape '\')
	LOOP
        EXECUTE IMMEDIATE    'DROP TABLE "'||tableRec.TABLE_NAME||'"';
    end loop;
	
	for tableRec in (SELECT VIEW_NAME FROM USER_VIEWS 
	                  WHERE   VIEW_NAME like(p_App_ID||'\_%\_VW') escape '\')
	LOOP
        EXECUTE IMMEDIATE    'DROP VIEW "'||tableRec.VIEW_NAME||'"';
    end loop;


  EXCEPTION
       WHEN OTHERS THEN
            dbms_output.put_line     ('UTILITY:Remove Stage Tables for application '||p_App_ID||'-'||SQLERRM);
  END ADW_CLEAN;
  
begin
  ADW_CLEAN('PDM');
  DELETE FROM ADW_ETL_LOG;
  DELETE FROM ADW_ETL_STAGE WHERE  APP_ID = 'PDM';
  DELETE FROM ADW_PROCESS_LOG;
  DELETE FROM ADW_PROCESS_EXECUTION;
  DELETE FROM ADW_APPLICATION where APP_ID = 'PDM';
  COMMIT;
  
end;