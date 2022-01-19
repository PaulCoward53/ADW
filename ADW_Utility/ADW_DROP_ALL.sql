 CREATE OR REPLACE PROCEDURE ADW_DROP_ALL is
  BEGIN
	
	--
	
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE
                      from ALL_OBJECTS O
					  left outer join ALL_OBJECTS B on B.OBJECT_NAME = O.OBJECT_NAME AND B.OBJECT_TYPE <> O.OBJECT_TYPE
                     where O.OWNER = 'ADWADMIN' and O.OBJECT_NAME NOT LIKE '%$$%' AND O.OBJECT_NAME <> 'ADW_DROP_ALL' AND B.OBJECT_NAME is null
					 AND O.OBJECT_TYPE IN ('TABLE'))
	LOOP
		dbms_output.put_line('DROP TABLE '||objectRec.OBJECT_NAME||' CASCADE CONSTRAINTS');
		EXECUTE IMMEDIATE 'DROP TABLE '||objectRec.OBJECT_NAME||' CASCADE CONSTRAINTS';
	END LOOP;
	
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE
                      from ALL_OBJECTS O
                     where O.OWNER = 'ADWADMIN' and O.OBJECT_NAME NOT LIKE '%$$%' AND O.OBJECT_NAME <> 'ADW_DROP_ALL' 
					 AND O.OBJECT_TYPE IN ('SEQUENCE','VIEW','PROCEDURE','FUNCTION','PACKAGE','MATERIALIZED VIEW'))
	LOOP
		dbms_output.put_line('DROP '||objectRec.OBJECT_TYPE||' '||objectRec.OBJECT_NAME);
		EXECUTE IMMEDIATE 'DROP '||objectRec.OBJECT_TYPE||' '||objectRec.OBJECT_NAME;
	END LOOP;
	
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE
                      from ALL_OBJECTS O
                     where O.OWNER = 'ADWADMIN' and O.OBJECT_NAME NOT LIKE '%$$%' AND O.OBJECT_NAME <> 'ADW_DROP_ALL' 
					 AND O.OBJECT_TYPE IN ('SCHEDULE'))
	LOOP
		dbms_output.put_line('dbms_scheduler.drop_schedule ('''||objectRec.OBJECT_NAME||''') ');
		EXECUTE IMMEDIATE 'BEGIN dbms_scheduler.drop_schedule ('''||objectRec.OBJECT_NAME||'''); END;';
	END LOOP;
	
    dbms_output.put_line('-----------------------------------------------------------------------------');	
    dbms_output.put_line('Summary of what is left ');	
	
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE
                      from ALL_OBJECTS O
                     where OWNER = 'ADWADMIN' and OBJECT_NAME NOT LIKE '%$$%')
	LOOP
        dbms_output.put_line('Object: '||objectRec.OBJECT_NAME ||' Type: '|| objectRec.OBJECT_TYPE||' Still exists');	
	END LOOP;
	
  EXCEPTION
       WHEN OTHERS THEN
            dbms_output.put_line(SQLERRM);
  END ADW_DROP_ALL;
