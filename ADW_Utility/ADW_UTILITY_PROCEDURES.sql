 CREATE OR REPLACE PROCEDURE ADW_HEALTH_CHECK is
	v_NoViolations NUMBER;
  BEGIN

	ADW_PROCESS.PROCESS_BEGIN('ADW_HEALTH_CHECK');
	
    -- RULE 1: All object names must be prefixed with application ID defined in the ADW_APPLICATION table
	--
	ADW_PROCESS.PROCESS_LOG('RULE 1: All object names must be prefixed with application ID defined in the ADW_APPLICATION table');
	
	v_NoViolations := 0;
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE
                      from ALL_OBJECTS O
                     left outer join ADW_APPLICATION A on O.OBJECT_NAME like A.APP_ID||'\_%' escape '\'
                     where OWNER = 'ADWADMIN' and OBJECT_NAME NOT LIKE '%$$%' and A.APP_ID is null
					 AND O.OBJECT_TYPE IN ('VIEW','SEQUENCE','PROCEDURE','FUNCTION','PACKAGE','TRIGGER'))
	LOOP
		ADW_PROCESS.PROCESS_ERROR(' Object: '||objectRec.OBJECT_NAME ||' of type '||objectRec.OBJECT_TYPE||' is not prefixed with application name');
		v_NoViolations := v_NoViolations +1;
	END LOOP;

    -- RULE 2: All objects must be valid
	--
	ADW_PROCESS.PROCESS_LOG('RULE 2: All objects must be valid');

	-- First we need to compile invalid objects
	-- VIEWS,PROCEDURE,FUNCTIONS, AND TRIGGERS
	
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE
                      from ALL_OBJECTS O
                     left outer join ADW_APPLICATION A on O.OBJECT_NAME like A.APP_ID||'\_%' escape '\'
                     where OWNER = 'ADWADMIN' and OBJECT_NAME NOT LIKE '%$$%' and A.APP_ID is not null
					   AND O.STATUS <> 'VALID'	 AND O.OBJECT_TYPE IN ('VIEW','PROCEDURE','FUNCTION','PACKAGE','TRIGGER'))
	LOOP
		EXECUTE IMMEDIATE 'ALTER '||objectRec.OBJECT_TYPE||' '||objectRec.OBJECT_NAME||' compile';
	END LOOP;

	-- PACKAGES
	
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE
                      from ALL_OBJECTS O
                     left outer join ADW_APPLICATION A on O.OBJECT_NAME like A.APP_ID||'\_%' escape '\'
                     where OWNER = 'ADWADMIN' and OBJECT_NAME NOT LIKE '%$$%' and A.APP_ID is not null
					   AND O.STATUS <> 'VALID'	 AND O.OBJECT_TYPE IN ('PACKAGE BODY'))
	LOOP
		EXECUTE IMMEDIATE 'ALTER PACKAGE '||objectRec.OBJECT_NAME||' compile body';
	END LOOP;
	
	-- MATERIALIZED VIEW
	
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE
                      from ALL_OBJECTS O
                     left outer join ADW_APPLICATION A on O.OBJECT_NAME like A.APP_ID||'\_%' escape '\'
                     where OWNER = 'ADWADMIN' and OBJECT_NAME NOT LIKE '%$$%' and A.APP_ID is not null
					   AND O.STATUS <> 'VALID'	 AND O.OBJECT_TYPE IN ('MATERIALIZED VIEW'))
	LOOP
		EXECUTE IMMEDIATE 'ALTER MATERIALIZED VIEW '||objectRec.OBJECT_NAME||' compile';
	END LOOP;
	
	-- Report any errors left
	
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE,O.STATUS
                      from ALL_OBJECTS O
                     left outer join ADW_APPLICATION A on O.OBJECT_NAME like A.APP_ID||'\_%' escape '\'
                     where OWNER = 'ADWADMIN' and OBJECT_NAME NOT LIKE '%$$%' and A.APP_ID is not null 
					   AND O.STATUS <> 'VALID' AND O.OBJECT_TYPE IN ('VIEW','PROCEDURE','FUNCTION','TRIGGER','PACKAGE','PACKAGE BODY','MATERIALIZED VIEW'))
    LOOP
		ADW_PROCESS.PROCESS_ERROR(' Object: '||objectRec.OBJECT_NAME ||' of type '||objectRec.OBJECT_TYPE||' is '||objectRec.STATUS);
		v_NoViolations := v_NoViolations +1;
	END LOOP;
	
	
	ADW_PROCESS.PROCESS_LOG('RULE 3: Views must end in _V or _VW');
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE,O.STATUS
                      from ALL_OBJECTS O
                     where OWNER = 'ADWADMIN' and OBJECT_NAME NOT LIKE '%$$%' 
					   AND (O.OBJECT_NAME not like '%\_V' escape '\' AND O.OBJECT_NAME not like '%\_VW' escape '\')
					   AND  O.OBJECT_TYPE IN ('VIEW'))
    LOOP
		ADW_PROCESS.PROCESS_ERROR(' Object: '||objectRec.OBJECT_NAME ||' Not a correct View name');
		v_NoViolations := v_NoViolations +1;
	END LOOP;
	
	ADW_PROCESS.PROCESS_LOG('RULE 4: Materialized Views must end in _MV');
	
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE,O.STATUS
                      from ALL_OBJECTS O
                     where OWNER = 'ADWADMIN' and OBJECT_NAME NOT LIKE '%$$%' 
					   AND  O.OBJECT_NAME not like '%\_MV' escape '\'
					   AND  O.OBJECT_TYPE IN ('MATERIALIZED VIEW'))
    LOOP
		ADW_PROCESS.PROCESS_ERROR(' Object: '||objectRec.OBJECT_NAME ||' Not a correct Materialized View name');
		v_NoViolations := v_NoViolations +1;
	END LOOP;

	ADW_PROCESS.PROCESS_LOG('RULE 4: Sequences must end in _SEQ');
	
	for objectRec in (select O.OBJECT_NAME,O.OBJECT_TYPE,O.STATUS
                      from ALL_OBJECTS O
                     where OWNER = 'ADWADMIN' and OBJECT_NAME NOT LIKE '%$$%' 
					   AND  O.OBJECT_NAME not like '%\_SEQ' escape '\'
					   AND  O.OBJECT_TYPE IN ('SEQUENCE'))
    LOOP
		ADW_PROCESS.PROCESS_ERROR(' Object: '||objectRec.OBJECT_NAME ||' Not a Sequence name');
		v_NoViolations := v_NoViolations +1;
	END LOOP;
	
	if v_NoViolations = 0 THEN
		ADW_PROCESS.PROCESS_LOG(' NO ERRORS ENCOUNTERED ');
	ELSE
		ADW_PROCESS.PROCESS_LOG(' '||v_NoViolations||' ERRORS ENCOUNTERED ');
	
	END IF;
	
	ADW_PROCESS.PROCESS_END();
  EXCEPTION
       WHEN OTHERS THEN
            dbms_output.put_line('ADW_HEALTH_CHECK -'||SQLERRM);
            ADW_PROCESS.PROCESS_ERROR('ADW_HEALTH_CHECK -'||SQLERRM);
  END ADW_HEALTH_CHECK;
