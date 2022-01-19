DROP PACKAGE ADW_SECURITY;

CREATE OR REPLACE PACKAGE ADW_SECURITY AS
  TABLE_ACCESS EXCEPTION;

--
-- Set global parameters
--
    PROCEDURE SET_ACCESS(p_SchemaName VARCHAR2 DEFAULT NULL);        
    PROCEDURE UPDATE_ETL_ACCESS;

    PROCEDURE HELP;

END ADW_SECURITY;
/

CREATE OR REPLACE PACKAGE BODY ADW_SECURITY AS
----------------------------------------------------------------------------------------------------------
-- SCRIPT: ADW_SECURITY
--
-- PURPOSE:
--   Perform security for Application Data Warehouse
--
-- DEPENDENCIES
--   
--     ADW_PROCESS : Package installed
--     ADW_UTILITY : Package installed
--
-- EXECUTION:
--   
--   
--    
--
-- SYNTAX:
--    N/A
--
-- HISTORY:
--  12-Jan-2022   P. Coward    Initial version
--
-----------------------------------------------------------------------------------------------------------

---------------------------------
-- Declare procedures
--

    Stage_Exception EXCEPTION;
    PRAGMA EXCEPTION_INIT(Stage_Exception, -20001);
----------------------------------
-- Declare Global Variables
--
    g_PACKAGE_VERSION  CONSTANT VARCHAR2(20) := 'V1.0.0 (Feb 22,2021)';
    g_PROCESS_NAME              VARCHAR2(20) := 'ADW_SECURITY';
    g_PROCESS_STATUS            VARCHAR2(20) := '';
    g_SESSION_NO                NUMBER      := 0;  
    g_PROCEDURE                 VARCHAR2(32);

    g_APP_ID                    VARCHAR2(32);    
    g_INT_TYPE                  VARCHAR2(32);    
    
    g_Start_Time                DATE;
	--
	-- Grant for given schema
	--
	PROCEDURE EXECUTE_GRANT(p_SchemaName VARCHAR2) is
	BEGIN
		    DBMS_OUTPUT.PUT_LINE('GRANTS FOR '||p_SchemaName);
			FOR grantRec  IN (SELECT O.PRIV,A.OBJECT_NAME,S.SCHEMA_NAME 
							    FROM ADW_SECURITY_SCHEMA S
								LEFT JOIN ADW_SECURITY_OBJECT   O ON S.ROLE = O.ROLE AND O.ACTIVE_IND = 'Y'
								LEFT JOIN ADW_SECURITY_ROLE     R ON S.ROLE = R.ROLE AND R.ACTIVE_IND = 'Y'
								LEFT JOIN ALL_OBJECTS           A ON     A.OBJECT_TYPE in ('TABLE','MATERIALIZED VIEW','VIEW')
																	   AND A.OWNER='ADWADMIN' 
																	   AND A.OBJECT_NAME NOT LIKE '%\_B' ESCAPE '\' 
																	   AND A.OBJECT_NAME LIKE O.OBJECT_SPEC ESCAPE '\' 
								 WHERE S.ACTIVE_IND = 'Y' AND S.SCHEMA_NAME = p_SchemaName
							minus
							  SELECT UTP.privilege,AO.OBJECT_NAME,UTP.GRANTEE
								FROM USER_TAB_PRIVS UTP
							   INNER JOIN ALL_OBJECTS AO on AO.OBJECT_NAME = UTP.TABLE_NAME AND AO.OWNER = 'ADWADMIN'
							   WHERE     UTP.TYPE in ('TABLE','MATERIALIZED VIEW','VIEW') 
							         AND UTP.GRANTOR = 'ADWADMIN'
								     AND UTP.GRANTEE = p_SchemaName)
			LOOP
  			    DBMS_OUTPUT.PUT_LINE('GRANT '||grantRec.PRIV||' ON '||grantRec.OBJECT_NAME||' TO '||grantRec.SCHEMA_NAME);
				EXECUTE IMMEDIATE    'GRANT '||grantRec.PRIV||' ON '||grantRec.OBJECT_NAME||' TO '||grantRec.SCHEMA_NAME;
			END LOOP;

    EXCEPTION
        WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('GRANT ERROR:'||SQLERRM);
  		  ADW_PROCESS.PROCESS_ERROR('GRANT ERROR:'||SQLERRM);   
	END EXECUTE_GRANT;
	--
	-- Revoke for given schema
	--
	PROCEDURE EXECUTE_REVOKE(p_SchemaName VARCHAR2) is
	BEGIN
		    DBMS_OUTPUT.PUT_LINE('REVOKES FOR '||p_SchemaName);
			FOR revokeRec  IN (SELECT UTP.privilege,AO.OBJECT_NAME,UTP.GRANTEE
   							     FROM USER_TAB_PRIVS UTP
								INNER JOIN ALL_OBJECTS AO on AO.OBJECT_NAME = UTP.TABLE_NAME AND AO.OWNER = 'ADWADMIN'
								 WHERE UTP.TYPE in ('TABLE','MATERIALIZED VIEW','VIEW') AND UTP.GRANTOR = 'ADWADMIN'
									   AND UTP.GRANTEE = p_SchemaName
							  minus
							   SELECT O.PRIV,A.OBJECT_NAME,S.SCHEMA_NAME 
								 FROM ADW_SECURITY_SCHEMA S
								 LEFT JOIN ADW_SECURITY_OBJECT   O ON S.ROLE = O.ROLE AND O.ACTIVE_IND = 'Y'
								 LEFT JOIN ADW_SECURITY_ROLE     R ON S.ROLE = R.ROLE AND R.ACTIVE_IND = 'Y'
								 LEFT JOIN ALL_OBJECTS           A ON     A.OBJECT_TYPE in ('TABLE','MATERIALIZED VIEW','VIEW')
																      AND A.OWNER='ADWADMIN' 
																	  AND A.OBJECT_NAME NOT LIKE '%\_B' ESCAPE '\' 
																	  AND A.OBJECT_NAME LIKE O.OBJECT_SPEC ESCAPE '\' 
								 WHERE S.ACTIVE_IND = 'Y' AND S.SCHEMA_NAME = p_SchemaName)

			LOOP
  			    DBMS_OUTPUT.PUT_LINE('REVOKE '||revokeRec.privilege||' ON '||revokeRec.OBJECT_NAME||' FROM '||revokeRec.GRANTEE);
				EXECUTE IMMEDIATE    'REVOKE '||revokeRec.privilege||' ON '||revokeRec.OBJECT_NAME||' FROM '||revokeRec.GRANTEE;
			END LOOP;

    EXCEPTION
        WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('REVOKE ERROR:'||SQLERRM);
		  ADW_PROCESS.PROCESS_ERROR('REVOKE ERROR:'||SQLERRM);   
	END EXECUTE_REVOKE;
------------------------------------------------------------------------------------------
--   PROCEDURE SET_ACCESS
--   Perform ACCESS on user objects
--
    PROCEDURE SET_ACCESS(p_SchemaName VARCHAR2 DEFAULT NULL) IS
        v_SchemaName     VARCHAR2(32);
        v_Role           VARCHAR2(32);
        V_Privledge      VARCHAR2(32);
        v_Table_Name     VARCHAR2(32);
        v_StartTime      DATE;
    BEGIN
       
		ADW_PROCESS.PROCESS_BEGIN(g_PROCESS_NAME,p_SchemaName);
		
        g_SESSION_NO := ADW_PROCESS.GET_SESSION_NO;
              
        g_START_TIME    := SYSDATE;
		
		if p_SchemaName is null THEN
			-- Get all Valid Schema and then process one at a time
			
			FOR schemaRec  IN (SELECT UNIQUE SCHEMA_NAME FROM ADW_SECURITY_SCHEMA)
			LOOP
				EXECUTE_GRANT (schemaRec.SCHEMA_NAME);
				EXECUTE_REVOKE(schemaRec.SCHEMA_NAME);
			END LOOP;
			
		ELSE
			EXECUTE_GRANT (p_SchemaName);
			EXECUTE_REVOKE(p_SchemaName);
		END IF;
		
		ADW_PROCESS.PROCESS_END;
        
    END SET_ACCESS;
	
------------------------------------------------------------------------------------------
--   PROCEDURE UPDATE_ETL_ACCESS
--   Perform update access based on ETL
--
    PROCEDURE UPDATE_ETL_ACCESS IS
    BEGIN
       
		ADW_PROCESS.PROCESS_BEGIN(g_PROCESS_NAME,'UPDATE_ETL_ACCESS');
		
        g_SESSION_NO := ADW_PROCESS.GET_SESSION_NO;
              
        g_START_TIME    := SYSDATE;
		
		-- Get all Schema based on BACKUP Table changes
			
		FOR schemaRec  IN (	SELECT UNIQUE UTP.GRANTEE
							  FROM USER_TAB_PRIVS UTP
							 WHERE UTP.TYPE ='TABLE' AND UTP.GRANTOR = 'ADWADMIN'
						   	   and UTP.TABLE_NAME like '%\_B' escape '\')
		LOOP
				EXECUTE_GRANT (schemaRec.GRANTEE);
				EXECUTE_REVOKE(schemaRec.GRANTEE);
		END LOOP;
		
		ADW_PROCESS.PROCESS_END;
        
    END UPDATE_ETL_ACCESS;
------------------------------------------------------------------------------------------  
--
------------------------------------------------------------------------------------------
--   PROCEDURE HELP
--   General help contained within all procedures
--
  PROCEDURE HELP IS

     BEGIN  

       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('- The administration package ADW_SECURITY                            -');
       dbms_output.put_line('-   Current Global Settings                                          -');
       dbms_output.put_line('-     VERSION       = '||g_PACKAGE_VERSION);
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-  PROCESSING ROUTINES                                               -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_SECURITY.SET_ACCESS(<schema>)                         -');     
       dbms_output.put_line('-        : Execute all grants for given user if user missing then    -');
       dbms_output.put_line('-          all users                                                 -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_SECURITY.UPDATE_ETL_ACCESS()                          -');     
       dbms_output.put_line('-        : Update grants based on ETL Stage file changes             -');
       dbms_output.put_line('-  HELP                                                              -');
       dbms_output.put_line('-     call ADW_SECURITY.HELP();                                      -');
       dbms_output.put_line('-           : General Help report (This Report)                      -');

     END HELP;

END ADW_SECURITY;
/
