DROP PACKAGE ADW_NOTIFY;

CREATE OR REPLACE PACKAGE ADW_NOTIFY AS
  TABLE_ACCESS EXCEPTION;

  FUNCTION  VERSION      RETURN VARCHAR;
  
  PROCEDURE EVENT_TEST (p_NotifyName VARCHAR2);
  PROCEDURE GROUP_TEST (p_GroupName VARCHAR2);
  
  PROCEDURE HELP                  ;

END ADW_NOTIFY;
/

CREATE OR REPLACE PACKAGE BODY ADW_NOTIFY AS
----------------------------------------------------------------------------------------------------------
-- SCRIPT: ADW_NOTIFY
--
-- PURPOSE:
--   Process mail notifications for the Data Reporting
--
-- DEPENDENCIES
--    ADW_UTILITY : Package Installed
--
-- EXECUTION:
--   This Package enables notifications required for data reporting
--
-- HISTORY:                
--  23-Feb-2021   Paul Coward Initial Version
-----------------------------------------------------------------------------------------------------------
-- Declare mail procedure as local function

-----------------------------------------------------------------------------------------------------------
-- Declare Global Variables

  g_PACKAGE_VERSION  CONSTANT VARCHAR(256) := 'ADW_NOTIFY V1.0.0 (Feb 23,2021)';
  
  g_NotifyName  VARCHAR2(40);

-----------------------------------------------------------------------------------------------------------
--
-- Get Current Version of this package
--
 FUNCTION VERSION RETURN VARCHAR IS
    BEGIN
        RETURN g_PACKAGE_VERSION ;
    END;
    
-----------------------------------------------------------------------------------------------------------
--
-- Get Count based on SQL statements provided
--
 FUNCTION TestSQLEvent (p_SQLStatements  IN CLOB) return NUMBER
   is
       v_Count             NUMBER;
       v_ErrorSection      VARCHAR2(80);
       v_StartPos          NUMBER;
       v_EndPos            NUMBER;
       v_SingleSQL         CLOB;
       v_RecCount          NUMBER;
    BEGIN
        v_RecCount := 0;
        v_StartPos := 1;
        v_EndPos   :=  INSTR(p_SQLStatements,';');
        
 
        WHILE INSTR(SUBSTR(p_SQLStatements,v_StartPos),';')  > 0
        LOOP
            v_SingleSQL :=LTRIM(RTRIM(REPLACE(REPLACE(SUBSTR(p_SQLStatements,v_StartPos,v_EndPos-v_StartPos),chr(10),' '),chr(13),' ')));
            
            if length(v_SingleSQL) > 1 then
				DBMS_OUTPUT.PUT_LINE('RUN '|| v_SingleSQL);
                EXECUTE IMMEDIATE 'select count(0) from ('||v_SingleSQL||')' into v_Count;
                v_RecCount := v_RecCount + v_Count;
				DBMS_OUTPUT.PUT_LINE('Result '|| v_Count);
            end if;
			
            v_StartPos := INSTR(SUBSTR(p_SQLStatements,v_StartPos),';') + v_StartPos;
            v_EndPos   := INSTR(SUBSTR(p_SQLStatements,v_StartPos),';') + v_StartPos - 1;
        END LOOP;
        
        v_SingleSQL :=LTRIM(RTRIM(REPLACE(REPLACE(SUBSTR(p_SQLStatements,v_StartPos),chr(10),' '),chr(13),' ')));
		
        if length(v_SingleSQL) > 0 then
			DBMS_OUTPUT.PUT_LINE('RUN:'|| v_SingleSQL);
            EXECUTE IMMEDIATE 'select count(0) from ('||v_SingleSQL||')' into v_Count;
			DBMS_OUTPUT.PUT_LINE('Result '|| v_Count);
            v_RecCount := v_RecCount + v_Count;
        end if;
		
        return v_RecCount;
		
    EXCEPTION
        
        WHEN OTHERS THEN
           DBMS_OUTPUT.PUT_LINE('SQL Error on '|| v_SingleSQL);
           DBMS_OUTPUT.PUT_LINE(' '|| SQLERRM);
		
           RAISE_APPLICATION_ERROR(-20001,'TestSQLEvent:'||SQLERRM);

   END TestSQLEvent; 
   
   
   FUNCTION BuildReport (p_SQLStatements  IN CLOB) return CLOB
   is
       v_Count             NUMBER;
	   v_Length	           NUMBER;
       v_ActStartPos       NUMBER;
       v_SelectPos         NUMBER;
       v_EndPos            NUMBER;
	   v_SQLPrefix         VARCHAR2(1);
       v_String            CLOB;
       v_ResultString      CLOB;
       v_Result            CLOB;
       c_LineBreak         VARCHAR2(10)  := '<br />';
    BEGIN
		
        v_Result      := '';
        v_ActStartPos := 1;
		v_Length      := length(p_SQLStatements);

        WHILE v_ActStartPos <= v_Length
        LOOP
			v_SelectPos   :=  INSTR(upper(substr(p_SQLStatements,v_ActStartPos)),'SELECT');
			
			if v_SelectPos > 0 then
				--
				-- Process pre select
				--
				if v_SelectPos > 1 then
					v_SQLPrefix := substr(p_SQLStatements,v_ActStartPos + v_SelectPos - 2,1);
					if v_SQLPrefix = '!' then
						v_String := LTRIM(RTRIM(SUBSTR(p_SQLStatements,v_ActStartPos,v_SelectPos-2)));
					else
						v_String := LTRIM(RTRIM(SUBSTR(p_SQLStatements,v_ActStartPos,v_SelectPos-1)));
					end if;

					if length(v_String) > 1 then
						v_Result := v_Result||v_String;
					end if;
				else
					v_SQLPrefix := '';
				end if;
				--
				-- Process Select command
				--
				v_ActStartPos := v_ActStartPos + v_SelectPos-1;
				v_EndPos      := INSTR(substr(p_SQLStatements,v_ActStartPos),';');
				
				if v_EndPos > 0 then
					v_String := LTRIM(RTRIM(REPLACE(REPLACE(SUBSTR(p_SQLStatements,v_ActStartPos,v_EndPos-1),chr(10),' '),chr(13),' ')));
					v_ActStartPos := v_ActStartPos + v_EndPos;
				else
					v_String := LTRIM(RTRIM(REPLACE(REPLACE(SUBSTR(p_SQLStatements,v_ActStartPos),chr(10),' '),chr(13),' ')));
					v_ActStartPos := v_Length + 1;
				end if;
				
				if length(v_String) > 1 then
					if v_SQLPrefix = '!' then
						EXECUTE IMMEDIATE v_String into v_ResultString;
						v_Result := v_Result||v_ResultString;
					else
						v_Result := v_Result||ADW_UTILITY.GET_TABLE_HTML(v_String);
					end if;
				end if;

			else
				v_String := LTRIM(RTRIM(SUBSTR(p_SQLStatements,v_ActStartPos,v_length-v_ActStartPos)));
				if length(v_String) > 1 then
					v_Result := v_Result||v_String;
				end if;
				v_ActStartPos := v_length + 1;
			end if;

        END LOOP;
		
        return v_Result;
    EXCEPTION
        
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE(SQLERRM);
            RAISE_APPLICATION_ERROR(-20001,SQLERRM);

   END BuildReport;
   
   
   PROCEDURE SQL_EXECUTE (p_SQLStatements  IN CLOB)
    is
       v_Count             NUMBER;
       v_ErrorSection      VARCHAR2(80);
       v_StartPos          NUMBER;
       v_EndPos            NUMBER;
	   v_Length            NUMBER;
       v_SingleSQL         CLOB;
    BEGIN
        v_StartPos := 1;
		v_Length   := length(p_SQLStatements);
        
        DBMS_OUTPUT.PUT_LINE('Execute SQL'||v_Length);
 
        WHILE v_StartPos < v_Length
        LOOP
			v_EndPos    :=  INSTR(substr(p_SQLStatements,v_StartPos),';');
			if v_EndPos > 0 then
				v_SingleSQL :=LTRIM(RTRIM(REPLACE(REPLACE(SUBSTR(p_SQLStatements,v_StartPos,v_EndPos-1),chr(10),' '),chr(13),' ')));
				v_StartPos  := v_StartPos + v_EndPos + 1;
			else
				v_SingleSQL :=LTRIM(RTRIM(REPLACE(REPLACE(SUBSTR(p_SQLStatements,v_StartPos),chr(10),' '),chr(13),' ')));
				v_StartPos  := v_Length + 1;
			end if;
            
            if length(v_SingleSQL) > 1 then
                DBMS_OUTPUT.PUT_LINE('Run '||v_SingleSQL);
                EXECUTE IMMEDIATE v_SingleSQL;
            end if;

		END LOOP;
        
    EXCEPTION
        
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20001,SQLERRM);
   END SQL_EXECUTE; 
-----------------------------------------------------------------------------------------------------------
--
-- PROCEDURE: EVENT_TEST
--
--
  PROCEDURE EVENT_TEST(p_NotifyName VARCHAR2) is

    v_Count         NUMBER;
    v_LastDate      DATE;
    v_RecCount      NUMBER;
    v_ReportText    CLOB;
	v_BodyText      CLOB;
    c_CRLF          VARCHAR2(2)   := chr(13)||chr(10);
    c_LineBreak     VARCHAR2(10)  := '<br />';
    
    
    CURSOR c_SQL_Notify is 
           SELECT REPORT_NAME,EVENT_TEST,EVENT_PASS_RESPONSE,EVENT_FAIL_RESPONSE,EVENT_COMPLETE_SQL
             FROM ADW_NOTIFY_LIST
            WHERE active_ind = 'Y' AND NOTIFY_NAME = p_NotifyName
		   order by NOTIFY_NAME;
           
    BEGIN

    dbms_output.put_line('------------------------NOTIFICATION PROCESS START----------------------------');
    
    FOR  r_SQL_Notify in c_SQL_Notify         
    LOOP
        
        DBMS_OUTPUT.put_line ('Notify : '||p_NotifyName);
		
		g_NotifyName := p_NotifyName;
        
        v_RecCount := TestSQLEvent(r_SQL_Notify.EVENT_TEST);
		
        SELECT MAX(NVL(NL.EVENT_LAST_EXECUTED,NVL(P.PROCESS_END_DATE,SYSDATE-365))) into v_LastDate 
		  FROM      ADW_NOTIFY_LIST       NL
		  LEFT JOIN ADW_PROCESS_EXECUTION P  ON P.PROCESS_NAME = NL.NOTIFY_NAME
		 WHERE NL.NOTIFY_NAME = p_NotifyName;
		
        if v_RecCount > 0 then
            
            DBMS_OUTPUT.put_line ('Event Pass - Last Date : '||v_LastDate);

			IF  r_SQL_Notify.EVENT_PASS_RESPONSE is not null THEN
				v_ReportText := r_SQL_Notify.EVENT_PASS_RESPONSE;
				v_ReportText := REPLACE(v_ReportText, '%LAST_DATE%'   ,to_char(v_LastDate,'dd/mm/yyyy'));                     
				v_ReportText := REPLACE(v_ReportText, '%CURRENT_DATE%',to_char(sysdate   ,'dd/mm/yyyy'));                     
				v_ReportText := REPLACE(v_ReportText, '%LBREAK%'      ,c_LineBreak);                     
				v_ReportText := LTRIM(RTRIM(v_ReportText)); 
				DBMS_OUTPUT.put_line (v_ReportText);

				v_BodyText:= BuildReport(v_ReportText);
				ADW_MAIL.MailMessage(p_NotifyName,'N','NOTIFY',v_BodyText);
				
			END IF;
        
            SQL_EXECUTE(r_SQL_Notify.EVENT_COMPLETE_SQL);
            commit;
			
        ELSIF  r_SQL_Notify.EVENT_FAIL_RESPONSE is not null THEN
		
            DBMS_OUTPUT.put_line ('Event Fail - Last Date : '||v_LastDate);

 			v_ReportText := r_SQL_Notify.EVENT_FAIL_RESPONSE;
            v_ReportText := REPLACE(v_ReportText, '%LAST_DATE%'   ,to_char(v_LastDate,'dd/mm/yyyy'));                     
            v_ReportText := REPLACE(v_ReportText, '%CURRENT_DATE%',to_char(sysdate   ,'dd/mm/yyyy'));                     
            v_ReportText := REPLACE(v_ReportText, '%LBREAK%'      ,c_LineBreak);                     
            v_ReportText := LTRIM(RTRIM(v_ReportText)); 
			
			v_BodyText:= BuildReport(v_ReportText);
			ADW_MAIL.MailMessage(p_NotifyName,'E','NOTIFY',v_BodyText);
        
        end if;
		
        dbms_output.put_line('Update Date');
        UPDATE ADW_NOTIFY_LIST
		   SET EVENT_LAST_EXECUTED = SYSDATE 
		 WHERE NOTIFY_NAME  = g_NotifyName;
		 
        dbms_output.put_line('Update Process'||g_NotifyName);
		ADW_PROCESS.PROCESS_EXECUTE(g_NotifyName,'EVENT_TEST');
        dbms_output.put_line('DONE');
		
    END LOOP;    
    
    EXCEPTION
        
        WHEN OTHERS THEN
            dbms_output.put_line(SQLERRM);
            RAISE_APPLICATION_ERROR(-20001,'Notify Event:'||p_NotifyName||':'||SQLERRM);
    
    END EVENT_TEST;
-----------------------------------------------------------------------------------------------------------
--
-- PROCEDURE: GROUP_TEST
--
--
  PROCEDURE GROUP_TEST(p_GroupName VARCHAR2) is

    v_Count         NUMBER;
    v_LastDate      DATE;
    v_RecCount      NUMBER;
    v_ReportText    CLOB;
	v_BodyText      CLOB;
    c_CRLF          VARCHAR2(2)   := chr(13)||chr(10);
    c_LineBreak     VARCHAR2(10)  := '<br />';
    
    
    CURSOR c_SQL_Group is 
           SELECT NOTIFY_NAME,REPORT_NAME,EVENT_TEST,EVENT_PASS_RESPONSE,EVENT_FAIL_RESPONSE,EVENT_COMPLETE_SQL
             FROM ADW_NOTIFY_LIST
            WHERE active_ind = 'Y' AND GROUP_NAME = p_GroupName
		   order by NOTIFY_NAME;
           
    BEGIN

    dbms_output.put_line('------------------------NOTIFICATION PROCESS START----------------------------');
    
    FOR  r_SQL_Notify in c_SQL_Group         
    LOOP
        
        DBMS_OUTPUT.put_line ('Notify : '||r_SQL_Notify.NOTIFY_NAME);
		
		g_NotifyName := r_SQL_Notify.NOTIFY_NAME;
        
        v_RecCount := TestSQLEvent(r_SQL_Notify.EVENT_TEST);
		
        SELECT MAX(NVL(NL.EVENT_LAST_EXECUTED,NVL(P.PROCESS_END_DATE,SYSDATE-365))) into v_LastDate 
		  FROM      ADW_NOTIFY_LIST       NL
		  LEFT JOIN ADW_PROCESS_EXECUTION P  ON P.PROCESS_NAME = NL.NOTIFY_NAME
		 WHERE NL.NOTIFY_NAME = g_NotifyName;
		
        if v_RecCount > 0 then
            
            DBMS_OUTPUT.put_line ('Event Pass - Last Date : '||v_LastDate);

			IF  r_SQL_Notify.EVENT_PASS_RESPONSE is not null THEN
				v_ReportText := r_SQL_Notify.EVENT_PASS_RESPONSE;
				v_ReportText := REPLACE(v_ReportText, '%LAST_DATE%'   ,to_char(v_LastDate,'dd/mm/yyyy'));                     
				v_ReportText := REPLACE(v_ReportText, '%CURRENT_DATE%',to_char(sysdate   ,'dd/mm/yyyy'));                     
				v_ReportText := REPLACE(v_ReportText, '%LBREAK%'      ,c_LineBreak);                     
				v_ReportText := LTRIM(RTRIM(v_ReportText)); 
				DBMS_OUTPUT.put_line (v_ReportText);

				v_BodyText:= BuildReport(v_ReportText);
				ADW_MAIL.MailMessage(g_NotifyName,'N','NOTIFY',v_BodyText);

			END IF;
        
            SQL_EXECUTE(r_SQL_Notify.EVENT_COMPLETE_SQL);
            commit;
			
        ELSIF  r_SQL_Notify.EVENT_FAIL_RESPONSE is not null THEN
		
            DBMS_OUTPUT.put_line ('Event Fail - Last Date : '||v_LastDate);

 			v_ReportText := r_SQL_Notify.EVENT_FAIL_RESPONSE;
            v_ReportText := REPLACE(v_ReportText, '%LAST_DATE%'   ,to_char(v_LastDate,'dd/mm/yyyy'));                     
            v_ReportText := REPLACE(v_ReportText, '%CURRENT_DATE%',to_char(sysdate   ,'dd/mm/yyyy'));                     
            v_ReportText := REPLACE(v_ReportText, '%LBREAK%'      ,c_LineBreak);                     
            v_ReportText := LTRIM(RTRIM(v_ReportText)); 
			DBMS_OUTPUT.put_line (v_ReportText);
			v_BodyText:= BuildReport(v_ReportText);
			ADW_MAIL.MailMessage(g_NotifyName,'E','NOTIFY',v_BodyText);
        
        end if;
		
        UPDATE ADW_NOTIFY_LIST
		   SET EVENT_LAST_EXECUTED = SYSDATE 
		 WHERE NOTIFY_NAME  = g_NotifyName;
		 
		ADW_PROCESS.PROCESS_EXECUTE(g_NotifyName,'EVENT_TEST');
		
    END LOOP;    
    
    EXCEPTION
        
        WHEN OTHERS THEN
            dbms_output.put_line(SQLERRM);
            RAISE_APPLICATION_ERROR(-20001,'Group Event:'||p_GroupName||' Notify:'||g_NotifyName||':'||SQLERRM);
    
    END GROUP_TEST;

------------------------------------------------------------------------------------------
--   PROCEDURE HELP
--   General help contained within all procedures
--
  PROCEDURE HELP IS

    BEGIN  

       dbms_output.put_line('-                                                                            -');
       dbms_output.put_line('- The administration package ADW_NOTIFY contains procedures                  -');
       dbms_output.put_line('- for general administration process message logging of the                  -');
       dbms_output.put_line('- error reporting service                                                    -');
       dbms_output.put_line('-   Current Global Settings                                                  -');
       dbms_output.put_line('-     VERSION       = '||g_PACKAGE_VERSION);
       dbms_output.put_line('-                                                                            -');
       
	   dbms_output.put_line('-                                                                            -');
       dbms_output.put_line('-     ADW_NOTIFY.NOTIFY_NAME(p_NotifyName)                                   -');
       dbms_output.put_line('-         : Perform run test SQL based on Notify Name                        -');
       
	   dbms_output.put_line('-                                                                            -');
       dbms_output.put_line('-     ADW_NOTIFY.GROUP_NAME(p_GroupName)                                     -');
       dbms_output.put_line('-         : Perform run test SQL based on Notify Group Name                  -');
       
	   dbms_output.put_line('-                                                                            -');
       dbms_output.put_line('-  HELP                                                                      -');
       dbms_output.put_line('-     call ADW_NOTIFY.HELP();                                                -');
       dbms_output.put_line('-           : General Help report (This Report)                              -');

    END HELP;
     
END ADW_NOTIFY;
/
