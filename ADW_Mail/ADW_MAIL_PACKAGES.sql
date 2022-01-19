CREATE OR REPLACE PACKAGE ADW_MAIL AS
  TABLE_ACCESS EXCEPTION;

--
-- Set global parameters
--
  
  FUNCTION  VERSION              RETURN VARCHAR;

  PROCEDURE PROCESS     (p_ProcessName VARCHAR2 DEFAULT null,p_ReportName VARCHAR2 DEFAULT null);
  
  PROCEDURE MailMessage (p_ReportName IN VARCHAR2, p_MessageType IN VARCHAR2,p_ProcessName IN VARCHAR2,p_MessageText IN CLOB);

  PROCEDURE HELP;

END ADW_MAIL;
/

CREATE OR REPLACE PACKAGE BODY ADW_MAIL AS
----------------------------------------------------------------------------------------------------------
-- SCRIPT: ADW_MAIL
--
-- AUTHOR: Paul Coward
--
-- PURPOSE:
--   General Reporting used in Application Data Warehouse
--
-- DEPENDENCIES
--  
--
--
-- SYNTAX:
--    N/A
--
-- HISTORY:
--  16-Apr-2021    P. Coward  Initial version
--
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-- Declare mail procedure as local function

--
-- Declare Global Variables
--
   g_PACKAGE_VERSION  CONSTANT VARCHAR(60) := 'ADW_MAIL V1.0.0 (Apr 16,2021)';

-----------------------------------------------------------------------------------------------------------
--
-- Get Current Version of this package
--
  FUNCTION VERSION RETURN VARCHAR   IS
    BEGIN
        RETURN g_PACKAGE_VERSION ;

    END VERSION;
-----------------------------------------------------------------------------------------------------------
--
-- PROCEDURE: PROCESS_REPORT
--
-- Purpose: To Report process results to users defined in ADW_MAIL
--

  PROCEDURE PROCESS(p_ProcessName VARCHAR2 DEFAULT null,p_ReportName VARCHAR2 DEFAULT null) is

  
       
      TYPE REF_cur is REF CURSOR;
      PROCESS_cur  REF_cur;
      
     
	  v_CompletedSQL  VARCHAR2(4000);
	  v_ErroredSQL    VARCHAR2(4000);
      v_ReportSQL     VARCHAR2(4000);

      v_Message        CLOB;

      v_ProcessName   VARCHAR2(256);
      v_ReportName    VARCHAR2(256);
      v_SessionNo     NUMBER;
      v_ErrorCount    NUMBER;
  BEGIN


    dbms_output.put_line('-');
    dbms_output.put_line('---------------- Process Report -----------------------');
    dbms_output.put_line('-');

	v_ProcessName := NVL(p_ProcessName,'ALL');
	v_ReportName  := NVL(p_ReportName, v_ProcessName);
	
	if v_ProcessName = 'ALL' then
		v_CompletedSQL := 'SELECT SESSION_NO from ADW_PROCESS_EXECUTION '||
					' WHERE  PROCESS_REPORT_DATE is null and PROCESS_STATUS in (''COMPLETED'')'||
					' GROUP BY SESSION_NO '||
					' ORDER BY SESSION_NO ';
		v_ErroredSQL := 'SELECT SESSION_NO from ADW_PROCESS_EXECUTION '||
					' WHERE  PROCESS_REPORT_DATE is null and PROCESS_STATUS in (''ERRORED'')'||
					' GROUP BY SESSION_NO '||
					' ORDER BY SESSION_NO ';
	else
		v_CompletedSQL := 'SELECT SESSION_NO from ADW_PROCESS_EXECUTION '||
					' WHERE  PROCESS_REPORT_DATE is null and PROCESS_NAME like '''||v_ProcessName||''' and PROCESS_STATUS in (''COMPLETED'')'||
					' GROUP BY SESSION_NO '||
					' ORDER BY SESSION_NO ';
		v_ErroredSQL := 'SELECT SESSION_NO from ADW_PROCESS_EXECUTION '||
					' WHERE  PROCESS_REPORT_DATE is null and PROCESS_NAME like '''||v_ProcessName||''' and PROCESS_STATUS in (''ERRORED'')'||
					' GROUP BY SESSION_NO '||
					' ORDER BY SESSION_NO ';
	end if;

	--
	-- Report on Completed Processes
	--
	
    OPEN PROCESS_cur FOR v_CompletedSQL;
    LOOP
        FETCH PROCESS_cur into v_SessionNo;
        EXIT WHEN PROCESS_cur%NOTFOUND;
		
		select PROCESS_NAME into v_ProcessName 
		  FROM ADW_PROCESS_EXECUTION 
		 WHERE SESSION_NO = v_SessionNo AND ROWNUM = 1 
		 ORDER BY PROCESS_ID;
		 
		dbms_output.put_line('Process(N) Session:'||v_SessionNo||' Name:'||v_ProcessName);
		 
        v_message := '';
		
		v_ReportSQL := 'SELECT MESSAGE_TYPE TYPE,MESSAGE_TEXT MESSAGE FROM ADW_PROCESS_LOG where SESSION_NO = '||v_SessionNo||' and MESSAGE_TYPE <> ''D'' ORDER BY PROCESS_LOG_ID';
 		v_Message :=  ADW_UTILITY.GET_TABLE_HTML(v_ReportSQL);
		
		MailMessage(v_ReportName,'N',v_ProcessName, v_Message); 

        UPDATE ADW_PROCESS_EXECUTION 
		   SET PROCESS_REPORT_DATE = sysdate
		 WHERE SESSION_NO = v_SessionNo;
		 
        commit;
    END LOOP;
        

    OPEN PROCESS_cur FOR v_ErroredSQL;
    LOOP
        FETCH PROCESS_cur into v_SessionNo;
        EXIT WHEN PROCESS_cur%NOTFOUND;
		
		select PROCESS_NAME into v_ProcessName 
		  FROM ADW_PROCESS_EXECUTION 
		 WHERE SESSION_NO = v_SessionNo AND ROWNUM = 1 
		 ORDER BY PROCESS_ID;
		 
        v_message := '';
		dbms_output.put_line('Process(E) Session:'||v_SessionNo||' Name:'||v_ProcessName);
		
		v_ReportSQL := 'SELECT MESSAGE_TYPE TYPE,MESSAGE_TEXT MESSAGE FROM ADW_PROCESS_LOG where SESSION_NO = '||v_SessionNo||' and MESSAGE_TYPE <> ''D'' ORDER BY PROCESS_LOG_ID';
        
		v_Message := ADW_UTILITY.GET_TABLE_HTML(v_ReportSQL);

		MailMessage(v_ReportName,'E', v_ProcessName,v_Message); 

        UPDATE ADW_PROCESS_EXECUTION SET PROCESS_REPORT_DATE = sysdate where SESSION_NO = v_SessionNo;
        commit;
    END LOOP;
EXCEPTION

       WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE(SQLERRM);
		  RAISE_APPLICATION_ERROR(-20001,'Report MailMessage :'||SQLERRM);

  END PROCESS;
--   
-- Private procedure to mail message to define role
--
PROCEDURE MailMessage(p_ReportName IN VARCHAR2, p_MessageType IN VARCHAR2,p_ProcessName IN VARCHAR2, p_MessageText IN CLOB) is
      
	  TYPE REF_cur is REF CURSOR;

      MAIL_cur REF_cur;

      v_DestinationEmail  VARCHAR2(250);
      v_SubjectLine       VARCHAR2(250);

      v_SourceEmail       VARCHAR2(250) := 'pcoward@shaw.ca';
      v_Subject           VARCHAR2(250) := 'Oracle Security Alert for ';
      v_MailHost          VARCHAR2(50) := 'smtp.us-ashburn-1.oci.oraclecloud.com';
      v_BodyText          VARCHAR2(4000)  := '';
      v_MailBody          CLOB := '';
      v_MailConn          utl_smtp.Connection;
      crlf                VARCHAR2(2)  := chr(13)||chr(10);
	  v_NoSent            NUMBER;
  
 
BEGIN

	v_NoSent := 0;
         
	open MAIL_cur for SELECT   U.E_MAIL_ADDRESS,R.SUBJECT_LINE,R.BODY_TEXT
					    FROM      ADW_MAIL_GROUP      R
	  	               INNER JOIN ADW_MAIL_GROUP_USER G ON G.GROUP_NAME = R.GROUP_NAME  AND G.ACTIVE_IND = 'Y'
		               INNER JOIN ADW_MAIL_USER       U ON U.USER_NAME    = G.USER_NAME     AND U.ACTIVE_IND = 'Y'
		              WHERE R.REPORT_NAME  = p_ReportName   AND R.ACTIVE_IND = 'Y'
						AND R.REPORT_TYPE  = p_MessageType
					 GROUP BY U.E_MAIL_ADDRESS,R.SUBJECT_LINE,R.BODY_TEXT;

	LOOP

		fetch MAIL_cur into v_DestinationEmail,v_SubjectLine,v_BodyText;
		EXIT WHEN MAIL_cur%NOTFOUND;
		
		v_NoSent := v_NoSent + 1;
		
		v_SubjectLine := substr(REPLACE(v_SubjectLine,'%DATE%',        to_char(sysdate, 'Dy, DD Mon YYYY')),1,250);
		v_SubjectLine := substr(REPLACE(v_SubjectLine,'%CRLF%',        crlf)                               ,1,250);
		v_SubjectLine := substr(REPLACE(v_SubjectLine,'%PROCESS_NAME%', p_ProcessName)                      ,1,250);
		v_SubjectLine := substr(REPLACE(v_SubjectLine,'%REPORT_NAME%', p_ReportName)                       ,1,250);
		
		v_MailBody := v_BodyText;  
		v_MailBody := REPLACE(v_MailBody,'%LOG%',         p_MessageText);
		v_MailBody := REPLACE(v_MailBody,'%DATE%',        to_char(sysdate, 'Dy, DD Mon YYYY'));
		v_MailBody := REPLACE(v_MailBody,'%CRLF%',        crlf);
		v_MailBody := REPLACE(v_MailBody,'%PROCESS_NAME%',p_ProcessName);
		v_MailBody := REPLACE(v_MailBody,'%REPORT_NAME%', p_ReportName);
		  
		BEGIN
			DBMS_OUTPUT.PUT_LINE('  -- Send to '||v_DestinationEmail);
			INSERT INTO ADW_MAIL_SEND
			  (SEND_TO,SENT_FROM,SUBJECT_LINE,BODY_TEXT)
            VALUES
              (v_DestinationEmail,v_SourceEmail,v_SubjectLine,v_MailBody);
		    commit;
		EXCEPTION
		   WHEN OTHERS THEN
				DBMS_OUTPUT.PUT_LINE(SQLERRM);
				RAISE_APPLICATION_ERROR(-20001,SQLERRM);
	    END;
			
	END LOOP;
	close  MAIL_cur;
EXCEPTION
   WHEN OTHERS THEN
		DBMS_OUTPUT.PUT_LINE(SQLERRM);
		RAISE_APPLICATION_ERROR(-20001,'Report MailMessage :'||SQLERRM);

      
END MailMessage;

--
--  ----------------------------------------------------------------------------------------
--   PROCEDURE HELP
--   General help contained within all procedures
--
  PROCEDURE HELP IS

     BEGIN  

       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('- The administration package ADW_MAIL contains procedures            -');
       dbms_output.put_line('- for general reporting                                              -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-   Current Global Settings                                          -');
       dbms_output.put_line('-     VERSION       = '||g_PACKAGE_VERSION);
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-                                                                    -');
 
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     ADW_MAIL.PROCESS(<processName>,<ReportName>)                   -');
       dbms_output.put_line('-           : Mail outstanding processes using the report defined    -');
       dbms_output.put_line('-                                                                    -');
  
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     ADW_MAIL.MailMessage(<Report>,<Message Type>,<text>)           -');
       dbms_output.put_line('-           : To mail user in report given text                      -');
       dbms_output.put_line('-                                                                    -');

       dbms_output.put_line('-  HELP                                                              -');
       dbms_output.put_line('-     call ADW_MAIL.HELP();                                          -');
       dbms_output.put_line('-           : General Help report (This Report)                      -');

     END HELP;

END ADW_MAIL;
/
