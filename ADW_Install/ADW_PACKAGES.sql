PROMPT   --------------------------------------------------------------------------------
PROMPT   -- ADW Packages
PROMPT
PROMPT   -- ADW_UTILITY
PROMPT

CREATE OR REPLACE PACKAGE ADW_UTILITY AS
  TABLE_ACCESS EXCEPTION;

--
-- Set global parameters
--
  
  FUNCTION  VERSION              RETURN VARCHAR;
  FUNCTION  GET_DURATION_TIME    (p_StartDate IN DATE, p_EndDate IN DATE) RETURN VARCHAR;
  FUNCTION  GET_DURATION_MINUTES (p_StartDate IN DATE, p_EndDate IN DATE) RETURN NUMBER;
  
  FUNCTION  GET_TABLE_HTML       (p_Query     IN VARCHAR) RETURN CLOB;
  

  PROCEDURE MVIEW_REFRESH        (p_MatViewName     IN VARCHAR);
  PROCEDURE REMOVE_APP_STAGE_TABLES (p_App_ID       IN VARCHAR);
  PROCEDURE BUILD_APP_PROD_VIEWS (p_App_ID       IN VARCHAR);

  PROCEDURE REMOVE_TABLE         (p_TableName       IN VARCHAR);
  PROCEDURE REMOVE_VIEW          (p_ViewName        IN VARCHAR);
  PROCEDURE SQL_EXECUTE          (p_SQL             IN CLOB);
  PROCEDURE SLEEP                (p_MILLI_SECONDS   IN NUMBER);
  
  PROCEDURE HELP;

END ADW_UTILITY;
/
CREATE OR REPLACE PACKAGE BODY ADW_UTILITY AS
----------------------------------------------------------------------------------------------------------
-- SCRIPT: ADW_UTILITY
--
-- AUTHOR: Paul Coward
--
-- PURPOSE:
--   General Utilities used in Integrated Data Warehouse
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

--
-- Declare Global Variables
--
   g_PACKAGE_VERSION  CONSTANT VARCHAR(60) := 'ADW_UTILITY V1.0.0 (Apr 16,2021)';

-----------------------------------------------------------------------------------------------------------
--
-- Get Current Version of this package
--
  FUNCTION VERSION RETURN VARCHAR   IS
    BEGIN
        RETURN g_PACKAGE_VERSION ;

    END VERSION;
	
--  ----------------------------------------------------------------------------------------
--  ----------------------------------------------------------------------------------------
--  Get text differences (# Hrs # Min # Sec) between two date time 

  FUNCTION  GET_DURATION_TIME(p_StartDate IN DATE, p_EndDate IN DATE) RETURN VARCHAR IS
        v_DiffTime     number(20,6);
        v_DiffHours    number;
        v_DiffMinutes  number;
        v_DiffSeconds  number(20,2);
        
    BEGIN
        v_DiffTime    := ABS(p_EndDate - p_StartDate);
        v_DiffHours   := FLOOR(v_DiffTime * 24);
        v_DiffMinutes := FLOOR((v_DiffTime * 24 * 60)-(v_DiffHours * 60));
        v_DiffSeconds := (v_DiffTime * 24 * 3600)-(v_DiffHours * 3600)-(v_DiffMinutes * 60);
		
        if v_DiffHours > 0 then
            RETURN v_DiffHours||' Hrs '||v_DiffMinutes||' Min '||to_char(v_DiffSeconds,'99.99')||' Sec';
        end if;
		
        if v_DiffMinutes > 0 then
            RETURN v_DiffMinutes||' Min '||to_char(v_DiffSeconds,'99.99')||' Sec';
        end if;
        RETURN to_char(v_DiffSeconds,'99.99')||' Sec';
    
    END GET_DURATION_TIME;

--  ----------------------------------------------------------------------------------------
--  ----------------------------------------------------------------------------------------
--  Get duration between two date time in minutes 
--
 FUNCTION  GET_DURATION_MINUTES(p_StartDate IN DATE, p_EndDate IN DATE) RETURN NUMBER IS
    BEGIN
        RETURN ABS(p_EndDate - p_StartDate) * 24 * 60;
    END GET_DURATION_MINUTES;
	
--  ----------------------------------------------------------------------------------------
--  ----------------------------------------------------------------------------------------
--
--  Create HTML output for a given query with field names as headings
--
 FUNCTION GET_TABLE_HTML(p_Query IN VARCHAR) RETURN  CLOB IS
    ctxh            dbms_xmlgen.ctxhandle;
    xslt_tranfsorm  XMLTYPE;
    v_TableBody     CLOB;  
    crlf            VARCHAR2(2)  := chr(10)||chr(13);
    c_LineBreak     VARCHAR2(10)  := '<br />';

    BEGIN
		ctxh:= dbms_xmlgen.newcontext(p_Query);   
		  -- XSLT Transformation to HTML 
		  xslt_tranfsorm := NEW XMLTYPE('
			<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
			  <xsl:template match="/ROWSET">
				 <table  border="1" style="width:100%">    
				  <tr>
					 <xsl:for-each select="ROW[1]/*">
					   <th> <xsl:value-of select="name()"/></th>
					   </xsl:for-each>
					 <xsl:apply-templates/>
				  </tr>
				 </table>
			  </xsl:template>
			  <xsl:template match="ROW">
				<tr><xsl:apply-templates/></tr>
			  </xsl:template>
			  <xsl:template match="ROW/*">
				<td style="text-align:left;"><xsl:value-of select="."/></td>
			  </xsl:template>
			</xsl:stylesheet>');  
			
		  dbms_xmlgen.setnullhandling(ctxh, dbms_xmlgen.empty_tag);
		
		  dbms_xmlgen.setxslt(ctxh, xslt_tranfsorm);
		
		  v_TableBody := dbms_xmlgen.getxml(ctxh);
		
		  dbms_xmlgen.closecontext(ctxh);   

		  RETURN v_TableBody;
    
    EXCEPTION
        
        WHEN OTHERS THEN
            dbms_output.put_line(SQLERRM);
			v_TableBody := 'ERROR FROM: '||p_Query||crlf||' '||SQLERRM;
    
    RETURN v_TableBody;
  END GET_TABLE_HTML;

--  ----------------------------------------------------------------------------------------
--  ----------------------------------------------------------------------------------------
--
--  Materialized View Refresh
--
 PROCEDURE MVIEW_REFRESH (p_MatViewName  IN VARCHAR) is
    v_StartTime    date;
  BEGIN
        v_StartTime := sysdate;

        ADW_PROCESS.PROCESS_DEBUG_LOG('REFRESH MatView '||p_MatViewName);
        
        EXECUTE IMMEDIATE 'ALTER MATERIALIZED VIEW ' || p_MatViewName || ' COMPILE'; 
        DBMS_MVIEW.REFRESH('"ADWADMIN".'||p_MatViewName,'C', ATOMIC_REFRESH=>FALSE);

        SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName           => 'ADWADMIN',
          TabName           => p_MatViewName,
          Estimate_Percent  => 20,
          Method_Opt        => 'FOR ALL COLUMNS SIZE AUTO ',
          Degree            => 4,
          Cascade           => TRUE,
          No_Invalidate     => TRUE);

        ADW_PROCESS.PROCESS_DEBUG_LOG('REFRESH Completed '||p_MatViewName||' in '||ADW_UTILITY.GET_DURATION_TIME(v_StartTime,sysdate));

    EXCEPTION

       WHEN OTHERS THEN
            dbms_output.put_line     (p_MatViewName||'-'||SQLERRM);
            ADW_PROCESS.PROCESS_ERROR(p_MatViewName||'-'||SQLERRM);
  END MVIEW_REFRESH;
  
--  ----------------------------------------------------------------------------------------
--  ----------------------------------------------------------------------------------------
--
--  Remove Tables from Stage/Production and Backup Data layers
--
 PROCEDURE REMOVE_APP_STAGE_TABLES (p_App_ID  IN VARCHAR) is
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

  EXCEPTION
       WHEN OTHERS THEN
            dbms_output.put_line     ('UTILITY:Remove Stage Tables for application '||p_App_ID||'-'||SQLERRM);
            ADW_PROCESS.PROCESS_ERROR('UTILITY:Remove Stage Tables for application '||p_App_ID||'-'||SQLERRM);
  END REMOVE_APP_STAGE_TABLES;

--  ----------------------------------------------------------------------------------------
--  ----------------------------------------------------------------------------------------
--   Create all missing views in the production layer for a given application

  PROCEDURE BUILD_APP_PROD_VIEWS (p_App_ID IN VARCHAR) is
	v_ViewName      varchar2(128);
	v_ViewFieldList CLOB;
	v_Count         NUMBER;
	
   BEGIN
		for tableRec in (SELECT TABLE_NAME FROM USER_TABLES 
						  WHERE   TABLE_NAME like(p_App_ID||'\_%\_P') escape '\')
		LOOP
			v_ViewName := SUBSTR(tableRec.TABLE_NAME,1,length(tableRec.TABLE_NAME)-1)||'VW';
			dbms_output.put_line     (v_ViewName);

			-- Only create view if it does not exist
			
			SELECT COUNT(0) INTO v_Count FROM USER_VIEWS WHERE VIEW_NAME = v_ViewName;
			if v_Count = 0 then
				v_ViewFieldList := '';
				for colummnRec in (select COLUMN_NAME,DATA_TYPE,DATA_LENGTH FROM  user_tab_cols 
									WHERE TABLE_NAME = tableRec.TABLE_NAME ORDER BY COLUMN_ID)
				LOOP
					v_ViewFieldList := v_ViewFieldList||'"'||colummnRec.COLUMN_NAME||'",';
				END LOOP;
				v_ViewFieldList := SUBSTR(v_ViewFieldList,1,length(v_ViewFieldList)-1);
				
				-- Create View
				
				dbms_output.put_line ('CREATE OR REPLACE VIEW '||v_ViewName||'('||v_ViewFieldList||') as ( SELECT '||v_ViewFieldList||' FROM '||tableRec.TABLE_NAME||')');
				EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW '||v_ViewName||'('||v_ViewFieldList||') as ( SELECT '||v_ViewFieldList||' FROM '||tableRec.TABLE_NAME||')';
			end if;
		end loop;
  
   EXCEPTION
       WHEN OTHERS THEN
            dbms_output.put_line     ('UTILITY:Build production views application '||p_App_ID||'-'||SQLERRM);
            ADW_PROCESS.PROCESS_ERROR('UTILITY:Build production views application '||p_App_ID||'-'||SQLERRM);
  END BUILD_APP_PROD_VIEWS;
  
--  ----------------------------------------------------------------------------------------
--  ----------------------------------------------------------------------------------------
--
--  Remove Table if exists
--
 PROCEDURE REMOVE_TABLE (p_TableName  IN VARCHAR) is
    v_Count    number;
  BEGIN
        
    SELECT COUNT(0) INTO v_Count FROM USER_TABLES WHERE TABLE_NAME = p_TableName;
    if v_Count = 1 then
        EXECUTE IMMEDIATE    'DROP TABLE "'||p_TableName||'"';
    end if;

  EXCEPTION
       WHEN OTHERS THEN
            dbms_output.put_line     ('UTILITY:Remove table '||p_TableName||'-'||SQLERRM);
            ADW_PROCESS.PROCESS_ERROR('UTILITY:Remove table '||p_TableName||'-'||SQLERRM);
  END REMOVE_TABLE;

--  ----------------------------------------------------------------------------------------
--  ----------------------------------------------------------------------------------------
--
--  Remove View if exists
--
 PROCEDURE REMOVE_VIEW (p_ViewName  IN VARCHAR) is
    v_Count    number;
  BEGIN
        
    SELECT COUNT(0) INTO v_Count FROM USER_VIEWS WHERE VIEW_NAME = p_ViewName;
    if v_Count = 1 then
        EXECUTE IMMEDIATE    'DROP VIEW "'||p_ViewName||'"';
    end if;

  EXCEPTION
       WHEN OTHERS THEN
            dbms_output.put_line     ('UTILITY:Remove View '||p_ViewName||'-'||SQLERRM);
            ADW_PROCESS.PROCESS_ERROR('UTILITY:Remove View '||p_ViewName||'-'||SQLERRM);
  END REMOVE_VIEW;

--  ----------------------------------------------------------------------------------------
--  ----------------------------------------------------------------------------------------
--
--  SQL Execute given sql text seperated by ';'
--
 PROCEDURE SQL_EXECUTE(p_SQL IN CLOB) is
	v_SQL        CLOB;
	v_ExecuteSQL CLOB;
  BEGIN
	v_SQL :=LTRIM(RTRIM( p_SQL));
	
	while INSTR(v_SQL,';') > 0 
	 LOOP
		v_ExecuteSQL := LTRIM(RTRIM(SUBSTR(v_SQL,1,instr(v_SQL,';')-1)));
		if LENGTH(v_ExecuteSQL) > 10 THEN
			execute immediate v_ExecuteSQL;
		end if;
		
		v_SQL := LTRIM(RTRIM(SUBSTR(v_SQL,instr(v_SQL,';')+1)));
	 END LOOP;
	
	if length(v_SQL) > 10 THEN
		execute immediate v_SQL;
	end if;
	

  EXCEPTION
       WHEN OTHERS THEN
            dbms_output.put_line('UTILITY:SQL_EXECUTE -'||SQLERRM);
            ADW_PROCESS.PROCESS_ERROR('UTILITY:SQL_EXECUTE -'||SQLERRM);
  END SQL_EXECUTE;
  
--  ----------------------------------------------------------------------------------------
--  ----------------------------------------------------------------------------------------
--   PROCEDURE SLEEP
--   General help contained within all procedures

PROCEDURE SLEEP (p_MILLI_SECONDS IN NUMBER) 
  AS LANGUAGE JAVA NAME 'java.lang.Thread.sleep(long)';

--
--  ----------------------------------------------------------------------------------------
--   PROCEDURE HELP
--   General help contained within all procedures
--
  PROCEDURE HELP IS

     BEGIN  

       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('- The administration package ADW_UTILITY contains procedures         -');
       dbms_output.put_line('- for general utility                                                -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-   Current Global Settings                                          -');
       dbms_output.put_line('-     VERSION       = '||g_PACKAGE_VERSION);
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-                                                                    -');
 
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('- FUNCTIONS                                                          -');
       dbms_output.put_line('-     ADW_UTILITY.GET_DURATION_TIME(<start_date>,<end_date>)         -');
       dbms_output.put_line('-           : This function will return a text of the number of      -');
       dbms_output.put_line('-             hours, minutes and seconds between the start and end   -');
       dbms_output.put_line('-             date                                                   -');
       dbms_output.put_line('-                                                                    -');
	   
       dbms_output.put_line('-     ADW_UTILITY.GET_DURATION_MINUTES(<start_date>,<end_date>)      -');
       dbms_output.put_line('-           : This function will returns number of minutes           -');
       dbms_output.put_line('-             between start and end date                             -');
       dbms_output.put_line('-                                                                    -');
	   
       dbms_output.put_line('-     ADW_UTILITY.GET_TABLE_HTML(<sql>)                              -');
       dbms_output.put_line('-           : This function will returns HTML of table entries       -');
       dbms_output.put_line('-             resulting from the SQL                                 -');
       dbms_output.put_line('-                                                                    -');

       dbms_output.put_line('- PROCEDURES                                                         -');
       dbms_output.put_line('-     ADW_UTILITY.REMOVE_APP_STAGE_TABLES(<app_id>)                  -');
       dbms_output.put_line('-           : To drop all stage tables for application <app_id>      -');
       dbms_output.put_line('-                                                                    -');

       dbms_output.put_line('-     ADW_UTILITY.BUILD_APP_PROD_VIEWS(<app_id>)                     -');
       dbms_output.put_line('-           : To create view of data in production data layer        -');
       dbms_output.put_line('-             for application <app_id>                               -');
       dbms_output.put_line('-                                                                    -');

       dbms_output.put_line('-     ADW_UTILITY.MVIEW_REFRESH(<mat view name>)                     -');
       dbms_output.put_line('-           : To refresh and compile a materialized view             -');
       dbms_output.put_line('-                                                                    -');
 
       dbms_output.put_line('-     ADW_UTILITY.REMOVE_TABLE(<table name>)                         -');
       dbms_output.put_line('-           : To remove table if it exists                           -');
       dbms_output.put_line('-                                                                    -');

       dbms_output.put_line('-     ADW_UTILITY.SQL_EXECUTE(<sql to execute>)                      -');
       dbms_output.put_line('-           : Will execute SQL clob. Each SQL command separated by ; -');
       dbms_output.put_line('-                                                                    -');

       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     ADW_UTILITY.SLEEP(<Milli-seconds>)                             -');
       dbms_output.put_line('-           : To wait <Milli-seconds> and then return                -');
       dbms_output.put_line('-                                                                    -');
	   
       dbms_output.put_line('-  HELP                                                              -');
       dbms_output.put_line('-     call ADW_UTILITY.HELP();                                       -');
       dbms_output.put_line('-           : General Help report (This Report)                      -');

     END HELP;

END ADW_UTILITY;
/

PROMPT   -----------------------------------------------------------------------------------'
PROMPT   -- ADW_PROCESS
PROMPT

CREATE OR REPLACE PACKAGE ADW_PROCESS AS
  TABLE_ACCESS EXCEPTION;
  
  FUNCTION  VERSION      RETURN VARCHAR;
 
  FUNCTION  GET_PROCESS_NAME RETURN VARCHAR;
  FUNCTION  GET_SESSION_NO   RETURN NUMBER;
  FUNCTION  NEW_SESSION_NO   RETURN NUMBER;

  PROCEDURE CLEAR_SESSION;
  
  PROCEDURE PROCESS_EXECUTE(p_ProcessName VARCHAR2             , p_ProcedureName VARCHAR2 DEFAULT null,
                            p_Host        VARCHAR2 DEFAULT null, p_User          VARCHAR2 DEFAULT null);
							
  PROCEDURE PROCESS_BEGIN  (p_ProcessName VARCHAR2 DEFAULT null, p_ProcedureName VARCHAR2 DEFAULT null,
                            p_Host        VARCHAR2 DEFAULT null, p_User          VARCHAR2 DEFAULT null);
  
  PROCEDURE PROCESS_LOG      (p_Message VARCHAR2);
  PROCEDURE PROCESS_DEBUG_LOG(p_Message VARCHAR2);
  PROCEDURE PROCESS_ERROR    (p_Message VARCHAR2);
  
  PROCEDURE PROCESS_END;
  
  PROCEDURE PROCESS_CLOSE  (p_HoursOld NUMBER DEFAULT 4);
  
  PROCEDURE PROCESS_FLUSH;
  PROCEDURE HELP                  ;

END ADW_PROCESS;
/

CREATE OR REPLACE PACKAGE BODY ADW_PROCESS AS
----------------------------------------------------------------------------------------------------------
-- SCRIPT: ADW_PROCESS
-- 
-- Author: Paul Coward
--
-- PURPOSE:
--   Process management and reporting for the Integrated Data Warehouse
--
-- DEPENDENCIES
--    IDW_UTILITY : Package Installed
--
-- EXECUTION:
--   This Package enables the management of process within the Integrated Data Warehouse (IDW).
--
-- SYNTAX:
--    N/A
--
-- HISTORY:
--  14-Apr-2021               Initial version
--
-----------------------------------------------------------------------------------------------------------
-- Declare mail procedure as local function

-----------------------------------------------------------------------------------------------------------
-- Declare Global Variables

  g_PACKAGE_VERSION  CONSTANT VARCHAR(40) := 'V1.0.0 (Apr 14,2021)';
  g_PROCESS_NAME              VARCHAR(256):= '';
  g_PROCEDURE_NAME            VARCHAR(256):= '';
  g_PROCESS_STATUS            VARCHAR(20) := '';
  g_SESSION_NO                NUMBER      := 0;
  g_HOST                      VARCHAR(256):= '';
  g_USER                      VARCHAR(256):= '';
  
  g_processCount              INTEGER     := 0;
  
  TYPE pNames IS VARRAY(10) OF VARCHAR2(256); 
  g_processNames              pNames      := pNames('', '', '', '', '', '', '', '', '', '');

  TYPE pID IS VARRAY(10) OF INTEGER; 
  g_processID                 pID         := pID(0,0,0,0,0,0,0,0,0,0);
  
  TYPE pDate IS VARRAY(10) OF DATE; 
  g_PROCESS_START_TIME        pDate       := pDate(sysdate,sysdate,sysdate,sysdate,sysdate,sysdate,sysdate,sysdate,sysdate,sysdate)   ;


-----------------------------------------------------------------------------------------------------------
--
-- Get Current Version of this package
--
  FUNCTION VERSION RETURN VARCHAR   IS
    BEGIN
        RETURN g_PACKAGE_VERSION ;

    END;
--
-- Get Current Process Name
--
  FUNCTION GET_PROCESS_NAME RETURN VARCHAR IS
    BEGIN
 
        RETURN g_PROCESS_NAME;
    END;
--
-- Get Session NUMBER. If not defined then set it
--
  FUNCTION GET_SESSION_NO RETURN NUMBER IS
    BEGIN

        RETURN g_SESSION_NO;
    END;
--
-- -------------------------------------------------------------------------------------------------------
--
-- Get New Session NUMBER. 
--
  FUNCTION NEW_SESSION_NO RETURN NUMBER IS
    BEGIN
	
	   -- End Old Session before starting new one

       if (g_SESSION_NO > 0) then
            PROCESS_END;
       end if;

       select ADW_SESSION_SEQ.nextval into g_SESSION_NO from dual;

       RETURN g_SESSION_NO;
    END;
--
-- -------------------------------------------------------------------------------------------------------
--
-- Clear current Session. 
--
  PROCEDURE CLEAR_SESSION IS
    BEGIN
	
	   g_SESSION_NO := 0;
	   
   END;
	
-- -------------------------------------------------------------------------------------------------------
-- PROCESS_EXECUTE - To insert of update end time on process 0 session NUMBER
--
  PROCEDURE PROCESS_EXECUTE(p_ProcessName VARCHAR2             ,p_ProcedureName VARCHAR2 DEFAULT null,
                            p_Host        VARCHAR2 DEFAULT null,p_User          VARCHAR2 DEFAULT null) IS
    v_Count      NUMBER;
    v_owner      VARCHAR2(30);
    v_ProcName  VARCHAR2(30);
    v_lineno     NUMBER;
    v_caller_t   VARCHAR2(255);
	
   BEGIN
		owa_util.who_called_me( v_owner, v_ProcName, v_lineno, v_caller_t );
		
		g_PROCEDURE_NAME := substr(p_ProcedureName,1,256);
		g_HOST           := substr(NVL(p_Host,v_ProcName),1,256);
		g_USER           := substr(NVL(p_User,v_caller_t),1,256);
		
		SELECT COUNT(0) into v_Count
		  FROM ADW_PROCESS_EXECUTION
		 WHERE PROCESS_NAME = upper(p_ProcessName) AND SESSION_NO = 0;
		
		IF v_Count > 0 then
			UPDATE ADW_PROCESS_EXECUTION
			  SET PROCEDURE_NAME   = g_PROCEDURE_NAME,
			      PROCESS_END_DATE = SYSDATE,
			      PROCESS_HOST     = g_HOST,
				  PROCESS_USER     = g_USER
			 WHERE PROCESS_NAME = upper(p_ProcessName) AND SESSION_NO = 0;
		ELSE
		
            INSERT INTO ADW_PROCESS_EXECUTION
               (SESSION_NO,
                PROCESS_NAME,
                PROCEDURE_NAME,
                PROCESS_STATUS,
                PROCESS_START_DATE,
                PROCESS_END_DATE,
				PROCESS_HOST,
				PROCESS_USER)
            VALUES
               (0,
                upper(p_ProcessName),
                g_PROCEDURE_NAME,
                'COMPLETED',
                SYSDATE,
                SYSDATE,
				g_HOST,
				g_USER);
		END IF;
		COMMIT;
	
	EXCEPTION

       WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE(SQLERRM);
          RAISE_APPLICATION_ERROR(-20001,'Process Execute:'||SQLERRM);

    END PROCESS_EXECUTE;
-- -------------------------------------------------------------------------------------------------------
-- PROCESS_BEGIN - Defines the start of a process thread. 
--                 The NONE default will result in the process being set to call procedure name
--                 Setting the Start time of the process
--    
   
  PROCEDURE PROCESS_BEGIN(p_ProcessName VARCHAR2 DEFAULT null,p_ProcedureName VARCHAR2 DEFAULT null,
                          p_Host        VARCHAR2 DEFAULT null,p_User          VARCHAR2 DEFAULT null) IS
    v_owner       VARCHAR2(30);
    v_ProcName    VARCHAR2(30);
    v_lineno      NUMBER;
    v_caller_t    VARCHAR2(255);
    v_Count       NUMBER;
	v_ProcessName VARCHAR2(256);
	v_Date        DATE;

    BEGIN
    
        owa_util.who_called_me( v_owner, v_ProcName, v_lineno, v_caller_t );
		
		v_ProcessName    := substr(NVL(p_ProcessName ,v_ProcName),1,256);
		g_PROCEDURE_NAME := substr(p_ProcedureName,1,256);
		g_HOST           := substr(NVL(p_Host,v_ProcName),1,256);
		g_USER           := substr(NVL(p_User,v_caller_t),1,256);
		
        --
		-- Test to see if this a continuation of last session
		--
        if (g_SESSION_NO = 0) then
            select ADW_SESSION_SEQ.nextval into g_SESSION_NO from dual;
			
  		    g_processCount                       := 1;
		    g_processNames(g_processCount)       := v_ProcessName;
			g_PROCESS_NAME                       := v_ProcessName;
            g_PROCESS_START_TIME(g_processCount) := sysdate;
			g_PROCESS_STATUS                     := 'PROCESSING';    
		
            INSERT INTO ADW_PROCESS_EXECUTION
               (SESSION_NO,
                PROCESS_NAME,
                PROCEDURE_NAME,
                PROCESS_STATUS,
                PROCESS_START_DATE,
				PROCESS_HOST,
				PROCESS_USER)
            VALUES
               (g_SESSION_NO,
                g_PROCESS_NAME,
                g_PROCEDURE_NAME,
                g_PROCESS_STATUS,
                g_PROCESS_START_TIME(g_processCount),
				g_HOST,
				g_USER);
        else
   		    g_processCount := g_processCount + 1;
			if g_processCount > 10 then
    		    g_processCount := 10;
			end if;
		    g_processNames(g_processCount)       := v_ProcessName;
			g_PROCEDURE_NAME                     := v_ProcessName;
            g_PROCESS_START_TIME(g_processCount) := sysdate;
			g_PROCESS_STATUS                     := 'PROCESSING';    
		
            INSERT INTO ADW_PROCESS_EXECUTION
               (SESSION_NO,
                PROCESS_NAME,
                PROCEDURE_NAME,
                PROCESS_STATUS,
                PROCESS_START_DATE,
				PROCESS_HOST,
				PROCESS_USER)
            VALUES
               (g_SESSION_NO,
                g_PROCESS_NAME,
                g_processNames(g_processCount),
                g_PROCESS_STATUS,
                g_PROCESS_START_TIME(g_processCount),
				g_HOST,
				g_USER);
		
		end if;

		v_Date := g_PROCESS_START_TIME(g_processCount);
        PROCESS_LOG(g_processNames(g_processCount)||' STARTED at '||to_char(v_Date, 'yyyy/mm/dd hh:mi:ss'));
        commit;
		
		select MAX(PROCESS_ID) into g_processID(g_processCount) from ADW_PROCESS_EXECUTION WHERE SESSION_NO = g_SESSION_NO;
		

	EXCEPTION

       WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE(SQLERRM);
		  rollback;
          RAISE_APPLICATION_ERROR(-20001,'Process Begin:'||SQLERRM);
 
    END PROCESS_BEGIN;
    
-- -------------------------------------------------------------------------------------------------------
-- PROCESS_LOG   - Enables the program to log a message to the process thread
--    

  PROCEDURE PROCESS_LOG(p_Message VARCHAR2) is
  
    BEGIN
        
        if (g_SESSION_NO = 0) then
           return;
        end if;
		
        --
        -- Make Entry into PROCESS LOG
        --
        INSERT INTO ADW_PROCESS_LOG
           (PROCESS_NAME,
            SESSION_NO,
            MESSAGE_TYPE,
            MESSAGE_TEXT)
        VALUES
           (NVL(g_PROCESS_NAME,'UNKNOWN'),
            g_SESSION_NO,
            'N',
            substr(' '||p_Message,1,2000));

		DBMS_OUTPUT.PUT_LINE(p_Message);
            
    EXCEPTION

       WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(SQLERRM);
        rollback;
        PROCESS_ERROR(SQLERRM);
  END PROCESS_LOG;
    
-- -------------------------------------------------------------------------------------------------------
-- PROCESS_DEBUG_LOG - Enables the program to log a debug message to the process thread
--                     A debug message will not be sent in a report unless requested with debug on
--    
  PROCEDURE PROCESS_DEBUG_LOG(p_Message VARCHAR2) is
  BEGIN
        if (g_SESSION_NO = 0) then
           return;
        end if;
		
        INSERT INTO ADW_PROCESS_LOG
           (PROCESS_NAME,
            SESSION_NO,
            MESSAGE_TYPE,
            MESSAGE_TEXT)
        VALUES
           (NVL(g_PROCESS_NAME,'UNKNOWN'),
            g_SESSION_NO,
            'D',
            substr(' '||p_Message,1,2000));
			
         DBMS_OUTPUT.PUT_LINE(p_Message);
   
    EXCEPTION

       WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(SQLERRM);
        rollback;
        PROCESS_ERROR(SQLERRM);

  END PROCESS_DEBUG_LOG;
-- -------------------------------------------------------------------------------------------------------
-- PROCESS_LOG   - Enables the program to log a message to the process thread
--    

  PROCEDURE PROCESS_ERROR(p_Message VARCHAR2) is
     BEGIN
        
        if (g_SESSION_NO = 0) then
           return;
        end if;
		
        g_PROCESS_STATUS := 'ERRORED';    
        
        INSERT INTO ADW_PROCESS_LOG
           (PROCESS_NAME,
            SESSION_NO,
            MESSAGE_TYPE,
            MESSAGE_TEXT)
        VALUES
           (NVL(g_PROCESS_NAME,'UNKNOWN'),
            g_SESSION_NO,
            'E',
            substr(' '||p_Message,1,2000));
		
		g_PROCESS_STATUS := 'ERRORED';		
        
		DBMS_OUTPUT.PUT_LINE('E: '||p_Message);

        EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(SQLERRM);
        RAISE_APPLICATION_ERROR(-20001,'PROCESS Error exception was raised');
        
    END PROCESS_ERROR;
    
-- -------------------------------------------------------------------------------------------------------
-- PROCESS_END    - This will mark the end of a process thread 
--                  Setting the end time of the process
--    
  PROCEDURE PROCESS_END is
	v_Count   integer;
    BEGIN
		if g_SESSION_NO > 0 THEN
			if g_processCount > 1 then
				PROCESS_LOG(g_processNames(g_processCount)||' ENDED at '||' '||g_PROCESS_STATUS||' '||
				               ADW_UTILITY.GET_DURATION_TIME(g_PROCESS_START_TIME(g_processCount),sysdate));
				
				UPDATE ADW_PROCESS_EXECUTION
				  SET PROCESS_END_DATE = sysdate
				 WHERE PROCESS_ID= g_processID(g_processCount);
				 
				commit; 
				g_processCount := g_processCount - 1;
			else
				select count(0) into v_Count FROM ADW_PROCESS_LOG where SESSION_NO   = g_SESSION_NO AND MESSAGE_TYPE = 'E';
				if v_Count > 0 then
					g_PROCESS_STATUS :=  'ERRORED';
				else
					g_PROCESS_STATUS := 'COMPLETED';
				end if;
				
				PROCESS_LOG(g_processNames(g_processCount)||' ENDED at '||' '||g_PROCESS_STATUS||' '||
				               ADW_UTILITY.GET_DURATION_TIME(g_PROCESS_START_TIME(g_processCount),sysdate));
			
				UPDATE ADW_PROCESS_EXECUTION
				  SET PROCESS_END_DATE = sysdate
				 WHERE PROCESS_ID= g_processID(g_processCount);
				 
				UPDATE ADW_PROCESS_EXECUTION
					SET PROCESS_STATUS = g_PROCESS_STATUS
				 WHERE SESSION_NO  = g_SESSION_NO;
				 
				g_processCount := 0;
				g_SESSION_NO   := 0;
			end if;
			  
			commit;
		end if;
		
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(SQLERRM);
        rollback;
        PROCESS_ERROR(SQLERRM);
    END PROCESS_END;
 
-----------------------------------------------------------------------------------------------------------
--
-- PROCEDURE: PROCESS_FLUSH
--
-- Purpose: To flush report log over 60 days old
--

  PROCEDURE PROCESS_FLUSH is
    BEGIN
	--
	--    FLUSH LOGS OLDER THAN 60 DAYS
	--
	dbms_output.put_line('FLUSH DATA LOGS OVER 60 DAYS OLD');

	DELETE ADW_PROCESS_LOG
 	 WHERE ROW_CREATE_DATE < (sysdate - 60);
	 
	commit;

  END PROCESS_FLUSH;
-----------------------------------------------------------------------------------------------------------
--
-- PROCEDURE: PROCESS_CLOSE
--
-- Purpose: To close outstanding processes over 4 hours since last log
--

  PROCEDURE PROCESS_CLOSE(p_HoursOld NUMBER DEFAULT 4) is
	  TYPE REF_cur is REF CURSOR;

      PROC_cur REF_cur;
	  v_SessionNo        NUMBER;
	  v_SessionOpenDate  DATE;
	  v_SessionCloseDate DATE;
	  v_ErrorCount       NUMBER;
	  v_ProcessStatus    VARCHAR2(40);
	  v_Message          VARCHAR2(2000);
	  
	  
   BEGIN
	dbms_output.put_line('Close Processes Over '||p_HoursOld||' hours old');

	open PROC_cur for SELECT   E.SESSION_NO,MIN(L.ROW_CREATE_DATE) SESSION_OPEN_DATE,MAX(L.ROW_CREATE_DATE) SESSION_CLOSE_DATE,SUM(DECODE(L.MESSAGE_TYPE,'E',1,0)) ERROR_COUNT
					    FROM  ADW_PROCESS_EXECUTION E
				   INNER JOIN ADW_PROCESS_LOG       L ON L.SESSION_NO = E.SESSION_NO
		              WHERE E.PROCESS_STATUS NOT IN ('ERRORED','COMPLETED')
					 GROUP BY E.SESSION_NO
					 HAVING MAX(L.ROW_CREATE_DATE) <  (SYSDATE - p_HoursOld/24);

	LOOP
		fetch PROC_cur into v_SessionNo,v_SessionOpenDate,v_SessionCloseDate,v_ErrorCount;
		EXIT WHEN PROC_cur%NOTFOUND;
		
		dbms_output.put_line('  Close '||v_SessionNo||' Error Count '||v_ErrorCount);
		if v_ErrorCount > 0 then
			v_ProcessStatus := 'ERRORED';
		else
			v_ProcessStatus := 'COMPLETED';
		end if;
		v_Message := 'PROCESS_CLOSE '||v_ProcessStatus||' '||ADW_UTILITY.GET_DURATION_TIME(v_SessionOpenDate,v_SessionCloseDate);

        INSERT INTO ADW_PROCESS_LOG
           (PROCESS_NAME,
            SESSION_NO,
            MESSAGE_TYPE,
            MESSAGE_TEXT,
			ROW_CREATE_DATE)
        VALUES
           ('PROCESS_CLOSE',
            v_SessionNo,
            'N',
            v_Message,
			v_SessionCloseDate);

		UPDATE ADW_PROCESS_EXECUTION
		   SET PROCEDURE_NAME   = NVL(PROCEDURE_NAME,'PROCESS_CLOSE'),
		       PROCESS_STATUS   = v_ProcessStatus ,
			   PROCESS_END_DATE = v_SessionCloseDate
		WHERE SESSION_NO   = v_SessionNo;
		commit;
		
	END LOOP;
	close  PROC_cur;
	

  END PROCESS_CLOSE;
--
--  -----------------------------------------------------------------------------------------------  
--  -----------------------------------------------------------------------------------------------  
--

--  ----------------------------------------------------------------------------------------
--   PROCEDURE HELP
--   General help contained within Package
--
  PROCEDURE HELP IS

     BEGIN  

       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('- The Integrated Data Warehouse Processing procedures                -');
       dbms_output.put_line('- ADW_PROCESS                                                        -');
       dbms_output.put_line('-   Current Global Settings                                          -');
       dbms_output.put_line('-     VERSION       = '||g_PACKAGE_VERSION);
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     ADW_PROCESS.GET_PROCESS_NAME;                                  -');
       dbms_output.put_line('-         : Return current process name                              -');
       dbms_output.put_line('-     ADW_PROCESS.GET_SESSION_NO;                                    -');
       dbms_output.put_line('-         : Return current session NUMBER 0 = None defined           -');
       dbms_output.put_line('-     ADW_PROCESS.NEW_SESSION_NO;                                    -');
       dbms_output.put_line('-         : Set new session NUMBER                                   -');

       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-  PROCESSING ROUTINES                                               -');
       dbms_output.put_line('-     call ADW_PROCESS.PROCESS_BEGIN(<process>,<procedure>);         -');
       dbms_output.put_line('-        : Begin a given process. If <process> missing than calling  -');
       dbms_output.put_line('-          procedure will be used                                    -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_PROCESS.PROCESS_LOG(<message>);                       -');
       dbms_output.put_line('-        : Write message to current process log                      -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_PROCESS.PROCESS_DEBUG_LOG(<message>);                 -');
       dbms_output.put_line('-        : Write message to current process log mark as debug type   -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_PROCESS.PROCESS_ERROR(<message>);                     -');
       dbms_output.put_line('-        : Write error message                                       -');
       dbms_output.put_line('-                                                                    -');
 
       dbms_output.put_line('-     call ADW_PROCESS.PROCESS_END;                                  -');
       dbms_output.put_line('-        : Close process set end time                                -');

       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_PROCESS.CLEAR_SESSION;                                -');
       dbms_output.put_line('-        : To clear session number                                   -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_PROCESS.PROCESS_CLOSE(<HoursOld>)                     -');
       dbms_output.put_line('-        : To Clean outstanding processes over HoursOld default 4    -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_PROCESS.PROCESS_FLUSH;                                -');
       dbms_output.put_line('-        : To Flush log entries over 60 days old                     -');
 
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-  PROCESS REPORTING                                                 -');
       dbms_output.put_line('-     call ADW_PROCESS.PROCESS_REPORT(<process>,<report>);           -');
       dbms_output.put_line('-           : To generate report on defined process to given report  -');
	   dbms_output.put_line('-             group                                                  -');
       dbms_output.put_line('-             if the <process> is null then current process will be  -');
       dbms_output.put_line('-             reported                                               -');
       dbms_output.put_line('-          ** if <process> = ALL then all outstanding processes will -');
       dbms_output.put_line('-             be reported                                            -');
 
       dbms_output.put_line('-  HELP                                                              -');
       dbms_output.put_line('-     call ADW_PROCESS.HELP();                                       -');
       dbms_output.put_line('-           : General Help report (This Report)                      -');

     END HELP;

END ADW_PROCESS;
/

PROMPT   --------------------------------------------------------------------------------
PROMPT   -- ADW_SECURITY

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
PROMPT   --------------------------------------------------------------------------------'
PROMPT   -- ADW_ETL

CREATE OR REPLACE PACKAGE ADW_ETL AS
  TABLE_ACCESS EXCEPTION;

  FUNCTION  VERSION      RETURN VARCHAR;
  
  PROCEDURE LOAD_APP      (p_APP_ID     IN VARCHAR2);
  PROCEDURE LOAD_GROUP    (p_GroupName  IN VARCHAR2);
  PROCEDURE BUILD_INDEX   (p_IndexTable IN VARCHAR, p_IndexList IN CLOB  , p_ParentTable IN VARCHAR, p_Prefix IN VARCHAR);

  PROCEDURE MOVE_STAGE_TO_PRODUCTION (p_ETL_LOG_GROUP_ID IN NUMBER);
  PROCEDURE MOVE_TABLE  (p_FromTable  IN VARCHAR, p_ToTable   IN VARCHAR);  
  
  PROCEDURE HELP                  ;

END ADW_ETL;
/
CREATE OR REPLACE PACKAGE BODY ADW_ETL AS
----------------------------------------------------------------------------------------------------------
-- SCRIPT: ADW_ETL
--
-- PURPOSE:
--   Process to load data from different Sources for the Application Date Warehouse
--
-- DEPENDENCIES
--     ADW_PROCESS : Package required
--
-- EXECUTION:
--   This Package transfer data from Source to Staging area
--
-- SYNTAX:
--    N/A
--
-- HISTORY:
--  Mar 19-2021  P. Coward        Initial Version
-----------------------------------------------------------------------------------------------------------

-- Declare  Local procedures

    PROCEDURE LOAD_STAGE ( p_APP_ID VARCHAR2,p_GroupName VARCHAR2) ;
 	PROCEDURE STAGE_CREATE;
	PROCEDURE STAGE_ETL;
    PROCEDURE STAGE_LOCAL;
	PROCEDURE STAGE_INDEX;
 
    Stage_Exception EXCEPTION;
    PRAGMA EXCEPTION_INIT(Stage_Exception, -20001);
--
-- Declare Global Variables
--
     g_PACKAGE_VERSION  CONSTANT VARCHAR(20) := 'V1.0.0(Mar 19,2021)';
	 
	 
     g_PROCESS_NAME        VARCHAR(20) := 'ADW_ETL';
     g_PROCESS_STATUS      VARCHAR(20);
     g_SESSION_NO          NUMBER;
     g_START_TIME          DATE;

     g_ETL_STAGE_ID        NUMBER(22);
     g_APP_ID              VARCHAR2(4);
     g_ETL_GROUP           VARCHAR2(32);
     g_ETL_SEQ             NUMBER(22);
     g_ETL_TYPE            VARCHAR2(32);
     g_ETL_TYPE_PARM       VARCHAR2(256);
	 g_ETL_COMMIT_SIZE     NUMBER(22);
     g_ETL_SRC_SCHEMA      VARCHAR2(256);
     g_ETL_SRC_TABLE       VARCHAR2(256);
	 
     g_CONVERT_UPPER       VARCHAR2(1);
     g_ADW_TABLE           VARCHAR2(128);
     g_STAGE_INDEX         VARCHAR2(256);
	 
     g_STAGE_TABLE         VARCHAR2(128);
     g_PROD_TABLE          VARCHAR2(128);
     g_BACKUP_TABLE        VARCHAR2(128);
     
     g_STAGE_CREATE_SQL    CLOB;
     g_STAGE_SELECT_SQL    CLOB;
     g_STAGE_INSERT_SQL    CLOB;
	 
	 g_RECORD_COUNT_IND    VARCHAR2(1);
     g_VALIDATE_IND        VARCHAR2(1);
     g_MIN_RECORDS         NUMBER(22);
     g_MAX_RECORDS         NUMBER(22);
	 
     g_ETL_LOG_ID          NUMBER(22);
     g_ETL_LOG_GROUP_ID    NUMBER(22);
     g_ETL_START_DATE      DATE;
	 g_ETL_END_DATE        DATE;
	 
	 g_STAGE_VIEW          VARCHAR2(32);
	 g_ColumnList          CLOB;
	 g_ELTColumnList       CLOB;
	 
     g_TotalRecCount       NUMBER       := 0;
     g_RecCount            NUMBER       := -1;

     
-----------------------------------------------------------------------------------------------------------
--
-- Get Current Version of this package
--
  FUNCTION VERSION RETURN VARCHAR   IS
    BEGIN
        RETURN g_PACKAGE_VERSION ;

    END;

-----------------------------------------------------------------------------------------------------------
-- PROCEDURE LOAD_APP
-- To load stage into production based on application ID
--------------------------------------------------

	PROCEDURE LOAD_APP ( p_APP_ID VARCHAR2) IS
		v_DateCreated      DATE;
		v_START_TIME       DATE;
		v_EXECUTION_MINS   NUMBER;
		v_TotalTable       NUMBER;
		v_Count            NUMBER;
	BEGIN
		--- 
		--- Initialization messages
		---

		g_SESSION_NO := ADW_PROCESS.GET_SESSION_NO;
		
		if g_SESSION_NO = 0 then
			ADW_PROCESS.PROCESS_BEGIN('ADW_ETL','LOAD_APP:'||p_APP_ID);
			g_SESSION_NO := ADW_PROCESS.GET_SESSION_NO;
		end if;

		ADW_PROCESS.PROCESS_LOG(' Application '  ||p_APP_ID||' started at '||to_char(SYSDATE, 'yyyy/mm/dd hh:mi:ss'));
		
		LOAD_STAGE(p_APP_ID,NULL);
					

	EXCEPTION
		WHEN Stage_Exception THEN
			NULL;
		
		WHEN OTHERS THEN
			DBMS_OUTPUT.PUT_LINE('ADW_ETL ERROR: '||SQLERRM);
			ADW_PROCESS.PROCESS_ERROR('ADW_ETL ERROR: '||SQLERRM);
            RAISE_APPLICATION_ERROR(-20001,SQLERRM);
	END LOAD_APP;

-----------------------------------------------------------------------------------------------------------
-- PROCEDURE LOAD_GROUP
-- To load stage into production based on Group Name
--------------------------------------------------

    PROCEDURE LOAD_GROUP ( p_GroupName VARCHAR2) IS
        v_DateCreated      DATE;
        v_START_TIME       DATE;
        v_EXECUTION_MINS   NUMBER;
        v_TotalTable       NUMBER;
        v_Count            NUMBER;
    BEGIN
        --- 
        --- Initialization messages
        ---

        g_SESSION_NO := ADW_PROCESS.GET_SESSION_NO;
		if g_SESSION_NO = 0 then
			ADW_PROCESS.PROCESS_BEGIN('ADW_ETL','LOAD_GROUP:'||p_GroupName);
			g_SESSION_NO := ADW_PROCESS.GET_SESSION_NO;
		end if;

        ADW_PROCESS.PROCESS_LOG(' Group '  ||p_GroupName||' started at '||to_char(SYSDATE, 'yyyy/mm/dd hh:mi:ss'));
		
		LOAD_STAGE(NULL, p_GroupName);
					

    EXCEPTION
        WHEN Stage_Exception THEN
            NULL;
        
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ADW_ETL GROUP ERROR: '||SQLERRM);
            ADW_PROCESS.PROCESS_ERROR('ADW_ETL GROUP ERROR: '||SQLERRM);
            RAISE_APPLICATION_ERROR(-20001,SQLERRM);
    END LOAD_GROUP;
	
------------------------------------------------------------------------------------------------------------------
-- PROCEDURE Build index from list or same as Parent Table
--------------------------------------------------

 PROCEDURE BUILD_INDEX(p_IndexTable IN VARCHAR, p_IndexList IN CLOB, p_ParentTable IN VARCHAR, p_Prefix IN VARCHAR) is
	  v_IndexList         VARCHAR2(256);
	  v_IndexName         VARCHAR2(256);
	  v_IndexColumn       VARCHAR2(256);
	  v_Count             number;
	  v_PrefixString      VARCHAR2(256);
	  
      v_NoIndexes         number;
	  
	  -- -----------------------------------------------------------------------------------------
	  -- THIS WILL GET INDEXES FROM PRODUCTION TABLE
	  
	  CURSOR c_Index IS
		  select ind.index_name,
				 LISTAGG('"'||ind_col.column_name||'"', ',') WITHIN GROUP(order by ind_col.column_position) as COLUMNLIST,
				 ind.index_type
			from sys.all_indexes ind
			inner join sys.all_ind_columns ind_col on ind.owner = ind_col.index_owner
			and ind.index_name = ind_col.index_name
			where ind.table_owner = 'ADWADMIN' AND ind.table_name= p_ParentTable
			group by ind.index_name,
				   ind.index_type
			order by ind.index_name;
	BEGIN
		
		--------------------------------------------------------------------
		-- Build indexes defined in table
		--
		
		if p_IndexList is null or LENGTH(RTRIM(LTRIM(p_IndexList))) < 1 then
			--------------------------------------------------------------------
			-- Transfer Parent table indexes to Stage table
			--
			select count(0) into v_Count from user_tables where table_name = p_ParentTable;
			if v_Count = 1 then
				v_NoIndexes := 0;
				for indexRec in c_Index
				LOOP
					 v_NoIndexes := v_NoIndexes + 1;
					 v_IndexName := p_Prefix||'_'||v_NoIndexes||'_IDX';
					 
					 if indexRec.INDEX_TYPE = 'UNIQUE' then
						 execute immediate 'create unique index '||v_IndexName||' ON '||p_IndexTable||'('||indexRec.COLUMNLIST||')';
					 else
						 execute immediate 'create index '||v_IndexName||' ON '||p_IndexTable||'('||indexRec.COLUMNLIST||')';
					 end if;
				END LOOP;
			end if;
			
		  else
			--------------------------------------------------------------------
			-- Create index on columns defined in p_IndexList
			--  EACH INDEX IS SEPERATED BY A SEMI COLON ";" 
			--  EACH COLUMN IS SEPERATED BY A COMMA ","
			--  DOES NOT SUPPORT UNIQUE INDEXES (THESE CAN BE MANUALLY APPLIED)
			--
			v_IndexList :=LTRIM(RTRIM(p_IndexList));
			
			v_NoIndexes := 1;
			v_IndexName := p_Prefix||'_'||v_NoIndexes||'_IDX';
			
			while INSTR(v_IndexList,';') > 0 
			 LOOP
				v_IndexColumn := LTRIM(RTRIM(SUBSTR(v_IndexList,1,INSTR(v_IndexList,';')-1)));
				if LENGTH(v_IndexColumn) > 1 THEN
					 execute immediate 'create index '||v_IndexName||' ON '||p_IndexTable||'('||v_IndexColumn||')';
				end if;
				
				v_IndexList := LTRIM(RTRIM(SUBSTR(v_IndexList,INSTR(v_IndexList,';')+1)));
				
  			    v_NoIndexes := v_NoIndexes + 1;
				v_IndexName := p_Prefix||'_'||v_NoIndexes||'_IDX';
			 END LOOP;
			
			if LENGTH(v_IndexList) > 0 THEN
				execute immediate 'create index '||v_IndexName||' ON '||p_IndexTable||'('||v_IndexList||')';
			end if;
        end if;
		
		
    EXCEPTION
            
       WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20001,'BUILD_INDEX on '||p_IndexTable||' '||SQLERRM);
        
  END BUILD_INDEX;
  
---------------------------------------------------------------------------------------------------------------------------------------------
-- PROCEDURE STAGE_VALIDATE
--  To Validate table and data in Stage table
--------------------------------------------------
  PROCEDURE STAGE_VALIDATE is
	v_Count      number;
	v_ErrorCount number;
	v_errmsg     VARCHAR2(200);
	v_StageTable VARCHAR2(200);
	v_ProdTable  VARCHAR2(200);
	
    BEGIN
		for ETLValidationRec in (SELECT L.ETL_LOG_ID,S.APP_ID,S.ETL_SRC_SCHEMA,S.ETL_SRC_TABLE,S.ADW_TABLE,S.VALIDATE_IND,S.MIN_RECORDS,S.MAX_RECORDS
                   			       FROM ADW_ETL_LOG        L 
			                     INNER JOIN ADW_ETL_STAGE  S ON S.ETL_STAGE_ID = L.ETL_STAGE_ID
							     WHERE L.ETL_LOG_ID = g_ETL_LOG_ID)
		LOOP
									  
			if ETLValidationRec.VALIDATE_IND = 'Y' then
				-- ------------------------------------------------------------------------------------
				-- IF PRODUCTION TABLE EXISTS THEN COMPARE COLUMNS BETWEEN TABLES
				--   VALIDATION ERROR WHEN
				--     DATA_TYPE OR DATA_LENGTH HAS CHANGED
				--     FIELD NOT IN STAGE BUT IN PRODUCTION
				--
				--   ** NEW COLUMN IN STAGE IS NOT FLAGGED AS ERROR 
				--
				v_StageTable := ETLValidationRec.APP_ID||'_'||ETLValidationRec.ADW_TABLE||'_S';
				v_ProdTable  := ETLValidationRec.APP_ID||'_'||ETLValidationRec.ADW_TABLE||'_P';
				
				select count(0) into v_Count FROM USER_TABLES WHERE TABLE_NAME = v_ProdTable;
				
				if(v_Count > 0) then
				
					v_ErrorCount := 0;
					for colummnRec in (select COLUMN_NAME,DATA_TYPE,DATA_LENGTH FROM  user_tab_cols 
										WHERE (COLUMN_NAME,DATA_TYPE,DATA_LENGTH) not in
											 (SELECT p.COLUMN_NAME,p.DATA_TYPE,p.DATA_LENGTH FROM  user_tab_cols p where TABLE_NAME = v_ProdTable)
										 and TABLE_NAME = v_StageTable)
					LOOP
						DBMS_OUTPUT.PUT_LINE     (' Validate: '||v_StageTable||' Column '||colummnRec.COLUMN_NAME||' is added or changed');
						ADW_PROCESS.PROCESS_ERROR(' Validate: '||v_StageTable||' Column '||colummnRec.COLUMN_NAME||' is added or changed');
						v_ErrorCount := v_ErrorCount + 1;
					END LOOP;
					
					for colummnRec in (select COLUMN_NAME FROM  user_tab_cols 
										WHERE (COLUMN_NAME) not in
											 (SELECT p.COLUMN_NAME FROM  user_tab_cols p where TABLE_NAME = v_StageTable)
										 and TABLE_NAME = v_ProdTable)
					LOOP
						DBMS_OUTPUT.PUT_LINE     (' Validate: '||v_StageTable||' Column '||colummnRec.COLUMN_NAME||' is removed');
						ADW_PROCESS.PROCESS_ERROR(' Validate: '||v_StageTable||' Column '||colummnRec.COLUMN_NAME||' is removed');
						v_ErrorCount := v_ErrorCount + 1;
					END LOOP;
					
					if v_ErrorCount > 0 then
						UPDATE ADW_ETL_LOG 
						 SET ETL_MESSAGE  = 'Stage Validated Failed Field Differences'
						 WHERE ETL_LOG_ID = g_ETL_LOG_ID;
						ADW_PROCESS.PROCESS_ERROR(' Validate: '||v_StageTable||' Validation Failed');
						commit;
						RAISE_APPLICATION_ERROR(-20001,' '||v_StageTable||' Validation Failed');
					end if;
					
				end if;
				--
				-- Check record count between Min and Max Records if specified
				--
				
				if (g_RecCount > -1) THEN
					IF (ETLValidationRec.MIN_RECORDS IS NOT NULL AND g_RecCount < ETLValidationRec.MIN_RECORDS) THEN
						v_errmsg :=' Validate: '||v_StageTable||' Has '||g_RecCount||' Records which is less than '||ETLValidationRec.MIN_RECORDS;
						ADW_PROCESS.PROCESS_ERROR(v_errmsg);
						
						UPDATE ADW_ETL_LOG 
						 SET ETL_MESSAGE  = v_errmsg
						 WHERE ETL_LOG_ID = g_ETL_LOG_ID;
						commit;
						RAISE_APPLICATION_ERROR(-20001,v_errmsg);
					
					ELSIF (ETLValidationRec.MAX_RECORDS IS NOT NULL AND ETLValidationRec.MAX_RECORDS > 0 and g_RecCount > ETLValidationRec.MAX_RECORDS ) then
						v_errmsg :=' Validate: '||v_StageTable||' Has '||g_RecCount||' Records with a MAX of '||ETLValidationRec.MAX_RECORDS;
						ADW_PROCESS.PROCESS_ERROR(v_errmsg);
						
						UPDATE ADW_ETL_LOG 
						 SET ETL_MESSAGE  = v_errmsg
						 WHERE ETL_LOG_ID = g_ETL_LOG_ID;
						commit;
						RAISE_APPLICATION_ERROR(-20001,v_errmsg);
					END IF;
				END IF;
				
				UPDATE ADW_ETL_LOG 
				 SET ETL_MESSAGE  = 'Stage Validated Completed '
				 WHERE ETL_LOG_ID = g_ETL_LOG_ID;
			ELSE
				UPDATE ADW_ETL_LOG 
				 SET ETL_MESSAGE  = 'Stage Validated Skipped '
				 WHERE ETL_LOG_ID = g_ETL_LOG_ID;
			END IF;
			
		END LOOP;
		
		COMMIT;
		
    EXCEPTION
       WHEN Stage_Exception THEN
        RAISE_APPLICATION_ERROR(-20001,SQLERRM);
        
       WHEN OTHERS THEN
        ADW_PROCESS.PROCESS_ERROR(SQLERRM);
        RAISE_APPLICATION_ERROR(-20001,'VALIDATE: '||v_StageTable||' '||SQLERRM);
                
  END STAGE_VALIDATE;
  
-----------------------------------------------------------------------------------------------------------
-- MOVE_STAGE_TO_PRODUCTION
-- To move tables from stage data layer to production data layer
--------------------------------------------------

    PROCEDURE MOVE_STAGE_TO_PRODUCTION(p_ETL_LOG_GROUP_ID IN NUMBER) is
        v_DateCreated      DATE;
        v_START_TIME       DATE;
        v_EXECUTION_MINS   NUMBER;
        v_TotalTable       NUMBER;
        v_Count            NUMBER;
    BEGIN
		FOR stageRec  IN (SELECT L.ETL_LOG_ID,S.APP_ID,S.ADW_TABLE
						   FROM ADW_ETL_LOG         L 
						  INNER JOIN ADW_ETL_STAGE  S ON S.ETL_STAGE_ID = L.ETL_STAGE_ID
						  WHERE L.ETL_LOG_GROUP_ID = p_ETL_LOG_GROUP_ID 
						  ORDER BY L.ETL_LOG_ID)
			LOOP
				-- Transfer to Production
				
				select count(0) into v_Count from user_tables where table_name = stageRec.APP_ID||'_'|| stageRec.ADW_TABLE||'_P';
				if v_Count = 1 then
					MOVE_TABLE(stageRec.APP_ID||'_'|| stageRec.ADW_TABLE||'_P',stageRec.APP_ID||'_'|| stageRec.ADW_TABLE||'_B');
				end if;
				
				MOVE_TABLE(stageRec.APP_ID||'_'|| stageRec.ADW_TABLE||'_S',stageRec.APP_ID||'_'|| stageRec.ADW_TABLE||'_P');
				
				UPDATE ADW_ETL_LOG 
					SET ETL_MESSAGE = 'Completed'
				 WHERE ETL_LOG_ID = stageRec.ETL_LOG_ID;
			
			END LOOP;
			
			COMMIT;
			ADW_SECURITY.UPDATE_ETL_ACCESS();
			
  END MOVE_STAGE_TO_PRODUCTION;
  
------------------------------------------------------------------------------------------------------------------
-- PROCEDURE Move tables between Data Layers
--------------------------------------------------
PROCEDURE MOVE_TABLE (p_FromTable IN VARCHAR, p_ToTable IN VARCHAR) is 
 		v_Count  NUMBER;	
     BEGIN
           
        -- remove destination if exists
       
        select count(0) into v_Count from user_tables where table_name = p_ToTable;
        if v_Count = 1 then
             EXECUTE IMMEDIATE    'DROP TABLE '||p_ToTable;
        end if;

        -- Rename table 
		
        execute immediate 'ALTER TABLE "'||p_FromTable||'" RENAME TO "'||p_ToTable || '"';

     EXCEPTION
            
        WHEN OTHERS THEN
             ADW_PROCESS.PROCESS_ERROR(' MOVE TABLE '||p_FromTable||' '||SQLERRM);
             RAISE_APPLICATION_ERROR(-20001,'MOVE_TABLE '||p_FromTable||' '||SQLERRM);
        
 END MOVE_TABLE;

-----------------------------------------------------------------------------------------------------------
-- PROCEDURE LOAD_STAGE
-- To load stage into production based on given SQL
--------------------------------------------------

    PROCEDURE LOAD_STAGE ( p_APP_ID VARCHAR2,p_GroupName VARCHAR2) IS
        v_DateCreated      DATE;
        v_START_TIME       DATE;
        v_EXECUTION_MINS   NUMBER;
        v_TotalTable       NUMBER;
        v_Count            NUMBER;
    BEGIN
         v_TotalTable    := 0;
         g_TotalRecCount := 0;
         g_START_TIME    := SYSDATE;
		 g_ETL_LOG_ID    := 0;
		 
		 -- All ETL Tables for this Stage process will be assigned the same Log Group ID
		 
		 select ADW_ETL_LOG_GROUP_SEQ.nextval into g_ETL_LOG_GROUP_ID from dual;
         
         FOR stageRec  IN (SELECT ETL_STAGE_ID    ,APP_ID,ETL_GROUP,ETL_SEQ         ,ETL_TYPE,ETL_TYPE_PARM,ETL_COMMIT_SIZE,
		                          ETL_SRC_SCHEMA  ,ETL_SRC_TABLE   ,CONVERT_UPPER   ,ADW_TABLE       ,
								  STAGE_INDEX     ,STAGE_CREATE_SQL,STAGE_SELECT_SQL,STAGE_INSERT_SQL,
								  RECORD_COUNT_IND,VALIDATE_IND    ,MIN_RECORDS     ,MAX_RECORDS        
  						     FROM ADW_ETL_STAGE 
					        WHERE  ( (p_APP_ID is not null and APP_ID = p_APP_ID)
					             or (p_GroupName is not null and ETL_GROUP = p_GroupName)) 
								 AND ACTIVE_IND = 'Y' AND ETL_TYPE in ('DBINSERT','DBCOPY','LOCAL')
					ORDER BY ETL_SEQ,ETL_STAGE_ID)
         LOOP
			BEGIN
				g_ETL_STAGE_ID     := stageRec.ETL_STAGE_ID;
				g_APP_ID           := stageRec.APP_ID;
				g_ETL_GROUP        := stageRec.ETL_GROUP;
				g_ETL_SEQ          := stageRec.ETL_SEQ;
				g_ETL_TYPE         := stageRec.ETL_TYPE;
				g_ETL_TYPE_PARM    := stageRec.ETL_TYPE_PARM;
				g_ETL_COMMIT_SIZE  := NVL(stageRec.ETL_COMMIT_SIZE,100000);
				g_ETL_SRC_SCHEMA   := stageRec.ETL_SRC_SCHEMA;
				g_ETL_SRC_TABLE    := stageRec.ETL_SRC_TABLE;
				
				g_ADW_TABLE        := stageRec.ADW_TABLE;
				g_CONVERT_UPPER    := stageRec.CONVERT_UPPER;
				g_STAGE_INDEX      := stageRec.STAGE_INDEX;
				--
				-- Build name for Stage/Production and backup tables
				--
				g_STAGE_TABLE      := g_APP_ID||'_'||g_ADW_TABLE||'_S';
				g_PROD_TABLE       := g_APP_ID||'_'||g_ADW_TABLE||'_P';
				g_BACKUP_TABLE     := g_APP_ID||'_'||g_ADW_TABLE||'_B';
				--
				-- For special case you can build the create, etl and move statements 
				--
				g_STAGE_CREATE_SQL := stageRec.STAGE_CREATE_SQL;
				g_STAGE_SELECT_SQL := stageRec.STAGE_SELECT_SQL;
				g_STAGE_INSERT_SQL := stageRec.STAGE_INSERT_SQL;
				
				g_RECORD_COUNT_IND := stageRec.RECORD_COUNT_IND;
				g_VALIDATE_IND     := stageRec.VALIDATE_IND;
				g_MIN_RECORDS      := stageRec.MIN_RECORDS;
				g_MAX_RECORDS      := stageRec.MAX_RECORDS;
	 
				DBMS_OUTPUT.PUT_LINE   (' ETL Start: '||g_ETL_SRC_SCHEMA||'.'||g_ETL_SRC_TABLE||' Type: '||g_ETL_TYPE||' Parm: '||g_ETL_TYPE_PARM);              
				ADW_PROCESS.PROCESS_LOG(' ETL Start: '||g_ETL_SRC_SCHEMA||'.'||g_ETL_SRC_TABLE||' Type: '||g_ETL_TYPE||' Parm: '||g_ETL_TYPE_PARM);              
				
				-- Build Log Entry for Each ETL

				select ADW_ETL_LOG_SEQ.nextval into g_ETL_LOG_ID from dual;
				
				g_ETL_START_DATE := SYSDATE;
				g_STAGE_VIEW     := 'ADW_ETL_'||g_APP_ID||'_'||g_ETL_STAGE_ID||'_VW';
				g_ColumnList     := '';
				
				insert into ADW_ETL_LOG
				  (ETL_LOG_ID  ,ETL_STAGE_ID  ,ETL_LOG_GROUP_ID  ,ETL_START_DATE  ,ETL_MESSAGE  )
				 values
				  (g_ETL_LOG_ID,g_ETL_STAGE_ID,g_ETL_LOG_GROUP_ID,g_ETL_START_DATE,'ETL Started');
				commit;
				
				g_RecCount  := -1;
				
				CASE g_ETL_TYPE
				
				  WHEN 'DBINSERT' THEN
						STAGE_CREATE;
						STAGE_ETL;
						STAGE_INDEX;
						STAGE_VALIDATE;
				
				  WHEN 'DBCOPY' THEN
						STAGE_CREATE;
						STAGE_ETL;
						STAGE_INDEX;
						STAGE_VALIDATE;

				  WHEN 'LOCAL' THEN
						STAGE_LOCAL;
						STAGE_INDEX;
						STAGE_VALIDATE;
				END CASE;
                
				-----------------------------------------------------------------------------------------------------------------
                ADW_PROCESS.PROCESS_DEBUG_LOG('  Stage Load '||g_STAGE_TABLE||' with '|| to_char(g_RecCount,'99999999') || '  Records in  ' ||ADW_UTILITY.GET_DURATION_TIME(g_ETL_START_DATE,g_ETL_END_DATE));                
                DBMS_OUTPUT.PUT_LINE         ('  Stage Load '||g_STAGE_TABLE||' with '|| to_char(g_RecCount,'99999999') || '  Records in  ' ||ADW_UTILITY.GET_DURATION_TIME(g_ETL_START_DATE,g_ETL_END_DATE));                

                if g_RecCount > 0 then
					g_TotalRecCount := g_TotalRecCount + g_RecCount;
				end if;
				
                v_TotalTable := v_TotalTable + 1;
                                         
                COMMIT;
                                
            EXCEPTION
		        WHEN Stage_Exception THEN
					RAISE_APPLICATION_ERROR(-20001,SQLERRM);

                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE    (' '||g_ETL_SRC_SCHEMA||'.' || g_ETL_SRC_TABLE || ':'||SQLERRM);
                    ADW_PROCESS.PROCESS_ERROR(' '||g_ETL_SRC_SCHEMA||'.' || g_ETL_SRC_TABLE || ':'||SQLERRM);
                    RAISE_APPLICATION_ERROR(-20001,SQLERRM);
            END;            
            
         END LOOP;
		 
        -- --------------------------------------------------------------------------------------
		
		if v_TotalTable = 0 then
			if p_APP_ID is null then
				DBMS_OUTPUT.PUT_LINE     (' No Tables Defined for Group '||p_GroupName);                
				ADW_PROCESS.PROCESS_ERROR(' No Tables Defined for Group '||p_GroupName);
			else
				DBMS_OUTPUT.PUT_LINE     (' No Tables Defined for Application '||p_APP_ID);                
				ADW_PROCESS.PROCESS_ERROR(' No Tables Defined for Application '||p_APP_ID);
			end if;
			RAISE_APPLICATION_ERROR(-20001,' No Tables Defined');
			COMMIT;
		else
			DBMS_OUTPUT.PUT_LINE         ('  Move Stage to Production ');                
			ADW_PROCESS.PROCESS_DEBUG_LOG('  Move Stage to Production ');   
			COMMIT;
			
			
			-- --------------------------------------------------------------------------------------
			--
			-- Move to Production since we have no errors.
			--
			if g_ETL_LOG_ID > 0 then
				MOVE_STAGE_TO_PRODUCTION(g_ETL_LOG_GROUP_ID);

			end if;
			
			ADW_PROCESS.PROCESS_DEBUG_LOG(' Total Records Transferred for '||v_TotalTable||' is '||g_TotalRecCount||' records in '||ADW_UTILITY.GET_DURATION_TIME(g_START_TIME,sysdate));                
		end if;
		
    EXCEPTION
        WHEN Stage_Exception THEN
 			RAISE_APPLICATION_ERROR(-20001,SQLERRM);
       
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ETL ERROR: '||SQLERRM);
            ADW_PROCESS.PROCESS_ERROR('ETL ERROR: '||SQLERRM);
			RAISE_APPLICATION_ERROR(-20001,'ETL ERROR: '||SQLERRM);
    END LOAD_STAGE;

--------------------------------------------------
-- PROCEDURE STAGE_CREATE
--  To Build Stage Table
--------------------------------------------------
   PROCEDURE STAGE_CREATE is
		v_ViewFieldList	CLOB;
		v_SourceTable   VARCHAR2(1000);
		v_SQLCommand    CLOB;
 
     BEGIN
	 
		ADW_UTILITY.REMOVE_TABLE(g_STAGE_TABLE);
		ADW_UTILITY.REMOVE_VIEW(g_STAGE_VIEW);
		
		g_ColumnList := '';
		g_ELTColumnList := '';
		v_ViewFieldList := '';
		
 		-- Check for user provided ETL Commands
		
		if g_STAGE_CREATE_SQL is null or LENGTH(RTRIM(LTRIM(g_STAGE_CREATE_SQL))) < 1 then
			if g_ETL_SRC_SCHEMA = '' or g_ETL_SRC_SCHEMA is null then
				v_SourceTable := '"'||g_ETL_SRC_TABLE||'"';
			else
				v_SourceTable := g_ETL_SRC_SCHEMA||'."'||g_ETL_SRC_TABLE||'"';
			end if;
			
			if g_ETL_TYPE_PARM is not null then
				v_SourceTable := v_SourceTable||'@"'||g_ETL_TYPE_PARM||'"';
			end if;
			
			v_SQLCommand :='CREATE VIEW '||g_STAGE_VIEW||' AS (SELECT * FROM '||v_SourceTable||')';
			EXECUTE IMMEDIATE (v_SQLCommand);
			
			-- Need to validate view
			
			EXECUTE IMMEDIATE 'SELECT COUNT(0) FROM '||g_STAGE_VIEW||' where rownum < 0';
			
			--
			-- Build list of columns in source table 
			--
			
			g_ColumnList := '';
			g_ELTColumnList := '';
			v_ViewFieldList := '';
			for colummnRec in (select COLUMN_NAME,DATA_TYPE,DATA_LENGTH FROM  user_tab_cols 
								WHERE TABLE_NAME = g_STAGE_VIEW and COLUMN_NAME NOT IN ('SHARE') ORDER BY COLUMN_ID)
			LOOP
			   if g_CONVERT_UPPER = 'Y' THEN
					if colummnRec.DATA_LENGTH = 0 then
						v_ViewFieldList := v_ViewFieldList||'CAST("'||colummnRec.COLUMN_NAME||'" AS VARCHAR2(1)) '||colummnRec.COLUMN_NAME||',';
					else
						v_ViewFieldList := v_ViewFieldList||'"'||colummnRec.COLUMN_NAME||'" '||colummnRec.COLUMN_NAME||',';
					end if;
			   else
				   if colummnRec.DATA_LENGTH = 0 then
						v_ViewFieldList := v_ViewFieldList||'CAST("'||colummnRec.COLUMN_NAME||'" AS VARCHAR2(1)) "'||colummnRec.COLUMN_NAME||'",';
					else
						v_ViewFieldList := v_ViewFieldList||'"'||colummnRec.COLUMN_NAME||'",';
					end if;
				END IF;
				
				g_ColumnList    := g_ColumnList   ||'"'       ||colummnRec.COLUMN_NAME||'",';
				g_ELTColumnList := g_ELTColumnList||'c1_rec."'||colummnRec.COLUMN_NAME||'",';
			END LOOP;
			v_ViewFieldList := substr(v_ViewFieldList,1,length(v_ViewFieldList)-1);
			g_ColumnList    := substr(g_ColumnList   ,1,length(g_ColumnList)-1);
			g_ELTColumnList := substr(g_ELTColumnList,1,length(g_ELTColumnList)-1);
        
			--
			-- Create Stage Table
			--
			IF g_ETL_TYPE = 'DBCOPY' THEN
				execute immediate 'CREATE TABLE '||g_STAGE_TABLE||' as ( SELECT '||v_ViewFieldList||',sysdate ADW_ETL_DATE FROM '||g_STAGE_VIEW||')';
				g_RecCount := SQL%ROWCOUNT;
			ELSE
				execute immediate 'CREATE TABLE '||g_STAGE_TABLE||' as ( SELECT '||v_ViewFieldList||',sysdate ADW_ETL_DATE FROM '||g_STAGE_VIEW||' WHERE ROWNUM < 0)';
			END IF;

			-- Remove Constraints since table is created from view
			for colummnRec in (select CONSTRAINT_NAME FROM user_cons_columns
								WHERE TABLE_NAME = g_STAGE_TABLE)
			LOOP
				execute immediate 'ALTER TABLE '||g_STAGE_TABLE||' DROP CONSTRAINT '||colummnRec.CONSTRAINT_NAME;
			END LOOP;
		end if;
		
		UPDATE ADW_ETL_LOG 
		 SET ETL_MESSAGE  = 'Stage Built Completed'
		 WHERE ETL_LOG_ID = g_ETL_LOG_ID;
		COMMIT;
	 
    EXCEPTION
       WHEN Stage_Exception THEN
            dbms_output.put_line(SQLERRM);
			RAISE_APPLICATION_ERROR(-20001,SQLERRM);
        
       WHEN OTHERS THEN
        ADW_PROCESS.PROCESS_ERROR('STAGE_CREATE:'||g_ETL_SRC_TABLE||' '||SQLERRM);
        RAISE_APPLICATION_ERROR(-20001,'STAGE_CREATE:'||g_ETL_SRC_TABLE||' '||SQLERRM);
        
  END STAGE_CREATE;
  
---------------------------------------------------------------------------------------------------------------------------------------------
-- PROCEDURE STAGE_ETL
--  To ETL data to Stage table
--------------------------------------------------
 PROCEDURE STAGE_ETL is
	v_Count        number;
	v_STAGE_PROC   VARCHAR2(128);
    v_crlf         VARCHAR2(2)  := chr(13)||chr(10);
    BEGIN
		if g_STAGE_INSERT_SQL is null or LENGTH(RTRIM(LTRIM(g_STAGE_INSERT_SQL))) < 1 then
			
			  -- --------------------------------- INSERT EACH RECORDS INTO TABLE ----------------------------
			  -- THIS IS A BLOCK COPY PROCESS BASED IN THE COMMIT SIZE
			  -- CREATE A PROCEDURE IS CREATED TO COPY EACH RECORD
			  --   * THIS IS SLOWER PROCESS BUT BETTER IF THERE ARE ISSUES WITH CONNECTIONS BETWEEN 
			  --     ORACLE INSTANCES
			  -- --------------------------------------------------------------------------------------------- 

			IF g_ETL_TYPE = 'DBINSERT' THEN
				
				if g_ColumnList != '' THEN
					v_STAGE_PROC := 'ADW_ETL_'||g_APP_ID||'_'||g_ETL_STAGE_ID||'_PROC';
					
					EXECUTE IMMEDIATE 'CREATE OR REPLACE  PROCEDURE '||v_STAGE_PROC||' is '||v_crlf||
							 ' CURSOR c1 IS    SELECT '||g_ColumnList||' FROM '||g_ETL_SRC_SCHEMA||'."'||g_ETL_SRC_TABLE||'"@'||g_ETL_TYPE_PARM||';'||v_crlf||
							 '   v_RecCount          number;'||v_crlf||
							 '   v_sysdate           DATE;'||v_crlf||
							 'BEGIN '||v_crlf||
							 '   v_RecCount:=0;'||v_crlf||
							 '   DBMS_OUTPUT.PUT_LINE('''||v_STAGE_PROC||' for Table '||g_ETL_SRC_TABLE||''');'||v_crlf||
							 '   for c1_rec in c1 '||v_crlf||
							 '   loop'||v_crlf||
							 '      insert into '||g_STAGE_TABLE||'('||g_ColumnList||',ADW_ETL_DATE)'||v_crlf||
							 '          values ('||g_ELTColumnList||',SYSDATE);'||v_crlf||
							 '       if MOD(v_RecCount,'||g_ETL_COMMIT_SIZE||') = 0   then'||v_crlf||
							 '           commit;'||v_crlf||
							 '           DBMS_OUTPUT.put_line (''COMMIT ''||v_RecCount||'' ''||TO_CHAR (SYSDATE, ''yyyymmdd hh:mi:ss''));'||v_crlf||
							 '       end if;'||v_crlf||
							 '       v_RecCount := v_RecCount + 1;'||v_crlf||
							 '   end loop;'||v_crlf||
							 '   commit;'||v_crlf||
							 'EXCEPTION '||v_crlf||
							 '   WHEN OTHERS THEN '||v_crlf||
							 '     ADW_PROCESS.PROCESS_ERROR(''ETL: ''||SQLERRM); '||v_crlf||
							 '     RAISE_APPLICATION_ERROR(-20001,''ETL: ''||SQLERRM); '||v_crlf||
							 'END '||v_STAGE_PROC||';'||v_crlf;
					 
					-- RUN AND DROP THE PROCEDURE
					EXECUTE IMMEDIATE 'Begin '||v_STAGE_PROC||'; end;';
					EXECUTE IMMEDIATE 'DROP PROCEDURE '||v_STAGE_PROC;
				ELSE
					ADW_PROCESS.PROCESS_ERROR(       'STAGE_ETL: Can not use create SQL on type DBINSERT for '||g_ETL_SRC_TABLE);
					RAISE_APPLICATION_ERROR  (-20001,'STAGE_ETL: Can not use create SQL on type DBINSERT for '||g_ETL_SRC_TABLE);
				END IF;
		    END IF;
		--------------------------------- EXECUTE USER DEFINED SCRIPT ----------------------------
		ELSE
			ADW_UTILITY.SQL_EXECUTE(g_STAGE_INSERT_SQL);
		END IF;
		
	    --
		-- GET NUMBER OF RECORDS IN TABLE IF COUNT REQUIRED
		--
		if g_RECORD_COUNT_IND = 'Y' then
			EXECUTE IMMEDIATE 'SELECT COUNT(0) FROM "'||g_STAGE_TABLE||'"' into g_RecCount;
		end if;
		
		ADW_UTILITY.REMOVE_VIEW(g_STAGE_VIEW);
		
		-- Were done so Update Log

		g_ETL_END_DATE := SYSDATE;
		UPDATE ADW_ETL_LOG 
		 SET ETL_MESSAGE  = 'Source ETL to Stage Completed',
			 ETL_NO_ROWS  = g_RecCount,
			 ETL_END_DATE = g_ETL_END_DATE
		 WHERE ETL_LOG_ID = g_ETL_LOG_ID;
		COMMIT;
		
    EXCEPTION
       WHEN Stage_Exception THEN
        RAISE_APPLICATION_ERROR(-20001,SQLERRM);
        
       WHEN OTHERS THEN
        ADW_PROCESS.PROCESS_ERROR('STAGE_ETL:'||g_ETL_SRC_TABLE||' '||SQLERRM);
        RAISE_APPLICATION_ERROR(-20001,'STAGE_ETL: '||g_ETL_SRC_TABLE||' '||SQLERRM);
                
  END STAGE_ETL;
  
  ---------------------------------------------------------------------------------------------------------------
  -- PROCEDURE STAGE_LOCAL
  --  Move external table into stage for processing
  --------------------------------------------------
  PROCEDURE STAGE_LOCAL is
      
      v_sql                   VARCHAR2(512);
      v_StageName             VARCHAR2(32);
      v_Count                 number;
 
     BEGIN
 
		ADW_UTILITY.REMOVE_TABLE(g_STAGE_TABLE);
 
		if g_STAGE_CREATE_SQL is null or LENGTH(RTRIM(LTRIM(g_STAGE_CREATE_SQL))) < 1 then
			select count(0) into v_Count from user_tables where table_name = g_ETL_SRC_TABLE;
			if v_Count = 1 then
				EXECUTE IMMEDIATE 'CREATE TABLE '||g_STAGE_TABLE||' AS (SELECT '||g_ETL_SRC_SCHEMA||'."'||g_ETL_SRC_TABLE||'".*,SYSDATE ADW_ETL_DATE FROM '||g_ETL_SRC_SCHEMA||'."'||g_ETL_SRC_TABLE||'")';
				g_RecCount := SQL%ROWCOUNT;
			else
				ADW_PROCESS.PROCESS_ERROR     ('Table '||g_ETL_SRC_TABLE||' Not Located in ADW');
				RAISE_APPLICATION_ERROR(-20001,'Table '||g_ETL_SRC_TABLE||' Not Located in ADW');
			end if;
		--------------------------------- EXECUTE USER DEFINED SCRIPT ----------------------------
		ELSE
			ADW_UTILITY.SQL_EXECUTE(g_STAGE_CREATE_SQL);
		END IF;

		g_ETL_END_DATE := SYSDATE;
		UPDATE ADW_ETL_LOG 
		 SET ETL_MESSAGE  = 'Local to Stage Completed',
			 ETL_NO_ROWS  = g_RecCount,
			 ETL_END_DATE = g_ETL_END_DATE
		 WHERE ETL_LOG_ID = g_ETL_LOG_ID;
		COMMIT;
		
    EXCEPTION
        WHEN Stage_Exception THEN
            dbms_output.put_line(SQLERRM);
            
        WHEN OTHERS THEN
             ADW_PROCESS.PROCESS_ERROR(SQLERRM);
             RAISE_APPLICATION_ERROR(-20001,'  EXTERNAL: '||SQLERRM);
        
  END STAGE_LOCAL;
 
------------------------------------------------------------------------------------------------------------------
-- PROCEDURE STAGE_INDEX
--------------------------------------------------
 PROCEDURE STAGE_INDEX is
	  
	BEGIN
	
		BUILD_INDEX(g_STAGE_TABLE, g_STAGE_INDEX, g_PROD_TABLE , g_APP_ID||'_'||g_ETL_LOG_ID); 
        
        SYS.DBMS_STATS.GATHER_TABLE_STATS (
            OwnName             => 'ADWADMIN',
            TabName             => g_STAGE_TABLE,
            Estimate_Percent    => 20,
            Method_Opt          => 'FOR ALL COLUMNS SIZE AUTO ',
            Degree              => 4,
            Cascade             => TRUE,
            No_Invalidate       => TRUE);
        
		UPDATE ADW_ETL_LOG 
		 SET ETL_MESSAGE  = 'Stage Index Completed'
		 WHERE ETL_LOG_ID = g_ETL_LOG_ID;
		COMMIT;
		
    EXCEPTION
        WHEN Stage_Exception THEN
            dbms_output.put_line(SQLERRM);
            
       WHEN OTHERS THEN
            ADW_PROCESS.PROCESS_ERROR('STAGE_INDEX on '||g_STAGE_TABLE||' '||SQLERRM);
            RAISE_APPLICATION_ERROR(-20001,'STAGE_INDEX on '||g_STAGE_TABLE||' '||SQLERRM);
        
  END STAGE_INDEX;
 

--------------------------------------------------
-- PROCEDURE HELP
-- General help contained within all procedures
--------------------------------------------------

  PROCEDURE HELP IS
  
    BEGIN
    
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('- The Staging loading package ADW_ETL contains procedures            -');
       dbms_output.put_line('- for building Staging views, staging and Production tables for the  -');
       dbms_output.put_line('- Application Data Warehouse (ADW)                                   -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-   Current Global Settings                                          -');
       dbms_output.put_line('-     VERSION       = '||g_PACKAGE_VERSION);
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-  PROCESSING ROUTINES                                               -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_ETL.LOAD_APP(<application id>);                       -');
       dbms_output.put_line('-        : to load ETL based on application ID                       -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_ETL.LOAD_GROUP(<group name>);                         -');
       dbms_output.put_line('-        : to load ETL based on group name                           -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_ETL.BUILD_INDEX(<index table>,<index list>,           -');
       dbms_output.put_line('-                             <parent table>, <prefix>)              -');
       dbms_output.put_line('-        :To build index on <index Table> from ,<index list>         -');
       dbms_output.put_line('-         or from the sample parent table                            -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-     call ADW_ETL.MOVE_STAGE_TO_PRODUCTION(<Log Group ID>)          -');
       dbms_output.put_line('-                             <parent table>, <prefix>)              -');
       dbms_output.put_line('-        :To move the Stage tables logged in ADW_ETL_LOG             -');
       dbms_output.put_line('-         <Log Group ID> to Production tables Layer                  -');
       dbms_output.put_line('-                                                                    -');

       dbms_output.put_line('-     call ADW_ETL.MOVE_TABLE(<from table>,<to table>)               -');
       dbms_output.put_line('-        :To remove <to_table> is exists                             -'); 
       dbms_output.put_line('-         then rename <from table> to <to table>                     -');
       dbms_output.put_line('-                                                                    -');
       dbms_output.put_line('-  HELP                                                              -');
       dbms_output.put_line('-     call ADW_ETL.HELP();                                           -');
       dbms_output.put_line('-           : General Help report (This Report)                      -');
   
    END HELP;
    


END ADW_ETL;
/

PROMPT   --------------------------------------------------------------------------------'
PROMPT   -- ADW_NOTIFY

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
--  23-May-2021   Paul Coward Initial Version
-----------------------------------------------------------------------------------------------------------
-- Declare mail procedure as local function

-----------------------------------------------------------------------------------------------------------
-- Declare Global Variables

  g_PACKAGE_VERSION  CONSTANT VARCHAR(256) := 'ADW_NOTIFY V1.0.0 (May 23,2021)';
  
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


PROMPT   --------------------------------------------------------------------------------'
PROMPT   -- ADW_MAIL

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
--  16-Jun-2021    P. Coward  Initial version
--
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-- Declare mail procedure as local function

--
-- Declare Global Variables
--
   g_PACKAGE_VERSION  CONSTANT VARCHAR(60) := 'ADW_MAIL V1.0.0 (Jun 16,2021)';

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

      v_SourceEmail       VARCHAR2(250) := '<your e-mail>';
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

PROMPT   --------------------------------------------------------------------------------'
PROMPT   -- ADW_HEALTH_CHECK

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
            ADW_PROCESS.PROCESS_ERROR('ADW_HEALTH_CHECK:'||SQLERRM);
  END ADW_HEALTH_CHECK;
 /

PROMPT   --------------------------------------------------------------------------------'
PROMPT   -- ADW_SAMPLE Oracle Build

CREATE OR REPLACE PROCEDURE ADW_SAMPLE_BUILD AS 
    Stage_Exception EXCEPTION;
    PRAGMA EXCEPTION_INIT(Stage_Exception, -20001);
BEGIN
	ADW_PROCESS.PROCESS_BEGIN('ADW_SAMPLE_BUILD');

	ADW_ETL.LOAD_APP('<APPLICATION_NAME>');

	ADW_PROCESS.PROCESS_END;

EXCEPTION

	WHEN Stage_Exception THEN
		ADW_PROCESS.PROCESS_END;

    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE(SQLERRM);
	  ADW_PROCESS.PROCESS_ERROR('ADW_SAMPLE_BUILD:'||SQLERRM);
 	  ADW_PROCESS.PROCESS_END;

END ADW_SAMPLE_BUILD;

/


