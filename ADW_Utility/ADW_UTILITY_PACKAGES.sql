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
