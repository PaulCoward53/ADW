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
