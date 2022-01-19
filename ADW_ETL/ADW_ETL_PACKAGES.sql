DROP PACKAGE ADW_ETL;

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
--  Feb 19-2021  P. Coward        Initial Version
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
     g_PACKAGE_VERSION  CONSTANT VARCHAR(20) := 'V1.0.0(Feb 19,2021)';
	 
	 
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
