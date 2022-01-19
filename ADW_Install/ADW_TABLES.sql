PROMPT   --------------------------------------------------------------------------------
PROMPT   -- ADW Tables
PROMPT
PROMPT   -- ADW_APPLICATION
PROMPT
CREATE TABLE ADW_APPLICATION
(
  APP_ID              VARCHAR2(4)          NOT NULL,
  DATA_OWNER          VARCHAR2(256),
  DATA_STEWARD        VARCHAR2(256),
  DATA_CUSTODIAN      VARCHAR2(256),
  DESCRIPTION         VARCHAR2(2000),
  ROW_CREATE_USER     VARCHAR2(40)         DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE     DATE                 DEFAULT sysdate,
  ROW_MODIFY_USER     VARCHAR2(40),
  ROW_MODIFY_DATE     DATE,
  CONSTRAINT ADW_APPLICATION_EXEC_PK         PRIMARY KEY (APP_ID)
);

COMMENT ON COLUMN ADW_APPLICATION.APP_ID                is 'Application unique prefix';
COMMENT ON COLUMN ADW_APPLICATION.DATA_OWNER            is 'Application Data Owner';
COMMENT ON COLUMN ADW_APPLICATION.DATA_STEWARD          is 'Applciation Data Steward';
COMMENT ON COLUMN ADW_APPLICATION.DATA_CUSTODIAN        is 'Applciation Data Custodian';
COMMENT ON COLUMN ADW_APPLICATION.DESCRIPTION           is 'Application Description';
COMMENT ON COLUMN ADW_APPLICATION.ROW_CREATE_USER       is 'User who created row';
COMMENT ON COLUMN ADW_APPLICATION.ROW_CREATE_DATE       is 'Date Row Created';
COMMENT ON COLUMN ADW_APPLICATION.ROW_MODIFY_USER       is 'User who modified row';
COMMENT ON COLUMN ADW_APPLICATION.ROW_MODIFY_DATE       is 'Date Row modified';


CREATE OR REPLACE TRIGGER ADW_APPLICATION_BI 
  BEFORE INSERT ON ADW_APPLICATION FOR EACH ROW
BEGIN
	
	:new.APP_ID        := upper(:new.APP_ID);

	:new.ROW_CREATE_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_CREATE_DATE  := sysdate;

	:new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_MODIFY_DATE  := sysdate;
END;
/
CREATE OR REPLACE TRIGGER ADW_APPLICATION_BU 
  BEFORE UPDATE ON ADW_APPLICATION FOR EACH ROW
BEGIN
	:new.APP_ID        := upper(:new.APP_ID);
	
	:new.ROW_CREATE_USER  := :old.ROW_CREATE_USER ;
	:new.ROW_CREATE_DATE  := :old.ROW_CREATE_DATE ;

	:new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_MODIFY_DATE  := sysdate;
END;
/
PROMPT   --------------------------------------------------------------------------------
PROMPT   -- ADW_PROCESS

CREATE TABLE ADW_PROCESS_EXECUTION
(
  PROCESS_ID          NUMBER               GENERATED ALWAYS AS IDENTITY,
  SESSION_NO          NUMBER               NOT NULL,
  PROCESS_NAME        VARCHAR2(256)        NOT NULL,
  PROCEDURE_NAME      VARCHAR2(256),
  PROCESS_STATUS      VARCHAR2(20)         DEFAULT 'PROCESSING',
  PROCESS_START_DATE  DATE                 DEFAULT sysdate,
  PROCESS_END_DATE    DATE,
  PROCESS_REPORT_DATE DATE,
  PROCESS_HOST        VARCHAR2(256),
  PROCESS_USER        VARCHAR2(256),
  ROW_CREATE_USER     VARCHAR2(40)         DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE     DATE                 DEFAULT sysdate,
  ROW_MODIFY_USER     VARCHAR2(40),
  ROW_MODIFY_DATE     DATE,
  CONSTRAINT ADW_PROCESS_EXEC_STAT_CHK   CHECK       (PROCESS_STATUS IN ('PROCESSING','COMPLETED','ERRORED')),
  CONSTRAINT ADW_PROCESS_EXEC_PK         PRIMARY KEY (PROCESS_ID)
);

COMMENT ON COLUMN ADW_PROCESS_EXECUTION.PROCESS_ID            is 'Unique ID for given process';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.SESSION_NO            is 'Session number assigned to Process';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.PROCESS_NAME          is 'Name of the process or procedure';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.PROCEDURE_NAME        is 'Procedure which called package';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.PROCESS_STATUS        is 'Process status (PROCESSING, COMPLETED, ERRORED)';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.PROCESS_START_DATE    is 'Data/Time process started';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.PROCESS_END_DATE      is 'Data/Time process ended';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.PROCESS_REPORT_DATE   is 'Data/Time process reported';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.PROCESS_HOST          is 'process host name';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.PROCESS_USER          is 'process user name';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.ROW_CREATE_USER       is 'User who created row';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.ROW_CREATE_DATE       is 'Date Row Created';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.ROW_MODIFY_USER       is 'User who modified row';
COMMENT ON COLUMN ADW_PROCESS_EXECUTION.ROW_MODIFY_DATE       is 'Date Row modiffied';


CREATE OR REPLACE TRIGGER ADW_PROCESS_EXECUTION_BI 
  BEFORE INSERT ON ADW_PROCESS_EXECUTION FOR EACH ROW
BEGIN
	
	:new.PROCESS_NAME     := upper(:new.PROCESS_NAME);
	:new.PROCEDURE_NAME   := upper(:new.PROCEDURE_NAME);

	:new.ROW_CREATE_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_CREATE_DATE  := sysdate;

	:new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_MODIFY_DATE  := sysdate;
END;
/
CREATE OR REPLACE TRIGGER ADW_PROCESS_EXECUTION_BU 
  BEFORE UPDATE ON ADW_PROCESS_EXECUTION FOR EACH ROW
BEGIN
	:new.PROCESS_NAME     := upper(:new.PROCESS_NAME);
	:new.PROCEDURE_NAME   := upper(:new.PROCEDURE_NAME);

	:new.ROW_CREATE_USER  := :old.ROW_CREATE_USER ;
	:new.ROW_CREATE_DATE  := :old.ROW_CREATE_DATE ;

	:new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_MODIFY_DATE  := sysdate;
END;
/

CREATE TABLE ADW_PROCESS_LOG
(
  PROCESS_LOG_ID    NUMBER                      GENERATED ALWAYS AS IDENTITY,
  PROCESS_NAME      VARCHAR2(256)               NOT NULL,
  SESSION_NO        NUMBER                      DEFAULT 0,
  MESSAGE_TYPE      VARCHAR2(1)                 DEFAULT 'N',
  MESSAGE_TEXT      VARCHAR2(2000),
  ROW_CREATE_USER   VARCHAR2(40)                DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE   DATE                        DEFAULT sysdate,
  CONSTRAINT ADW_PROCESS_LOG_PK         PRIMARY KEY (PROCESS_LOG_ID),
  CONSTRAINT ADW_PROCESS_LOG_TYPE_CHK   CHECK       (MESSAGE_TYPE IN ('N','E','D'))
); 

COMMENT ON COLUMN ADW_PROCESS_LOG.PROCESS_LOG_ID        is 'Message log sequence number (order placed in log)';
COMMENT ON COLUMN ADW_PROCESS_LOG.PROCESS_NAME          is 'Name of the process or procedure';
COMMENT ON COLUMN ADW_PROCESS_LOG.SESSION_NO            is 'Session Number for process or procedure';
COMMENT ON COLUMN ADW_PROCESS_LOG.MESSAGE_TYPE          is 'Message Type (N-Normal E-Error or D-Debug)';
COMMENT ON COLUMN ADW_PROCESS_LOG.MESSAGE_TEXT          is 'Message Text';
COMMENT ON COLUMN ADW_PROCESS_LOG.ROW_CREATE_USER       is 'User who created row';
COMMENT ON COLUMN ADW_PROCESS_LOG.ROW_CREATE_DATE       is 'Date Row Created';

CREATE OR REPLACE TRIGGER ADW_PROCESS_LOG_BI 
  BEFORE INSERT ON ADW_PROCESS_LOG FOR EACH ROW
BEGIN
	
	:new.PROCESS_NAME    := upper(:new.PROCESS_NAME);
	:new.MESSAGE_TYPE    := upper(:new.MESSAGE_TYPE);

END;
/

CREATE OR REPLACE TRIGGER ADW_PROCESS_LOG_BU 
  BEFORE UPDATE ON ADW_PROCESS_LOG FOR EACH ROW
BEGIN
	:new.PROCESS_NAME    := upper(:new.PROCESS_NAME);
	:new.MESSAGE_TYPE    := upper(:new.MESSAGE_TYPE);

END;
/

PROMPT   --------------------------------------------------------------------------------
PROMPT   -- ADW_SECURITY

CREATE TABLE ADW_SECURITY_ROLE
(
  ROLE                  VARCHAR2(32)           NOT NULL,
  ROLE_OWNER            VARCHAR2(32),
  APPROVED_DATE         DATE,
  REMARK                VARCHAR2(2000),
  ACTIVE_IND            VARCHAR2(1)          DEFAULT 'Y',
  ROW_CREATE_USER       VARCHAR2(40)         DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE       DATE                 DEFAULT sysdate,
  ROW_MODIFY_USER       VARCHAR2(40),
  ROW_MODIFY_DATE       DATE,
  CONSTRAINT ADW_SECURITY_ROLE_PK           PRIMARY KEY (ROLE), 
  CONSTRAINT ADW_SECURITY_ROLE_ACT_CHK      CHECK       (ACTIVE_IND IN ('Y','N'))
);

COMMENT ON COLUMN ADW_SECURITY_ROLE.ROLE             is 'ROLE Name';
COMMENT ON COLUMN ADW_SECURITY_ROLE.ROLE_OWNER       is 'ROLE Business Owner';
COMMENT ON COLUMN ADW_SECURITY_ROLE.APPROVED_DATE    is 'Date Role Approved by Owner';
COMMENT ON COLUMN ADW_SECURITY_ROLE.REMARK           is 'ROLE Remark';
COMMENT ON COLUMN ADW_SECURITY_ROLE.ACTIVE_IND       is 'record Active indicator';
COMMENT ON COLUMN ADW_SECURITY_ROLE.ROW_CREATE_USER  is 'User who created row';
COMMENT ON COLUMN ADW_SECURITY_ROLE.ROW_CREATE_DATE  is 'Date Row Created';
COMMENT ON COLUMN ADW_SECURITY_ROLE.ROW_MODIFY_USER  is 'User who modified row';
COMMENT ON COLUMN ADW_SECURITY_ROLE.ROW_MODIFY_DATE  is 'Date Row modified';


CREATE OR REPLACE TRIGGER ADW_SECURITY_ROLE_BI 
  BEFORE INSERT ON ADW_SECURITY_ROLE FOR EACH ROW
    BEGIN
 		  :new.ROLE             := upper(:new.ROLE);
		  :new.ACTIVE_IND       := upper(:new.ACTIVE_IND);
		  
          :new.ROW_CREATE_USER  := sys_context( 'userenv', 'os_user' );
          :new.ROW_CREATE_DATE  := sysdate;
         
          :new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE  := sysdate;
    END;
/
CREATE OR REPLACE TRIGGER ADW_SECURITY_ROLE_BU 
  BEFORE UPDATE ON ADW_SECURITY_ROLE FOR EACH ROW
    BEGIN
 		  :new.ROLE             := upper(:new.ROLE);
		  :new.ACTIVE_IND       := upper(:new.ACTIVE_IND);
		  
          :new.ROW_CREATE_USER  := :old.ROW_CREATE_USER ;
          :new.ROW_CREATE_DATE  := :old.ROW_CREATE_DATE ;
         
          :new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE  := sysdate;
    END;
/

CREATE TABLE ADW_SECURITY_SCHEMA
(
  SCHEMA_NAME           VARCHAR2(32)         NOT NULL,
  ROLE                  VARCHAR2(32)         NOT NULL,
  APPROVED_DATE         DATE                 DEFAULT sysdate,
  ACTIVE_IND            VARCHAR2(1)          DEFAULT 'Y',
  ROW_CREATE_USER       VARCHAR2(40)         DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE       DATE                 DEFAULT sysdate,
  ROW_MODIFY_USER       VARCHAR2(40),
  ROW_MODIFY_DATE       DATE,
  CONSTRAINT ADW_SECURITY_SCHEMA_PK         PRIMARY KEY (SCHEMA_NAME,ROLE),
  CONSTRAINT ADW_SECURITY_SCHEMA_ACT_CHK    CHECK       (ACTIVE_IND IN ('Y','N')),
  CONSTRAINT ADW_SECURITY_SCHEMA_FK01       FOREIGN KEY (ROLE)
                                            REFERENCES   ADW_SECURITY_ROLE(ROLE)

);

COMMENT ON COLUMN ADW_SECURITY_SCHEMA.SCHEMA_NAME      is 'Schema Name';
COMMENT ON COLUMN ADW_SECURITY_SCHEMA.ROLE             is 'Role Schema has access to';
COMMENT ON COLUMN ADW_SECURITY_SCHEMA.APPROVED_DATE    is 'Date access was approved';
COMMENT ON COLUMN ADW_SECURITY_SCHEMA.ACTIVE_IND       is 'record Active indicator';
COMMENT ON COLUMN ADW_SECURITY_SCHEMA.ROW_CREATE_USER  is 'User who created row';
COMMENT ON COLUMN ADW_SECURITY_SCHEMA.ROW_CREATE_DATE  is 'Date Row Created';
COMMENT ON COLUMN ADW_SECURITY_SCHEMA.ROW_MODIFY_USER  is 'User who modified row';
COMMENT ON COLUMN ADW_SECURITY_SCHEMA.ROW_MODIFY_DATE  is 'Date Row modified';


CREATE OR REPLACE TRIGGER ADW_SECURITY_SCHEMA_BI 
  BEFORE INSERT ON ADW_SECURITY_SCHEMA FOR EACH ROW
    BEGIN
 		  :new.ROLE             := upper(:new.ROLE);
 		  :new.SCHEMA_NAME      := upper(:new.SCHEMA_NAME);
 		  :new.ACTIVE_IND       := upper(:new.ACTIVE_IND);
 		  
          :new.ROW_CREATE_USER  := sys_context( 'userenv', 'os_user' );
          :new.ROW_CREATE_DATE  := sysdate;
         
          :new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE  := sysdate;
    END;
/
CREATE OR REPLACE TRIGGER ADW_SECURITY_SCHEMA_BU 
  BEFORE UPDATE ON ADW_SECURITY_SCHEMA FOR EACH ROW
    BEGIN
 		  :new.ROLE             := upper(:new.ROLE);
 		  :new.SCHEMA_NAME      := upper(:new.SCHEMA_NAME);
 		  :new.ACTIVE_IND       := upper(:new.ACTIVE_IND);
		  
		  
          :new.ROW_CREATE_USER  := :old.ROW_CREATE_USER ;
          :new.ROW_CREATE_DATE  := :old.ROW_CREATE_DATE ;
         
          :new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE  := sysdate;
    END;
/

CREATE TABLE ADW_SECURITY_OBJECT
(
  ROLE                  VARCHAR2(32)         NOT NULL,
  PRIV                  VARCHAR2(32)         DEFAULT 'SELECT',
  OBJECT_SPEC           VARCHAR2(200)        NOT NULL,
  REMARK                VARCHAR2(2000),
  ACTIVE_IND            VARCHAR2(1)          DEFAULT 'Y',
  ROW_CREATE_USER       VARCHAR2(40)         DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE       DATE                 DEFAULT sysdate,
  ROW_MODIFY_USER       VARCHAR2(40),
  ROW_MODIFY_DATE       DATE,
  CONSTRAINT ADW_SECURITY_OBJECT_PK          PRIMARY KEY (ROLE,PRIV,OBJECT_SPEC),
  CONSTRAINT ADW_SECURITY_OBJECT_AC_CHK      CHECK       (ACTIVE_IND IN ('Y','N')),
  CONSTRAINT ADW_SECURITY_OBJECT_PR_CHK      CHECK       (PRIV IN ('SELECT','DELETE','INSERT','UPDATE','EXECUTE')),
  CONSTRAINT ADW_SECURITY_OBJECT_FK01        FOREIGN KEY (ROLE)
                                             REFERENCES   ADW_SECURITY_ROLE(ROLE)
);

COMMENT ON COLUMN ADW_SECURITY_OBJECT.ROLE             is 'ROLE Name';
COMMENT ON COLUMN ADW_SECURITY_OBJECT.PRIV             is 'ROLE Priviledge to grant';
COMMENT ON COLUMN ADW_SECURITY_OBJECT.OBJECT_SPEC      is 'Object specification';
COMMENT ON COLUMN ADW_SECURITY_OBJECT.REMARK           is 'Object Remark';
COMMENT ON COLUMN ADW_SECURITY_OBJECT.ACTIVE_IND       is 'Record Active indicator';
COMMENT ON COLUMN ADW_SECURITY_OBJECT.ROW_CREATE_USER  is 'User who created row';
COMMENT ON COLUMN ADW_SECURITY_OBJECT.ROW_CREATE_DATE  is 'Date Row Created';
COMMENT ON COLUMN ADW_SECURITY_OBJECT.ROW_MODIFY_USER  is 'User who modified row';
COMMENT ON COLUMN ADW_SECURITY_OBJECT.ROW_MODIFY_DATE  is 'Date Row modified';



CREATE OR REPLACE TRIGGER ADW_SECURITY_OBJECT_BI 
  BEFORE INSERT ON ADW_SECURITY_OBJECT FOR EACH ROW
    BEGIN
 		  :new.ROLE             := upper(:new.ROLE);
 		  :new.PRIV             := upper(:new.PRIV);
 		  :new.OBJECT_SPEC      := upper(:new.OBJECT_SPEC);
 		  :new.ACTIVE_IND       := upper(:new.ACTIVE_IND);
		  
          :new.ROW_CREATE_USER  := sys_context( 'userenv', 'os_user' );
          :new.ROW_CREATE_DATE  := sysdate;
         
          :new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE  := sysdate;
    END;
/
CREATE OR REPLACE TRIGGER ADW_SECURITY_OBJECT_BU 
  BEFORE UPDATE ON ADW_SECURITY_OBJECT FOR EACH ROW
    BEGIN
 		  :new.ROLE             := upper(:new.ROLE);
 		  :new.PRIV             := upper(:new.PRIV);
 		  :new.OBJECT_SPEC      := upper(:new.OBJECT_SPEC);
 		  :new.ACTIVE_IND       := upper(:new.ACTIVE_IND);
		  
          :new.ROW_CREATE_USER  := :old.ROW_CREATE_USER ;
          :new.ROW_CREATE_DATE  := :old.ROW_CREATE_DATE ;
         
          :new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE  := sysdate;
    END;
/

PROMPT   --------------------------------------------------------------------------------
PROMPT   -- ADW_ETL

CREATE TABLE ADW_ETL_STAGE
(
  ETL_STAGE_ID        NUMBER               GENERATED ALWAYS AS IDENTITY,
  APP_ID              VARCHAR2(4)          NOT NULL,
  ETL_GROUP           VARCHAR2(32),
  ETL_SEQ             NUMBER(22)           DEFAULT 0,
  ETL_TYPE            VARCHAR2(40)         NOT NULL,
  ETL_TYPE_PARM       VARCHAR2(256),
  ETL_COMMIT_SIZE	  NUMBER(22)           DEFAULT 100000,
  ETL_SRC_SCHEMA      VARCHAR2(256)        DEFAULT 'ADWADMIN',
  ETL_SRC_TABLE       VARCHAR2(256)        NOT NULL,
  ADW_TABLE           VARCHAR2(128)        NOT NULL,
  CONVERT_UPPER       VARCHAR2(1)          DEFAULT 'Y',
  STAGE_INDEX         VARCHAR2(256),
  STAGE_CREATE_SQL    CLOB,
  STAGE_SELECT_SQL    CLOB,
  STAGE_INSERT_SQL    CLOB,
  RECORD_COUNT_IND    VARCHAR2(1)          DEFAULT 'Y',
  VALIDATE_IND        VARCHAR2(1)          DEFAULT 'Y',
  MIN_RECORDS         NUMBER(22),
  MAX_RECORDS         NUMBER(22),
  ACTIVE_IND          VARCHAR2(1)          DEFAULT 'Y',
  ROW_CREATE_USER     VARCHAR2(40)         DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE     DATE                 DEFAULT sysdate,
  ROW_MODIFY_USER     VARCHAR2(40),
  ROW_MODIFY_DATE     DATE,
  CONSTRAINT ADW_ETL_STAGE_PK              PRIMARY KEY (ETL_STAGE_ID),
  CONSTRAINT ADW_ETL_STAGE_ELT_TYPE_CHK    CHECK       (ETL_TYPE IN ('DBINSERT','DBCOPY','LOCAL','MSINSERT','BCPTRANSFER')),
  CONSTRAINT ADW_ETL_STAGE_CONUP_CHK      CHECK        (CONVERT_UPPER    IN ('Y','N')),
  CONSTRAINT ADW_ETL_STAGE_ACTIND_CHK      CHECK       (ACTIVE_IND       IN ('Y','N')),
  CONSTRAINT ADW_ETL_STAGE_VALIND_CHK      CHECK       (VALIDATE_IND     IN ('Y','N')),
  CONSTRAINT ADW_ETL_STAGE_CNTIND_CHK      CHECK       (RECORD_COUNT_IND IN ('Y','N')),
  CONSTRAINT ADW_ETL_STAGE_APP_FK01        FOREIGN KEY (APP_ID)
                                           REFERENCES   ADW_APPLICATION(APP_ID)
);

COMMENT ON COLUMN ADW_ETL_STAGE.ETL_STAGE_ID     is 'Unique stage ID';
COMMENT ON COLUMN ADW_ETL_STAGE.APP_ID           is 'Application prefix';
COMMENT ON COLUMN ADW_ETL_STAGE.ETL_GROUP        is 'Group that ETL belongs to';
COMMENT ON COLUMN ADW_ETL_STAGE.ETL_SEQ          is 'Sequence order in application or group ETL';
COMMENT ON COLUMN ADW_ETL_STAGE.ETL_TYPE         is 'Type of ETL';
COMMENT ON COLUMN ADW_ETL_STAGE.ETL_TYPE_PARM    is 'Parameter to support ETL Type';
COMMENT ON COLUMN ADW_ETL_STAGE.ETL_COMMIT_SIZE  is 'Number of records on commit';
COMMENT ON COLUMN ADW_ETL_STAGE.ADW_TABLE        is 'ADW Table name unique to application';
COMMENT ON COLUMN ADW_ETL_STAGE.CONVERT_UPPER    is 'Convert column names to upper case';
COMMENT ON COLUMN ADW_ETL_STAGE.STAGE_INDEX      is 'List of table columns to index';
COMMENT ON COLUMN ADW_ETL_STAGE.STAGE_CREATE_SQL is 'Stage Table Create SQL (optional)';
COMMENT ON COLUMN ADW_ETL_STAGE.STAGE_SELECT_SQL is 'Stage Table Select SQL (optional for MS/SQL)';
COMMENT ON COLUMN ADW_ETL_STAGE.STAGE_INSERT_SQL is 'Stage Table Insert SQL (optional)';
COMMENT ON COLUMN ADW_ETL_STAGE.RECORD_COUNT_IND is 'Count number of records on ETL';
COMMENT ON COLUMN ADW_ETL_STAGE.VALIDATE_IND     is 'validation required indicator';
COMMENT ON COLUMN ADW_ETL_STAGE.MIN_RECORDS      is 'on validation min number of records in ETL';
COMMENT ON COLUMN ADW_ETL_STAGE.MAX_RECORDS      is 'on validation max number of records in ETL';
COMMENT ON COLUMN ADW_ETL_STAGE.ACTIVE_IND       is 'record Active indicator';
COMMENT ON COLUMN ADW_ETL_STAGE.ROW_CREATE_USER  is 'User who created row';
COMMENT ON COLUMN ADW_ETL_STAGE.ROW_CREATE_DATE  is 'Date Row Created';
COMMENT ON COLUMN ADW_ETL_STAGE.ROW_MODIFY_USER  is 'User who modified row';
COMMENT ON COLUMN ADW_ETL_STAGE.ROW_MODIFY_DATE  is 'Date Row modified';


CREATE OR REPLACE TRIGGER ADW_ETL_STAGE_BI 
  BEFORE INSERT ON ADW_ETL_STAGE FOR EACH ROW
  
BEGIN

	:new.APP_ID           := upper(:new.APP_ID);
	:new.ETL_GROUP        := upper(:new.ETL_GROUP);

	if :new.ADW_TABLE is null then
		:new.ADW_TABLE := upper(SUBSTR(:new.ETL_SRC_TABLE,1,128-(LENGTH(:new.APP_ID)+3)));
	end if;
	:new.ADW_TABLE        := upper(SUBSTR(:new.ADW_TABLE,1,128-(LENGTH(:new.APP_ID)+3)));

	:new.CONVERT_UPPER    := upper(:new.CONVERT_UPPER);
	
	if :new.CONVERT_UPPER  = 'Y' THEN
		:new.ADW_TABLE := replace(:new.ADW_TABLE,' ','_');
	END IF;
	
	:new.RECORD_COUNT_IND := upper(:new.RECORD_COUNT_IND);
	:new.VALIDATE_IND     := upper(:new.VALIDATE_IND);
	:new.ACTIVE_IND       := upper(:new.ACTIVE_IND);

	:new.ROW_CREATE_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_CREATE_DATE  := sysdate;

	:new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_MODIFY_DATE  := sysdate;
END;
/
CREATE OR REPLACE TRIGGER ADW_ETL_STAGE_BU 
  BEFORE UPDATE ON ADW_ETL_STAGE FOR EACH ROW
BEGIN
	:new.APP_ID           := upper(:new.APP_ID);
	:new.ETL_GROUP        := upper(:new.ETL_GROUP);

	if :new.ADW_TABLE is null then
		:new.ADW_TABLE := upper(SUBSTR(:new.ETL_SRC_TABLE,1,128-(LENGTH(:new.APP_ID)+3)));
	end if;
	:new.ADW_TABLE        := upper(SUBSTR(:new.ADW_TABLE,1,128-(LENGTH(:new.APP_ID)+3)));

	:new.CONVERT_UPPER    := upper(:new.CONVERT_UPPER);

	if :new.CONVERT_UPPER  = 'Y' THEN
		:new.ADW_TABLE := replace(:new.ADW_TABLE,' ','_');
	END IF;
	
	:new.RECORD_COUNT_IND := upper(:new.RECORD_COUNT_IND);
	:new.VALIDATE_IND     := upper(:new.VALIDATE_IND);
	:new.ACTIVE_IND       := upper(:new.ACTIVE_IND);
	
	:new.ROW_CREATE_USER  := :old.ROW_CREATE_USER ;
	:new.ROW_CREATE_DATE  := :old.ROW_CREATE_DATE ;

	:new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_MODIFY_DATE  := sysdate;
END;
/

PROMPT   --------------------------------------------------------------------------------
PROMPT   -- ADW_ETL

CREATE TABLE ADW_ETL_LOG
(
  ETL_LOG_ID          NUMBER(22)          NOT NULL,
  ETL_STAGE_ID        NUMBER(22)          NOT NULL,
  ETL_LOG_GROUP_ID    NUMBER(22)          NOT NULL,
  ETL_START_DATE      DATE                DEFAULT sysdate,
  ETL_END_DATE        DATE,
  ETL_MESSAGE         VARCHAR2(2000)      DEFAULT 'Build ',
  ETL_NO_ROWS         NUMBER(22)          DEFAULT -1,
  ROW_CREATE_USER     VARCHAR2(40)        DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE     DATE                DEFAULT sysdate,
  ROW_MODIFY_USER     VARCHAR2(40),
  ROW_MODIFY_DATE     DATE,
  CONSTRAINT ADW_ETL_LOG_PK         PRIMARY KEY (ETL_LOG_ID),
  CONSTRAINT ADW_ETL_LOG_STAGE_FK01 FOREIGN KEY (ETL_STAGE_ID)
                                     REFERENCES   ADW_ETL_STAGE(ETL_STAGE_ID)
);

COMMENT ON COLUMN ADW_ETL_LOG.ETL_LOG_ID            is 'ETL Log ID';
COMMENT ON COLUMN ADW_ETL_LOG.ETL_STAGE_ID          is 'ETL Stage ID';
COMMENT ON COLUMN ADW_ETL_LOG.ETL_LOG_GROUP_ID      is 'ETL Log Grouping';
COMMENT ON COLUMN ADW_ETL_LOG.ETL_START_DATE        is 'Date/Time ETL Started';
COMMENT ON COLUMN ADW_ETL_LOG.ETL_END_DATE          is 'Date/Time ETL Completed';
COMMENT ON COLUMN ADW_ETL_LOG.ETL_MESSAGE           is 'ETL Result message';
COMMENT ON COLUMN ADW_ETL_LOG.ETL_NO_ROWS           is 'Number of rows Transferred';
COMMENT ON COLUMN ADW_ETL_LOG.ROW_CREATE_USER       is 'User who created row';
COMMENT ON COLUMN ADW_ETL_LOG.ROW_CREATE_DATE       is 'Date Row Created';
COMMENT ON COLUMN ADW_ETL_LOG.ROW_MODIFY_USER       is 'User who modified row';
COMMENT ON COLUMN ADW_ETL_LOG.ROW_MODIFY_DATE       is 'Date Row modified';


CREATE OR REPLACE TRIGGER ADW_ETL_LOG_BI 
  BEFORE INSERT ON ADW_ETL_LOG FOR EACH ROW
BEGIN
	if :new.ETL_LOG_ID is null then
		select ADW_ETL_LOG_SEQ.nextval into :new.ETL_LOG_ID from dual;
	end if;
	
	:new.ROW_CREATE_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_CREATE_DATE  := sysdate;

	:new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_MODIFY_DATE  := sysdate;
END;
/
CREATE OR REPLACE TRIGGER ADW_ETL_LOG_BU 
  BEFORE UPDATE ON ADW_ETL_LOG FOR EACH ROW
BEGIN
	
	:new.ROW_CREATE_USER  := :old.ROW_CREATE_USER ;
	:new.ROW_CREATE_DATE  := :old.ROW_CREATE_DATE ;

	:new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
	:new.ROW_MODIFY_DATE  := sysdate;
END;
/

PROMPT   --------------------------------------------------------------------------------
PROMPT   -- ADW_NOTIFY_LIST

CREATE TABLE ADW_NOTIFY_LIST
(
  NOTIFY_NAME                   VARCHAR2(40)            NOT NULL,
  REPORT_NAME                   VARCHAR2(40),
  GROUP_NAME                    VARCHAR2(40),
  EVENT_TEST                    CLOB,
  EVENT_PASS_RESPONSE           CLOB,
  EVENT_FAIL_RESPONSE           CLOB,
  EVENT_COMPLETE_SQL            CLOB,
  EVENT_LAST_EXECUTED           DATE,
  ACTIVE_IND                    VARCHAR2(1)            DEFAULT 'Y',
  ROW_CREATE_USER               VARCHAR2(40)           DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE               DATE                   DEFAULT sysdate,
  ROW_MODIFY_USER               VARCHAR2(40),
  ROW_MODIFY_DATE               DATE,
  CONSTRAINT ADW_NOTIFY_LIST_PK       PRIMARY KEY (NOTIFY_NAME),  
  CONSTRAINT ADW_NOTIFY_LIST_ACT_CHK  CHECK (ACTIVE_IND IN ('Y','N'))
);

COMMENT ON COLUMN ADW_NOTIFY_LIST.NOTIFY_NAME         IS 'Name of notification';
COMMENT ON COLUMN ADW_NOTIFY_LIST.REPORT_NAME         IS 'what Report mail to use';
COMMENT ON COLUMN ADW_NOTIFY_LIST.GROUP_NAME          IS 'Group notification belongs to';
COMMENT ON COLUMN ADW_NOTIFY_LIST.EVENT_TEST          IS 'SQL for Event Test';
COMMENT ON COLUMN ADW_NOTIFY_LIST.EVENT_PASS_RESPONSE IS 'If rows from EVENT_TEST sql use this response message';
COMMENT ON COLUMN ADW_NOTIFY_LIST.EVENT_FAIL_RESPONSE IS 'If NO rows from EVENT_TEST sql use this response message';
COMMENT ON COLUMN ADW_NOTIFY_LIST.EVENT_LAST_EXECUTED IS 'Last time this notification was eceuted';
COMMENT ON COLUMN ADW_NOTIFY_LIST.EVENT_COMPLETE_SQL  IS 'SQL to execute on completion of successful event test';
COMMENT ON COLUMN ADW_NOTIFY_LIST.ACTIVE_IND          IS 'A Y/N flag indicating whether this row of data is currently active or valid';
COMMENT ON COLUMN ADW_NOTIFY_LIST.ROW_CREATE_USER     IS 'System user who created this row of data.';
COMMENT ON COLUMN ADW_NOTIFY_LIST.ROW_CREATE_DATE     IS 'Date that the row was created on.';
COMMENT ON COLUMN ADW_NOTIFY_LIST.ROW_MODIFY_USER     IS 'Application login id of the user who last changed the row';
COMMENT ON COLUMN ADW_NOTIFY_LIST.ROW_MODIFY_DATE     IS 'System date of the last time the row was changed.';

CREATE OR REPLACE TRIGGER ADW_NOTIFY_LIST_BI
  BEFORE INSERT ON ADW_NOTIFY_LIST FOR EACH ROW
BEGIN
          :new.NOTIFY_NAME      := upper(:new.NOTIFY_NAME);
          :new.REPORT_NAME      := nvl(upper(:new.REPORT_NAME),:new.NOTIFY_NAME);
          :new.GROUP_NAME       := upper(:new.GROUP_NAME);
          :new.ACTIVE_IND       := upper(:new.ACTIVE_IND);

          :new.ROW_CREATE_USER  := sys_context( 'userenv', 'os_user' );
          :new.ROW_CREATE_DATE  := sysdate;
        END;
/
CREATE OR REPLACE TRIGGER ADW_NOTIFY_LIST_BU
  BEFORE UPDATE ON ADW_NOTIFY_LIST FOR EACH ROW
BEGIN
          :new.NOTIFY_NAME      := upper(:new.NOTIFY_NAME);
          :new.REPORT_NAME      := nvl(upper(:new.REPORT_NAME),:new.NOTIFY_NAME);
          :new.GROUP_NAME       := upper(:new.GROUP_NAME);
          :new.ACTIVE_IND       := upper(:new.ACTIVE_IND);

          :new.ROW_CREATE_USER  := :old.ROW_CREATE_USER;
          :new.ROW_CREATE_DATE  := :old.ROW_CREATE_DATE;
          
          :new.ROW_MODIFY_USER  := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE  := sysdate;
        END;
/


PROMPT   --------------------------------------------------------------------------------'
PROMPT   -- ADW_MAIL

CREATE TABLE ADW_MAIL_USER
(
  USER_NAME         VARCHAR2(40)           NOT NULL,
  USER_FULL_NAME    VARCHAR2(80),
  USER_ROLE         VARCHAR2(60),
  E_MAIL_ADDRESS    VARCHAR2(200)          NOT NULL,
  ACTIVE_IND        VARCHAR2(1)            DEFAULT 'Y',
  ROW_CREATE_USER   VARCHAR2(40)           DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE   DATE                   DEFAULT sysdate,
  ROW_MODIFY_USER   VARCHAR2(40),
  ROW_MODIFY_DATE   DATE,
  CONSTRAINT ADW_MAIL_USER_PK           PRIMARY KEY (USER_NAME),  
  CONSTRAINT ADW_MAIL_USER_ACT_CHK   CHECK       (ACTIVE_IND IN ('Y','N'))
);

COMMENT ON COLUMN ADW_MAIL_USER.USER_NAME       is 'Unique name of User to be referenced as in system';
COMMENT ON COLUMN ADW_MAIL_USER.USER_FULL_NAME  is 'User full name';
COMMENT ON COLUMN ADW_MAIL_USER.USER_ROLE       is 'Role of user (Future)';
COMMENT ON COLUMN ADW_MAIL_USER.E_MAIL_ADDRESS  is 'Users e-mail address ';
COMMENT ON COLUMN ADW_MAIL_USER.ACTIVE_IND      is 'If this user is currently active';
COMMENT ON COLUMN ADW_MAIL_USER.ROW_CREATE_USER is 'User who created row';
COMMENT ON COLUMN ADW_MAIL_USER.ROW_CREATE_DATE is 'Date Row Created';
COMMENT ON COLUMN ADW_MAIL_USER.ROW_MODIFY_USER is 'User who modified row';
COMMENT ON COLUMN ADW_MAIL_USER.ROW_MODIFY_DATE is 'Date Row modified';


CREATE OR REPLACE TRIGGER ADW_MAIL_USER_BI 
  BEFORE INSERT ON ADW_MAIL_USER FOR EACH ROW
    BEGIN
          :new.USER_NAME       := upper(:new.USER_NAME);
          :new.ACTIVE_IND      := upper(:new.ACTIVE_IND);
		  
          :new.ROW_CREATE_USER := sys_context( 'userenv', 'os_user' );
          :new.ROW_CREATE_DATE := sysdate;
         
          :new.ROW_MODIFY_USER   := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE := sysdate;
    END;
/
CREATE OR REPLACE TRIGGER ADW_MAIL_USER_BU 
  BEFORE UPDATE ON ADW_MAIL_USER FOR EACH ROW
    BEGIN
          :new.USER_NAME       := upper(:new.USER_NAME);
          :new.ACTIVE_IND      := upper(:new.ACTIVE_IND);
		  
          :new.ROW_CREATE_USER := :old.ROW_CREATE_USER ;
          :new.ROW_CREATE_DATE := :old.ROW_CREATE_DATE ;
         
          :new.ROW_MODIFY_USER := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE := sysdate;
    END;
/
CREATE TABLE ADW_MAIL_GROUP
(
  REPORT_NAME      VARCHAR2(40)         NOT NULL,
  REPORT_TYPE      VARCHAR2(1)          DEFAULT 'N',
  GROUP_NAME       VARCHAR2(40)         DEFAULT 'ALL',
  SUBJECT_LINE     VARCHAR2(200),
  BODY_TEXT        VARCHAR2(4000),
  ACTIVE_IND       VARCHAR2(1)          DEFAULT 'Y',
  ROW_CREATE_USER  VARCHAR2(40)         DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE  DATE                 DEFAULT sysdate,
  ROW_MODIFY_USER  VARCHAR2(40),
  ROW_MODIFY_DATE  DATE,
  CONSTRAINT ADW_MAIL_GROUP_PK        PRIMARY KEY (REPORT_NAME,GROUP_NAME,REPORT_TYPE),
  CONSTRAINT ADW_MAIL_GROUP_ACT_CHK   CHECK       (ACTIVE_IND IN ('Y','N')),
  CONSTRAINT ADW_MAIL_GROUP_TYP_CHK   CHECK       (REPORT_TYPE IN ('N','E'))
);
COMMENT ON COLUMN ADW_MAIL_GROUP.REPORT_NAME     is 'Report Name';
COMMENT ON COLUMN ADW_MAIL_GROUP.REPORT_TYPE     is 'Report Type (E- Errored or N- Normal)';
COMMENT ON COLUMN ADW_MAIL_GROUP.GROUP_NAME      is 'Group to mail process results to';
COMMENT ON COLUMN ADW_MAIL_GROUP.SUBJECT_LINE    is 'Subject line for message';
COMMENT ON COLUMN ADW_MAIL_GROUP.BODY_TEXT       is 'Body of message';
COMMENT ON COLUMN ADW_MAIL_GROUP.ACTIVE_IND      is 'If this message notify is currently active';
COMMENT ON COLUMN ADW_MAIL_GROUP.ROW_CREATE_USER is 'User who created row';
COMMENT ON COLUMN ADW_MAIL_GROUP.ROW_CREATE_DATE is 'Date Row Created';
COMMENT ON COLUMN ADW_MAIL_GROUP.ROW_MODIFY_USER is 'User who modified row';
COMMENT ON COLUMN ADW_MAIL_GROUP.ROW_MODIFY_DATE is 'Date Row modified';

CREATE OR REPLACE TRIGGER ADW_MAIL_GROUP_BI 
  BEFORE INSERT ON ADW_MAIL_GROUP FOR EACH ROW
    BEGIN
          :new.REPORT_NAME    := upper(:new.REPORT_NAME);
          :new.GROUP_NAME     := upper(:new.GROUP_NAME);
          :new.REPORT_TYPE    := upper(:new.REPORT_TYPE);
          :new.ACTIVE_IND     := upper(:new.ACTIVE_IND);
		  
          :new.ROW_CREATE_USER := sys_context( 'userenv', 'os_user' );
          :new.ROW_CREATE_DATE := sysdate;
         
          :new.ROW_MODIFY_USER := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE := sysdate;
    END;
/
CREATE OR REPLACE TRIGGER ADW_MAIL_GROUPS_BU 
  BEFORE UPDATE ON ADW_MAIL_GROUP FOR EACH ROW
    BEGIN
          :new.REPORT_NAME    := upper(:new.REPORT_NAME);
          :new.GROUP_NAME     := upper(:new.GROUP_NAME);
          :new.REPORT_TYPE    := upper(:new.REPORT_TYPE);
          :new.ACTIVE_IND     := upper(:new.ACTIVE_IND);

          :new.ROW_CREATE_USER := :old.ROW_CREATE_USER ;
          :new.ROW_CREATE_DATE := :old.ROW_CREATE_DATE ;
         
          :new.ROW_MODIFY_USER := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE := sysdate;
    END;
/
CREATE TABLE ADW_MAIL_GROUP_USER
(
  GROUP_NAME       VARCHAR2(40)         NOT NULL,
  USER_NAME        VARCHAR2(40)         NOT NULL,
  ACTIVE_IND       VARCHAR2(1)          DEFAULT 'Y',
  ROW_CREATE_USER  VARCHAR2(40)         DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE  DATE                 DEFAULT sysdate,
  ROW_MODIFY_USER  VARCHAR2(40),
  ROW_MODIFY_DATE  DATE,
  CONSTRAINT ADW_MAIL_GROUP_USER_PK        PRIMARY KEY (GROUP_NAME,USER_NAME),
  CONSTRAINT ADW_MAIL_GROUP_USER_ACT_CHK   CHECK       (ACTIVE_IND IN ('Y','N'))
);
COMMENT ON COLUMN ADW_MAIL_GROUP_USER.GROUP_NAME    is 'Report Name';
COMMENT ON COLUMN ADW_MAIL_GROUP_USER.USER_NAME       is 'Report Type (E- Errored or N- Normal)';
COMMENT ON COLUMN ADW_MAIL_GROUP_USER.ACTIVE_IND      is 'If this message notify is currently active';
COMMENT ON COLUMN ADW_MAIL_GROUP_USER.ROW_CREATE_USER is 'User who created row';
COMMENT ON COLUMN ADW_MAIL_GROUP_USER.ROW_CREATE_DATE is 'Date Row Created';
COMMENT ON COLUMN ADW_MAIL_GROUP_USER.ROW_MODIFY_USER is 'User who modified row';
COMMENT ON COLUMN ADW_MAIL_GROUP_USER.ROW_MODIFY_DATE is 'Date Row modified';

CREATE OR REPLACE TRIGGER ADW_MAIL_GROUP_USER_BI 
  BEFORE INSERT ON ADW_MAIL_GROUP_USER FOR EACH ROW
    BEGIN
          :new.GROUP_NAME      := upper(:new.GROUP_NAME);
          :new.USER_NAME       := upper(:new.USER_NAME);
          :new.ACTIVE_IND      := upper(:new.ACTIVE_IND);
		  
          :new.ROW_CREATE_USER := sys_context( 'userenv', 'os_user' );
          :new.ROW_CREATE_DATE := sysdate;
         
          :new.ROW_MODIFY_USER := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE := sysdate;
    END;
/
CREATE OR REPLACE TRIGGER ADW_MAIL_GROUP_USER_BU 
  BEFORE UPDATE ON ADW_MAIL_GROUP_USER FOR EACH ROW
    BEGIN
          :new.GROUP_NAME      := upper(:new.GROUP_NAME);
          :new.USER_NAME       := upper(:new.USER_NAME);
          :new.ACTIVE_IND      := upper(:new.ACTIVE_IND);

          :new.ROW_CREATE_USER := :old.ROW_CREATE_USER ;
          :new.ROW_CREATE_DATE := :old.ROW_CREATE_DATE ;
         
          :new.ROW_MODIFY_USER := sys_context( 'userenv', 'os_user' );
          :new.ROW_MODIFY_DATE := sysdate;
    END;
/
CREATE TABLE ADW_MAIL_SEND
(
  MAIL_ID	        NUMBER              GENERATED ALWAYS AS IDENTITY,
  SEND_TO           VARCHAR2(256),
  SENT_FROM         VARCHAR2(256),
  SUBJECT_LINE      VARCHAR2(256),
  BODY_TEXT         CLOB,
  ROW_CREATE_USER   VARCHAR2(40)        DEFAULT sys_context( 'userenv', 'os_user' ),
  ROW_CREATE_DATE   DATE                DEFAULT sysdate,
  CONSTRAINT ADW_MAIL_SEND_PK           PRIMARY KEY (MAIL_ID)
);

COMMENT ON COLUMN ADW_MAIL_SEND.MAIL_ID         is 'Unique name of User to be referenced as in system';
COMMENT ON COLUMN ADW_MAIL_SEND.SEND_TO         is 'User full name';
COMMENT ON COLUMN ADW_MAIL_SEND.SENT_FROM       is 'Role of user (Future)';
COMMENT ON COLUMN ADW_MAIL_SEND.BODY_TEXT       is 'Users e-mail address ';
COMMENT ON COLUMN ADW_MAIL_SEND.ROW_CREATE_USER is 'User who created row';
COMMENT ON COLUMN ADW_MAIL_SEND.ROW_CREATE_DATE is 'Date Row Created';

