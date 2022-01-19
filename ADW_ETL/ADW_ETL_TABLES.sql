

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
