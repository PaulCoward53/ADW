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

INSERT INTO ADW_APPLICATION (APP_ID, DATA_OWNER, DATA_STEWARD, DATA_CUSTODIAN, DESCRIPTION) VALUES ('ADW', 'Paul Coward', 'Paul Coward', 'Paul Coward', 'Application Data Warehouse')
