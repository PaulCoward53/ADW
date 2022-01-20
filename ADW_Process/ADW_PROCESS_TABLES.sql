
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

