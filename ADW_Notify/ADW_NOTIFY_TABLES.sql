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

