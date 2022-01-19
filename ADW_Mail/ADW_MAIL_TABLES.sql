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
CREATE OR REPLACE TRIGGER ADW_MAIL_GROUP_BU 
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
