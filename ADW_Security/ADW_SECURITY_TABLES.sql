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
