#
#  Author: Paul Coward
#  Purpose: To Transfer MS/SQL Data to a Oracle ADW server
# 
#  Date:  Jan 5,2022 
#
#  Modifications
#    Revision     Author          Date            Description
#    1			  Paul Coward	  Jan 5,2022      Initial Version
#


import argparse
import os
import io
import configparser

import base64

import sys 
sys.path.insert(0, 'C:\ADW\pythonLib')  # path for the python libraries it required

from OracleConnect import OracleConnect  
from MSSQLConnect  import MSSQLConnect   
from ADWUtility    import RunCommand,SQLConvertNameUpper,GetPrintTime

import argparse
import os
import cx_Oracle
import pyodbc
import datetime
import tempfile
import ntpath
import shutil
import re
import time

global ADWService
global ADWServiceCur

global SQLCon
global SQLCur

global g_work_dir

global g_ColumnName
global g_OracleColumnType
global g_SelectColumn

global g_etl_log_groupID
global g_etl_log_ID

global g_StageTableName
global g_ProdTableName
global g_BackupTableName

global g_etl_stage_ID
global g_app_ID
global g_etl_group
global g_etl_seq
global g_etl_type
global g_etl_type_parm
global g_etl_commit_size
global g_etl_src_schema
global g_etl_src_table
global g_convert_upper
global g_adw_Table
global g_stage_index
global g_stage_create_sql
global g_stage_select_sql
global g_stage_insert_sql
global g_record_count_ind
global g_validate_ind
global g_min_records
global g_max_records

global g_table_count
global g_total_records
global g_record_count

global g_LastTime


###################################################################################################
# Create Stage Tables

def stageCreate():
    global ADWService
    global ADWServiceCur

    global g_ColumnName
    global g_OracleColumnType
    global g_SelectColumn
 
    global g_convert_upper
    global g_validate_ind

    global g_etl_log_ID
    global g_etl_log_groupID

    global g_etl_src_schema
    global g_etl_src_table

    global g_stage_create_sql
    global g_StageTableName

    ADWServiceCur.callproc('ADW_UTILITY.REMOVE_TABLE', [g_StageTableName])

    #
    # -------------------------------------------------------------------------------------------------------------------------------------
    #
    # We need to have a column names to transfer
    #

    sqlstr = "SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION " \
             "  FROM INFORMATION_SCHEMA.columns " \
             "WHERE TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' "\
             "ORDER BY ORDINAL_POSITION"  % (g_etl_src_schema,g_etl_src_table)
    TableColResult = SQLCur.execute(sqlstr).fetchall()
    g_ColumnName = []
    g_SelectColumn=[]
    g_OracleColumnType = []
    for r_ColumnName,r_ColumnType,r_CharMaxLen,r_NumPrec,r_NumScale,r_DateTimePrec in TableColResult:

#            print ( g_etl_src_table,r_ColumnName,r_ColumnType.upper())
            columnType = r_ColumnType.upper()
            if columnType == 'IMAGE':
                continue
            #end if

            g_ColumnName.append(r_ColumnName)

            if    columnType == 'CHAR'\
               or columnType == 'VARCHAR' \
               or columnType == 'NCHAR' \
               or columnType == 'NVARCHAR' \
               or columnType == 'TEXT':
                if r_CharMaxLen > 32672:
                    g_OracleColumnType.append('CLOB')
                else:
                    g_OracleColumnType.append('VARCHAR2(%d)' %(r_CharMaxLen))
                #end if
                g_SelectColumn.append('ltrim(rtrim("%s")) [%s]' %(r_ColumnName,r_ColumnName))
            
            elif columnType == 'UNIQUEIDENTIFIER':
                g_OracleColumnType.append('VARCHAR2(36)')

            elif columnType == 'NTEXT':
                if r_CharMaxLen > 32672:
                    g_OracleColumnType.append('CLOB')
                    g_SelectColumn.append('convert(varchar(8000),[%s])' %(r_ColumnName))
                else:
                    g_OracleColumnType.append('VARCHAR2(%d)' %(r_CharMaxLen))
                    g_SelectColumn.append('convert(varchar(%s),[%s])' %(r_CharMaxLen,r_ColumnName))
                #end if
            elif columnType == 'INT'\
              or columnType == 'TINYINT'\
              or columnType == 'BIGINT'\
              or columnType == 'SMALLINT':
                g_OracleColumnType.append('NUMBER(%d)' %(r_NumPrec))
                g_SelectColumn.append('[%s]' %(r_ColumnName))

            elif columnType == 'FLOAT'\
              or columnType == 'BINARY_DOUBLE'\
              or columnType == 'REAL':
                g_OracleColumnType.append('FLOAT')
                g_SelectColumn.append('[%s]' %(r_ColumnName))

            elif columnType == 'DATE'\
              or columnType == 'DATETIME'\
              or columnType == 'DATETIME2'\
              or columnType == 'TIMESTAMP':
                g_OracleColumnType.append('DATE')
                g_SelectColumn.append(' CONVERT(VARCHAR, [%s], 120)' %(r_ColumnName))

            elif columnType == 'MONEY'\
              or columnType == 'SMALLMONEY'\
              or columnType == 'NUMERIC'\
              or columnType == 'DECIMAL':
                g_OracleColumnType.append('NUMBER(%d,%d)' %(r_NumPrec,r_NumScale))
                g_SelectColumn.append('[%s]' %(r_ColumnName))

            elif columnType == 'BIT':
                g_OracleColumnType.append('NUMBER(1)')
                g_SelectColumn.append('[%s]' %(r_ColumnName))

#           Skip these data types

            elif columnType == 'IMAGE'\
              or columnType == 'GEOGRAPHY':
                g_ColumnName.pop()

#           Report error based on validate flag keep going
            else:
                print ( r_ColumnName,r_ColumnType.upper(),r_CharMaxLen,r_NumPrec,r_NumScale,r_DateTimePrec)
                g_ColumnName.pop()

                if g_validate_ind == 'Y':
                    raise Exception(' Table %s Column %s unknown Data Type %s ' %(g_etl_src_table,r_ColumnName,columnType))
                else:
                    ADWService.processLog('   *Warning - Table %s Column %s unknown Data Type %s ' %(g_etl_src_table,r_ColumnName,columnType))
                #end if
            #end if

    #END FOR


    if len(g_ColumnName) < 1:
        raise Exception(' Catalog %s Table %s can not be located ' %(g_etl_src_schema,g_etl_src_table))
    #END IF

    # Build Stage Table if not specified

    if g_stage_create_sql is None:   

        v_sqlCreate = 'CREATE TABLE "%s" (' %(g_StageTableName)
        v_sep = ' '
        for ii in range(len(g_ColumnName)):
            if g_convert_upper == 'Y':
                v_sqlCreate = '%s%s"%s" %s ' %(v_sqlCreate,v_sep,SQLConvertNameUpper(g_ColumnName[ii]),g_OracleColumnType[ii])
            else:
                v_sqlCreate = '%s%s"%s" %s ' %(v_sqlCreate,v_sep,g_ColumnName[ii],g_OracleColumnType[ii])
            v_sep = ','
        #END FOR
        v_sqlCreate = '%s,ADW_ETL_DATE DATE DEFAULT SYSDATE) ' %(v_sqlCreate)
#        print (v_sqlCreate)
        ADWServiceCur.callproc('ADW_UTILITY.SQL_EXECUTE', [v_sqlCreate])

    else:
#        print (g_stage_create_sql)
        ADWServiceCur.callproc('ADW_UTILITY.SQL_EXECUTE', [g_stage_create_sql])
    #end if
    ADWServiceCur.execute("UPDATE ADW_ETL_LOG SET ETL_MESSAGE = 'Stage Built Completed' "\
   		                  " WHERE ETL_LOG_ID = %s " %(g_etl_log_ID))
#end DEF

###################################################################################################
# Do ELT for MSInsert

def stageETLMSInsert():
    global ADWService
    global ADWServiceCur
    global SQLCon
    global SQLCur

    global g_ColumnName
    global g_SelectColumn
    global g_convert_upper
    
    global g_etl_src_schema
    global g_etl_src_table
    global g_StageTableName
    global g_stage_select_sql
    global g_stage_insert_sql

    global g_etl_log_ID
    global g_record_count_ind
    global g_record_count

    if g_stage_select_sql is None:   
        v_sqlSelect = 'SELECT '
        v_sep = ' '
        for ii in range(len(g_SelectColumn)):
            v_sqlSelect += '%s%s' %(v_sep,g_SelectColumn[ii])
            v_sep = ','
        #END FOR
        v_sqlSelect += ' FROM [%s].[%s]' %(g_etl_src_schema,g_etl_src_table)
#        print (v_sqlSelect)
    else:
        v_sqlSelect = g_stage_select_sql
    #end if

    if g_stage_insert_sql is None:   
        v_sqlInsert = 'INSERT INTO "%s" (' %(g_StageTableName)
        v_sqlValues = ') VALUES ('
        v_sep = ' '
        for ii in range(len(g_ColumnName)):
            if g_convert_upper == 'Y':
                v_sqlInsert += '%s"%s"' %(v_sep,SQLConvertNameUpper(g_ColumnName[ii]))
            else:
                v_sqlInsert += '%s"%s"' %(v_sep,g_ColumnName[ii])
            #end if
            if g_OracleColumnType[ii] =='DATE':
                v_sqlValues += "%sTO_DATE(:%d,'YYYY-MM-DD HH24:MI:SS')" %(v_sep,ii+1)
            else:
                v_sqlValues += '%s:%d' %(v_sep,ii+1)
            #end if
            v_sep = ','
        #END FOR
        v_sqlInsert += v_sqlValues + ")"
#        print (v_sqlInsert)
    else:
        v_sqlInsert = g_stage_insert_sql
    #end if

    v_Data = SQLCur.execute(v_sqlSelect).fetchall()
#    print (v_Data)
    ADWServiceCur.executemany(v_sqlInsert,v_Data)

    

    if g_record_count_ind == 'Y':
        ADWServiceCur.execute('select count(0) from "%s"' %(g_StageTableName))
        g_record_count, = ADWServiceCur.fetchone()

        ADWServiceCur.execute("UPDATE ADW_ETL_LOG " \
    		                  "SET ETL_MESSAGE   = 'ETL Insert to Stage Completed', "\
			                      "ETL_NO_ROWS   = %d," \
			                      "ETL_END_DATE  = sysdate "\
                              " WHERE ETL_LOG_ID = %d " %(g_record_count,g_etl_log_ID))
    else:
        ADWServiceCur.execute("UPDATE ADW_ETL_LOG "\
                              "SET ETL_MESSAGE  = 'ETL Insert to Stage Completed', "\
                                  "ETL_END_DATE = sysdate "
                            " WHERE ETL_LOG_ID = %s " %(g_etl_log_ID))
    #end if
#end DEF

###################################################################################################
# Do ELT for BCPTransfer

def stageETLBCPTransfer():
    global ADWService
    global ADWServiceCur
    global SQLCon
    global SQLCur
    global g_work_dir


    global g_ColumnName
    global g_SelectColumn
    global g_convert_upper
    
    global g_adw_Table
    global g_etl_src_schema
    global g_etl_src_table
    global g_StageTableName
    global g_stage_select_sql
    global g_stage_insert_sql

    global g_etl_log_ID
    global g_record_count_ind
    global g_record_count

    if g_stage_select_sql is None:   
        v_sqlSelect = 'SELECT '
        v_sep = ' '
        for ii in range(len(g_SelectColumn)):
            v_sqlSelect += '%s%s' %(v_sep,g_SelectColumn[ii])
            v_sep = ','
        #END FOR
        v_sqlSelect += ' FROM [%s].[%s]' %(g_etl_src_schema,g_etl_src_table)
#        print (v_sqlSelect)
    else:
        v_sqlSelect = g_stage_select_sql
    #end if

    g_record_count=0
    bcpCmd = 'BCP "%s" queryout "%s/%s.dat" -S %s -d %s -c -x -T -t"|"' %(v_sqlSelect,g_work_dir,g_adw_Table,SQLCon.Server,SQLCon.Catalog)
    for lines in RunCommand(bcpCmd):
        if 'rows copied' in str(lines):
            g_record_count = int(lines.split(' ')[0])
        #end if
    #end for

    # Now SQL Load data set into Oracle ADW..

    datFileName = "%s/%s.dat" %(g_work_dir,g_adw_Table)
    ctlFileName = "%s/%s.ctl" %(g_work_dir,g_adw_Table)
    errFileName = "%s/%s.bad" %(g_work_dir,g_adw_Table)
    logFileName = "%s/%s.log" %(g_work_dir,g_adw_Table)

    if g_stage_insert_sql is None: 
        ctlString = 'LOAD DATA INTO TABLE "%s" FIELDS TERMINATED BY "|" (' %(g_StageTableName) 
        v_sep = ' '
        for ii in range(len(g_ColumnName)):
            if g_convert_upper == 'Y':
                ctlString = '%s%s"%s" ' %(ctlString,v_sep,SQLConvertNameUpper(g_ColumnName[ii]))
            else:
                ctlString = '%s%s"%s" ' %(ctlString,v_sep,g_ColumnName[ii])
            #end if
            if g_OracleColumnType[ii] == "DATE":
                ctlString = '%s DATE "YYYY-MM-DD HH24:MI:SS" ' %(ctlString)
            #end if

            v_sep = ','
        #END FOR
        ctlString = '%s) ' %(ctlString)
    else:
        ctlString = g_stage_insert_sql
    #end if
     
    ctlfile = open(ctlFileName, "w")
    ctlfile.write(ctlString)
    ctlfile.close()

    ldrCmd = 'sqlldr %s/%s@%s data=%s control=%s log=%s bad=%s' \
     %(ADWService.UserName,ADWService.Password,ADWService.Server,datFileName,ctlFileName,logFileName,errFileName)
    
    RunCommand(ldrCmd)

    nrec=0
    logfile = open(logFileName, "r")
    for lines in logfile.readlines():
        if 'Rows successfully' in lines:
            nrec = int(lines.split('Rows')[0])
        #end if
    #end for
    logfile.close()

    # cleanup files

    if nrec != g_record_count:
        ADWService.processLog(' * Caution %d records written but only %d loaded' %(g_record_count,nrec))
        print(' * Caution %d records written but only %d loaded' %(g_record_count,nrec))
    else:
        if os.path.exists(datFileName): os.remove(datFileName)
        if os.path.exists(ctlFileName): os.remove(ctlFileName)
        if os.path.exists(errFileName): os.remove(errFileName)
        if os.path.exists(logFileName): os.remove(logFileName)
    #end if

    if g_record_count_ind == 'Y':
#        ADWServiceCur.execute('select count(0) from "%s"' %(g_StageTableName))
#        g_record_count, = ADWServiceCur.fetchone()

        ADWServiceCur.execute("UPDATE ADW_ETL_LOG " \
    		                  "SET ETL_MESSAGE   = 'ETL BCP to Stage Completed', "\
			                      "ETL_NO_ROWS   = %d," \
			                      "ETL_END_DATE  = sysdate "\
                              " WHERE ETL_LOG_ID = %d " %(g_record_count,g_etl_log_ID))
    else:
        ADWServiceCur.execute("UPDATE ADW_ETL_LOG "\
                              "SET ETL_MESSAGE  = 'ETL BCP to Stage Completed', "\
                                  "ETL_END_DATE = sysdate "
                            " WHERE ETL_LOG_ID = %s " %(g_etl_log_ID))
    #end if
#end DEF

###################################################################################################
# Build index on Stage Table

def stageIndex():
    global ADWService
    global ADWServiceCur
    
    global g_app_ID
    global g_etl_log_ID
    global g_StageTableName
    global g_stage_index
    global g_ProdTableName
    
    prefixText = '%s_%s' %(g_app_ID,g_etl_log_ID)
    ADWServiceCur.callproc('ADW_ETL.BUILD_INDEX', [g_StageTableName,g_stage_index,g_ProdTableName,prefixText])

    ADWServiceCur.execute("UPDATE ADW_ETL_LOG SET ETL_MESSAGE = 'Stage Index Completed' "\
   		                  " WHERE ETL_LOG_ID = %s " %(g_etl_log_ID))
#end DEF

###################################################################################################
# Validate Stage Table

def stageValidate():
    global ADWService
    global ADWServiceCur
    
    global g_StageTableName
    global g_ProdTableName

    global g_validate_ind
    global g_record_count_ind
    global g_min_records
    global g_max_records

    global g_etl_log_ID
    global g_record_count
    global g_total_records

    if g_validate_ind == 'Y':
        ADWServiceCur.execute("select count(0) FROM USER_TABLES WHERE TABLE_NAME = '%s'" %(g_ProdTableName))
        v_count, = ADWServiceCur.fetchone()

        if v_count > 0:
            v_ErrorCount = 0
            sqlstr ="select COLUMN_NAME,DATA_TYPE,DATA_LENGTH FROM  user_tab_cols "\
                    " WHERE (COLUMN_NAME,DATA_TYPE,DATA_LENGTH) not in "\
                    "       (SELECT p.COLUMN_NAME,p.DATA_TYPE,p.DATA_LENGTH "\
                    "          FROM  user_tab_cols p where TABLE_NAME = '%s' )"\
                    "   and TABLE_NAME = '%s'" %(g_ProdTableName,g_StageTableName)

            validationRec = ADWServiceCur.execute(sqlstr).fetchall()
            for r_column_name,r_data_type,r_data_length_etl_stage_ID in validationRec:
                eMsg = " Validate: %s Column %s is added or changed" %(g_StageTableName,r_column_name)
                ADWService.processError(eMsg)
                v_ErrorCount += 1
            #end for

            sqlstr ="select COLUMN_NAME FROM  user_tab_cols "\
                    " WHERE (COLUMN_NAME) not in "\
                    "       (SELECT p.COLUMN_NAME  FROM  user_tab_cols p where TABLE_NAME = '%s' )"\
                    "   and TABLE_NAME = '%s'" %(g_StageTableName,g_ProdTableName)

            validationRec = ADWServiceCur.execute(sqlstr).fetchall()
            for r_column_name in validationRec:
                eMsg = " Validate: %s Column %s was removed" %(g_StageTableName,r_column_name)
                ADWService.processError(eMsg)
                v_ErrorCount += 1
            #end for
            
            if v_ErrorCount > 0:
                ADWServiceCur.execute("UPDATE ADW_ETL_LOG " \
                            "SET ETL_MESSAGE   = 'Stage Validated Failed Field Differences'"\
                            " WHERE ETL_LOG_ID = %d " %(g_etl_log_ID))
                raise Exception('Stage Validated Failed Field Differences')
            #end if
            
            # Check record count between Min and Max Records if specified
            
            if g_record_count > -1:
                if g_record_count < g_min_records:
                    eMsg = " Validate: %s To few records %d is less than %d" %(g_StageTableName,g_record_count,g_min_records)
                    ADWService.processError(eMsg)
                    ADWServiceCur.execute("UPDATE ADW_ETL_LOG " \
                            "SET ETL_MESSAGE   = '%s'"\
                            " WHERE ETL_LOG_ID = %d " %(eMsg,g_etl_log_ID))
                #end if
                if g_record_count > g_max_records:
                    eMsg = " Validate: %s Too many records %d is greater than %d" %(g_StageTableName,g_record_count,g_max_records)
                    ADWServiceCur.execute("UPDATE ADW_ETL_LOG " \
                            "SET ETL_MESSAGE   = '%s'"\
                            " WHERE ETL_LOG_ID = %d " %(eMsg,g_etl_log_ID))
                    ADWService.processError(eMsg)
                #end if
            #end if
        #end if
        ADWServiceCur.execute("UPDATE ADW_ETL_LOG " \
                "SET ETL_MESSAGE   = 'Stage Validated Completed '"\
                " WHERE ETL_LOG_ID = %d " %(g_etl_log_ID))
    else:
        ADWServiceCur.execute("UPDATE ADW_ETL_LOG " \
                "SET ETL_MESSAGE   = 'Stage Validated Skipped '"\
                " WHERE ETL_LOG_ID = %d " %(g_etl_log_ID))
    #end if

    if g_record_count > 0:
        g_total_records  += g_record_count
    #end if

#end DEF

# -------------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------------
#                    Main Program Start here 
# -------------------------------------------------------------------------------------------------------------------------

v_ProgramName = 'MSSQL_ETL'
v_Version = "V 1.0.0 Jan 6,2022"
v_parser = argparse.ArgumentParser(prog=v_ProgramName,
         description='To perform MS/SQL ETL for Application Data Warehouse (ADW)')

v_parser.add_argument('-AppID', default=r"NW",
                      help= "Application to Process into ADW Stage")
v_parser.add_argument('-GroupID', default=r"None",
                      help= "ETL Group to Process into ADW Stage")
v_parser.add_argument('-WorkDir', default=r"c:/temp",
                      help= "ETL Working Directory")
v_parser.add_argument('--version', action='version', version=v_ProgramName + " " + v_Version)

v_args = v_parser.parse_args()

print (v_ProgramName + " " + v_Version)

p_app_id   = v_args.AppID
p_group_id = v_args.GroupID
g_work_dir = v_args.WorkDir

g_LastTime=0
g_total_records = 0

ADWService = OracleConnect('ADW_PROD')
ADWServiceCur = ADWService.cursor()
try:
    #
    # Get ETL Log ID
    #
    ADWServiceCur.execute("select ADW_ETL_LOG_GROUP_SEQ.nextval from dual")
    g_etl_log_groupID, = ADWServiceCur.fetchone()
    #
    # Get Tables to Transfer using MSInsert and BCPTransfer
    #
    sqlstr = "select ETL_STAGE_ID,APP_ID,ETL_GROUP,ETL_SEQ,ETL_TYPE,ETL_TYPE_PARM,ETL_COMMIT_SIZE, " \
            "       ETL_SRC_SCHEMA,ETL_SRC_TABLE,CONVERT_UPPER,ADW_TABLE,STAGE_INDEX," \
            "       STAGE_CREATE_SQL,STAGE_SELECT_SQL,STAGE_INSERT_SQL," \
            "       RECORD_COUNT_IND,VALIDATE_IND,NVL(MIN_RECORDS,0),NVL(MAX_RECORDS,99999999999)" \
            "  FROM  ADW_ETL_STAGE WHERE "
    if p_group_id == "None":
        ADWService.processBegin("MSI-APP-%s" %(str(p_app_id)) )
        sqlstr = "%s APP_ID    = '%s' and ETL_TYPE in ('MSINSERT','BCPTRANSFER') AND ACTIVE_IND = 'Y' order by APP_ID,ETL_SEQ,ETL_STAGE_ID" %(sqlstr,str(p_app_id))
    else:
        ADWService.processBegin("MSI-GROUP-%s" %(str(p_group_id)) )
        sqlstr = "%s ETL_GROUP = '%s' and ETL_TYPE in ('MSINSERT','BCPTRANSFER') AND ACTIVE_IND = 'Y' order by APP_ID,ETL_SEQ,ETL_STAGE_ID" %(sqlstr,str(p_group_id))
    #END IF

    g_table_count   = 0
    g_total_records = 0
    startJob = time.time()

    StageTablesRec = ADWServiceCur.execute(sqlstr).fetchall()
    c_app_ID = ' '
    c_ErrorCount = 0
    for g_etl_stage_ID    ,g_app_ID          ,g_etl_group       ,g_etl_seq         ,g_etl_type   ,g_etl_type_parm,g_etl_commit_size, \
        g_etl_src_schema  ,g_etl_src_table   ,g_convert_upper   ,g_adw_Table       ,g_stage_index,             \
        g_stage_create_sql,g_stage_select_sql,g_stage_insert_sql,                           \
        g_record_count_ind,g_validate_ind    ,g_min_records     ,g_max_records in StageTablesRec:
        
        startProcess = time.time()
        g_record_count = -1
        c_ErrorCount = 0
        
        if c_app_ID != g_app_ID:
            c_app_ID = g_app_ID
            c_ErrorCount = 0
            # -------------------------------------------
            # -- Open SQL Server with to select data
            # --

            SQLCon = MSSQLConnect(g_etl_type_parm) 
            SQLCur = SQLCon.cursor()
        #END IF
        
        print (' Processing Table %s using %s '%(g_etl_src_table,g_etl_type))
        g_table_count += 1

        g_StageTableName  = '%s_%s_S' %(g_app_ID,g_adw_Table) 
        g_ProdTableName   = '%s_%s_P' %(g_app_ID,g_adw_Table) 
        g_BackupTableName = '%s_%s_B' %(g_app_ID,g_adw_Table) 

        ADWServiceCur.execute("select ADW_ETL_LOG_SEQ.nextval from dual")
        g_etl_log_ID, = ADWServiceCur.fetchone()


        ADWServiceCur.execute("insert into ADW_ETL_LOG (ETL_LOG_ID  ,ETL_STAGE_ID  ,ETL_LOG_GROUP_ID  ,ETL_START_DATE  ,ETL_MESSAGE  )" \
			" values (%d,%d,%d,sysdate,'ETL Started')" %(g_etl_log_ID,g_etl_stage_ID,g_etl_log_groupID))

        if g_etl_type == 'MSINSERT':
            stageCreate()
            stageETLMSInsert()
            stageIndex()
            stageValidate()

        elif g_etl_type == 'BCPTRANSFER':
            stageCreate()
            stageETLBCPTransfer()
            stageIndex()
            stageValidate()
        #end if
                
        ADWService.commit()
        
        endProcess = time.time()
        print ('  %s Loaded : %6d in (%s)' % (g_etl_src_table,g_record_count,GetPrintTime(endProcess-startProcess)))
        ADWService.processLog(' %s Loaded : %6d in (%s)' % (g_etl_src_table,g_record_count,GetPrintTime(endProcess-startProcess)))
        g_table_count += 1

    #END FOR

    if g_table_count == 0:
        if p_group_id == "None":
            raise Exception('No Tables Defined for Application %s' %(p_app_id))
        else:
            raise Exception('No Tables Defined for Group %s' %(p_group_id))
        #end if
    #end if

    ADWServiceCur.callproc('ADW_ETL.MOVE_STAGE_TO_PRODUCTION', [g_etl_log_groupID])

    endJob = time.time()

    rString ='Number of SQL Server Tables Transfered %d Total number of Records %d in (%s)' % (g_table_count,g_total_records,GetPrintTime(endJob-startJob))
    ADWService.processLog(rString)
    print(rString)
    ADWService.processEnd()

                
    ADWService.close()

except Exception as eMsg:
    ADWService.processError(str(eMsg))
    print(str(eMsg))
    ADWService.processEnd()
