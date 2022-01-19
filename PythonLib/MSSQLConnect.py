#
#  Author: Paul Coward
#  Purpose: To connect to Microsoft SQL Server
# 
#  Date:  Mar 5/2021
#
#  Modifications
#    Revision     Author          Date            Description
#    1			  Paul Coward	 Mar  5,2021      Initial Version
#
import argparse
import pyodbc
import os
import io
import configparser
import base64
import socket
import getpass

from ADWUtility import ADWDecode


class MSSQLConnect(object):

    def __init__(self,ServiceName, ConfigFile=r"c:\Temp\ADW_CONNECTIONS.ini" ): 
    
        self.service_name   = ServiceName
        self.config_file    = ConfigFile
        self.Schema         = None
        self.UserName       = None
        self.Password       = None
        self.Server         = None
        self.connection     = None
        self.service_cursor = None
        try:
            if os.path.exists(ConfigFile):
                config = configparser.ConfigParser()
                config.sections()
                config.read(ConfigFile)
                
                # List all contents
                for section in config.sections():
                    if section == 'Services':
                        for SerName in config.options(section):
                            if SerName.upper() == ServiceName.upper():
                                v_Parms =  ADWDecode(config.get(section, SerName))
                                v_Type,v_Catalog,v_Schema,v_Server,v_LDAP,v_UserName,v_Password =  v_Parms.split(',')
                                if v_Type != 'SQLServer':
                                    raise Exception("Service %s is not Type SQLServer" %(SerName.upper()))
                                #end if
                                print("MS/SQL Access: %s User: %s Host: %s" %(self.service_name,v_Schema,v_Server))
                                self.Catalog  = v_Catalog
                                self.Schema   = v_Schema
                                self.Server   = v_Server
                                self.UserName = v_UserName
                                self.Password = v_Password
#                                self.connection  = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=%s;' %(v_Host,v_Schema))
                                if v_LDAP == 'Y':
                                    self.connection  = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=%s;' % (v_Server,v_Catalog))
                                else:
                                    self.connection  = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' % (v_Server,v_Catalog,v_UserName,v_Password))
                                #end if
                                self.service_cursor=self.connection.cursor()
                                return
                        #end for
                    #end if
                #end for
            else:
                raise Exception('Missing Configuration File %s' %(ConfigFile))
            #end if


        except Exception as eMsg:
            raise Exception('MSQLCursor: %s' %(str(eMsg)))
        #end try
        
        raise Exception('MSQLCursor: Can not locate Service %s' %(self.service_name))

    def cursor(self):
        try:
            return(self.service_cursor)

        except Exception as eMsg:
            raise Exception('Oracle cursor: %s' %(str(eMsg)))
        #end try

    def commit(self):

        try:
            self.connection.commit()

        except Exception as eMsg:
            raise Exception('Oracle Commit: %s' %(str(eMsg)))
        #end try

    def close(self):

        try:
            self.connection.close()

        except Exception as eMsg:
            raise Exception('Oracle Close: %s' %(str(eMsg)))
        #end try

#end class

if __name__ == "__main__":

    try:
        cur =MSSQLConnect("MSAP_PROD")
        cur.commit()
        
        testCur=cur.cursor()

        sqlstr = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.columns WHERE TABLE_NAME = 'Project'"
        TableColResult = testCur.execute(sqlstr).fetchall()
        g_ColumnName = []
        g_ColumnType = []
        for r_ColumnName,r_ColumnType in TableColResult:
                g_ColumnName.append(r_ColumnName)
                g_ColumnType.append(r_ColumnType.upper())
        #END FOR

 
        if len(g_ColumnName) < 1:
            print (sqlstr)
            print ('-- No Columns in table')
        #END IF


                    
        v_sqlFields = ''
        v_sqlType   = ''
        v_sep = ' '
        for ii in range(len(g_ColumnName)):
            v_sqlFields = v_sqlFields + v_sep + '"' + g_ColumnName[ii] + '"'
            v_sep = ','
        #END FOR
        print(v_sqlFields)

    except Exception as eMsg:
        print('Error: %s' %(str(eMsg)))
    #end try
    
