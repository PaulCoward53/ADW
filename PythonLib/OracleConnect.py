#
#  Author: Paul Coward
#  Purpose: To provide connections to oracle for python 
# 
#  Date:  Jan 10/2022
#
#  Modifications
#    Revision     Author          Date            Description
#    1			  Paul Coward	 Jan 10/2022      Initial Version
#
import argparse
import cx_Oracle
import os
import io
import configparser
import base64
import socket
import getpass

from ADWUtility import ADWDecode


class OracleConnect(object):

    def __init__(self,ServiceName, ConfigFile=r"c:\Temp\ADW_CONNECTIONS.ini" ): 
    
        self.service_name   = ServiceName
        self.config_file    = ConfigFile
        self.connection     = None
        self.service_cursor = None
        self.Catalog        = None
        self.Schema         = None
        self.UserName       = None
        self.Password       = None
        self.Server         = None
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
                                if v_Type != 'Oracle':
                                    raise Exception("Service %s is not Type Oracle" %(SerName.upper()))
                                #end if

                                print("Oracle Access: %s User: %s Host: %s" %(self.service_name,v_UserName,v_Server))
                                self.Catalog        = v_Catalog
                                self.Schema         = v_Schema
                                self.UserName       = v_UserName
                                self.Password       = v_Password
                                self.Server         = v_Server

                                if v_LDAP == 'Y':
                                    raise Exception("LDAP connection not defined for Oracle service %s " %(SerName.upper()))
                                else:
                                    self.connection = cx_Oracle.connect(v_UserName,v_Password,v_Server)
                                #endif

                                self.service_cursor=self.connection.cursor()
                                return
                        #end for
                    #end if
                #end for
            else:
                raise Exception('Missing Configuration File %s' %(ConfigFile))
            #end if


        except Exception as eMsg:
            raise Exception('OracleCursor: %s' %(str(eMsg)))
        #end try
        
        raise Exception('OracleCursor: Can not locate Service %s' %(self.service_name))

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

    def processBegin(self,process_name ):

        try:
 
            self.service_cursor.callproc('ADW_PROCESS.PROCESS_BEGIN', [process_name])

        except Exception as eMsg:
            raise Exception('Oracle processBegin: %s' %(str(eMsg)))
        #end try

    def processLog(self,p_Message ):

        try:
 
            self.service_cursor.callproc('ADW_PROCESS.PROCESS_LOG', [p_Message])

        except Exception as eMsg:
            raise Exception('Oracle processLog: %s' %(str(eMsg)))
        #end try

    def processError(self,p_Message ):

        try:
 
            self.service_cursor.callproc('ADW_PROCESS.PROCESS_Error', [p_Message])

        except Exception as eMsg:
            raise Exception('Oracle processError: %s' %(str(eMsg)))
        #end try

    def processEnd(self):

        try:
 
            self.service_cursor.callproc('ADW_PROCESS.PROCESS_END')

        except Exception as eMsg:
            raise Exception('Oracle processEnd: %s' %(str(eMsg)))
        #end try

    def processExecute(self,p_ProgramName,p_ProcedureName='Python' ):

        try:
            v_Host = socket.gethostname()
            v_User = getpass.getuser()
            self.service_cursor.callproc('ADW_PROCESS.PROCESS_EXECUTE', [p_ProgramName,p_ProcedureName,v_Host,v_User])

        except Exception as eMsg:
            raise Exception('Oracle processExecute: %s' %(str(eMsg)))
        #end try
    #end DEF
#end class

if __name__ == "__main__":

    try:
        cur =OracleConnect("ADW_PROD")
        cur.processExecute("TEST")
        cur.processBegin('test Cursor')
        cur.processLog('test Cursor')
        cur.processEnd()
        cur.commit()
        
        testCur=cur.cursor()
        testCur.callproc('ADW_PROCESS.PROCESS_EXECUTE', ['ME','ME','Here','Who'])

    except Exception as eMsg:
        print('Error: %s' %(str(eMsg)))
    #end try
    
