#
#  Author: Paul Coward
#  Purpose: To set or display contents of ADW Connection Configuration file
# 
#  Date:  Mar 5/2021
#
#  Modifications
#    Revision     Author          Date            Description
#    1			  Paul Coward	 Mar  5,2021      Initial Version
#
import argparse
import os
from pathlib import Path
import io
import configparser
from PyQt5 import QtWidgets

import cx_Oracle
import pyodbc

from PyQt5 import QtWidgets as qtw
from PyQt5 import QtCore as qtc
from PyQt5 import QtGui
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication,QMessageBox,QStyle,QWidget,QLineEdit
import sys 
sys.path.insert(0, 'C:\ADW\pythonLib')  # path for the python libraries it required

from ADWUtility import ADWDecode,ADWEncode

from ADWCWindow import Ui_ADWConnections 

class ADWConnectionsWindow(qtw.QMainWindow ):

    def __init__(self):

        super(ADWConnectionsWindow, self).__init__()

        self.ui = Ui_ADWConnections()
    
        self.ui.setupUi(self)

        self.ui.lsServiceList.itemSelectionChanged.connect(self.SelectService)

        self.ui.btnAddService.clicked.connect(self.AddService)
        self.ui.btnDeleteService.clicked.connect(self.DeleteService)
        self.ui.btnReloadFile.clicked.connect(self.ReloadFile)

        self.ui.btnSaveFile.clicked.connect(self.SaveFile)
        self.ui.cbLDAP.clicked.connect(self.LDAP_Checked)

        self.ui.btnLoginTest.clicked.connect(self.LoginTest)
        self.ui.btnSetPassword.clicked.connect(self.SetPassword)

        self.ui.btnShowPassword.setIcon(self.style().standardIcon(getattr(QStyle, 'SP_MessageBoxQuestion')))
        self.ui.btnShowPassword.clicked.connect(self.ShowPassword)

        self.ui.txtPassword.textChanged.connect(self.PasswordChanged)

        self.ui.actionSave.triggered.connect(self.SaveFile)
        self.ui.actionSave_As.triggered.connect(self.SaveAsFile)
        self.ui.actionExit.triggered.connect(self.CloseApp)

    def closeEvent(self, event):
            close = QtWidgets.QMessageBox.question(self,
                                         "Close Program",
                                         "Contents of data changes will be lost.\nDo you want to Close anyway?",
                                         QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
            if close == QtWidgets.QMessageBox.Yes:
                event.accept()
            else:
                event.ignore()
    
    def ShowPassword(self):
        if self.ui.txtPassword.echoMode() == QtWidgets.QLineEdit.Password:
            self.ui.txtPassword.setEchoMode(QtWidgets.QLineEdit.Normal)
        else:
            self.ui.txtPassword.setEchoMode(QtWidgets.QLineEdit.Password)
        #END IF

    def PasswordChanged(self):
        self.ui.btnSetPassword.setEnabled(True)
        self.SaveCurrentService()

    
    def SetPassword(self):
        reply = QMessageBox.question(
            self,
            "Set Password",
            "This will change all the passwords for user (%s) to same value.\nDo you want to Set Passwords anyway?" %(self.serviceUserName[self.currentServiceIndex]),
            QMessageBox.Yes,
            QMessageBox.No,
        )
        if reply == QMessageBox.Yes:
            self.ui.btnSetPassword.setEnabled(False)
            c_UserName = self.ui.txtUserName.text()
            c_Password = self.ui.txtPassword.text()
            ii=0
            for idx, UserName in enumerate(self.serviceUserName):
                if UserName == c_UserName:
                    self.servicePassword[idx] = c_Password
                    ii += 1
                #end if
            #end for
            qtw.QMessageBox.information(self,"Password Set","%d Passwords have been set" %(ii))
        #end if
        self.ui.txtPassword.setEchoMode(QtWidgets.QLineEdit.Password)

    def LoginTest(self):
        self.SaveCurrentService()

        if self.serviceType[self.currentServiceIndex] == 'Oracle':
            self.LoginTestOracle()
        else:
            self.LoginTestSQLServer()
        #end if

    def LoginTestOracle(self):
        try:
            self.SaveCurrentService()
            if self.serviceLDAP[self.currentServiceIndex] == 'Y':
                raise Exception("Oracle LDAP Test Not Defined")
            else:
                ORCon = cx_Oracle.connect(self.serviceUserName[self.currentServiceIndex],
                                          self.servicePassword[self.currentServiceIndex], 
                                          self.serviceServer[self.currentServiceIndex])
                ORCur=ORCon.cursor()

            #endif
            ORCur.execute("select count(0) from all_tables where owner ='%s'" %(self.serviceCatalog[self.currentServiceIndex]))
            noTables = ORCur.fetchone()[0]
            qtw.QMessageBox.information(self,"Oracle Test","%d Tables visible to account" %(noTables))
        except Exception as eMsg:
            qtw.QMessageBox.warning(self,"Oracle Test Failed",str(eMsg))
        #end try

    def LoginTestSQLServer(self):
        try:
            self.SaveCurrentService()
            if self.serviceLDAP[self.currentServiceIndex] == 'Y':
                MSCon = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=%s;' \
                    % (self.serviceServer[self.currentServiceIndex],
                       self.serviceCatalog[self.currentServiceIndex]))
            else:
                MSCon = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' 
                     % (self.serviceServer[self.currentServiceIndex],
                        self.serviceCatalog[self.currentServiceIndex],
                        self.serviceUserName[self.currentServiceIndex],
                        self.servicePassword[self.currentServiceIndex]))
            #endif
            MSCur = MSCon.cursor()
 
            MSCur.execute("select count(0)  FROM INFORMATION_SCHEMA.tables WHERE TABLE_CATALOG = '%s' and TABLE_SCHEMA = '%s'"
                    %(self.serviceCatalog[self.currentServiceIndex],self.serviceSchema[self.currentServiceIndex]))
            noTables = MSCur.fetchone()[0]
            qtw.QMessageBox.information(self,"SQL Server Test","%d Tables visible to account" %(noTables))

        except Exception as eMsg:
            qtw.QMessageBox.warning(self,"SQL Server Test Failed",str(eMsg))
        #end try

    def CloseApp(self):
        self.SaveCurrentService()
    
        reply = QMessageBox.question(
            self,
            "Close Program",
            "Contents of data changes will be lost.\nDo you want to Close anyway?",
            QMessageBox.Yes,
            QMessageBox.No,
        )

        if reply == QMessageBox.Yes: 
            sys.exit()
        #end if

    def SaveAsFile(self):
        self.SaveCurrentService()

        p = Path(self.configFileName)
        fileName, _ = qtw.QFileDialog.getSaveFileName(self,"QFileDialog.getSaveFileName()", str(p.parent),"Ini Files (*.ini);;All Files (*)")
        if fileName:
            self.WriteFile(fileName)
        #end if

    def SaveFile(self):
        self.SaveCurrentService()
        reply = QMessageBox.question(
                self,
                "Save to File",
                "Contents of files will be lost.\nDo you want to overwrite the file?",
                QMessageBox.Yes,
                QMessageBox.No,
            )

        if reply == QMessageBox.Yes:
            self.WriteFile(self.configFileName)
        #end if

    def WriteFile(self,v_ConfigFileName):
        self.SaveCurrentService()
        
        self.configFileName=v_ConfigFileName
        self.ui.lblConfigFileName.setText(v_ConfigFileName)

        cfgfile = open(self.configFileName, "w")

        # Add content to the file
        Config = configparser.ConfigParser()

        Config.add_section("Services")
        ii=0
        for sName in self.serviceName:
            v_code1=ADWEncode(r"%s,%s,%s,%s,%s,%s,%s" \
                        %(self.serviceType[ii],\
                        self.serviceCatalog[ii],\
                        self.serviceSchema[ii],\
                        self.serviceServer[ii],\
                        self.serviceLDAP[ii],\
                        self.serviceUserName[ii],\
                        self.servicePassword[ii]))
            Config.set("Services", sName.upper(), v_code1)
            ii += 1
        #end for

        Config.write(cfgfile)
        cfgfile.close()

    def ReloadFile(self):
        self.SaveCurrentService()
        reply = QMessageBox.question(
                self,
                "Reload File",
                "Unsaved changes will be lost.\nDo you want to reload the file?",
                QMessageBox.Yes,
                QMessageBox.No,
            )

        if reply == QMessageBox.Yes:  
            self.LoadServices(self.configFileName)

    def AddService(self):
        self.SaveCurrentService()
        service, ok = qtw.QInputDialog.getText(self, 'Input Dialog',
                                        'Enter Service name:')
        if ok :
            if service == '':
                qtw.QMessageBox.warning(self, "Invalid Name", "Missing Server Name")
            elif ' ' in service or ',' in service:
                qtw.QMessageBox.warning(self,"Invalid Name","Invalid Name can not contain space or comma")
            else:
                if service.upper() in self.serviceName:
                    qtw.QMessageBox.warning(self,"Invalid Name","Service already on defined")
                else:
                    self.serviceName.append(service.upper())
                    if self.currentServiceIndex > -1:                          
                        ii = self.currentServiceIndex
                        self.serviceType.append(self.serviceType[ii])
                        self.serviceCatalog.append(self.serviceCatalog[ii])
                        self.serviceSchema.append(self.serviceSchema[ii])
                        self.serviceLDAP.append(self.serviceLDAP[ii])
                        self.serviceUserName.append(self.serviceUserName[ii])
                        self.servicePassword.append(self.servicePassword[ii])
                        self.serviceServer.append(self.serviceServer[ii])

                    else:
                        self.serviceType.append('Oracle')
                        self.serviceCatalog.append('')
                        self.serviceSchema.append('')
                        self.serviceLDAP.append('N')
                        self.serviceUserName.append('')
                        self.servicePassword.append('')
                        self.serviceServer.append('')
                    #end if
                    self.currentServiceIndex = len(self.serviceName)-1
                    self.ui.lsServiceList.addItem(service.upper())
                    self.SetService(self.currentServiceIndex)
                #end if
            #end if
        #end if
    #end def

    def DeleteService(self):
        if len(self.serviceName) == 1:
                qtw.QMessageBox.warning(self,"Delete Error","You can not delete final service")
        elif self.currentServiceIndex > -1:
            ii = self.currentServiceIndex 
            self.serviceName.pop(ii)
            self.serviceType.pop(ii)
            self.serviceCatalog.pop(ii)
            self.serviceSchema.pop(ii)
            self.serviceLDAP.pop(ii)
            self.serviceUserName.pop(ii)
            self.servicePassword.pop(ii)
            self.serviceServer.pop(ii)

            if self.currentServiceIndex >= len(self.serviceName):
                 self.currentServiceIndex = 0
            #end if
            self.BuildServiceList()
            self.SetService(self.currentServiceIndex)
        else:
            qtw.QMessageBox.warning(self, "Can not Delete", "No services to delete")
        #end if
    #end def

    def SelectService(self):
       
        self.SaveCurrentService()
        self.SetService(self.ui.lsServiceList.currentRow())

    def SaveCurrentService(self):
        if self.currentServiceIndex > -1:
            if self.ui.rbtnOracle.isChecked():
                self.serviceType[self.currentServiceIndex]='Oracle'
            else:
                self.serviceType[self.currentServiceIndex]='SQLServer'
            #end if

            self.serviceCatalog[self.currentServiceIndex]=self.ui.txtCatalogName.text()
            self.serviceSchema[self.currentServiceIndex]=self.ui.txtSchemaName.text()
            self.serviceServer[self.currentServiceIndex]=self.ui.txtServerName.text()
            
            if self.ui.cbLDAP.isChecked():
                self.serviceLDAP[self.currentServiceIndex]='Y'
            else:
                self.serviceLDAP[self.currentServiceIndex]='N'
            #end if

            self.serviceUserName[self.currentServiceIndex]=self.ui.txtUserName.text()
            self.servicePassword[self.currentServiceIndex]=self.ui.txtPassword.text()
        #end if
    #end def

    def BuildServiceList(self):
        self.ui.lsServiceList.clear()
        for sName in self.serviceName:
            self.ui.lsServiceList.addItem(sName.upper())
        #end for
    #end def

    def SetService(self,idx):
        if  idx >= len(self.serviceName):
             idx=0
        #end if

        self.currentServiceIndex = idx

        self.ui.txtServiceName.setText(self.serviceName[idx])
        self.ui.txtCatalogName.setText(self.serviceCatalog[idx])
        self.ui.txtSchemaName.setText(self.serviceSchema[idx])

        if self.serviceType[idx] == 'Oracle':
            self.ui.rbtnOracle.setChecked(True)
        else:
            self.ui.rbtnSQLServer.setChecked(True)
        #end if

        if self.serviceLDAP[idx] == 'Y':
            self.ui.cbLDAP.setChecked(True)
            self.ui.txtUserName.setHidden(True)
            self.ui.txtPassword.setHidden(True)
            self.ui.lblUserName.setHidden(True)
            self.ui.lblPassword.setHidden(True)
            self.ui.btnSetPassword.setHidden(True)
            self.ui.btnShowPassword.setHidden(True)
        else:
            self.ui.cbLDAP.setChecked(False)
            self.ui.txtUserName.setHidden(False)
            self.ui.txtPassword.setHidden(False)
            self.ui.lblUserName.setHidden(False)
            self.ui.lblPassword.setHidden(False)
            self.ui.btnSetPassword.setHidden(False)
            self.ui.btnShowPassword.setHidden(False)

        #end if

        self.ui.txtUserName.setText(self.serviceUserName[idx])
        self.ui.txtPassword.setText(self.servicePassword[idx])

        self.ui.txtServerName.setText(self.serviceServer[idx])

        self.ui.txtServiceName.setEnabled(False)
        
        self.ui.btnSetPassword.setEnabled(False)
        self.ui.txtPassword.setEchoMode(QtWidgets.QLineEdit.Password)
    #end def

    def LoadServices(self,p_ConfigFileName):

        self.configFileName = p_ConfigFileName
        self.ui.lblConfigFileName.setText(p_ConfigFileName)

        self.currentServiceIndex= -1
        self.serviceName= []
        self.serviceType= []
        self.serviceCatalog= []
        self.serviceSchema= []
        self.serviceLDAP= []
        self.serviceUserName= []
        self.servicePassword= []
        self.serviceServer= []


        if os.path.isfile(p_ConfigFileName):
            self.config = configparser.ConfigParser()
            self.config.sections()
            self.config.read(p_ConfigFileName)
            
            ii=0
            for ServiceName in self.config.options('Services'):
                type,catalog,schema,server,ldap,userName,password = ADWDecode(self.config.get('Services', ServiceName)).split(',')

                self.serviceName.append(ServiceName.upper())
                self.serviceType.append(type)
                self.serviceCatalog.append(catalog)
                self.serviceSchema.append(schema)
                self.serviceLDAP.append(ldap)
                self.serviceUserName.append(userName)
                self.servicePassword.append(password)
                self.serviceServer.append(server)
                ii += 1
            #end for
            self.BuildServiceList()
            if ii > 0:
                self.SetService(0)
    #end def

    def LDAP_Checked(self):
        if self.ui.cbLDAP.isChecked():
            self.ui.cbLDAP.setChecked(True)
            self.ui.txtUserName.setHidden(True)
            self.ui.txtPassword.setHidden(True)
            self.ui.lblUserName.setHidden(True)
            self.ui.lblPassword.setHidden(True)
            self.ui.btnSetPassword.setHidden(True)
            self.ui.btnShowPassword.setHidden(True)
        else:
            self.ui.cbLDAP.setChecked(False)
            self.ui.txtUserName.setHidden(False)
            self.ui.txtPassword.setHidden(False)
            self.ui.lblUserName.setHidden(False)
            self.ui.lblPassword.setHidden(False)
            self.ui.btnSetPassword.setHidden(False)
            self.ui.btnShowPassword.setHidden(False)
        #end if
    #end def

if __name__ == "__main__":

    v_ProgramName = 'ADW_ConfigurationSet'
    v_Version = "V 1.0.1 Jan 8,2022"
    v_parser = argparse.ArgumentParser(prog=v_ProgramName,
            description='To edit contents of Application Data Warehouse (ADW) configuration access file')
    v_parser.add_argument('-Config', default=r"c:\ADW\ADW_Connections.ini",
                        help= "Configuration file to Edit")
    v_parser.add_argument('-Password', default=r"None",
                        help= "ADWAmin Password for localhost/orcl")
    v_parser.add_argument('--version', action='version', version=v_ProgramName + " " + v_Version)

    v_args = v_parser.parse_args()

    print (v_ProgramName + " " + v_Version)

    v_ConfigFileName = v_args.Config

    print ("Configuration file %s " %(v_ConfigFileName))

    app =qtw.QApplication([])
    widget = ADWConnectionsWindow()

    # Create New File if not present

    if not os.path.isfile(v_ConfigFileName):

        cfgfile = open(v_ConfigFileName, "w")
        Config = configparser.ConfigParser()

        Config.add_section("Services")
        v_code1=ADWEncode(r"Oracle,ADW,ADWADMIN,host,N,ADWADMIN,<Your Password>" )
        Config.set("Services", "ADW_PROD", v_code1)
        
        Config.write(cfgfile)
        cfgfile.close()
    #end if

    
    widget.LoadServices(v_ConfigFileName)
    
    widget.show()
    app.exec()
