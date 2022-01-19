#
#  Author: Paul Coward
#  Purpose: To send mail in ADW_MAIL_SENT table in Application Data Warehouse
# 
#  Date:  Mar 5/2021
#
#  Modifications
#    Revision     Author          Date            Description
#    1			  Paul Coward	 Mar  5,2021      Initial Version
#
import argparse
import cx_Oracle
import smtplib
from email.mime.text      import MIMEText
from email.mime.image     import MIMEImage
from email.mime.multipart import MIMEMultipart

import sys
sys.path.append('C:/ADW/PythonLib/')
from OracleConnect import OracleConnect 


v_ProgramName = 'ADW_SendMail'
v_Version = "V 1.0.0 Jan 8,2022"
v_parser = argparse.ArgumentParser(prog=v_ProgramName,
         description='To send mail of Application Data Warehouse (ADW) table ADW_MAIL_SEND')
v_parser.add_argument('-Connection', default=r"ADW_PROD",
                      help= "ADW Connection to use")
v_parser.add_argument('-Sender', default=r"<valid sender account>",
                      help= "Who Mail Sent from")
v_parser.add_argument('-STMP', default=r"mail.shaw.ca",
                      help= "STMP Mail server")
v_parser.add_argument('--version', action='version', version=v_ProgramName + " " + v_Version)

v_args = v_parser.parse_args()

print (v_ProgramName + " " + v_Version)
try:

    ADWService = OracleConnect(v_args.Connection)
    ADWServiceCur = ADWService.cursor()

    sqlstr ="select MAIL_ID,SEND_TO,SENT_FROM,SUBJECT_LINE,BODY_TEXT from ADW_MAIL_SEND"
    MailRec = ADWServiceCur.execute(sqlstr).fetchall()


    for v_mailID,v_SendTo,v_SentFrom,v_Subject,v_Message in MailRec:
        print('Print Mail: %s to %s ' %(v_mailID,v_SendTo))
        smtpObj = smtplib.SMTP(v_args.STMP)

        v_sender = v_args.Sender
        receivers = [v_SendTo]

        msg = MIMEMultipart('alternative')
        msg['Subject'] = v_Subject
        msg['From'] = 'ADW ADMINISTRATOR <%s>' %(v_sender)
        msg['To'] = v_SendTo

        part1 = MIMEText(str(v_Message), 'html')
        msg.attach(part1)

        smtpObj.sendmail(v_sender, receivers, msg.as_string())
        smtpObj.quit()    

        ADWServiceCur.execute("delete from ADW_MAIL_SEND where MAIL_ID = %s" %(v_mailID))

        print( "Successfully sent email to %s" %(v_SendTo))
    #end for

    ADWService.processExecute(v_ProgramName)

    ADWService.commit()

except Exception as eMsg:
    ADWService.processBegin(v_ProgramName)
    ADWService.processError(str(eMsg))
    print('Error:',eMsg)
    ADWService.processEnd()

