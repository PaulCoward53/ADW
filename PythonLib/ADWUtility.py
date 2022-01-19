#
#  Author: Paul Coward
#  Purpose: General ADW Python Utilities
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
import subprocess

#  ---------------------------------------------------------------------------
#  Encode string using seed key provided

def ADWEncode(inputText,Encodekey=r"Your ADW Key"):
    try:
        enc = []
        for i in range(len(inputText)):
            key_c = Encodekey[i % len(Encodekey)]
            enc_c = chr((ord(inputText[i]) + ord(key_c)) % 256)
            enc.append(enc_c)
        return base64.urlsafe_b64encode("".join(enc).encode()).decode()
    except Exception as eMsg:
        raise Exception("Encode Error:%s"%(str(eMsg))) 
    #end try
#end def

#  ---------------------------------------------------------------------------
#  Decode string using seed key provided

def ADWDecode(encodedText,Decodekey=r"Your ADW Key"):
    try:
        dec = []
        encodedText = base64.urlsafe_b64decode(encodedText).decode()
        for i in range(len(encodedText)):
            key_c = Decodekey[i % len(Decodekey)]
            dec_c = chr((256 + ord(encodedText[i]) - ord(key_c)) % 256)
            dec.append(dec_c)
        return "".join(dec)
    except Exception as eMsg:
        raise Exception("Decode Error:%s"%(str(eMsg))) 
    #end try
#end def

#  ---------------------------------------------------------------------------
#  Run a DOS Command

def RunCommand(command):
    p = subprocess.Popen(command,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT,
                         shell=True)
    lines =  p.stdout.readlines()
    p.wait

    outLines = []
    for line in lines:
        outLines.append(line.decode("utf-8").replace('\n',''))
    #end for

    return outLines
#end def

#  ---------------------------------------------------------------------------
#  To remove non printing character from String

def RemoveNonPrintString(c_str=[]):
    for ii in range(len(c_str)):
      if ord(c_str[ii]) < 32 or ord(c_str[ii]) > 128:
          print ('Bad ' + str(ord(c_str[ii])))
#end def

#  ---------------------------------------------------------------------------
#  To Clean SQL String (replace single quotes with two single quotes)

def SQLCleanString(c_str=[]):
    v_str = ''
    for ii in range(len(c_str)):
        if (ord(c_str[ii]) < 127 and ord(c_str[ii]) > 31):
            if str(c_str[ii]) == "'":
                v_str = v_str + "''"
            else:
                v_str = v_str + str(c_str[ii])
            #END IF
        #END IF
    #END FOR
    return v_str
#end def

#  ---------------------------------------------------------------------------
#  To convert name to upper (replace blank with underscore)

def SQLConvertNameUpper(p_string):
    return (p_string.upper().replace(' ','_'))
#end def

#  ---------------------------------------------------------------------------
#  To print Hrs/Min/Sec based on number of seconds (difference between 2 times)

def GetPrintTime(p_seconds):  # Print Hrs/Min/Sec
    v_hrs = int(p_seconds/3600)
    v_min = int((p_seconds-v_hrs*3600)/60)
    v_sec = (p_seconds-v_hrs*3600-v_min*60)

    pTime = ' Total %d hrs %d min %5.2f sec' % (v_hrs,v_min,v_sec)

    return pTime
#end def

#  ---------------------------------------------------------------------------
#  Test library Routines

if __name__ == "__main__":
    try:
        v_Encode=ADWEncode(r"This is a text encoded")
        v_Decode=ADWDecode(v_Encode)

        print('Encoded: %s' %(v_Encode))
        print('Decoded: %s' %(v_Decode))

        v_Encode=ADWEncode(r"This is a text encoded",'Different Key')
        v_Decode=ADWDecode(v_Encode,'Different Key')

        print('Encoded: %s' %(v_Encode))
        print('Decoded: %s' %(v_Decode))

        for lines in RunCommand("dir"):
            print(lines)
        #end for

        vString='This is a name Upper conversion'
        print('%s converted to upper %s' %(vString,SQLConvertNameUpper(vString)))

    except Exception as eMsg:
        print("Error:%s"%(str(eMsg)))
