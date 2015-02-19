#!/usr/bin/python
import os
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name']) 
import subprocess
import datetime
import time
import sys
import paramiko

"""
def executeWindows(command, password, username,winsys):
    print("In ttWin.executeWindows.....")
    try:
        proc=subprocess.Popen(["/usr/bin/sshpass -p %s /usr/bin/ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no %s@%s %s" %(password, username, winsys, command)], stdout=subprocess.PIPE, shell=True)
        (out,err)=proc.communicate()
        print("Output: %s" % out) 
	if (err is not None):
            my_logger("err ExecuteWindows: ",err)
            sys.exit(-1)   
	elif proc.returncode !=0:
	    my_logger("Error!", proc.returncode)
	    sys.exit(-1)
        return out, err
    except:
        e = sys.exc_info()[0]
        print("Error Encountered %s. Exiting...." % e )
        sys.exit(-1)
"""
def validateWin(pwd1, usr, winsys):
    my_logger.debug("Validating Windows host: %s for username: %s"%(winsys, usr))
    try:
        ssh=paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(winsys, username=usr, password=pwd1)
        
    except:
        my_logger.error("Error in SSH session. Cannot reach host %s. Exiting! " %(winsys))
        sys.exit("Error in SSH session. Cannot reach host %s. Exiting! " %(winsys))
    print "Host validation successful for: %s, user: %s"%(winsys, usr) 
    my_logger.debug("Host validation successful for: %s, user: %s"%(winsys, usr))
    
def findWinZones(zone, windns,password, username, winsys):
    try:
        command="dnscmd " + windns + " /enumzones"
        (outFindZone,errFindZone)=executeWindows(command, password, username,winsys)
        if ("ERROR" in outFindZone) or ("ERROR" in errFindZone):
            print( "winDnsBackup failure. Exiting! Following error encountered: %s "% errFindZone)
            my_logger.error("outBackup: " %outFindZone)
            sys.exit("outBackup: " %outFindZone)
    except:
        my_logger.error("Error in validating zone.") 
        sys.exit("Error in validating zone.")
    if zone in outFindZone:
        print "NYC Zone validated. Continuing!"
        my_logger.debug("NYC Zone validated. Continuing!")
    else:
        my_logger.error("NYC zone validation failed. Please verify ttSettings. Exiting!")
        sys.exit("NYC zone validation failed. Please verify ttSettings. Exiting!")
                
def executeWindows(command, pwd1, usr, winsys):
    try:
        ssh=paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(winsys, username=usr, password=pwd1)
        stdin, stdout, stderr=ssh.exec_command(command)
        out= str(stdout.readlines())
        err=str(stderr.readlines())
        return out, err
    except:
        e = str(sys.exc_info())
        my_logger.error(sys.exit("Cannot reach host %s. Error in SSH session: %s. \n Exiting!" %(winsys,e)))

    
def winDnsBackup(suffix, windns, zone, password, username, winsys):
    #print("In ttWin.winDnsBackup.....")
    try:
        now=datetime.datetime.fromtimestamp(time.time())
        file_name = now.strftime('%Y_%m_%d_%H_%M_%S_') + suffix + ".dns"
        command = "dnscmd " + windns + " /zoneexport " + zone + " " + file_name
        #print("Running command: %s" % command)
        (outBackup,errBackup)=executeWindows(command, password, username,winsys)
        if "ERROR" in outBackup:
            print( "winDnsBackup failure. Exiting! Following error encountered: %s "% errBackup)
            my_logger.error( "winDnsBackup failure. Exiting! Following error encountered: %s "% errBackup)
            print("outBackup: " % outBackup)
            my_logger.error("outBackup: " % outBackup)
            sys.exit(-1)
        return outBackup, errBackup
    except:
        e = sys.exc_info()[0]
        my_logger.error(sys.exit("Error Encountered %s. Exiting...." % e ))
    
def enumRecords(k, windns, zone, password, username, winsys):
    my_logger.debug("Executing query: enumRecords for host: %s, user: %s, dns: %s"%(winsys, username, windns))
    try:
        command="dnscmd " + windns + " /enumrecords " + zone + " " + k
        #print("Running command: %s" % command)
#        proc=subprocess.Popen(["/usr/bin/sshpass -p %s /usr/bin/ssh %s@%s %s" %(password, username, winsys, command)], stdout=subprocess.PIPE, shell=True)
        (outEnum, errEnum)= executeWindows(command, password, username, winsys)
        
    except IOError as (errno, strerror):
        print "I/O error({0}): {1}. Exiting!".format(errno, strerror)
        sys.exit(-1)
    except ValueError:
        print "Could not convert data to an integer.Exiting!"
        sys.exit(-1)
    except:
        print "Unexpected error. Exiting!"
        sys.exit(-1)
    #print("Output is: %s" % outEnum)
    outSplit=outEnum.split(" ")
    str1='.com.'
    for i in outSplit:
            #print i
            if str1 in i:
                    currRec= i
            else:
                    currRec=""
    my_logger.debug("Enum records successful. Returning to main() with out:%s, err:%s, currRec: %s"%(outEnum,errEnum,currRec))
    return outEnum,errEnum,currRec
    """
    except:
        e = sys.exc_info()[0]
        print("Error Encountered %s. Exiting...." % e )
        sys.exit(-1)
    """
    
def addRecords(dictRecAdd, windns, zone, password, username, winsys):
    my_logger.debug("In ttWin.addRecords....")
    if len(dictRecAdd) == 0:
        my_logger.info("No records to be added. Returning to main()")
        command="NA"
        ret_str="NA"
        #return("No records added","No records added")
    else:
        try:
            for k,v in dictRecAdd.items():
                command="dnscmd " + windns + " /recordadd " + zone + " " + k + " CNAME " + v
                #print("Running command: %s" % command)
                (outAddRec,errAddRec)=executeWindows(command, password, username, winsys)
        except:
            sys.exit("Win Add records failed. Possible source of issue: host_windns: %s or zone_NYC_long: %s. Error Encountered %s. Exiting...." %(windns, zone,sys.exc_info()[0]))
        if "ERROR" in outAddRec:
            print( "Following error encountered: %s "% outAddRec)
            print( "Backing up DNS data")
            winDnsBackup("addRecords_Failure_Exiting")
            sys.exit(-1)
        ret_str=str("OutAddRec:%s,  ErrAddRec: %s"%(outAddRec, errAddRec))
        my_logger.debug("Command: %s, return_string: %s"%(command, ret_str))
        my_logger.debug("Returning to main()")
    return (command,ret_str)

def deleteRecords(dictRecDelete, windns, zone, password, username, winsys):
    my_logger.debug("In ttWin.deleteRecords....")
    if len(dictRecDelete) == 0:
        my_logger.info("No records to be deleted. Returning to main()")
        command="NA"
        ret_str="NA"
        #return("No records deleted","No records deleted")
    else:
        try:
            for k, v in dictRecDelete.items():
                command= "dnscmd " + windns + " /recorddelete " + zone + " " + k + " CNAME " + v + " /f"
                my_logger.debug("Running command: %s " % command)
                (outDelRec,errDelRec)=executeWindows(command, password, username, winsys)
        except:
            my_logger.error("Error Encountered in deleteRecords(). Exiting")
            sys.exit("Error Encountered in deleteRecords(). Exiting")
        if "ERROR" in outDelRec:
            print("Following error encountered: %s " % outDelRec)
            my_logger.error("Following error encountered: %s " % outDelRec)
            print("Backing up DNS data.....")
            my_logger.info("Backing up DNS data.....")
            winDnsBackup("addRecords_Failure_Exiting")
            sys.exit(-1)
        ret_str=str("OutDelRec: %s, ErrDelRec: %s" %(outDelRec, errDelRec))
        my_logger.debug("Returning to main() with command: %s, return_string: %s"%(command,ret_str))
    return(command,ret_str)
