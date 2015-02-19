#!/usr/bin/python 

#Program: dnsOps.py
#Author: Sudha

#Script to update AWS and NYC dns records

"""********************************************************************************************************************"""
print "Initializing setup..."
"""Import sys, re, os...start"""
print "Importing python libraries..."
try:
    print "Importing \'os\'...",
    import sys
    print "successful."
except:
    raise SystemExit("Failed. Exiting!")

try:
    print "Importing \'sys\'...",
    import os
    print "successful."
except:
    raise sys.exit("Failed. Exiting!")


"""Import sys, re...stop"""
"""********************************************************************************************************************"""

python_root = os.path.dirname(os.path.realpath(__file__)) + "/"
print "Working directory: %s" %python_root
sys.path.append(python_root + "ttLib")
from ttLib.ttSys import dirCreateTimestamp, get_s3_config
#from ttLib.ttAWS import get_s3_config
print "Downloading ttSettings from S3...",
try:
    s3_config_folder = "nyc-sys"
    s3_config_filename = "ttSettings.conf"
    global dict_ttSettings
    dict_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
    print"complete."  
    #print  dict_ttSettings
except:
    sys.exit("Download of ttSettings from S3 failed. Exiting! Error Encountered: %s." %str(sys.exc_info()))

PATH_ROOT = dict_ttSettings["PATH_ROOT"]
PATH_LOG = PATH_ROOT + dict_ttSettings["PATH_LOG"]

import logging
import logging.handlers

import ttLib.logger as logger
import ttLib.ttSys as ttSys
#level1='logging.INFO'
#print PATH_ROOT + PATH_LOG + "/dns"
(dir_backup, timestamp) = ttSys.dirCreateTimestamp(PATH_LOG + "/dns")
print"Backup and logging directory created successfully..."       
my_logger = logging.getLogger('dnsOps')
for h in my_logger.handlers:
    my_logger.removeHandler(h)
log_file = dir_backup + '/dnsOps.log'
loglevel = dict_ttSettings["log_level"]
try:
    confirm_loglevel = raw_input("\nCurrent loglevel is %s. Press [c/ C] to continue. To change the loglevel to 'DEBUG' enter [debug/ DEBUG]: "%loglevel)
    if confirm_loglevel.upper() == 'C':
        loglevel = "INFO"
    elif confirm_loglevel.upper() == "DEBUG":
        loglevel = "DEBUG"
    else:
        sys.exit("Incorrect entry received: %s. Exiting!"%confirm_loglevel)
except:
    sys.exit("Unexpected error encountered. Exiting!")
print"Received loglevel: %s"%loglevel
    
loglevel = getattr(logging, loglevel.upper())
for h in logger.get_handlers('dnsOps', log_file):
    my_logger.addHandler(h)
my_logger.setLevel(loglevel)
print"Logger created successfully. Continuing..."
my_logger.debug("Logger created successfully. Continuing...")
my_logger.debug("dict_ttSettings: "%dict_ttSettings)
global swapTimeStampDirPath
import ttLib.ttAWS as ttAWS
import ttLib.ttWin as ttWin

host_winsys = dict_ttSettings["service"]["winsys"]["hostname"]
host_winsys_username = dict_ttSettings["service"]["winsys"]["username"]
host_winsys_password = dict_ttSettings["service"]["winsys"]["password"]
host_windns = dict_ttSettings["service"]["dns"]["hostname"]
REGION_NAME = dict_ttSettings["aws"]["REGION_NAME"]
zone_AWS_long = dict_ttSettings["zone_AWS_long"]
zone_NYC_long = dict_ttSettings["zone_NYC_long"] 
"""
Constants
"""

dict_NYC = dict()     # stores public/ AWS related information
dict_AWS = dict()   # stores private/ NYC related information
dictNycTemp = dict()
dictAwsTemp = dict()
currAws = dict()
compAws = dict()

recordCheck = dict()
listRecord = list()
dictWinDns = dict()
dictRecDelete = dict()
dictWinStatus = dict()
dictRecAdd = dict()
dictRecCorrect = dict()


def main():
    my_logger.debug("In main module()")
    
    """Validate Winsys"""
    
    ttWin.validateWin(host_winsys_password, host_winsys_username, host_winsys)
    ttWin.findWinZones(zone_NYC_long, host_windns,host_winsys_password, host_winsys_username, host_winsys)
    """
    #This module finds the Zone ID (Z12...) for the given Zone Name (say, 3top.com)
    """

    my_logger.debug("In dnsOps...calling ttAWS.zoneR53 for collecting Zone ID information...")
    zone_Id = ttAWS.zoneR53(zone_AWS_long)
    my_logger.debug("Zone ID is: %s " % zone_Id)
    
    """
    print "In dnsOps: zone53:"
    print("zoneId = ",zone_Id)
    """
    
    """
                    #------BOTO AUTHENTICATION------
    #The auth_func in ttAWS checks if the import of boto packages
    (ec2, rds2 etc) is successful by checking
    #if the required packages have been installed
    and authentication information is accurately stored
    """
    my_logger.debug("Initializing Boto authentication....calling ttAWS.auth_func!")
    ttAWS.auth_func(REGION_NAME)
    """
                    #-----SELECT ENVIRONMENT-----
    #The selectEnviron module helps in finding the Env name for which the
    #DNS Operations have to be performed
    """
    my_logger.debug("In dnsOps...calling ttAWS.selectEnviron!")
    ENV_NAME = ttAWS.selectEnviron(REGION_NAME)
    my_logger.debug("The envName selected is: %s" % ENV_NAME)
    """#ENV_NAME is a.prod, b.prod, a.dev etc.."""
    """
                    #for h in my_logger.handlers:
    my_logger.removeHandler(h)----COLLECT EC2 DNS RECORDS-----
    #dnsNameMap function in dnsEc2 collects the following records:
    #fp-ec2-1,     mongodb,     mq,     search
    """
    my_logger.debug("Collecting DNS records from the new environment....")
    my_logger.debug("In dnsOps...calling ttAWS.dnsEc2ToDict for collecting DNS records from EC2...")
    (dictNycTemp, dictAwsTemp) = ttAWS.dnsEc2ToDict(REGION_NAME, ENV_NAME, zone_AWS_long)
    """Append the DNS records collected from EC2 to the global dictionaries"""
    print("Updating dict_NYC and dict_AWS with EC2 records...")
    my_logger.debug("Updating dict_NYC and dict_AWS with EC2 records...")
    dict_NYC.update(dictNycTemp)
    dict_AWS.update(dictAwsTemp)
    my_logger.debug("NYC DNS records from EC2: %s" % str(dict_NYC))
    my_logger.debug("AWS DNS records from EC2: %s" % str(dict_AWS))
    my_logger.debug("\nIn dnsOps: EC2:")
    my_logger.debug("\ndict_NYC:")
    my_logger.debug(dict_NYC)
    my_logger.debug("\ndict_AWS:")
    my_logger.debug(dict_AWS)
    
    """
                   # -----COLLECT BEANSTALK DNS RECORDS------
    #cnameEndpoint function in dnsBeanstalk collects the following records:
    #cname and endpointURL
    """
    my_logger.debug("In dnsOps...calling ttAWS.dnsBeanstalkToDict for collecting DNS records from BEANSTALK...")
    (dictNycTemp, dictAwsTemp) = ttAWS.dnsBeanstalkToDict(REGION_NAME, ENV_NAME, zone_AWS_long)
    """
    #Append the DNS records collected from BEANSTALK to the global dictionaries
    """
    my_logger.debug("Updating dict_NYC and dict_AWS with Beanstalk records...")
    dict_NYC.update(dictNycTemp)
    dict_AWS.update(dictAwsTemp)
    my_logger.debug("NYC DNS records from Beanstalk: %s" % str(dict_NYC))
    my_logger.debug("AWS DNS records from Beanstalk: %s" % str(dict_AWS))
    
    """
    print "\nIn dnsOps:Beanstalk:"
    print "\ndict_NYC:"
    print dict_NYC
    print "\ndict_AWS:"
    print dict_AWS
    """
    
    """
                   # -----COLLECT RDS DNS RECORDS------
    #rdsImport function in dnsRds collects the following records:
    #fp-rds-1
    """
    my_logger.debug("In dnsOps...calling ttAWS.dnsRdsToDict for collecting DNS records from RDS...")
    (dictNycTemp, dictAwsTemp) = ttAWS.dnsRdsToDict(REGION_NAME, ENV_NAME, zone_AWS_long)
    """
   # Append the DNS records collected from RDS to the global dictionaries
    """
    ("Updating dict_NYC and dict_AWS with RDS records...")
    dict_NYC.update(dictNycTemp)
    dict_AWS.update(dictAwsTemp)
    my_logger.debug("NYC DNS records from RDS: %s" % str(dict_NYC))
    my_logger.debug("AWS DNS records from RDS: %s" % str(dict_AWS))
    
    my_logger.debug( "\nIn dnsOps:RDS:")
    my_logger.debug("\ndict_NYC:")
    my_logger.debug(dict_NYC)
    my_logger.debug("\ndict_AWS:")
    my_logger.debug(dict_AWS)
       

    """
    #This will print the current status of the records
    """
    my_logger.debug("In dnsOps...calling ttAWS.currRecords!")
    currAws = ttAWS.route53Records(zone_Id, ENV_NAME)
    print("\nCurrent status of AWS Route53 records:\n ")
    for k, v in currAws.items():
        if ENV_NAME in k:
            print '{:30s} {:85s}'.format(str(k), str(v[0]))
            
    
    my_logger.debug("Current status of AWS Route53 records: %s"%currAws)
   
    list_dict_AWS = list()
    list_currAws = list()
    
    for k,v in currAws.items():
        list_currAws.append(k)
    for k,v in dict_AWS.items():
        list_dict_AWS.append(k)
    
    
    temp1 = dict()
    for k, v in currAws.items():
        if ENV_NAME in k:
            for k1, v1 in dict_AWS.items():
                if (k1 not in list_currAws):
                    temp1.update({k1: ("No Current Record", v1)})
                    #print temp1.update({k1: ("No Current Record", v1)})
                elif k in list_currAws:
                    if k == k1:
                        if ("fp-lb-1" in k):
                            rr_type = ttAWS.find_rrtype(zone_AWS_long, k)
                            if rr_type == "ALIAS":
                                if str(v).upper() != str(v1).upper():
                                    print "Deleting old ALIAS"
                                    ttAWS.del_cname(zone_AWS_long, k, rr_type)
                                    temp1.update({k: ("No Current Record- Deleted old ALIAS", v1)})
                            elif rr_type == "CNAME":
                                #print "rr_type found as %s"%rr_type 
                                print "Deleting CNAME"
                                ttAWS.del_cname(zone_AWS_long, k, rr_type)
                                temp1.update({k1: ("No Current Record- Deleted CNAME", v1)})
            if k not in list_dict_AWS:
                temp1.update({k: (v, "No Change")})
    
    #print temp1
    
    """
    #Check the current records on NYC DNS
    """
    my_logger.debug("Checking for current NYC records status with respect to the NYC records imported...")
    my_logger.debug(sorted(dict_NYC.keys()))
       
    print ("\n")
    for k,v in dict_NYC.items():
        if ENV_NAME in k:
            (outWinEnum, errWinEnum, currRecWinEnum) = ttWin.enumRecords(k, host_windns, zone_NYC_long, host_winsys_password, host_winsys_username, host_winsys)
            my_logger.debug("Output: %s: %s"%(v,currRecWinEnum))
            if "DNS_ERROR_NAME_DOES_NOT_EXIST" in outWinEnum:
                recordCheck.update({k: 'Record not found'})
            elif v.lower() in outWinEnum.lower():
                recordCheck.update({k: 'Record status current'})
            elif v.lower() not in outWinEnum.lower():
                recordCheck.update({k: 'Record status old'})
                dictRecDelete[k] = currRecWinEnum
            elif ("FAILED" in outWinEnum) or ("FAILED" in errWinEnum) or ("FAILED" in currRecWinEnum):
                my_logger.info(sys.exit("Error encountered: %s"%(outWinEnum, errWinEnum, currRecWinEnum)))
    dictTemp = recordCheck.copy()
    my_logger.debug("Current records status:%s" % recordCheck)
    
    dictTemp = dict([(k , [dictTemp[k], dict_NYC[k]]) for k in dictTemp])
    my_logger.debug("new dictTemp:%s " % dictTemp)
    
    for k,v in dictTemp.items():
        if 'Record not found' in v[0]:
            dictRecAdd[k] = v[1]
        elif 'Record status old' in v[0]:
            dictRecAdd[k] = v[1]
        elif 'Record status current' in v[0]:
            dictRecCorrect[k] = v[1]
    my_logger.debug("dictRecAdd:", dictRecAdd)
    my_logger.debug("dictRecCorrect:", dictRecCorrect)
    my_logger.debug("dictRecDelete",dictRecDelete)
    r53_update_count = 0
    for k in sorted(temp1):
        if ENV_NAME in k:
            temp_val = str(temp1[k][0]).split("\'")
            if str(temp_val[len(temp_val)-2]).lower() == str(temp1[k][1]).lower():
                continue
            else:
                r53_update_count = r53_update_count + 1
    
    if r53_update_count>0:
        print "Route53 updates required. Find the current and future values as follows:"
        my_logger.debug("Route53 updates required. Find the current and future values as follows:")
    else:
        print "No Route53 updates required. Find the current and future values as follows:"
        my_logger.debug("No Route53 updates required. Find the current and future values as follows:")
            
    if temp1:
        print "\nRoute 53 Updates:"
        
        my_logger.debug("\nRoute 53 Updates:")
        my_logger.debug("Records Name")
        for k in sorted(temp1):
            if (ENV_NAME in k) and ("fp-lb-1" not in k):
                temp_val = str(temp1[k][0]).split("\'")
                print "[CNAME RECORD]::{:40s}  [CURRENT VALUE]::{:85s}  [FUTURE VALUE]::{:85s}" .format(str(k), str(temp_val[len(temp_val)-2]), str(temp1[k][1]))
                str_k = str(k)
                str_v0 = str(v[0])
                str_v1 = str(v[1])
                my_logger.debug(str_k + str_v0 + str_v1)
                my_logger.debug(str_k + str_v0 + str_v1)
            if (ENV_NAME in k) and ("fp-lb-1" in k):
                temp_val = str(temp1[k][0]).split("\'")
                print "[ALIAS RECORD]::{:40s}  [CURRENT VALUE]::{:85s}  [FUTURE VALUE]::{:85s}" .format(str(k), str(temp_val[len(temp_val)-2]), str(temp1[k][1]))
                str_k = str(k)
                str_v0 = str(v[0])
                str_v1 = str(v[1])
                my_logger.debug(str_k + str_v0 + str_v1)
                my_logger.debug(str_k + str_v0 + str_v1)
                
                     
    my_logger.debug("NYC DNS Updates:: %s" % str(dictRecCorrect))
    
    if dictRecCorrect:
        print "\nNYC DNS Updates:"
        my_logger.debug("\nNYC DNS Updates: \nThe following records are up-to-date for NYC DNS.")
        my_logger.debug("dictRecCorrect: %s" %dictRecCorrect)
        print "\nThe following records are up-to-date for NYC DNS:"
        for k in sorted(dictRecCorrect):
            if "fp-lb-1" not in k:
                print "[CNAME RECORD]::{:40s}  [VALUE]::{:85s}" .format(str(k), str(dictRecCorrect[k]))
            elif "fp-lb-1" in k:
                print "[ALIAS RECORD]::{:40s}  [VALUE]::{:85s}" .format(str(k), str(dictRecCorrect[k]))
    
        
    my_logger.debug("\nDelete the following records for NYC DNS: %s" % str(sorted(dictRecDelete)))
    if dictRecDelete:
        print "\nDelete the following records for NYC DNS:"
        for k in sorted(dictRecDelete):
            if "fp-lb-1" not in k:
                print "[CNAME RECORD]::{:40s}  [VALUE]::{:65s}" .format(str(k), str(dictRecDelete[k]))
            elif "fp-lb-1" not in k:
                print "[ALIAS RECORD]::{:40s}  [VALUE]::{:65s}" .format(str(k), str(dictRecDelete[k]))
                
            
    my_logger.debug("\nAdd the following records for NYC DNS:%s" % str(sorted(dictRecAdd)))
    if dictRecAdd:
        print "\nAdd the following records for NYC DNS:" 
        for k in sorted(dictRecAdd):
            if "fp-lb-1" not in k:
                print "[CNAME RECORD]::{:40s}  [FUTURE VALUE]::{:85s}" .format(str(k), str(dictRecAdd[k]))
            elif "fp-lb-1" not in k:
                print "[ALIAS RECORD]::{:40s}  [FUTURE VALUE]::{:85s}" .format(str(k), str(dictRecAdd[k]))
    
    confirm_backup = raw_input("\nUpdate %s DNS records above in AWS Route 53 DNS and NYC Office DNS [y/n]:"%ENV_NAME)
    if confirm_backup == 'y' or confirm_backup == 'Y':
        
        
        #This will export the current AWS records into a file to allow for reverting in future 
        
       
        print("In dnsOps...calling ttAWS.cli53_export for backing up current AWS DNS Data...")
        my_logger.debug("In dnsOps...calling ttAWS.cli53_export for backing up current AWS DNS Data...")
        ttAWS.cli53_export(zone_AWS_long, dir_backup, '/beforeUpdate.txt')
    
        print("Before Update Backup Successful")
        my_logger.debug("Before Update Backup Successful")
        my_logger.info("NYC DNS Records:%s" % str(dict_NYC))
        
           # Update route53 DNS records (AWS)- take confirmation before updating
        
        try:
            my_logger.info( "In dnsOps...calling ttAWS.dnsUpdate!")
            out_aws_update = ttAWS.dnsUpdate(ENV_NAME, zone_Id, dict_AWS, zone_AWS_long)
            my_logger.info(out_aws_update)
            my_logger.info("AWS Import successful")
            print("AWS Import successful")
            
        except:
            e = sys.exc_info()[0]
            my_logger.info(sys.exit("Error Encountered: %s. AWS Import failed! Exiting..." % e ))
    
        my_logger.info("Backing up current records...")
        
        try:
            my_logger.info( "In dnsOps...calling ttWin.winDnsBackup!")
            ttWin.winDnsBackup("beforeUpdate", host_windns, zone_NYC_long, host_winsys_password, host_winsys_username, host_winsys)
            my_logger.info("Back up of current records succeeded...")
            print("Back up of current records succeeded...")
            
        except:
            e = sys.exc_info()[0]
            my_logger.info(sys.exit("Error Encountered: %s.Backup of records failed! Exiting..." % e))
            
        try:
            my_logger.info( "In dnsOps...calling ttWin.deleteRecords!")
            (cmd, out_del_rec) = ttWin.deleteRecords(dictRecDelete, host_windns, zone_NYC_long, host_winsys_password, host_winsys_username, host_winsys)
            cmd = str(cmd)
            out_del_rec = str(out_del_rec)
            my_logger.info("Command: %s, Out_del_rec: %s"%(cmd,out_del_rec))
            my_logger.info("Delete of old records complete...")
            print("Delete of old records complete...")
            
        except:
            e = sys.exc_info()[0]
            my_logger.info(sys.exit("Error Encountered: %s.Deleting records failed! Exiting..." % e))
            #sys.exit(-1)
        
        try:
            my_logger.info( "In dnsOps...calling ttWin.addRecords for adding records...")
            (cmd, out_add_rec) = ttWin.addRecords(dictRecAdd, host_windns, zone_NYC_long, host_winsys_password, host_winsys_username, host_winsys)
            cmd = str(cmd)
            out_add_rec = str(out_add_rec)
            my_logger.info("Command: %s, Out_del_rec: %s"%(cmd,out_add_rec))
            my_logger.info("Adding records complete....")
            print("Adding records complete....")
        except:
            e = sys.exc_info()[0]
            my_logger.info(sys.exit("Error Encountered: %s.Adding records failed! Exiting..." % e))
            #sys.exit(-1)   
        
        try:
            my_logger.info( "In dnsOps...calling ttWin.winDnsBackup for completing post update backup...")
            ttWin.winDnsBackup("afterUpdate", host_windns, zone_NYC_long, host_winsys_password, host_winsys_username, host_winsys)
            my_logger.info("Post update win_dns_backup successful...")
            print("Post update win_dns_backup successful...")
        except:
            e = sys.exc_info()[0]
            my_logger.info(sys.exit("Error Encountered: %s.Adding records failed! Exiting..." % e))
            #sys.exit(-1) 
       
        my_logger.info( "In dnsOps...calling ttAWS.cli53_export for backing up current AWS DNS Data...")
        ttAWS.cli53_export(zone_AWS_long, dir_backup, '/afterUpdate.txt')
    
        my_logger.info("After Update cli53 Backup Successful")
        print("Logfile location: %s"%dir_backup + "/dnsOps.log")
        
    else:
        my_logger.error(sys.exit("Confirmation not received. Received information: %s. Exiting..." % confirm_backup))
        print("Logfile location: %s"%dir_backup + "/dnsOps.log")


main()

