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
from pprint import pprint
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
    sys.exit("Download of ttSettings from S3 failed. Exiting! Error Encountered")

PATH_ROOT = dict_ttSettings["PATH_ROOT"]
PATH_LOG = PATH_ROOT + dict_ttSettings["PATH_LOG"]

import logging
import logging.handlers

import ttLib.logger as logger
import ttLib.ttSys as ttSys
#level1='logging.INFO'
#print PATH_ROOT + PATH_LOG + "/dns"
(dir_backup, timestamp) = dirCreateTimestamp(PATH_LOG + "/dns")
print"Backup and logging directory created successfully..."       
my_logger = logging.getLogger('dnsOps')
for h in my_logger.handlers:
    my_logger.removeHandler(h)
log_file = dir_backup + '/dnsOps.log'
loglevel = dict_ttSettings["log_level"].upper()
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
    ttWin.findWinZones(zone_NYC_long, host_windns, host_winsys_password, host_winsys_username, host_winsys)
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
    
    """Collect of list of items in currAWS dict and Future Aws Dict"""
    
    list_dict_AWS = list()
    list_currAws = list()
    for k,v in currAws.items():
        list_currAws.append(k)
    for k,v in dict_AWS.items():
        list_dict_AWS.append(k)
    
    
    """Create a consolidated list of records from current and future lists""" 
  
    list_total=list()
    for l in list_currAws:
        if l not in list_total:
            list_total.append(l)
    
    for l in list_dict_AWS:
        if l not in list_total:
            list_total.append(l)
    
    """Append Dummy entries for missing entries in curr AWS"""
    
    for l in list_total:
        if l not in currAws:
            currAws[l] = "No current value" 
    
    
            
    """Readable format dict"""
    
    read_currAws = {}
    
    for k, v in currAws.items():
        k1 = ''.join(str(e1) for e1 in k)
        v1 = ''.join(str(e2) for e2 in v)
        read_currAws[k1] = v1
        
    read_dict_AWS = {}
    
    for k, v in dict_AWS.items():
        k1 = ''.join(str(e1) for e1 in k)
        v1 = ''.join(str(e2) for e2 in v)
        read_dict_AWS[k1] = v1
    
    """Generate a comparison dictionary and set values as array"""
    
    compare_aws_dict={}
    dict_curr_rr_type={}
    dict_future_rr_type={}
    
    for l in list_total:
        dict_curr_rr_type[l] = ttAWS.find_rrtype(zone_AWS_long, l)
    
    for l in list_total:
        if "fp-lb-1" in l:
            dict_future_rr_type[l] = "ALIAS"
        else:
            dict_future_rr_type[l] = "CNAME"
    
    #print "Dict_rrtype:%s"%dict_rr_type 
    
    for l in list_total:
        l1 = ''.join(str(e1) for e1 in l)
        compare_aws_dict.setdefault(l1, [])
    
    for k, v in dict_curr_rr_type.items():
        for k1, v1 in compare_aws_dict.items():
            if k == k1:
                compare_aws_dict[k1].append(v)
    
    for k, v in read_currAws.items():
        for k1, v1 in compare_aws_dict.items():
            if k == k1:
                compare_aws_dict[k1].append(v)
    
    for k, v in dict_future_rr_type.items():
        for k1, v1 in compare_aws_dict.items():
            if k == k1:
                compare_aws_dict[k1].append(v)
    
    #print read_dict_AWS
                
    for k1, v1 in compare_aws_dict.items():
        if k1 in list_dict_AWS:
            for k, v in read_dict_AWS.items():
                if k == k1:
                    compare_aws_dict[k1].append(v)
        elif k1 not in list_dict_AWS:
            compare_aws_dict[k1].append(v1[1])
    
    compare_nyc_dict = dict()
    list_dict_NYC = list()
    for k,v in dict_NYC.items():
        list_dict_NYC.append(k)
    
    for l in list_dict_NYC:
        l1 = ''.join(str(e1) for e1 in l)
        compare_nyc_dict.setdefault(l1, [])
    
    for k,v in dict_NYC.items():
        if ENV_NAME in k:
            (outWinEnum, errWinEnum, currRecWinEnum) = ttWin.enumRecords(k, host_windns, zone_NYC_long, host_winsys_password, host_winsys_username, host_winsys)
            my_logger.debug("Output: %s: %s"%(v,currRecWinEnum))
            if "DNS_ERROR_NAME_DOES_NOT_EXIST" in outWinEnum.upper():
                recordCheck.update({k: 'Record not found'})
            elif v.upper() in outWinEnum.upper():
                recordCheck.update({k: 'Record status current'})
            elif v.upper() not in outWinEnum.upper():
                recordCheck.update({k: 'Record status old'})
                dictRecDelete[k] = currRecWinEnum
            elif ("FAILED" in outWinEnum.upper()) or ("FAILED" in errWinEnum.upper()) or ("FAILED" in currRecWinEnum.upper()):
                my_logger.error("Error encountered in Enum Records. Exiting!")
                sys.exit("Error encountered in Enum Records. Exiting!")
    dictTemp = recordCheck.copy()
    my_logger.debug("Current records status:%s" % recordCheck)
    
    dict_add_aws_record = {}
    dict_del_aws_record = {}
    
    for k, v in compare_aws_dict.items():
        if ("fp-lb-1" in k):
            if (v[0] == "CNAME"):
                dict_add_aws_record[k] = v[3]
                dict_del_aws_record[k] = v[0]
            elif (v[0] == "ALIAS") and (v[1].upper() != v[3].upper()):
                dict_add_aws_record[k] = v[3]
                dict_del_aws_record[k] = v[0]
            elif (v[0] == None):
                dict_add_aws_record[k] = v[3]
        elif (v[0] == "CNAME") and (v[1].upper() != v[3].upper()):
            dict_add_aws_record[k] = v[3]
        elif ("fp-lb-1" not in k) and (v[0]) == None:
            dict_add_aws_record[k] = v[3]
    
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
                my_logger.error("Error encountered in Enum Records. Exiting!")
                sys.exit("Error encountered in Enum Records. Exiting!")
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
    my_logger.debug("dictRecAdd:%s"% str(dictRecAdd))
    my_logger.debug("dictRecCorrect: %s"%str(dictRecCorrect))
    my_logger.debug("dictRecDelete: %s"%str(dictRecDelete))
    
    if (dict_del_aws_record) or (dict_add_aws_record):
        print "Route53 updates required. Find the current and future values as follows:"
        my_logger.debug("Route53 updates required. Find the current and future values as follows:")
    else:
        print "No Route53 updates required. Find the current and future values as follows:"
        my_logger.debug("No Route53 updates required. Find the current and future values as follows:")
    
    print '{:30s} {:15s} {:85s} {:15s} {:85s}'.format("RECORD NAME", "CURR R_TYPE", "CURRENT VALUE", "FUTURE R_TYPE", "FUTURE VALUE")
    print '{:30s} {:15s} {:85s} {:15s} {:85s}'.format("-----------", "-----------", "-------------", "-------------", "------------")
    for k, v in compare_aws_dict.items():
        print '{:30s} {:15s} {:85s} {:15s} {:85s}'.format(str(k), str(v[0]), str(v[1]), str(v[2]), str(v[3]))
    print "\n"    
    
    my_logger.debug("NYC DNS Updates:: %s" % str(dictRecCorrect))
    
    if dictRecCorrect:
        print "\nNYC DNS Updates:"
        my_logger.debug("\nNYC DNS Updates: \nThe following records are up-to-date for NYC DNS.")
        my_logger.debug("dictRecCorrect: %s" %dictRecCorrect)
        print "\nThe following records are up-to-date for NYC DNS:"
        for k in sorted(dictRecCorrect):
            print "[CNAME RECORD]::{:40s}  [VALUE]::{:85s}" .format(str(k), str(dictRecCorrect[k]))
            
        
    my_logger.debug("\nDelete the following records for NYC DNS: %s" % str(sorted(dictRecDelete)))
    if dictRecDelete:
        print "\nDelete the following records for NYC DNS:"
        for k in sorted(dictRecDelete):
            print "[CNAME RECORD]::{:40s}  [VALUE]::{:65s}" .format(str(k), str(dictRecDelete[k]))
               
            
    my_logger.debug("\nAdd the following records for NYC DNS:%s" % str(sorted(dictRecAdd)))
    if dictRecAdd:
        print "\nAdd the following records for NYC DNS:" 
        for k in sorted(dictRecAdd):
            print "[CNAME RECORD]::{:40s}  [FUTURE VALUE]::{:85s}" .format(str(k), str(dictRecAdd[k]))
            
            
    confirm_backup = raw_input("\nUpdate %s DNS records above in AWS Route 53 DNS and NYC Office DNS [y/n]:"%ENV_NAME)
    if confirm_backup == 'y' or confirm_backup == 'Y':
        
        print("In dnsOps...calling ttAWS.cli53_export for backing up current AWS DNS Data...")
        my_logger.debug("In dnsOps...calling ttAWS.cli53_export for backing up current AWS DNS Data...")
        ttAWS.cli53_export(zone_AWS_long, dir_backup, '/beforeUpdate.txt')
    
        print("Before Update AWS Backup Successful")
        my_logger.debug("Before Update AWS Backup Successful")
        
        
        try:
            print "Delete AWS Records: "
            pprint(dict_del_aws_record)
            for k,v in dict_del_aws_record.items():
                print "Deleting old %s record"%v
                ttAWS.del_cname(zone_AWS_long, k, v)
        except:
            my_logger.info("Error Encountered. Delete AWS Records failed! Exiting..." )
            sys.exit("Error Encountered. Delete AWS Records failed! Exiting..." )
            
        try:
            print "dict_add_aws_record: "
            pprint(dict_add_aws_record)
            my_logger.info( "In dnsOps...calling ttAWS.dnsUpdate!")
            out_aws_update = ttAWS.dnsUpdate(ENV_NAME, zone_Id, dict_add_aws_record, zone_AWS_long)
            my_logger.info(out_aws_update)
            my_logger.info("AWS Import successful")
            print("AWS Import successful")
        except:
            my_logger.info("Error Encountered. AWS Import failed! Exiting..." )
            sys.exit("Error Encountered. AWS Import failed! Exiting..." )
    
        my_logger.info("Backing up NYC current records...")
        
        try:
            my_logger.info( "In dnsOps...calling ttWin.winDnsBackup!")
            ttWin.winDnsBackup("beforeUpdate", host_windns, zone_NYC_long, host_winsys_password, host_winsys_username, host_winsys)
            my_logger.info("Back up of current NYC records succeeded...")
            print("Back up of current NYC records succeeded...")
        except:
            my_logger.info("Error Encountered.Backup of records failed! Exiting..." )
            sys.exit("Error Encountered.Backup of records failed! Exiting..." )
        
        try:
            my_logger.info( "In dnsOps...calling ttWin.deleteRecords!")
            (cmd, out_del_rec) = ttWin.deleteRecords(dictRecDelete, host_windns, zone_NYC_long, host_winsys_password, host_winsys_username, host_winsys)
            cmd = str(cmd)
            out_del_rec = str(out_del_rec)
            my_logger.info("Command: %s, Out_del_rec: %s"%(cmd,out_del_rec))
            my_logger.info("Delete of old records complete...")
            print("Delete of old records complete...")
            
        except:
            
            my_logger.info("Error Encountered.Deleting records failed! Exiting...")
            sys.exit("Error Encountered.Deleting records failed! Exiting...")
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
            
            my_logger.info("Error Encountered.Adding records failed! Exiting...")
            sys.exit("Error Encountered.Adding records failed! Exiting...")
            #sys.exit(-1)   
        
        try:
            my_logger.info( "In dnsOps...calling ttWin.winDnsBackup for completing post update backup...")
            ttWin.winDnsBackup("afterUpdate", host_windns, zone_NYC_long, host_winsys_password, host_winsys_username, host_winsys)
            my_logger.info("Post update win_dns_backup successful...")
            print("Post update win_dns_backup successful...")
        except:
            
            my_logger.info("Error Encountered.Adding records failed! Exiting...")
            sys.exit("Error Encountered.Adding records failed! Exiting...")
            #sys.exit(-1) 
       
        my_logger.info( "In dnsOps...calling ttAWS.cli53_export for backing up current AWS DNS Data...")
        ttAWS.cli53_export(zone_AWS_long, dir_backup, '/afterUpdate.txt')
    
        my_logger.info("After Update cli53 Backup Successful")
        print("Logfile location: %s"%dir_backup + "/dnsOps.log")
        
    else:
        my_logger.error("Confirmation not received. Received information: %s. Exiting..." % confirm_backup)
        sys.exit("Confirmation not received. Received information: %s. Exiting..." % confirm_backup)
        print("Logfile location: %s"%dir_backup + "/dnsOps.log")
    

main()

