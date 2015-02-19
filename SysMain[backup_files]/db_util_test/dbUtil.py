#!/usr/bin/env python
#
#Author: Sudha Kanupuru
#Description: Database Copy and Swap

"""********************************************************************************************************************""" 
print "Initializing setup..."
"""Import sys, re, os...START"""
#print "Importing python libraries..."
try:
    print "Importing \'sys\'...",
    import sys
except:
    raise SystemExit("sys import failed. Exiting!")
print "successful."
try:
    print "Importing \'os\'...",
    import os
except:
    sys.exit("os import failed. Exiting!")
print "successful."
try:
    print "Importing \'platform\'...",
    import platform
except:
    sys.exit("platform import failed. Exiting!")
print "successful."
from pprint import pprint
print "Importing python libraries successful!"

"""Import sys, re...STOP"""
"""********************************************************************************************************************"""
"""Download ttSettings.conf from S3...START"""

python_root=os.path.dirname(os.path.realpath(__file__)) + "/"
sys.path.append(python_root + "ttLib")

from ttLib.ttSys import get_s3_config
#print "Downloading ttSettings from S3...",

try:
    s3_config_foldername="nyc-sys"
    print s3_config_foldername
    s3_config_filename = "ttSettings.conf"
    print s3_config_filename
    global dict_sys_ttSettings
    dict_sys_ttSettings = get_s3_config(s3_config_foldername, s3_config_filename)
except:
    sys.exit("Error Encountered. Download of ttSettings from S3 failed. Exiting!")

print "Downloading ttSettings from S3 successful!"
PATH_ROOT = dict_sys_ttSettings["PATH_ROOT"]


"""Download ttSettings.conf from S3...STOP"""
"""********************************************************************************************************************"""
""" Create a timestamp directory to store log files...START"""
    
try:
    from ttLib.ttSys import dirCreateTimestamp
except:
    sys.exit("Error Encountered. \"from ttLib.ttSys import dirCreateTimestamp\"...failed. Exiting!")
     
_aws_swap_dir = PATH_ROOT + dict_sys_ttSettings["PATH_LOG"] + "/dbUtil"
global swapTimeStampDirPath
(swapTimeStampDirPath, timestamp) = dirCreateTimestamp(_aws_swap_dir)



""" Create a timestamp directory to store log files...STOP"""
"""********************************************************************************************************************"""
""" Implement Logging...START"""

print "Setting up logging..."
try:
    import logging
except:
    sys.exit("Error Encountered: \"import logging\"...failed. Exiting!" )
try:
    from ttLib.logger import get_handlers 
except:
    sys.exit("Error Encountered: \"from ttLib.logger import get_handlers\"...failed. Exiting!")

    
log_handler_name='dbUtil'
my_logger = logging.getLogger(log_handler_name)
for h in my_logger.handlers:
    my_logger.removeHandler(h)
log_file_name='dbUtil.log'
log_file_abs_path = swapTimeStampDirPath + "/" + log_file_name
#print"log_file_abs_path: ",log_file_abs_path
loglevel = dict_sys_ttSettings["log_level"].upper()
print"Received loglevel: %s"%loglevel

loglevel = getattr(logging, loglevel.upper())
for h in get_handlers(log_handler_name, log_file_abs_path):
    my_logger.addHandler(h)
my_logger.setLevel(loglevel)
my_logger.debug("Logger created successfully. Continuing...")
my_logger.debug("dict_sys_ttSettings: %s"%dict_sys_ttSettings)
print("Logging configured successfully!")
my_logger.info("Logging configured successfully. Initiating logging...")

print "\nLogfile Location: %s\n"%log_file_abs_path
try:
    from ttLib.ttAWS import env_ttSettings
except:
    my_logger.error("\'from ttLib.ttAWS import env_ttSettings\'...failed. Exiting!")
    sys.exit("\'from ttLib.ttAWS import env_ttSettings\' failed. Exiting!")
my_logger.info("\'from ttLib.ttAWS import env_ttSettings\' successful!")

""" Implement Logging...STOP"""
"""********************************************************************************************************************"""
"""ttDB Imports...START"""

try:
    from ttLib.ttDB import set_params, set_host 
except:
    my_logger.error("\'from ttLib.ttDB import validate_args\'...failed. Exiting!")
    sys.exit("\'from ttLib.ttDB import validate_args\'...failed. Exiting!")
my_logger.info("\'from ttLib.ttDB import validate_args\' successful!")
    
"""ttDB Imports STOP"""
"""********************************************************************************************************************"""
"""Mysql Imports...START"""

try:
    from ttLib.ttMysql import validate_mysql_accounts, mysql_operations, mysql_backup, mysql_restore
except:
    my_logger.error("\'from ttLib.ttMysql import set_mysql_host, validate_mysql_nyc_accounts, validate_mysql_aws_accounts, validate_mysql_source, mysql_operations\'...failed. Exiting!")
    sys.exit("\'from ttLib.ttMysql import set_mysql_host, validate_mysql_nyc_accounts, validate_mysql_aws_accounts, validate_mysql_source, mysql_operations\'...failed. Exiting!")
my_logger.info("\'from ttLib.ttMysql import set_mysql_host, validate_mysql_nyc_accounts, validate_mysql_aws_accounts, validate_mysql_source, mysql_operations\' successful!")

my_logger.info("Setting swapTimeStampDirPath: %s in ttMysql.py"%swapTimeStampDirPath)
from ttLib import ttMysql
ttMysql.swapTimeStampDirPath= swapTimeStampDirPath

"""Mysql Imports...STOP"""
"""********************************************************************************************************************"""
"""MongoDB Imports...START"""

try:
    from ttLib.ttMongoDB import mongodb_operations, validate_mongodb_accounts, set_mongodb_host, mongodb_backup, mongo_restore
except:
    my_logger.error("\'from ttLib.ttMongoDB import mongo_operations, validate_mongodb_target_accounts, validate_mongodb_source_accounts, set_mongodb_host\'...failed. Exiting!")
    sys.exit("\'from ttLib.ttMongoDB import mongo_operations, validate_mongodb_target_accounts, validate_mongodb_source_accounts, set_mongodb_host\'...failed. Exiting!")
my_logger.info("\'from ttLib.ttMongoDB import mongo_operations, validate_mongodb_target_accounts, validate_mongodb_source_accounts, set_mongodb_host\' successful!")

my_logger.info("Setting swapTimeStampDirPath: %s in ttMongoDB.py"%swapTimeStampDirPath)
from ttLib import ttMongoDB
ttMongoDB.swapTimeStampDirPath= swapTimeStampDirPath

"""MongoDB Imports...STOP"""
"""********************************************************************************************************************"""
"""Neo4j Imports...START"""
try:
    from ttLib import ttNeo
except:
    my_logger.error("\'from ttLib import ttNeo\'...failed. Exiting!")
    sys.exit("\'from ttLib import ttNeo\'...failed. Exiting!")
my_logger.info("\'from ttLib import ttNeo\' successful!")

try:
    from ttLib.ttNeo import validate_neo4j_accounts, neo4j_operations, neo4j_backup, neo4j_restore
except:
    my_logger.error("\'from ttLib.ttNeo import set_neo4j_host, validate_neo4j_nyc_target, validate_neo4j_aws_accounts, neo4j_operations\'...failed. Exiting!")
    sys.exit("\'from ttLib.ttNeo import set_neo4j_host, validate_neo4j_nyc_target, validate_neo4j_aws_accounts, neo4j_operations\'...failed. Exiting!")
my_logger.info("\'from ttLib.ttNeo import set_neo4j_host, validate_neo4j_nyc_target, validate_neo4j_aws_accounts, neo4j_operations\' successful!")

my_logger.info("Setting swapTimeStampDirPath: %s in ttNeo.py"%swapTimeStampDirPath)
ttNeo.swapTimeStampDirPath= swapTimeStampDirPath

"""Neo4j Imports...STOP"""
"""********************************************************************************************************************"""
"""Virtuoso Imports...START"""

try:
    from ttLib import ttVirtuoso 
except:
    my_logger.error("\'from ttLib import ttVirtuoso\'...failed. Exiting!")
    sys.exit("\'from ttLib import ttVirtuoso\'...failed. Exiting!")
my_logger.info("\'from ttLib import ttVirtuoso\' successful!")


try:
    from ttLib.ttVirtuoso import set_virtuoso_host, validate_virtuoso_accounts, validate_virtuoso_nyc_accounts, virtuoso_operations, virtuoso_backup
except:
    my_logger.error("\'from ttLib.ttVirtuoso import print1\'...failed. Exiting!")
    sys.exit("\'from ttLib.ttVirtuoso import print1\'...failed. Exiting!")
my_logger.info("\'from ttLib.ttVirtuoso import print1\' successful!")

my_logger.info("Setting swapTimeStampDirPath: %s in ttVirtuoso.py"%swapTimeStampDirPath)
ttVirtuoso.dirTimestamp = swapTimeStampDirPath
ttVirtuoso.timestamp = timestamp

"""Virtuoso Imports...STOP"""
"""********************************************************************************************************************"""
""" Import ttLx...START """

try:
    from ttLib.ttLx import callSubprocess
except:
    my_logger.error("Error encountered. \"from ttLib.ttLx import callSubprocess\"...failed. Exiting!")
    sys.exit("Error encountered. \"from ttLib.ttLx import callSubprocess\"...failed. Exiting!")
my_logger.info("\"from ttLib.ttLx import callSubprocess\"...successful")

""" Import ttLx...STOP """
"""********************************************************************************************************************"""
""" Import ttAWS...START """

try:
    from ttLib.ttAWS import findSourceSuffix, validate_cert_loc
except:
    my_logger.error("Error encountered. \"from ttLib.ttAWS import findSourceSuffix, validate_cert_loc\"...failed. Exiting!")
    sys.exit("Error encountered. \"from ttLib.ttAWS import findSourceSuffix, validate_cert_loc\"...failed. Exiting!")
my_logger.info("\"from ttLib.ttAWS import findSourceSuffix, validate_cert_loc\"...successful")

from ttLib.ttAWS import collect_security_groups_info, authorize_security_groups, revoke_security_groups, test_grants

my_logger.info("Importing 3top libraries (ttLib) successful!") 
    
"""Import ttAWS...STOP"""
"""********************************************************************************************************************"""

print "Initial setup complete..."

def validate_mysql(dict_source_ttSettings, dict_target_ttSettings, hosts):
    if (dict_source_ttSettings != 'NA') and (dict_target_ttSettings == 'NA'):
        source_host = hosts['mysql_source_host']
        my_logger.debug("Source_host: %s"%source_host)
        my_logger.debug("Calling \'validate_mysql_accounts(dict_source_ttSettings, %s)\' "%source_host)
        validate_mysql_accounts(dict_source_ttSettings, source_host)
        my_logger.debug("Source_host: %s mysql accounts validated successfully!"%source_host)
    
    elif (dict_target_ttSettings != 'NA') and (dict_source_ttSettings == 'NA'):
        target_host = hosts['mysql_target_host']
        my_logger.debug("Target_host: %s"%target_host)
        my_logger.debug("Calling \'validate_mysql_accounts(dict_target_ttSettings, %s)\' "%target_host)
        validate_mysql_accounts(dict_target_ttSettings, target_host)
        my_logger.debug("Target_host: %s mysql accounts validated successfully!"%target_host)
    
    elif (dict_target_ttSettings != 'NA') and (dict_source_ttSettings != 'NA'):
        source_host = hosts['mysql_source_host']
        my_logger.debug("Source_host: %s"%source_host)
        my_logger.debug("Calling \'validate_mysql_accounts(dict_source_ttSettings, %s)\' "%source_host)
        validate_mysql_accounts(dict_source_ttSettings, source_host)
        my_logger.debug("Source_host: %s mysql accounts validated successfully!"%source_host)
        target_host = hosts['mysql_target_host']
        my_logger.debug("Target_host: %s"%target_host)
        my_logger.debug("Calling \'validate_mysql_accounts(dict_target_ttSettings, %s)\' "%target_host)
        validate_mysql_accounts(dict_target_ttSettings, target_host)
        my_logger.debug("Target_host: %s mysql accounts validated successfully!"%target_host)
    
    elif (dict_target_ttSettings == 'NA') and (dict_source_ttSettings == 'NA'):
        my_logger.error("Both dict_source_ttSettings and dict_target_ttSettings found as 'NA'. Exiting!")
        sys.exit("Both dict_source_ttSettings and dict_target_ttSettings found as 'NA'. Exiting!")
    
    else:
        my_logger.error("Unexpected error encountered in validate_mysql(). Exiting!")
        sys.exit("Unexpected error encountered in validate_mysql(). Exiting!")
        
def validate_mongodb(dict_source_ttSettings, dict_target_ttSettings, hosts):
    if (dict_source_ttSettings != 'NA') and (dict_target_ttSettings == 'NA'):
        source_host = hosts['mongodb_source_host']
        my_logger.debug("Source_host: %s"%source_host)
        my_logger.debug("Calling \'validate_mongodb_accounts(dict_source_ttSettings, %s)\' "%source_host)
        validate_mongodb_accounts(dict_source_ttSettings, source_host)
        my_logger.debug("Source_host: %s mongodb accounts validated successfully!"%source_host)
    
    elif (dict_target_ttSettings != 'NA') and (dict_source_ttSettings == 'NA'):
        target_host = hosts['mongodb_target_host']
        my_logger.debug("Target_host: %s"%target_host)
        my_logger.debug("Calling \'validate_mongodb_accounts(dict_target_ttSettings, %s)\' "%target_host)
        validate_mongodb_accounts(dict_target_ttSettings, target_host)
        my_logger.debug("Target_host: %s mongodb accounts validated successfully!"%target_host)
    
    elif (dict_target_ttSettings != 'NA') and (dict_source_ttSettings != 'NA'):
        source_host = hosts['mongodb_source_host']
        my_logger.debug("Source_host: %s"%source_host)
        my_logger.debug("Calling \'validate_mongodb_accounts(dict_source_ttSettings, %s)\' "%source_host)
        validate_mongodb_accounts(dict_source_ttSettings, source_host)
        my_logger.debug("Source_host: %s mongodb accounts validated successfully!"%source_host)
        target_host = hosts['mongodb_target_host']
        my_logger.debug("Target_host: %s"%target_host)
        my_logger.debug("Calling \'validate_mongodb_accounts(dict_target_ttSettings, %s)\' "%target_host)
        validate_mongodb_accounts(dict_target_ttSettings, target_host)
        my_logger.debug("Target_host: %s mongodb accounts validated successfully!"%target_host)
    
    elif (dict_target_ttSettings == 'NA') and (dict_source_ttSettings == 'NA'):
        my_logger.error("Both dict_source_ttSettings and dict_target_ttSettings found as 'NA'. Exiting!")
        sys.exit("Both dict_source_ttSettings and dict_target_ttSettings found as 'NA'. Exiting!")
    
    else:
        my_logger.error("Unexpected error encountered in validate_mysql(). Exiting!")
        sys.exit("Unexpected error encountered in validate_mysql(). Exiting!")

def validate_neo4j(dict_source_ttSettings, dict_target_ttSettings, hosts):
    if (dict_source_ttSettings != 'NA') and (dict_target_ttSettings == 'NA'):
        source_host = hosts['neo4j_source_host']
        my_logger.debug("Source_host: %s"%source_host)
        my_logger.debug("Calling \'validate_neo4j_accounts(dict_source_ttSettings, %s)\' "%source_host)
        s_user = validate_neo4j_accounts(dict_source_ttSettings, source_host)
        my_logger.debug("Source_host: %s neo4j accounts validated successfully!"%source_host)
        t_user = 'NA'
    
    elif (dict_target_ttSettings != 'NA') and (dict_source_ttSettings == 'NA'):
        target_host = hosts['neo4j_target_host']
        my_logger.debug("Target_host: %s"%target_host)
        my_logger.debug("Calling \'validate_neo4j_accounts(dict_target_ttSettings, %s)\' "%target_host)
        t_user = validate_neo4j_accounts(dict_target_ttSettings, target_host)
        my_logger.debug("Target_host: %s neo4j accounts validated successfully!"%target_host)
        s_user = 'NA'
    
    elif (dict_target_ttSettings != 'NA') and (dict_source_ttSettings != 'NA'):
        source_host = hosts['neo4j_source_host']
        my_logger.debug("Source_host: %s"%source_host)
        my_logger.debug("Calling \'validate_neo4j_accounts(dict_source_ttSettings, %s)\' "%source_host)
        s_user = validate_neo4j_accounts(dict_source_ttSettings, source_host)
        my_logger.debug("Source_host: %s neo4j accounts validated successfully!"%source_host)
        target_host = hosts['neo4j_target_host']
        my_logger.debug("Target_host: %s"%target_host)
        my_logger.debug("Calling \'validate_neo4j_accounts(dict_target_ttSettings, %s)\' "%target_host)
        t_user = validate_neo4j_accounts(dict_target_ttSettings, target_host)
        my_logger.debug("Target_host: %s neo4j accounts validated successfully!"%target_host)
    
    elif (dict_target_ttSettings == 'NA') and (dict_source_ttSettings == 'NA'):
        my_logger.error("Both dict_source_ttSettings and dict_target_ttSettings found as 'NA'. Exiting!")
        sys.exit("Both dict_source_ttSettings and dict_target_ttSettings found as 'NA'. Exiting!")
    
    else:
        my_logger.error("Unexpected error encountered in validate_neo4j(). Exiting!")
        sys.exit("Unexpected error encountered in validate_neo4j(). Exiting!")
    
    return s_user, t_user

def validate_virtuoso(dict_source_ttSettings, dict_target_ttSettings, hosts):
    if (dict_source_ttSettings != 'NA') and (dict_target_ttSettings == 'NA'):
        source_host = hosts['virtuoso_source_host']
        my_logger.debug("Source_host: %s"%source_host)
        my_logger.debug("Calling \'validate_virtuoso_accounts(dict_source_ttSettings, %s, \'source\')\' "%source_host)
        s_user = validate_virtuoso_accounts(dict_source_ttSettings, source_host, 'source')
        my_logger.debug("Source_host: %s virtuoso accounts validated successfully!"%source_host)
        t_user = 'NA'
    
    elif (dict_target_ttSettings != 'NA') and (dict_source_ttSettings == 'NA'):
        target_host = hosts['virtuoso_target_host']
        my_logger.debug("Target_host: %s"%target_host)
        my_logger.debug("Calling \'validate_virtuoso_accounts(dict_target_ttSettings, %s)\' "%target_host)
        t_user = validate_virtuoso_accounts(dict_target_ttSettings, target_host, 'target')
        my_logger.debug("Target_host: %s virtuoso accounts validated successfully!"%target_host)
        s_user = 'NA'
    
    elif (dict_target_ttSettings != 'NA') and (dict_source_ttSettings != 'NA'):
        source_host = hosts['virtuoso_source_host']
        my_logger.debug("Source_host: %s"%source_host)
        my_logger.debug("Calling \'validate_virtuoso_accounts(dict_source_ttSettings, %s, \'source\')\' "%source_host)
        s_user = validate_virtuoso_accounts(dict_source_ttSettings, source_host, 'source')
        my_logger.debug("Source_host: %s virtuoso accounts validated successfully!"%source_host)
        target_host = hosts['virtuoso_target_host']
        my_logger.debug("Target_host: %s"%target_host)
        my_logger.debug("Calling \'validate_virtuoso_accounts(dict_target_ttSettings, %s)\' "%target_host)
        t_user = validate_virtuoso_accounts(dict_target_ttSettings, target_host, 'target')
        my_logger.debug("Target_host: %s virtuoso accounts validated successfully!"%target_host)
    
    elif (dict_target_ttSettings == 'NA') and (dict_source_ttSettings == 'NA'):
        my_logger.error("Both dict_source_ttSettings and dict_target_ttSettings found as 'NA'. Exiting!")
        sys.exit("Both dict_source_ttSettings and dict_target_ttSettings found as 'NA'. Exiting!")
    
    else:
        my_logger.error("Unexpected error encountered in validate_virtuoso(). Exiting!")
        sys.exit("Unexpected error encountered in validate_virtuoso(). Exiting!")
        
    return s_user, t_user
"""********************************************************************************************************************"""
"""main() Module definition... START"""

def main():
    my_logger.info("\nInitial setup complete...")
    my_logger.info("In main()...")
    
    param_list = sys.argv
    
    if len(param_list) == 1:
        my_logger.error("No arguments found. Exiting!")
        print("No arguments found. Required format: python dbUtil.py -s <SOURCE_ENV> -t <TARGET_ENV> -o <DB_OPERATION> -d <DB_SERVICE> [optional: -p <PROD_OVERRIDE>] \n or python dbUtil.py -o swap -d <DB_SERVICE> [optional: -p <PROD_ENV>].")
        sys.exit("Please use correct format and run again. Exiting!")
    elif len(param_list) < 5:
        my_logger.error("The number of arguments are too few. Exiting!")
        print("The number of arguments are too few. Required format: python dbUtil.py -s <SOURCE_ENV> -t <TARGET_ENV> -o <DB_OPERATION> -d <DB_SERVICE> [optional: -p <PROD_OVERRIDE>] \n or python dbUtil.py -o swap -d <DB_SERVICE> [optional: -p <PROD_ENV>].")
        sys.exit("Please use correct format and run again. Exiting!")
    else:
        db_dict = set_params(param_list)
        my_logger.info("Received:: db_dict: %s"%db_dict)
    
    #print "Received db_dict: %s"%db_dict
    
    db_service = db_dict['db_service']
    db_operation = db_dict['db_operation']
    
    if (db_dict['db_operation'] == 'copy') or (db_dict['db_operation'] == 'swap'):
        source_env = db_dict['source_env']
        target_env = db_dict['target_env']
    
    elif db_dict['db_operation'] == 'backup':
        source_env = db_dict['source_env']
        target_dir = db_dict['target_dir']
    
    elif db_dict['db_operation'] == 'restore':
        source_dir = db_dict['source_dir']
        target_env = db_dict['target_env']
    
    """Collecting arguments into variables...STOP"""
    """****************************************************************************************************************"""
    """Download of ttSettings based on source and target env...START"""
    
    if (db_operation == 'copy') or (db_operation == 'swap') or (db_operation == 'backup'):
        my_logger.info("Calling \"dict_source_ttSettings = env_ttSettings(%s)"%(source_env))
        dict_source_ttSettings = env_ttSettings(source_env)
        my_logger.info("Received dict_source_ttSettings = %s"%dict_source_ttSettings)
    
    if (db_operation == 'restore'):
        dict_source_ttSettings = 'NA'
        my_logger.info("Received dict_source_ttSettings = %s"%dict_source_ttSettings)
    
    if (db_operation == 'backup'):
        dict_target_ttSettings = 'NA'
        my_logger.info("Received dict_target_ttSettings = %s"%dict_target_ttSettings)
    
    if (db_operation == 'copy') or (db_operation == 'swap') or (db_operation == 'restore'):
        my_logger.info("Calling \"dict_target_ttSettings = env_ttSettings(%s)"%(target_env))
        dict_target_ttSettings = env_ttSettings(target_env)
        my_logger.info("Received dict_target_ttSettings = %s"%dict_target_ttSettings)
    
    """Download of ttSettings based on source and target env...STOP""" 
    """****************************************************************************************************************"""
    """ validate AWS CERT location...START"""
    
    """*******************
    SOURCE_ENV
    *******************"""
    if (db_operation == 'copy') or (db_operation == 'swap') or (db_operation == 'backup'):
        if ("aws.3top.com" in source_env):
            aws_cert = os.path.expanduser(dict_source_ttSettings["ec2"]["cert_private_key"])
            my_logger.info("Received \"aws.3top.com\" in %s"%source_env)
            my_logger.info("Calling validate_cert_loc() for aws_source_cert: %s"%(aws_cert))
            validate_cert_loc(aws_cert)
            my_logger.info("validate_cert_loc() with cert_name: %s successful for %s!"%(aws_cert, source_env))
            #my_logger.debug("Inserting aws_source_cert into db_dict...")
            #db_dict['source_cert'] = aws_cert
            
        elif "nyc.3top.com" in source_env:
            my_logger.error("\"aws.3top.com\" not in %s."%source_env)
            my_logger.debug("Inserting \'NA\' into db_dict...")
            db_dict['source_cert'] = 'NA'
    
    """*******************
    TARGET_ENV
    *******************"""   
    if (db_operation == 'copy') or (db_operation == 'swap') or (db_operation == 'restore'):
        if ("aws.3top.com" in target_env):
            aws_cert = os.path.expanduser(dict_target_ttSettings["ec2"]["cert_private_key"])
            my_logger.info("Received \"aws.3top.com\" in %s"%target_env)
            my_logger.info("Calling validate_cert_loc() for aws_target_cert: %s"%(aws_cert))
            validate_cert_loc(aws_cert)
            my_logger.info("validate_cert_loc() with cert_name: %s successful for %s!"%(aws_cert, target_env))
            my_logger.debug("Inserting aws_target_cert into db_dict...")
            db_dict['target_cert'] = aws_cert
        
        elif "nyc.3top.com" in target_env:
            my_logger.info("Received \"nyc.3top.com\" in %s"%target_env)
            my_logger.debug("Inserting \'NA\' into db_dict...")
            db_dict['target_cert'] = 'NA'
    
    """ calling validate_cert_loc()...STOP"""
    """****************************************************************************************************************"""
    """Insert service specific parameters...START"""
    
    services = ['mysql', 'mongodb', 'neo4j', 'virtuoso']
    #for k, v in db_dict.items():
    if db_service == 'all':
        for i in range(1,5):
            db_dict.update({('service' + str(i)) : { 'service_name' : services[i-1]}})
    else:
        for i in services:
            if db_service == i:
                db_dict.update({'service' : {'service_name' : i}})
    
   
            
    """Insert service specific parameters...START""" 
    """****************************************************************************************************************"""
    """ validate AWS MYSQL CERT location...START"""
    
    #pprint(db_dict)
    if (db_operation == 'copy') or (db_operation == 'swap') or (db_operation == 'backup'):
        if ("aws.3top.com" in db_dict['source_env']):
            for k, v in db_dict.items():
                if type(v) is dict:
                    if v['service_name'] == "mysql":
                        my_logger.info("Received \"aws.3top.com\" in %s"%source_env)
                        mysql_cert = os.path.expanduser(dict_source_ttSettings["mysql"]["certs"]["public"])
                        #print mysql_source_cert
                        my_logger.debug("Calling validate_cert_loc() for mysql_source_cert: %s" %(mysql_cert))
                        
                        """The location of cert in target has to be verified as MySQL does direct clone from source to target without need of admin"""
                        validate_cert_loc(mysql_cert)
                        my_logger.info("validate_cert_loc() with mysql_source_cert: %s successful for %s!"%(mysql_cert, source_env))
                        #my_logger.debug("Inserting mysql_source_cert into db_dict...")
                        #v['source_cert'] = mysql_cert
    if (db_operation == 'copy') or (db_operation == 'swap') or (db_operation == 'restore'):                
        if ("aws.3top.com" in db_dict['target_env']):
            for k, v in db_dict.items():
                if type(v) is dict:
                    if v['service_name'] == "mysql":
                        my_logger.info("Received \"aws.3top.com\" in %s"%target_env)
                        mysql_cert = os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"])
                        my_logger.debug("Calling validate_cert_loc() for mysql_target_cert: %s" %(mysql_cert))
                        
                        """The location of cert in target has to be verified as MySQL does direct clone from source to target without need of admin"""
                        validate_cert_loc(mysql_cert)
                        my_logger.info("validate_cert_loc() with mysql_target_cert: %s successful for %s!"%(mysql_cert, target_env))
                        #my_logger.debug("Inserting mysql_source_cert into db_dict...")
                        #v['target_cert'] = mysql_cert
    
    
    #print "after aws_mysql_cert loc()"
    #pprint(db_dict)   
    
    """ validate AWS MYSQL CERT location...STOP"""
    """****************************************************************************************************************"""
    """ calling set_host()...START"""
    
    db_dict = set_host(db_dict)
    #print "after set_host()"
    #pprint(db_dict)
    
    
    """****************************************************************************************************************"""
    """ Validate DB Accounts... START"""
    
    for k, v in db_dict.items():
        #print k, v
        if type(v) is dict:
            if v['service_name'] == 'mysql':
                validate_mysql(dict_source_ttSettings, dict_target_ttSettings, v)
            elif v['service_name'] == 'mongodb':
                validate_mongodb(dict_source_ttSettings, dict_target_ttSettings, v)
            elif v['service_name'] == 'neo4j':
                (neo4j_source_username, neo4j_target_username) = validate_neo4j(dict_source_ttSettings, dict_target_ttSettings, v)
                v['neo4j_source_username'] = neo4j_source_username
                v['neo4j_target_username'] = neo4j_target_username
            elif v['service_name'] == 'virtuoso':
                (virtuoso_source_username, virtuoso_target_username) = validate_virtuoso(dict_source_ttSettings, dict_target_ttSettings, v)
                v['virtuoso_source_username'] = virtuoso_source_username
                v['virtuoso_target_username'] = virtuoso_target_username
    
    """ Validate DB Accounts... STOP"""    
    """****************************************************************************************************************"""
    """ Find current working directory path: python_root...START"""
    
    global python_root
    my_logger.debug("Finding real path (working directory)...")
    python_root = os.path.dirname(os.path.realpath(__file__)) + "/"
    my_logger.debug("python_root:%s"%python_root)
    #print "found."
    
    """ Find current working directory path: python_root...STOP"""
    """********************************************************************************************************************"""
    """ Authorize Security groups...START"""
    my_logger.debug("checking for value of db_operation...")
    
    if db_operation == 'backup':
        my_logger.debug("db_operation received as %s. Setting target_dir as db_dict[\'target_dir\']..."%db_operation)
        target_dir = db_dict['target_dir']
        my_logger.debug("target_dir successfully set as: %s from db_dict[\'target_dir\']"%target_dir)
        my_logger.debug("setting db_dict['\service\'][\'target_dir\'] as target_dir...")
        db_dict['service']['target_dir'] = target_dir
        my_logger.debug("db_dict['\service\'][\'target_dir\'] set as %s"%db_dict['service']['target_dir'])
        my_logger.debug("Deleting db_dict[\'target_dir\']")
        del(db_dict['target_dir'])
        my_logger.debug("db_dict[\'target_dir\'] deleted successfully. Current db_dict: %s"%db_dict)
        
    elif db_operation == 'restore':
        my_logger.debug("db_operation received as %s. Setting source_dir as db_dict[\'source_dir\']..."%db_operation)
        source_dir = db_dict['source_dir']
        my_logger.debug("source_dir successfully set as: %s from db_dict[\'source_dir\']"%source_dir)
        my_logger.debug("setting db_dict['\service\'][\'source_dir\'] as source_dir...")
        db_dict['service']['source_dir'] = source_dir
        my_logger.debug("db_dict['\service\'][\'source_dir\'] set as %s"%db_dict['service']['source_dir'])
        my_logger.debug("Deleting db_dict[\'source_dir\']")
        del(db_dict['source_dir'])
        my_logger.debug("db_dict[\'source_dir\'] deleted successfully. Current db_dict: %s"%db_dict)
        
    elif (db_operation == 'swap') or (db_operation == 'copy'):    
        global region
        my_logger.debug("Collecting region information from dict_sys_ttSettings...")
        region = dict_sys_ttSettings["aws"]["REGION_NAME"]
        my_logger.debug("Received region as : %s"%region)
        
        if db_dict:
            my_logger.debug("Received db_dict not empty...")
            
            for k, v in db_dict.items():
                my_logger.debug("Finding if values of k: %s are dict..."%k)
                
                if type(v) is dict:
                    my_logger.debug("Received value v: %s is dict. Continuing..."%v)
                    my_logger.debug("Finding service_name...")
                    
                    if v['service_name'] == 'mongodb':
                        my_logger.debug("Received service_name as %s"%v['service_name'])
                        my_logger.debug("Collecting source and target hostnames...")
                        source_host = v['mongodb_source_host']
                        my_logger.debug("Received source_host: %s"%source_host)
                        target_host = v['mongodb_target_host']
                        my_logger.debug("Received target_host: %s"%target_host)
                        my_logger.debug("Finding if source_host is aws...")
                        
                        if "aws.3top.com" in source_host:
                            my_logger.debug("Received source_host: %s contains aws.3top.com ...")
                            my_logger.debug("Collecting security group information. Calling \'(m_conn, m_group_name, m_src_s_g_name, m_src_s_g_owner_id, m_ip_prot, m_f_port, m_t_port, m_s_grants) \
                            = collect_security_groups_info(%s, %s, %s)\' ..."%(region, source_host, target_host))
                            
                            try:
                                (m_conn, m_group_name, m_src_s_g_name, m_src_s_g_owner_id, m_ip_prot, m_f_port, m_t_port, m_s_grants) = collect_security_groups_info(region, source_host, target_host)
                            except:
                                my_logger.error("Collecting security group information failed. Exiting!")
                                sys.exit("Collecting security group information failed. Exiting!")
                            
                            my_logger.debug("Security group information collected successfully. ")
                            my_logger.debug("Received m_conn: %s, m_group_name: %s, m_src_s_g_name: %s, m_src_s_g_owner_id: %s, m_ip_prot: %s, m_f_port: %s, m_t_port: %s, m_s_grants: %s\
                            "%(m_conn, m_group_name, m_src_s_g_name, m_src_s_g_owner_id, m_ip_prot, m_f_port, m_t_port, m_s_grants))
                            
                            my_logger.debug("Confirming grants status. Calling \'m_grant_exist = test_grants(%s, %s)\'"%(m_s_grants, m_src_s_g_name))
                            
                            try:
                                m_grant_exist = test_grants(m_s_grants, m_src_s_g_name)
                            except:
                                my_logger.error("Error during test_grants(). Exiting!")
                                sys.exit("Error during test_grants(). Exiting!")
                            
                            my_logger.debug("Grant status received as m_grant_exist: %s"%m_grant_exist)
                            my_logger.debug("Checking if grants exist")
                            
                            if not m_grant_exist:
                                my_logger.debug("No grants found. Authorizing...")
                                print "No grants found! Authorizing..."
                                my_logger.debug("Authorizing security groups to allow access to %s from %s"%(source_host, target_host))
                                my_logger.debug("Calling \'authorize_security_groups(%s, %s, %s, %s, %s, %s, %s)\'"%(m_conn, m_group_name, m_src_s_g_name, m_src_s_g_owner_id, m_ip_prot, m_f_port, m_t_port))
                                
                                try:
                                    authorize_security_groups(m_conn, m_group_name, m_src_s_g_name, m_src_s_g_owner_id, m_ip_prot, m_f_port, m_t_port)
                                except:
                                    my_logger.error("Error encountered during authorize_security_groups for s_host: %s and t_host: %s. Exiting!"%(source_host, target_host))
                                    sys.exit("Error encountered during authorize_security_groups for s_host: %s and t_host: %s. Exiting!"%(source_host, target_host))
                                
                                my_logger.debug("%s has been authorized to pull data from %s"%(target_host, source_host))
                    
                    elif v['service_name'] == 'neo4j':
                        my_logger.debug("Received service_name as %s"%v['service_name'])
                        my_logger.debug("Collecting source and target hostnames...")
                        source_host = v['neo4j_source_host']
                        my_logger.debug("Received source_host: %s"%source_host)
                        target_host = v['neo4j_target_host']
                        my_logger.debug("Received target_host: %s"%target_host)
                        my_logger.debug("Finding if source_host is aws...")
                        
                        if "aws.3top.com" in source_host:
                            my_logger.debug("Received source_host: %s contains aws.3top.com ...")
                            my_logger.debug("Collecting security group information. Calling \'(n_conn, n_group_name, n_src_s_g_name, n_src_s_g_owner_id, n_ip_prot, n_f_port, n_t_port, n_s_grants) \
                            = collect_security_groups_info(%s, %s, %s)\' ..."%(region, source_host, target_host))
                            
                            try:
                                (n_conn, n_group_name, n_src_s_g_name, n_src_s_g_owner_id, n_ip_prot, n_f_port, n_t_port, n_s_grants) = collect_security_groups_info(region, source_host, target_host)
                            except:
                                my_logger.error("Collecting security group information failed. Exiting!")
                                sys.exit("Collecting security group information failed. Exiting!")
                            
                            my_logger.debug("Security group information collected successfully...")
                            my_logger.debug("Received n_conn: %s, n_group_name: %s, n_src_s_g_name: %s, n_src_s_g_owner_id: %s, n_ip_prot: %s, n_f_port: %s, n_t_port: %s, n_s_grants: %s\
                            "%(n_conn, n_group_name, n_src_s_g_name, n_src_s_g_owner_id, n_ip_prot, n_f_port, n_t_port, n_s_grants))
                            
                            my_logger.debug("Confirming grants status. Calling \'n_grant_exist = test_grants(%s, %s)\'"%(n_s_grants, n_src_s_g_name))
                            
                            try:
                                n_grant_exist = test_grants(n_s_grants, n_src_s_g_name)
                            except:
                                my_logger.error("Error during test_grants(). Exiting!")
                                sys.exit("Error during test_grants(). Exiting!")
                            
                            my_logger.debug("Grant status received as n_grant_exist: %s"%n_grant_exist)
                            my_logger.debug("Checking if grants exist")
                            
                            if not n_grant_exist:
                                my_logger.debug("No grants found. Authorizing...")
                                print "No grants found! Authorizing..."
                                my_logger.debug("Authorizing security groups to allow access to %s from %s"%(source_host, target_host))
                                my_logger.debug("Calling \'authorize_security_groups(%s, %s, %s, %s, %s, %s, %s)\'"%(n_conn, n_group_name, n_src_s_g_name, n_src_s_g_owner_id, n_ip_prot, n_f_port, n_t_port))
                                
                                try:
                                    authorize_security_groups(n_conn, n_group_name, n_src_s_g_name, n_src_s_g_owner_id, n_ip_prot, n_f_port, n_t_port)
                                except:
                                    my_logger.error("Error encountered during authorize_security_groups for s_host: %s and t_host: %s. Exiting!"%(source_host, target_host))
                                    sys.exit("Error encountered during authorize_security_groups for s_host: %s and t_host: %s. Exiting!"%(source_host, target_host))
                                
                                my_logger.debug("%s has been authorized to pull data from %s"%(target_host, source_host))
                
                
            
    #sys.exit()
    
    """ Authorize Security groups...STOP"""
    """********************************************************************************************************************"""
    """ Executing db_operation"""
    print " Executing db_operation"
    pprint(db_dict)
    
    #sys.exit()
    for k, v in db_dict.items():
        #print v
        if type(v) is dict:
            #print pprint(v)
            if v['service_name'] == 'mysql':
                if db_operation == 'backup':
                    mysql_source_host = v['mysql_source_host']
                    bdir = v['target_dir']
                    mysql_backup(db_operation, db_service, dict_source_ttSettings, mysql_source_host, my_logger,log_file_abs_path, bdir)
                    sys.exit()
                elif db_operation == 'restore':
                    rdir = v['source_dir']
                    pprint(db_dict)
                    mysql_target_host = v['mysql_target_host']
                    mysql_restore(dict_target_ttSettings, mysql_target_host, my_logger, log_file_abs_path, rdir)
                    sys.exit()
                elif (db_operation == 'copy') or (db_operation == 'swap'):
                    mysql_source_host = v['mysql_source_host']
                    mysql_target_host = v['mysql_target_host']
                    mysql_operations(db_operation, db_service, dict_source_ttSettings, dict_target_ttSettings,  mysql_source_host, mysql_target_host, my_logger,log_file_abs_path)
                    sys.exit()
            elif v['service_name'] == 'mongodb':
                if db_operation == 'backup':
                    mongodb_source_host = v['mongodb_source_host']
                    bdir = v['target_dir']
                    mongodb_backup(mongodb_source_host, dict_source_ttSettings, my_logger, log_file_abs_path, bdir)
                    sys.exit()
                elif db_operation == 'restore':
                    rdir = v['source_dir']
                    mongodb_target_host = v['mongodb_target_host']
                    mongo_restore(mongodb_target_host, dict_target_ttSettings, my_logger, log_file_abs_path, rdir)
                    sys.exit()
                elif (db_operation == 'copy') or (db_operation == 'swap'):
                    mongodb_source_host = v['mongodb_source_host']
                    mongodb_target_host = v['mongodb_target_host']
                    mongodb_operations(db_operation, db_service, dict_source_ttSettings, dict_target_ttSettings, mongodb_source_host, mongodb_target_host, my_logger,log_file_abs_path)
            elif v['service_name'] == 'neo4j':
                if db_operation == 'backup':
                    neo4j_source_host = v['neo4j_source_host']
                    bdir = v['target_dir']
                    neo4j_source_username = v['neo4j_source_username'] 
                    neo4j_backup(dict_source_ttSettings, neo4j_source_username, neo4j_source_host, bdir)
                    sys.exit()
                elif db_operation == 'restore':
                    rdir = v['source_dir']
                    neo4j_target_host = v['neo4j_target_host']
                    neo4j_restore(dict_target_ttSettings, neo4j_target_host, my_logger, log_file_abs_path, rdir)
                    sys.exit()
                elif (db_operation == 'copy') or (db_operation == 'swap'):
                    neo4j_source_host = v['neo4j_source_host']
                    neo4j_target_host = v['neo4j_target_host']
                    neo4j_source_username = v['neo4j_source_username']
                    neo4j_target_username = v['neo4j_target_username']
                    neo4j_operations(dict_source_ttSettings, dict_target_ttSettings, neo4j_source_username, neo4j_target_username, neo4j_source_host, neo4j_target_host)
            elif v['service_name'] == 'virtuoso':
                if db_operation == 'backup':
                    virtuoso_source_host = v['virtuoso_source_host']
                    bdir = v['target_dir']
                    l_host = "skanupuru-lxu1.nyc.3top.com"
                    l_user = "skanupuru"
                    virtuoso_source_username = v['virtuoso_source_username'] 
                    virtuoso_backup(dict_source_ttSettings, virtuoso_source_host, l_host , l_user, bdir)
                    sys.exit()
                elif db_operation == 'restore':
                    rdir = v['source_dir']
                    pprint(db_dict)
                    sys.exit()
                    virtuoso_target_host = v['virtuoso_target_host']
                    #virtuoso_restore(dict_target_ttSettings, dict_sys_ttSettings, virtuoso_target_host, my_logger, log_file_abs_path, rdir)
                elif (db_operation == 'copy') or (db_operation == 'swap'):
                    virtuoso_source_host = v['virtuoso_source_host']
                    virtuoso_target_host = v['virtuoso_target_host']
                    virtuoso_source_username = v['virtuoso_source_username']
                    virtuoso_target_username = v['virtuoso_target_username']
                    virtuoso_operations(dict_source_ttSettings, dict_target_ttSettings, virtuoso_source_username, virtuoso_target_username, virtuoso_source_host, virtuoso_target_host)
                
    """           virtuoso_backup(s_host, t_host, l_host , l_user)
    #Region = dict_sys_ttSettings["aws"]["REGION_NAME"]COPY
    if db_service == 'mysql':
        print "Executing mysql_operations..."
        my_logger.debug("calling mysql_operations() with source: %s, target: %s..."%(mysql_source_host, mysql_target_host))
        mysql_operations(db_operation, db_service, dict_source_ttSettings, dict_target_ttSettings, mysql_source_host, mysql_target_host, my_logger,log_file_abs_path)
        my_logger.info("MySQL operations completed successfully")
    
    for k, v in db_dict.items():
        #print k, v
        if type(v) is dict:
            if v['service_name'] == 'mongodb':
                mongodb_source_host = v['mongodb_source_host']
                mongodb_target_host = v['mongodb_target_host']
            elif v['service_name'] == 'neo4j':
                neo4j_source_host = v['neo4j_source_host']
                neo4j_target_host = v['neo4j_target_host']
            elif v['service_name'] == 'virtuoso':
                virtuoso_source_host = v['virtuoso_source_host']
                virtuoso_target_host = v['virtuoso_target_host']
    
    if db_service== "mongodb":
        print "Executing mongo_operations..."
        my_logger.debug("calling mongo_operations() with source: %s, target: %s..."%(mongodb_source_host, mongodb_target_host))
        mongodb_operations(db_operation, db_service, dict_source_ttSettings, dict_target_ttSettings, mongodb_source_host, mongodb_target_host, my_logger,log_file_abs_path)
        my_logger.info("MongoDB operations completed successfully")
    elif db_service == 'neo4j':
        local_host = raw_input("Enter local host name: ")
        local_user = raw_input("Enter username (for host:: %s): "%local_host)
    
        print "Executing neo4j_operations..."
        my_logger.debug("calling neo4j_operations() with source: %s , target: %s..."%(neo4j_source_host, neo4j_target_host))
        neo4j_operations(dict_source_ttSettings, dict_target_ttSettings, neo4j_source_username, neo4j_target_username, neo4j_source_host, neo4j_target_host)
        my_logger.info("Neo4j operations completed successfully")
    elif db_service == 'virtuoso':
        local_host = raw_input("Enter local host name: ")
        local_user = raw_input("Enter username (for host:: %s): "%local_host)
    
        print "Executing virtuoso_operations..."
        my_logger.debug("calling virtuoso_operations() with source: %s , target: %s..."%(virtuoso_source_host, virtuoso_target_host))
        virtuoso_operations(virtuoso_source_host, virtuoso_target_host,local_host, local_user)
        my_logger.info("Virtuoso operations completed successfully")
    elif db_service == 'all':
        print "Executing mysql_operations..."
        my_logger.debug("calling mysql_operations() with source: %s, target: %s..."%(mysql_source_host, mysql_target_host))
        mysql_operations(db_operation, db_service, dict_source_ttSettings, dict_target_ttSettings, mysql_source_host, mysql_target_host, my_logger,log_file_abs_path)
        my_logger.info("MySQL operations completed successfully")
        print "Executing mongo_operations..."
        my_logger.debug("calling mongo_operations() with source: %s, target: %s..."%(mongodb_source_host, mongodb_target_host))
        mongodb_operations(db_operation, db_service, dict_source_ttSettings, dict_target_ttSettings, mongodb_source_host, mongodb_target_host, my_logger,log_file_abs_path)
        my_logger.info("MongoDB operations completed successfully")
        
        local_host = raw_input("Enter local host name: ")
        local_user = raw_input("Enter username (for host:: %s): "%local_host)
    
        print "Executing neo4j_operations..."
        my_logger.debug("calling neo4j_operations() with source: %s , target: %s..."%(neo4j_source_host, neo4j_target_host))
        neo4j_operations(dict_source_ttSettings, dict_target_ttSettings, neo4j_source_username, neo4j_target_username, neo4j_source_host, neo4j_target_host)
        my_logger.info("Neo4j operations completed successfully")
        print "Executing virtuoso_operations..."
        my_logger.debug("calling virtuoso_operations() with source: %s , target: %s..."%(virtuoso_source_host, virtuoso_target_host))
        virtuoso_operations(virtuoso_source_host, virtuoso_target_host,local_host, local_user)
        my_logger.info("Virtuoso operations completed successfully")
    """
    """main() Module definition... START"""
    """main() Module definition... STOP"""
    """
    if (db_service == "mongodb") or (db_service == "all"):
        if not m_grant_exist:
            print "Revoking authorized grants!"
            my_logger.debug("Revoking security groups to allow access to %s from %s"%(mongodb_source_host, mongodb_target_host))
            try:
                revoke_security_groups(m_conn, m_group_name, m_src_s_g_name, m_src_s_g_owner_id, m_ip_prot, m_f_port, m_t_port)
            except:
                my_logger.error("Error encountered during revoke_security_groups for s_host: %s and t_host: %s. Exiting!"%(mongodb_source_host, mongodb_target_host))
                sys.exit("Error encountered during revoke_security_groups for s_host: %s and t_host: %s. Exiting!"%(mongodb_source_host, mongodb_target_host))
            my_logger.debug("%s authority to pull data from %s has been revoked successfully"%(mongodb_target_host, mongodb_source_host))
            
    if (db_service == "neo4j") or (db_service == "all"):
        if not n_grant_exist:
            print "Revoking authorized grants!"
            my_logger.debug("Revoking security groups to allow access to %s from %s"%(neo4j_source_host, neo4j_target_host))
            try:
                revoke_security_groups(n_conn, n_group_name, n_src_s_g_name, n_src_s_g_owner_id, n_ip_prot, n_f_port, n_t_port)
            except:
                my_logger.error("Error encountered during revoke_security_groups for s_host: %s and t_host: %s. Exiting!"%(neo4j_source_host, neo4j_target_host))
                sys.exit("Error encountered during revoke_security_groups for s_host: %s and t_host: %s. Exiting!"%(neo4j_source_host, neo4j_target_host))
            my_logger.debug("%s authority to pull data from %s has been revoked successfully"%(neo4j_target_host, mongodb_source_host))
    
    my_logger.info("Program execution complete. Exiting!")
    print "Program execution complete. Exiting!\nLogfile location: %s" %log_file_abs_path
    """

"""main() Module definition... STOP"""

print "Initializing program...calling main()"
main()


