#!/usr/bin/env python
#
#Author: Sudha Kanupuru
#Description: Datpython_root=os.path.dirname(os.path.realpath(__file__)) + "/"abase Copy and Swap for MongoDB

"""********************************************************************************************************************""" 
print "Initializing setup..."
"""Import sys, re, os...start"""
#print "Importing python libraries..."
try:
    #print "Importing \'sys\'...",
    import sys
except:
    raise SystemExit("sys import failed. Exiting!")
#print "successful."
try:
    #print "Importing \'os\'...",
    import os
except:
    sys.exit("os import failed. Exiting!")
#print "successful."

try:
    #print "Importing \'re\'...",
    import re
except:
    sys.exit("re import failed. Exiting!")
#print "successful."

print "Importing python libraries successful!"

"""Import sys, re...stop"""
"""********************************************************************************************************************"""
"""Download ttSettings.conf from S3...start"""

python_root=os.path.dirname(os.path.realpath(__file__)) + "/"
sys.path.append(python_root + "ttLib")

from ttLib.ttSys import get_s3_config, validate_args
#print "Downloading ttSettings from S3...",
try:
    s3_config_foldername="aws-prod/b"
    s3_config_filename = "ttSettings.conf"
    global dict_ttSettings
    dict_ttSettings = get_s3_config(s3_config_foldername, s3_config_filename)
except:
    sys.exit("Error Encountered: %s. Download of ttSettings from S3 failed. Exiting!" %str(sys.exc_info()))

print "Downloading ttSettings from S3 successful!"
PATH_ROOT = dict_ttSettings["PATH_ROOT"]


"""Download ttSettings.conf from S3...stop"""
"""********************************************************************************************************************"""
""" Create a timestamp directory to store log files...start"""
    
print "Creating timestamp directory...",
from ttLib.ttSys import dirCreateTimestamp
_aws_swap_dir = PATH_ROOT + dict_ttSettings["PATH_LOG"] + "/swap"
#print"_aws_swap_dir:", _aws_swap_dir
#print "Creating timestamp directory...",
global swapTimeStampDirPath
swapTimeStampDirPath = dirCreateTimestamp(_aws_swap_dir)
#print "swapTimeStampDirPath: ",swapTimeStampDirPath

""" Create a timestamp directory to store log files...stop"""
"""********************************************************************************************************************"""
""" Implement Logging...start"""

print "Setting up logging...",
import logging
from ttLib.logger import get_handlers 
log_handler_name='db_util'
my_logger = logging.getLogger(log_handler_name)
for h in my_logger.handlers:
    my_logger.removeHandler(h)
log_file_name='db_util.log'
log_file_abs_path = swapTimeStampDirPath + "/" + log_file_name
#print"log_file_abs_path: ",log_file_abs_path
for h in get_handlers(log_handler_name, log_file_abs_path):
    my_logger.addHandler(h)
my_logger.setLevel(logging.DEBUG)
#print "complete." 
#print "\nLog and backup location: %s"%swapTimeStampDirPath
my_logger.info("Logging configured successfully!")

""" Implement Logging...stop"""
"""********************************************************************************************************************"""
"""Mysql Imports Start"""    

from ttLib import ttMysql
from ttLib.ttMysql import set_mysql_host, validate_mysql_nyc_accounts, validate_mysql_aws_accounts, validate_mysql_source, mysql_operations
ttMysql.dict_ttSettings = dict_ttSettings
ttMysql.swapTimeStampDirPath= swapTimeStampDirPath

"""Mysql Imports Stop"""
"""********************************************************************************************************************"""
"""MongoDB Imports Start"""
from ttLib import ttMongoDB
from ttLib.ttMongoDB import mongo_operations, validate_mongodb_target_accounts, validate_mongodb_source_accounts, set_mongodb_host
ttMongoDB.dict_ttSettings = dict_ttSettings
ttMongoDB.swapTimeStampDirPath= swapTimeStampDirPath

"""MongoDB Imports Stop"""
"""********************************************************************************************************************"""
"""Neo4j Imports Start"""

from ttLib import ttNeo
from ttLib.ttNeo import set_neo4j_host, validate_neo4j_nyc_target, validate_neo4j_aws_accounts, neo4j_operations
ttNeo.dict_ttSettings = dict_ttSettings
ttNeo.swapTimeStampDirPath= swapTimeStampDirPath

"""Neo4j Imports Stop"""
"""********************************************************************************************************************"""
"""Initial arguments check Start"""

length_of_args=len(sys.argv)
if length_of_args<3:
    print "Argument format mismatch. Use: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target_env (for 'copy' only)]"
    sys.exit("Exiting!")
    
"""Initial arguments check Stop"""   
"""********************************************************************************************************************"""
""" Find ttLib and import its libraries...start """

try:
    from ttLib.ttLx import callSubprocess
except:
    sys.exit("Error encountered during import of callSubprocess from ttLib.ttLx. Exiting!%s" %str(sys.exc_info()))

try:
    from ttLib.ttAWS import findSourceSuffix, validate_cert_loc
except:
    sys.exit("Error encountered during import of findSourceSuffix from ttLib.ttAWS. Exiting! %s" %str(sys.exc_info()))

print("Importing 3top libraries (ttLib) successful!") 
    
"""Find ttLib and import its libraries...stop"""
"""********************************************************************************************************************"""

print "Initial setup complete."

"""********************************************************************************************************************"""
""" Set env based on the arguments... start"""

def set_env(operation, t_env):
    #print("In set_env()...")
    try:
        #print("Calling findSourceSuffix()...")
        s_env = findSourceSuffix()
        #print("Current active Environment (source environment): %s"%s_env)
        
        if operation == 'swap':
            if s_env == "a.prod.aws.3top.com":
                #global target_env
                t_env = "b.prod.aws.3top.com"
            else:
                #global target_env
                t_env="a.prod.aws.3top.com"
            #print("Target Environment set as: %s"%(t_env))
        elif operation == 'copy':
            print("Target Environment set as: %s"%(t_env))
            
    except:
        print("Error encountered in set_env. Exiting! %s" %str(sys.exc_info()))
        sys.exit(-1)
        #print source_env
    #print("set_env call successful. Returning to main()")
    return s_env, t_env

""" Set env based on the arguments... stop"""
"""********************************************************************************************************************"""
"""main() Module definition... start"""

def main():
    print("In main()...")
    
    """****************************************************************************************************************"""
    """ Collecting arguments into variables based on the format:
        db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix (for 'copy' only)]
        start
        """
    #print("Collecting arguments into variables...")
    if length_of_args == 4:
        target_env = sys.argv[3]
    elif length_of_args==3:
        target_env = " "    
    else:
        sys.exit("Arguments format mismatch.  Exiting! \nUse: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target_env (for 'copy' only)].")
        #sys.exit("Arguments mismatch.  Exiting!")
        #sys.exit(-1)
    print("Received target_env: %s"%target_env)
    #print "target_env: ", target_env
    
    db_operation = sys.argv[1]
    print("Received db_operation: %s"%db_operation)
    #print"db_operation: ", db_operation
    
    db_service = sys.argv[2]
    print("Received db_service: %s"%db_service)
    #print"db_service: ",db_service
    
    """Collecting arguments into variables...stop"""
    """****************************************************************************************************************"""
    """ calling validate_args()...start"""
    
    #print("Calling \"validate_args()\"...")
    validate_args(db_operation, db_service, target_env)
    print("Validate args successful. Back in main()...")
    
    """ calling validate_args()...stop"""
    """****************************************************************************************************************"""
    """ calling set_env()...start"""
    
    #print("Calling \"set_env()\"...")
    (source_env, target_env)=set_env(db_operation, target_env)
    print("set_env successful. Back in main()... Received:: source_env: %s, target_env: %s. "%(source_env, target_env))
    
    """ calling set_env()...stop"""
    """****************************************************************************************************************"""
    """Start download of ttSettings based on source env"""
    
    #dict_source_ttSettings = {}
    source_s3_folder = "aws-" + source_env.split(".")[1] + "/" + source_env.split(".")[0]
    print "Downloading ttSettings for %s from %s folder on S3..."%(source_env,source_s3_folder)
    try:
        s3_config_folder = source_s3_folder
        s3_config_filename = "ttSettings.conf"
        global dict_source_ttSettings
        dict_source_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
        print"complete."  
        #print  "dict_source_ttSettings: %s"%dict_source_ttSettings
    except:
        sys.exit("Download of ttSettings from S3 failed. Exiting! Error Encountered: %s." %str(sys.exc_info()))
        
    """Stop download of ttSettings based on source env""" 
    """****************************************************************************************************************"""    
    """Start download of ttSettings based on target env"""
    
    if "aws.3top.com" in target_env:
        #dict_source_ttSettings = {}
        target_s3_folder = "aws-" + target_env.split(".")[1] + "/" + target_env.split(".")[0]
    elif "nyc.3top.com" in target_env:
        #dict_source_ttSettings = {}
        target_s3_folder = "nyc-sys"
    print "Downloading ttSettings for %s from %s folder on S3..."%(target_env, target_s3_folder)
    try:
        s3_config_folder = target_s3_folder
        s3_config_filename = "ttSettings.conf"
        global dict_target_ttSettings
        dict_target_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
        print"complete."  
        #print  "dict_target_ttSettings: %s"%dict_target_ttSettings
    except:
        sys.exit("Download of ttSettings from S3 failed. Exiting! Error Encountered: %s." %str(sys.exc_info()))         
       
    """Stop download of ttSettings based on target env""" 
    """****************************************************************************************************************"""
    """ calling validate_cert_loc()...start"""
    
    if ("aws.3top.com" in source_env) or ("aws.3top.com" in target_env):
        #print("Calling \"validate_cert_loc()\"...")
        AWS_MYSQL_CERT = validate_cert_loc(dict_ttSettings)
        print("validate_cert_loc() successful. Back in main()... ")
    
    """ calling validate_cert_loc()...stop"""
    """****************************************************************************************************************"""
    """ calling set_host()...start"""
    
    #print("Calling \"set_host()\"...")
    if db_service == 'mysql':
        (mysql_source_host, mysql_target_host) = set_mysql_host(db_service, source_env,target_env)
        print("set_host successful. Back in main()... Received:: mysql_source_host: %s, mysql_target_host: %s."%(mysql_source_host, mysql_target_host))
    elif db_service == 'mongodb':
        (mongodb_source_host, mongodb_target_host)=set_mongodb_host(db_service, source_env, target_env)
        print("set_host successful. Back in main()... Received:: mongodb_source_host: %s, mongodb_target_host: %s."%(mongodb_source_host, mongodb_target_host))
    elif db_service == 'neo4j':
        (neo4j_source_host, neo4j_target_host)=set_neo4j_host(db_service, source_env, target_env)
        print("set_host successful. Back in main()... Received:: neo4j_source_host: %s, neo4j_target_host: %s."%(neo4j_source_host, neo4j_target_host))
    elif db_service == 'all':
        (mysql_source_host, mysql_target_host) = set_mysql_host(db_service, source_env,target_env)
        (mongodb_source_host, mongodb_target_host)=set_mongodb_host(db_service, source_env, target_env)
        (neo4j_source_host, neo4j_target_host)=set_neo4j_host(db_service, source_env, target_env)
        print("set_host successful. Back in main()... Received:: mysql_source_host: %s, mysql_target_host: %s."%(mysql_source_host, mysql_target_host))
        print("set_host successful. Back in main()... Received:: mongodb_source_host: %s, mongodb_target_host: %s."%(mongodb_source_host, mongodb_target_host))
        print("set_host successful. Back in main()... Received:: neo4j_source_host: %s, neo4j_target_host: %s."%(neo4j_source_host, neo4j_target_host))
        
    """ calling set_host()...stop"""
    """****************************************************************************************************************"""
    """ Validate DB Accounts... start"""
    
    """ Validate Source Accounts... start"""
    #print("Validating source host..."),
    if db_service == 'mysql':
        validate_mysql_source(AWS_MYSQL_CERT, mysql_source_host)
    elif db_service == 'mongodb':
        validate_mongodb_source_accounts(mongodb_source_host)
    elif db_service == 'neo4j':
        validate_neo4j_aws_accounts(neo4j_source_host)
    elif db_service == 'all':
        validate_mysql_source(AWS_MYSQL_CERT, mysql_source_host)
        validate_mongodb_source_accounts(mongodb_source_host)
        validate_neo4j_aws_accounts(neo4j_source_host)
        
           
    """ Validate Source Accounts... stop"""
    
    """ Validate Target Accounts... start"""
    #print("Validating target host..."),
    if db_service == 'mongodb':
        #print("Calling \"validate_mongodb_target_accounts\"...")
        validate_mongodb_target_accounts(mongodb_target_host)
    elif db_service == 'mysql':
        if "nyc.3top.com" in mysql_target_host:
            validate_mysql_nyc_accounts(mysql_target_host)
        elif "aws.3top.com" in mysql_target_host:
            validate_mysql_aws_accounts(AWS_MYSQL_CERT, mysql_target_host)
    elif db_service == 'neo4j':
        if "nyc.3top.com" in neo4j_target_host:
            validate_neo4j_nyc_target(neo4j_target_host)
        elif "aws.3top.com" in neo4j_target_host:
            validate_neo4j_aws_accounts(neo4j_target_host)
    elif db_service == 'all':
        validate_mongodb_target_accounts(mongodb_target_host)
        if "nyc.3top.com" in mysql_target_host:
            validate_mysql_nyc_accounts(mysql_target_host)
        elif "aws.3top.com" in mysql_target_host:
            validate_mysql_aws_accounts(AWS_MYSQL_CERT, mysql_target_host)
        if "nyc.3top.com" in neo4j_target_host:
            validate_neo4j_nyc_target(neo4j_target_host)
        elif "aws.3top.com" in neo4j_target_host:
            validate_neo4j_aws_accounts(neo4j_target_host)
            
    """ Validate Target Accounts... stop"""
    
    """ Validate DB Accounts... stop"""    
    """****************************************************************************************************************"""
    """ Find current working directory path: python_root...start"""
    
    global python_root
    #print "Finding real path (working directory)...",
    python_root = os.path.dirname(os.path.realpath(__file__)) + "/"
    #print"python_root:",python_root,
    #print "found."
    
    """ Find current working directory path: python_root...stop"""
    """********************************************************************************************************************"""
    """ Executing db_operation"""
    
    if db_service== "mongodb":
        print "Executing mongo_operations..."
        mongo_operations(db_operation, db_service, mongodb_source_host, mongodb_target_host, my_logger,log_file_abs_path)
        my_logger.info("MongoDB operations completed successfully")
    elif db_service == 'mysql':
        print "Executing mysql_operations..."
        mysql_operations(db_operation, db_service, AWS_MYSQL_CERT, mysql_source_host, mysql_target_host, my_logger,log_file_abs_path)
        my_logger.info("MySQL operations completed successfully")
    elif db_service == 'neo4j':
        print "Executing neo4j_operations..."
        neo4j_operations()
        my_logger.info("Neo4j operations completed successfully")
    elif db_service == 'all':
        print "Executing mongo_operations..."
        mongo_operations(db_operation, db_service, mongodb_source_host, mongodb_target_host, my_logger,log_file_abs_path)
        my_logger.info("MongoDB operations completed successfully")
        print "Executing mysql_operations..."
        mysql_operations(db_operation, db_service, AWS_MYSQL_CERT, mysql_source_host, mysql_target_host, my_logger,log_file_abs_path)
        my_logger.info("MySQL operations completed successfully")
        print "Executing neo4j_operations..."
        neo4j_operations()
        my_logger.info("Neo4j operations completed successfully")
    """main() Module definition... start"""
    """main() Module definition... stop"""
    
    my_logger.info("Program execution complete. Exiting!")
    print "Program execution complete. Exiting!\nLogfile location: %s" %log_file_abs_path
   

"""main() Module definition... stop"""

print "Initializing program...calling main()"
main()
