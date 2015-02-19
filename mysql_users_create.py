#!/usr/bin/env python
#
#Author: Sudha Kanupuru
#Description: Mysql users create

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
     
_aws_swap_dir = PATH_ROOT + dict_sys_ttSettings["PATH_LOG"] + "/mysqlUsers"
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
log_file_name='mysqlUsers.log'
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
from ttLib.ttAWS import env_ttSettings
from ttLib.ttLx import callSubprocess



def mysql_user_create():
    env_name = raw_input("Enter environment name (e.g., a.dev.aws.3top.com): ")
    if "aws.3top.com" in env_name:
        hostname = "fp-rds-1." + env_name
    elif "nyc.3top.com" in env_name:
        hostname = env_name
    dict_ttSettings = env_ttSettings(env_name)
    
    ebroot_password = raw_input("Enter ebroot password: ")
    
    if "aws.3top.com" in hostname:
        cert = dict_ttSettings["mysql"]["certs"]["public"]
        dba_arg = "mysql --user=ebroot -p" + ebroot_password + " --ssl-ca=" + os.path.expanduser(cert)  + " --host=" + hostname + " --port=3306 -e \"GRANT ALTER, ALTER ROUTINE, CREATE, CREATE ROUTINE, CREATE TEMPORARY TABLES, CREATE USER, CREATE VIEW, DELETE, DROP, EVENT, EXECUTE, GRANT OPTION, INDEX, INSERT, LOCK TABLES, PROCESS, REFERENCES, RELOAD, REPLICATION CLIENT, SELECT, SHOW DATABASES, SHOW VIEW, TRIGGER, UPDATE ON *.* TO '" + dict_ttSettings["mysql"]["users"]["dba"]["username"] + "'@'%' IDENTIFIED BY \'" + dict_ttSettings["mysql"]["users"]["dba"]["password"] + "\'; FLUSH PRIVILEGES;\" "
        
        r_arg = "mysql --user=ebroot -p" + ebroot_password + " --ssl-ca=" + os.path.expanduser(cert)  + " --host=" + hostname + " --port=3306 -e \"GRANT SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO '" + dict_ttSettings["mysql"]["users"]["r"]["username"] + "'@'%' IDENTIFIED BY \'" + dict_ttSettings["mysql"]["users"]["r"]["password"] + "\'; FLUSH PRIVILEGES;\" "
        
        rw_arg = "mysql --user=ebroot -p" + ebroot_password + " --ssl-ca=" + os.path.expanduser(cert)  + " --host=" + hostname + " --port=3306 -e \"GRANT SELECT, INSERT, UPDATE, EXECUTE, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO '" + dict_ttSettings["mysql"]["users"]["rw"]["username"] + "'@'%' IDENTIFIED BY \'" + dict_ttSettings["mysql"]["users"]["rw"]["password"] + "\'; FLUSH PRIVILEGES;\" "
    
    elif "nyc.3top.com" in hostname:
        dba_arg = "mysql --user=ebroot -p" + ebroot_password + " --host=" + hostname + " --port=3306 -e \"GRANT ALTER, ALTER ROUTINE, CREATE, CREATE ROUTINE, CREATE TEMPORARY TABLES, CREATE USER, CREATE VIEW, DELETE, DROP, EVENT, EXECUTE, GRANT OPTION, INDEX, INSERT, LOCK TABLES, PROCESS, REFERENCES, RELOAD, REPLICATION CLIENT, SELECT, SHOW DATABASES, SHOW VIEW, TRIGGER, UPDATE ON *.* TO '" + dict_ttSettings["mysql"]["users"]["dba"]["username"] + "'@'%' IDENTIFIED BY \'" + dict_ttSettings["mysql"]["users"]["dba"]["password"] + "\'; FLUSH PRIVILEGES;\" "
        
        r_arg = "mysql --user=ebroot -p" + ebroot_password + " --host=" + hostname + " --port=3306 -e \"GRANT SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO '" + dict_ttSettings["mysql"]["users"]["r"]["username"] + "'@'%' IDENTIFIED BY \'" + dict_ttSettings["mysql"]["users"]["r"]["password"] + "\'; FLUSH PRIVILEGES;\" "
        
        rw_arg = "mysql --user=ebroot -p" + ebroot_password + " --host=" + hostname + " --port=3306 -e \"GRANT SELECT, INSERT, INSERT, UPDATE, EXECUTE, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO '" + dict_ttSettings["mysql"]["users"]["rw"]["username"] + "'@'%' IDENTIFIED BY \'" + dict_ttSettings["mysql"]["users"]["rw"]["password"] + "\'; FLUSH PRIVILEGES;\" "
    
    
    print "Creating \'mysql-dba' user..."
    print "Executing command: \n%s"%dba_arg
    dba_arg_out = callSubprocess(dba_arg)
    
    print "Creating \'mysql-r' user..."
    print "Executing command: \n%s"%r_arg
    r_arg_out = callSubprocess(r_arg)
    
    print "Creating \'mysql-rw' user..."
    print "Executing command: \n%s"%rw_arg
    rw_arg_out = callSubprocess(rw_arg)
    print( "Grant privileges success!")
    
mysql_user_create()
