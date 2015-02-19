import sys
import os
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name'])
global swapTimeStampDirPath
#from fabfile import *
from fabfile import n_clone_aws_db, n_aws_hostname_check, n_nyc_hostname_check, n_backup_nyc_to_local, n_backup_aws_to_local, n_clone_nyc_db
from pprint import pprint
#print "hi"
"""********************************************************************************************************************"""
""" set host name based on env ... start"""

def set_neo4j_host(service, s_env,t_env):
    my_logger.debug("Starting operations in set_neo4j_host()...")
    if (service=='neo4j') or (service=='all'):
        neo4j_s_host='neo4j.' + s_env
        if ("aws.3top.com" in s_env) and ("aws.3top.com" in t_env):
            neo4j_t_host='neo4j.' + t_env 
        elif (t_env == "dev.nyc.3top.com"):
            neo4j_t_host = "neo4j.nyc.3top.com"
        elif ("nyc.3top.com" in t_env) and  (t_env != "dev.nyc.3top.com") :
            neo4j_t_host = t_env
    my_logger.debug("set_neo4j_host()... successful. Returning to main() with source_host: %s and target_host: %s"%(neo4j_s_host, neo4j_t_host))    
    return neo4j_s_host, neo4j_t_host

""" set host name based on env ... stop"""
"""********************************************************************************************************************"""
""" validate_neo4j_accounts() ... start"""

def validate_neo4j_accounts(dict_ttSettings, hostname):
    if "aws.3top.com" in hostname:
        my_logger.debug("Found \"aws.3top.com\" in hostname: %s"%hostname)
        username = dict_ttSettings["ec2"]["username"]
        keyfile = os.path.expanduser(dict_ttSettings["ec2"]["cert_private_key"])
        my_logger.debug("Received username: %s"%username)
        my_logger.debug("Received Keyfile: %s"%keyfile)
        try:
            n_aws_hostname_check(username, hostname, keyfile)
        except:
            my_logger.error("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
            sys.exit("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
    
    elif "nyc.3top.com" in hostname:
        my_logger.debug("Found \"3top.com\" in hostname: %s"%hostname)
        username = raw_input("Enter username for the host: %s: "%hostname)
        my_logger.debug("Received username: %s"%username)
        try:
            n_nyc_hostname_check(username, hostname)
        except:
            my_logger.error("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
            sys.exit("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
    else:
        my_logger.error("Hostname: %s not found. Exiting!"%hostname)
        sys.exit("Hostname: %s not found. Exiting!"%hostname)
    
    print "User: %s validated for Host: %s\n"%(username, hostname)    
    my_logger.debug("User: %s validated for Host: %s"%(username, hostname))
    return username
        

""" validate_neo4j_accounts() ... stop"""
"""********************************************************************************************************************"""
"""neo4j_operations() ...start"""

def neo4j_operations(dict_source_ttSettings, dict_target_ttSettings, s_user, t_user, s_host, t_host):
    try:
        log_dir = swapTimeStampDirPath
        log_dir_split=log_dir.split("/")
        timestamp= log_dir_split[len(log_dir_split)-1]
        print "Timestamp: %s"%timestamp
        backup_path = swapTimeStampDirPath + "/neo4j_backup"
        if os.path.exists(backup_path):
                os.makedirs(backup_path)
        
        source_suffix = s_host + "-Before"
        source_dir_backup=log_dir + "/neo4j_backup/" + source_suffix
        target_suffix = t_host + "-Before"
        target_dir_backup=log_dir + "/neo4j_backup/" + target_suffix 
        print "\nStarting backup operations for %s@%s"%(s_user,s_host)
        l_host=raw_input("Enter local hostname: ")
        l_user= raw_input("Enter username for local host:%s : "%l_host)
    
        if "aws.3top.com" in dict_source_ttSettings["Environment_Name"]:
            tar_source_file=n_backup_aws_to_local(s_user,s_host, l_user, l_host, dict_source_ttSettings["ec2"]["cert_private_key"], source_dir_backup, log_dir)
        elif "nyc.3top.com" in dict_source_ttSettings["Environment_Name"]:
            tar_source_file=n_backup_nyc_to_local(s_user,s_host, l_user, l_host, source_dir_backup, log_dir)
        my_logger.info("Source file (.tar) location: %s"%tar_source_file)
        print "\nStarting backup operations for %s@%s"%(t_user,t_host)
        
        if "aws.3top.com" in dict_target_ttSettings["Environment_Name"]:
            print "user: %s"%t_user
            print "host: %s"%t_host
            print "Dir Backup: %s"%target_dir_backup
            print"Log Dir: %s"%log_dir
            tar_target_file=n_backup_aws_to_local(t_user,t_host, l_user, l_host, dict_source_ttSettings["ec2"]["cert_private_key"], target_dir_backup, log_dir)
        elif "nyc.3top.com" in dict_target_ttSettings["Environment_Name"]:
            print "user: %s"%t_user
            print "host: %s"%t_host
            print "Dir Backup: %s"%target_dir_backup
            print"Log Dir: %s"%log_dir
            tar_target_file=n_backup_nyc_to_local(t_user,t_host, l_user, l_host, target_dir_backup, log_dir)
        my_logger.info("Target file (.tar) location: %s"%tar_target_file)
        
        if "nyc.3top.com" in dict_target_ttSettings["Environment_Name"]:
            print "\nStarting clone operations for %s@%s"%(t_user,t_host)
            n_clone_nyc_db(t_user,t_host, source_suffix, timestamp, 'NA')
        elif "aws.3top.com" in dict_target_ttSettings["Environment_Name"]:
            print "\nStarting clone operations for %s@%s"%(t_user,t_host)
            n_clone_aws_db(t_user,t_host, dict_target_ttSettings["ec2"]["cert_private_key"], source_suffix, timestamp, 'NA')
    except:
        my_logger.error("Encountered failure in Neo4j Operations. Exiting!")
        sys.exit("Encountered failure in Neo4j Operations. Exiting!")
    my_logger.debug("Neo4j operations() completed successfully. ")
    
"""neo4j_operations()...stop"""
"""********************************************************************************************************************"""
""" neo4j_backup()...start"""

def neo4j_backup(dict_source_ttSettings, s_user, s_host, backup_dir):
    try:
        log_dir = swapTimeStampDirPath
        log_dir_split=log_dir.split("/")
        timestamp= log_dir_split[len(log_dir_split)-1]
        print "Timestamp: %s"%timestamp
        backup_path = backup_dir
        if not os.path.exists(backup_path):
                os.makedirs(backup_path)
        
        source_suffix = "backup"
        source_dir_backup = backup_path
        print "\nStarting backup operations for %s@%s"%(s_user,s_host)
        l_host=raw_input("Enter local hostname: ")
        l_user= raw_input("Enter username for local host: %s: "%l_host)
    
        if "aws.3top.com" in dict_source_ttSettings["Environment_Name"]:
            tar_source_file=n_backup_aws_to_local(s_user,s_host, l_user, l_host, dict_source_ttSettings["ec2"]["cert_private_key"], source_dir_backup, log_dir)
        elif "nyc.3top.com" in dict_source_ttSettings["Environment_Name"]:
            tar_source_file=n_backup_nyc_to_local(s_user,s_host, l_user, l_host, source_dir_backup, log_dir)
        my_logger.info("Source file (.tar) location: %s"%tar_source_file)
        
    except:
        my_logger.error("Encountered failure in Neo4j Backup(). Exiting!")
        sys.exit("Encountered failure in Neo4j Backup(). Exiting!")
    my_logger.debug("Neo4j Backup() completed successfully. ")

""" neo4j_backup()...stop"""
"""********************************************************************************************************************"""      
"""neo4j_restore() ...start"""

def neo4j_restore(dict_target_ttSettings, t_host, my_logger, log_file_abs_path, restore_dir):
    try:
        log_dir = swapTimeStampDirPath
        log_dir_split=log_dir.split("/")
        timestamp= log_dir_split[len(log_dir_split)-1]
        print "Timestamp: %s"%timestamp
        backup_path = swapTimeStampDirPath + "/neo4j_backup"
        if os.path.exists(backup_path):
                os.makedirs(backup_path)
        
        suffix = "restore"
        l_host=raw_input("Enter local hostname: ")
        l_user= raw_input("Enter username for local host:%s : "%l_host)
    
        if "nyc.3top.com" in dict_target_ttSettings["Environment_Name"]:
            t_user = l_user
            print "\nStarting clone operations for %s@%s"%(t_user,t_host)
            n_clone_nyc_db(t_user,t_host, suffix, timestamp, restore_dir)
        elif "aws.3top.com" in dict_target_ttSettings["Environment_Name"]:
            t_user = dict_target_ttSettings["ec2"]["username"]
            print "\nStarting clone operations for %s@%s"%(t_user,t_host)
            n_clone_aws_db(t_user,t_host, dict_target_ttSettings["ec2"]["cert_private_key"], suffix, timestamp, restore_dir)
    except:
        my_logger.error("Encountered failure in Neo4j Operations. Exiting!")
        sys.exit("Encountered failure in Neo4j Operations. Exiting!")
    my_logger.debug("Neo4j operations() completed successfully. ")
    
"""neo4j_restore()...stop"""
"""********************************************************************************************************************"""






















