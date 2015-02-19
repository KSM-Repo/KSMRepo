import sys
import os
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name'])
global swapTimeStampDirPath
from fabfile import *
from fabfile import clone_db, aws_hostname_check, nyc_hostname_check, backup_neo_to_local
from pprint import pprint
"""********************************************************************************************************************"""
""" set host name based on env ... start"""

def set_neo4j_host(service, s_env,t_env):
    my_logger.debug("Starting operations in set_neo4j_host()...")
    if (service=='neo4j') or (service=='all'):
        neo4j_s_host='neo4j.' + s_env
        if ("aws.3top.com" in s_env) and ("aws.3top.com" in t_env):
            neo4j_t_host='neo4j.' + t_env 
        elif "nyc.3top.com" in t_env:
            neo4j_t_host=t_env
    my_logger.debug("set_neo4j_host()... successful. Returning to main() with source_host: %s and target_host: %s"%(neo4j_s_host, neo4j_t_host))    
    return neo4j_s_host, neo4j_t_host

""" set host name based on env ... stop"""
"""********************************************************************************************************************"""
""" validate_nyc_accounts ... start"""

def validate_neo4j_nyc_target(target_hostname, dict_ttSettings):
    for k,v in dict_ttSettings["neo4j"]["users"].items():
        if v["host"] == target_hostname:
            username = v["username"]
            try:
                nyc_hostname_check(username, target_hostname)
            except:
                my_logger.error("User:%s validation for Host: %s failed. Exiting!"%(username, target_hostname))
                sys.exit("User:%s validation for Host: %s failed. Exiting!"%(username, target_hostname))
            my_logger.debug("User: %s validated for Host: %s, Account_type: %s"%(username, target_hostname,k))
        """
        else:
            my_logger.error("Hostname: %s not found. Exiting!"%target_hostname)
            sys.exit("Hostname: %s not found. Exiting!"%target_hostname)
        """
        
        
""" validate_nyc_accounts ... start"""
"""********************************************************************************************************************"""    
""" validate_aws_accounts ... start"""

def validate_neo4j_aws_accounts(hostname, dict_ttSettings):
    for k,v in dict_ttSettings["neo4j"]["users"].items():
        if v["host"] == hostname:
            username = v["username"]
            keyfile = os.path.expanduser(dict_ttSettings["ec2"]["cert_private_key"])
            try:
                aws_hostname_check(username, hostname, keyfile)
                print "User: %s validated for Host: %s, Account_type: %s"%(username, hostname, k)
            except:
                my_logger.error("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
                sys.exit("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
            my_logger.debug("User: %s validated for Host: %s, Account_type: %s"%(username, hostname,k))
        """
        else:
            my_logger.error("Hostname: %s not found. Exiting!"%hostname)
            sys.exit("Hostname: %s not found. Exiting!"%hostname)
        """
        

""" validate_aws_accounts ... stop"""
"""********************************************************************************************************************"""
""" ...start"""

def neo4j_operations(dict_source_ttSettings, dict_target_ttSettings):
    try:
        log_dir = swapTimeStampDirPath
        log_dir_split=log_dir.split("/")
        timestamp= log_dir_split[len(log_dir_split)-1]
        print "Timestamp: %s"%timestamp
        backup_path = swapTimeStampDirPath + "/neo4j_backup"
        if not os.path.exists(backup_path):
                os.makedirs(backup_path)
        
        source_suffix = dict_source_ttSettings["neo4j"]["users"]["a_dev"]["host"] + "-Before"
        source_dir_backup=log_dir + "/neo4j_backup/" + source_suffix
        target_suffix = dict_target_ttSettings["neo4j"]["users"]["vm"]["host"] + "-Before"
        target_dir_backup=log_dir + "/neo4j_backup/" + target_suffix 
        print "\nStarting backup operations for %s@%s"%(dict_source_ttSettings["neo4j"]["users"]["a_dev"]["username"],dict_source_ttSettings["neo4j"]["users"]["a_dev"]["host"])
        tar_source_file=backup_neo_to_local(dict_source_ttSettings["neo4j"]["users"]["a_dev"]["username"], dict_source_ttSettings["neo4j"]["users"]["a_dev"]["host"], dict_source_ttSettings["ec2"]["cert_private_key"], source_dir_backup, log_dir)
        my_logger.info("Source file (.tar) location: %s"%tar_source_file)
        print "\nStarting backup operations for %s@%s"%(dict_target_ttSettings["neo4j"]["users"]["vm"]["username"], dict_target_ttSettings["neo4j"]["users"]["vm"]["host"])
        tar_target_file=backup_neo_to_local(dict_target_ttSettings["neo4j"]["users"]["vm"]["username"], dict_target_ttSettings["neo4j"]["users"]["vm"]["host"], dict_target_ttSettings["ec2"]["cert_private_key"], target_dir_backup, log_dir)
        my_logger.info("Target file (.tar) location: %s"%tar_target_file)
        
        print "\nStarting clone operations for %s@%s"%(dict_target_ttSettings["neo4j"]["users"]["vm"]["username"], dict_target_ttSettings["neo4j"]["users"]["vm"]["host"])
        clone_db(dict_target_ttSettings["neo4j"]["users"]["vm"]["username"], dict_target_ttSettings["neo4j"]["users"]["vm"]["host"], dict_target_ttSettings["ec2"]["cert_private_key"], source_suffix, timestamp)
    except:
        my_logger.error("Encountered failure in Neo4j Operations. Exiting!")
        sys.exit("Encountered failure in Neo4j Operations. Exiting!")
    my_logger.debug("Neo4j operations() completed successfully. ")

"""********************************************************************************************************************"""   











































