import sys
import os
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name'])
global swapTimeStampDirPath
from fabfile import *
from pprint import pprint

from fabric.context_managers import settings
import fabfile
from fabfile import virt_dump_one_graph_nyc, virt_dump_one_graph_aws, backup_virt_to_local, backup_to_aws, restore_rw,\
 remove_nyc_server_backup,remove_aws_server_backup, remove_temp_restore
from ttDB import one_graph_backup_nyc, one_graph_backup_aws

import sys
import os
import time
from datetime import datetime
#from ttLib.ttSys import get_s3_config, dirCreateTimestamp
from fabric.contrib.console import confirm
from contextlib import contextmanager as _contextmanager
python_root=os.path.dirname(os.path.realpath(__file__)) + "/"
print python_root
#from ttSys import dirCreateTimestamp
timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
folder_timestamp = datetime.now().strftime('%Y-%m-%d')

"""********************************************************************************************************************"""
""" set host name based on env ... start"""

def set_virtuoso_host(service, s_env,t_env):
    my_logger.debug("Starting operations in set_virtuoso_host()...")
    if (service=='virtuoso') or (service=='all'):
        virtuoso_s_host='virtuoso.' + s_env
        if ("aws.3top.com" in s_env) and ("aws.3top.com" in t_env):
            virtuoso_t_host='virtuoso.' + t_env 
        elif t_env == "nyc.3top.com":
            virtuoso_t_host="virtuoso." + t_env
    my_logger.debug("set_virtuoso_host()... successful. Returning to main() with source_host: %s and target_host: %s"%(virtuoso_s_host, virtuoso_t_host))    
    return virtuoso_s_host, virtuoso_t_host

""" set host name based on env ... stop"""
"""********************************************************************************************************************"""
""" validate_nyc_accounts ... start"""

def validate_virtuoso_nyc_target(target_hostname, dict_ttSettings):
    for k,v in dict_ttSettings["virtuoso"]["users"].items():
        if v["host"] == target_hostname:
            username = v["username"]
            try:
                nyc_hostname_check(username, target_hostname)
            except:
                my_logger.error("User:%s validation for Host: %s failed. Exiting!"%(username, target_hostname))
                sys.exit("User:%s validation for Host: %s failed. Exiting!"%(username, target_hostname))
            my_logger.debug("User: %s validated for Host: %s, Account_type: %s"%(username, target_hostname,k))
        
        else:
            my_logger.error("Hostname: %s not found. Exiting!"%target_hostname)
            sys.exit("Hostname: %s not found. Exiting!"%target_hostname)
        
""" validate_nyc_accounts ... start"""
"""********************************************************************************************************************"""    
""" validate_aws_accounts ... start"""

def validate_virtuoso_aws_accounts(hostname, dict_ttSettings):
    for k,v in dict_ttSettings["virtuoso"]["users"].items():
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
        
        else:
            my_logger.error("Hostname: %s not found. Exiting!"%hostname)
            sys.exit("Hostname: %s not found. Exiting!"%hostname)

""" validate_aws_accounts ... stop"""
"""********************************************************************************************************************"""



def virtuoso_operations():
    print "In main()..."
    
    """***************************************************************************************************************"""
    env.nyc_sub_path = "/usr/local/virtuoso-opensource/share/virtuoso/vad/3t_backup"
    one_graph_backup_nyc(env.nyc_virtuoso_user, env.nyc_virtuoso_password, env.nyc_sub_path, \
                         env.nyc_isql_path, env.nyc_host, env.nyc_user)
    source_before_dir = env.local_backup_dir + "/source-before/"
    if not os.path.exists(source_before_dir):
            os.makedirs(source_before_dir)
    nyc_source_dir = env.nyc_sub_path + "/" + env.timestamp + "/*"
    print "Calling backup_virt_to_local()...before"
    backup_virt_to_local(env.nyc_user, env.nyc_host, nyc_source_dir, source_before_dir)
    print "backup_virt_to_local()...before...successful!!!"
    print "Calling remove_nyc_server_backup()..."
    remove_nyc_server_backup(env.nyc_sub_path, env.timestamp, env.nyc_host, env.nyc_user)
    print "remove_nyc_server_backup()...successful!!!"

    env.aws_sub_path = "/usr/share/virtuoso-opensource-7.1/vad/3t_backup"
    aws_source_dir = one_graph_backup_aws(env.aws_virtuoso_user, env.aws_virtuoso_password, env.aws_sub_path, \
                                          env.aws_isql_path, env.aws_host, env.aws_user, env.aws_dev_key_file)
    aws_source_dir = env.aws_sub_path + "/" + env.timestamp + "/*" 
    target_before_dir = env.local_backup_dir + "/target-before/"
    if not os.path.exists(target_before_dir):
            os.makedirs(target_before_dir)
    print "Calling backup_virt_to_local()..."
    backup_virt_to_local(env.aws_user, env.aws_host, aws_source_dir, target_before_dir)
    
    print "Calling remove_aws_server_backup()..."
    remove_aws_server_backup(env.aws_sub_path, env.timestamp, env.aws_host, env.aws_user, env.aws_dev_key_file)
    print "remove_aws_server_backup()...successful!!!"
        
    target_temp_path="/tmp/virtuoso-restore/one_graph/" + env.timestamp
    print "Calling backup_to_aws()..."
    backup_to_aws(env.aws_user, env.aws_host, source_before_dir + "*", env.aws_dev_key_file, target_temp_path)
    
    print "Calling restore_rw()..."
    for k, v in dict_ttSettings["virtuoso"]["graphs"]["rw"].items():
        graph_name = v["graph_name"]
        restore_rw(env.aws_user, env.aws_host, env.aws_dev_key_file, graph_name, target_temp_path)
    
    print "remove_temp_restore()..."
    remove_temp_restore(env.aws_user, env.aws_host, env.aws_dev_key_file, target_temp_path)
    
    """***************************************************************************************************************"""
    one_graph_backup_nyc(env.nyc_virtuoso_user, env.nyc_virtuoso_password, env.nyc_sub_path, \
                         env.nyc_isql_path, env.nyc_host, env.nyc_user)
    source_after_dir = env.local_backup_dir + "/source-after/"
    if not os.path.exists(source_after_dir):
            os.makedirs(source_after_dir)
    nyc_source_dir = env.nyc_sub_path + "/" + env.timestamp + "/*"
    print "Calling backup_virt_to_local()...after"
    backup_virt_to_local(env.nyc_user, env.nyc_host, nyc_source_dir, source_before_dir)
    print "backup_virt_to_local()...after..successful!!!"
    print "Calling remove_nyc_server_backup()..."
    remove_nyc_server_backup(env.nyc_sub_path, env.timestamp, env.nyc_host, env.nyc_user)
    print "remove_nyc_server_backup()...successful!!!"

    env.aws_sub_path = "/usr/share/virtuoso-opensource-7.1/vad/3t_backup"
    aws_source_dir = one_graph_backup_aws(env.aws_virtuoso_user, env.aws_virtuoso_password, env.aws_sub_path, \
                                          env.aws_isql_path, env.aws_host, env.aws_user, env.aws_dev_key_file)
    aws_source_dir = env.aws_sub_path + "/" + env.timestamp + "/*" 
    target_before_dir = env.local_backup_dir + "/target-after/"
    if not os.path.exists(target_before_dir):
            os.makedirs(target_before_dir)
    print "Calling backup_virt_to_local()..."
    backup_virt_to_local(env.aws_user, env.aws_host, aws_source_dir, target_before_dir)
    
    print "Calling remove_aws_server_backup()..."
    remove_aws_server_backup(env.aws_sub_path, env.timestamp, env.aws_host, env.aws_user, env.aws_dev_key_file)
    print "remove_aws_server_backup()...successful!!!"
    """***************************************************************************************************************"""
    
    print "Clone operations successful!!!"


def one_graph_backup_nyc(virtuoso_user, virtuoso_password, subpath, isql_path, host, user):
    env.virtuoso_user = virtuoso_user
    env.virtuoso_password = virtuoso_password
    env.subpath = subpath
    env.isql_path = isql_path
    count = 0
    for k, v in dict_ttSettings["virtuoso"]["graphs"]["rw"].items():
        count = count + 1
        env.graph_dir_name = "graph_" + str(count)
        env.graph_name = v["graph_name"]
        env.path = "%(subpath)s/%(timestamp)s/3t_%(graph_dir_name)s-"%env
        env.dump_one_graph_subcmd= "EXEC=dump_one_graph(\'%(graph_name)s\',\'%(path)s\',10000000);"%env
        dump_one_graph_cmd = "%(isql_path)s -U %(virtuoso_user)s -P %(virtuoso_password)s \"%(dump_one_graph_subcmd)s\""%env
        
        print "Taking backup on SOURCE VIRTUOSO SERVER: %(virtuoso_user)s, GRAPH: %(graph_dir_name)s"%env
        print "Executing command: %s"%dump_one_graph_cmd
        
        virt_dump_one_graph_nyc(subpath, env.timestamp, dump_one_graph_cmd, host, user)
        

def one_graph_backup_aws(virtuoso_user, virtuoso_password, subpath, isql_path, host, user, keyfile):
    env.virtuoso_user = virtuoso_user
    env.virtuoso_password = virtuoso_password
    env.subpath = subpath
    env.isql_path = isql_path
    count = 0
    for k, v in dict_ttSettings["virtuoso"]["graphs"]["rw"].items():
        count = count + 1
        env.graph_dir_name = "graph_" + str(count)
        env.graph_name = v["graph_name"]
        env.path = "%(subpath)s/%(timestamp)s/3t_%(graph_dir_name)s-"%env
        env.dump_one_graph_subcmd= "EXEC=dump_one_graph(\'%(graph_name)s\',\'%(path)s\',10000000);"%env
        dump_one_graph_cmd = "%(isql_path)s -U %(virtuoso_user)s -P %(virtuoso_password)s \"%(dump_one_graph_subcmd)s\""%env
        
        print "Taking backup on SOURCE VIRTUOSO SERVER: %(virtuoso_user)s, GRAPH: %(graph_dir_name)s"%env
        print "Executing command: %s"%dump_one_graph_cmd
        
        virt_dump_one_graph_aws(subpath, env.timestamp, dump_one_graph_cmd, host, user, keyfile)