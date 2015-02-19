
from fabric.context_managers import settings
from fabfile import v_dump_one_graph_nyc, v_dump_one_graph_aws, v_backup_to_local, v_backup_to_aws, v_restore_rw
import fabfile
import sys
import os
import time
from datetime import datetime
#from ttLib.ttSys import get_s3_config, dirCreateTimestamp
import logging 
#import ttLib.logger as logger
from fabric.api import *
from fabric.utils import abort, error, warn
from fabric.contrib import files
from fabric.contrib.console import confirm
from contextlib import contextmanager as _contextmanager
python_root=os.path.dirname(os.path.realpath(__file__)) + "/"
print python_root
#from ttSys import dirCreateTimestamp
#timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
#folder_timestamp = datetime.now().strftime('%Y-%m-%d')
global timestamp

import boto
import ast
from boto.s3.key import Key

def get_s3_config(s3_folder, s3_filename):
    conn = boto.connect_s3()
    buck = conn.get_bucket('app-env')
    key = Key(buck)
    #print"S3_filename: ",s3_filename
    #key.key = "/aws-prod/" + s3_filename
    key.key = "/" + s3_folder + "/" + s3_filename

    """ Download the values in a file on S3 into a string
        """
    s3_file_contents = key.get_contents_as_string()
    #print"s3_file_contents: ", s3_file_contents

    """ Convert the s3 file contents into a dictionary
        """
    dict_s3_settings = {}
    #import ast
    dict_s3_settings = ast.literal_eval(s3_file_contents)
    #print type(dict_s3_settings)
    
    """ Return dictionary of settings to calling function
        """
    return dict_s3_settings

def dirCreateTimestamp(path):
    print("Initiating create_timestamp_directory to store logfile and backups")
    if not os.path.exists(path):
        os.makedirs(path)
    now = datetime.fromtimestamp(time.time())
    s = now.strftime('%Y_%m_%d_%H_%M_%S')
    #print s
    try:
        dir_name = path + "/" + s
        #print dir_name
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print("Directory creation successful!")
    except:
        print("Directory creation failed. Exiting!")
    return (dir_name, s)

print "Downloading ttSettings from S3...",
try:
    s3_config_folder = "aws-dev/a"
    s3_config_filename = "ttSettings.conf"
    global dict_ttSettings
    dict_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
    print"complete."  
    #print  dict_ttSettings
except:
    sys.exit("Download of ttSettings from S3 failed. Exiting! Error Encountered: %s." %str(sys.exc_info()))
    

env.nyc_host = "virtuoso.nyc.3top.com"
env.nyc_user = "skanupuru"
env.nyc_isql_path = "/usr/local/virtuoso-opensource/bin/isql"
env.nyc_dir = "/usr/local/virtuoso-opensource/var/lib/virtuoso/db/"
env.nyc_file = timestamp + "-virtuoso-backup"
env.nyc_virtuoso_user=dict_source_ttSettings["virtuoso"]["user"]["dba"]["username"]
env.nyc_virtuoso_password=dict_source_ttSettings["virtuoso"]["user"]["dba"]["password"]


env.aws_dev_key_file=dict_target_ttSettings["ec2"]["cert_private_key"]
env.aws_host = 'virtuoso.a.dev.aws.3top.com'
env.aws_user = 'ubuntu'
env.aws_dir = "~/tmp/db_util/"
env.aws_file="3t-virt-inc_dump_#1.bp"
env.aws_virtuoso_user=dict_target_ttSettings["virtuoso"]["user"]["dba"]["username"]
env.aws_virtuoso_password=dict_target_ttSettings["virtuoso"]["user"]["dba"]["password"]
env.aws_isql_path = "/usr/bin/isql-vt"

data_dir="/usr/local/3top"

local_host = "skanupuru-lxu1.nyc.3top.com"
local_user = "skanupuru"
path = "/usr/local/3top/log/db_util"
print "Creating timestamp directory..."
(dirTimestamp, env.timestamp) = dirCreateTimestamp(path)
env.local_backup_dir= dirTimestamp + "/virtuoso_backup"
if not os.path.exists(env.local_backup_dir):
            os.makedirs(env.local_backup_dir)
aws_file="3t-virt-inc_dump_#1.bp"

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
        
        v_dump_one_graph_nyc(subpath, env.timestamp, dump_one_graph_cmd, host, user)
        

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
        
        v_dump_one_graph_aws(subpath, env.timestamp, dump_one_graph_cmd, host, user, keyfile)
        

def main():
    print "In main()..."
    env.nyc_sub_path = "/usr/local/virtuoso-opensource/share/virtuoso/vad/3t_backup"
    one_graph_backup_nyc(env.nyc_virtuoso_user, env.nyc_virtuoso_password, env.nyc_sub_path, env.nyc_isql_path, env.nyc_host, env.nyc_user)
    nyc_source_dir = env.nyc_sub_path + "/" + env.timestamp + "/*" 
   
    
    source_before_dir = env.local_backup_dir + "/source-before/"
    if not os.path.exists(source_before_dir):
            os.makedirs(source_before_dir)
    print "Calling v_backup_to_local()..."
    v_backup_to_local(env.nyc_user, env.nyc_host, nyc_source_dir, source_before_dir)

    env.aws_sub_path = "/usr/share/virtuoso-opensource-7.1/vad/3t_backup"
    aws_source_dir = one_graph_backup_aws(env.aws_virtuoso_user, env.aws_virtuoso_password, env.aws_sub_path, env.aws_isql_path, env.aws_host, env.aws_user, env.aws_dev_key_file)
    aws_source_dir = env.aws_sub_path + "/" + env.timestamp + "/*" 
    
    target_before_dir = env.local_backup_dir + "/target-before/"
    if not os.path.exists(target_before_dir):
            os.makedirs(target_before_dir)
    print "Calling v_backup_to_local()..."
    v_backup_to_local(env.aws_user, env.aws_host, aws_source_dir, target_before_dir)
    
    target_temp_path="/tmp/virtuoso-restore/one_graph/" + env.timestamp
    print "Calling v_backup_to_aws()..."
    v_backup_to_aws(env.aws_user, env.aws_host, source_before_dir + "*", env.aws_dev_key_file, target_temp_path)
    
    print "Calling v_restore_rw()..."
    for k, v in dict_ttSettings["virtuoso"]["graphs"]["rw"].items():
        graph_name = v["graph_name"]
        v_restore_rw(env.aws_user, env.aws_host, env.aws_dev_key_file, graph_name, target_temp_path)
    
    
main()
"""
graph1 =  dict_ttSettings["virtuoso"]["graphs"]["rw"]["graph1"]["graph_name"]
print "Restoring backup for the graph: %s"%graph1
"""

env.dump_one_graph_subcmd= "EXEC=dump_one_graph(\'http://3top.com\',\'/usr/local/virtuoso-opensource/share/virtuoso/vad/3t_backup/3t-\',10000000);"%env
dump_one_graph_cmd = "%(nyc_isql_path)s -U %(nyc_virtuoso_user)s -P %(nyc_virtuoso_password)s \"%(dump_one_graph_subcmd)s\""%env
#print dump_one_graph_cmd

#print "Calling v_backup_to_local()..."
#srcdir = v_backup_to_local(env.nyc_user, env.nyc_host, env.local_backup_dir)
#print "Calling v_backup_to_aws()..."
#env.newdir = env.local_backup_dir + "/*"
#dstpath = v_backup_to_aws(env.aws_user, env.server_host_aws, env.newdir, env.aws_dev_key_file)
#v_restore_rw(env.aws_user, env.server_host_aws, env.aws_dev_key_file, graph1)






