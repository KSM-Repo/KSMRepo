global timestamp
import sys
import os
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name'])
global dirTimestamp
from fabfile import *
from pprint import pprint
from fabric.context_managers import settings
from fabfile import v_dump_one_graph_nyc, v_dump_one_graph_aws, v_pull_backup_to_local, v_push_backup_to_aws_target, v_restore_rw,\
 v_aws_hostname_check, v_nyc_hostname_check, delete_backup_on_aws_virtuoso, delete_backup_on_nyc_virtuoso, v_push_backup_to_nyc_target
import fabfile
import time
from datetime import datetime
from fabric.api import *
from fabric.utils import abort, error, warn
from fabric.contrib import files
from fabric.contrib.console import confirm
from contextlib import contextmanager as _contextmanager
python_root=os.path.dirname(os.path.realpath(__file__)) + "/"
#print python_root
global dict_source_ttSettings
global dict_target_ttSettings
#print "hi"
"""********************************************************************************************************************"""
""" set host name based on env ... start"""

def set_virtuoso_host(service, s_env,t_env):
    my_logger.debug("Starting operations in set_virtuoso_host()...")
    if (service == 'virtuoso') or (service == 'all'):
        virtuoso_s_host='virtuoso.' + s_env
        my_logger.debug("Set source_host as: %s"%(virtuoso_s_host))
        if ("aws.3top.com" in s_env) and ("aws.3top.com" in t_env):
            virtuoso_t_host='virtuoso.' + t_env 
        elif (t_env == "dev.nyc.3top.com"):
            virtuoso_t_host = "virtuoso.nyc.3top.com"
        elif ("nyc.3top.com" in t_env) and  (t_env != "dev.nyc.3top.com") :
            virtuoso_t_host = t_env 
        my_logger.debug("Set target_host as: %s"%(virtuoso_t_host))
    my_logger.debug("set_virtuoso_host()... successful. Returning to main() with source_host: %s and target_host: %s"%(virtuoso_s_host, virtuoso_t_host))    
    return virtuoso_s_host, virtuoso_t_host

""" set host name based on env ... stop"""
"""********************************************************************************************************************"""
""" validate_aws_accounts ... start"""
"""
/usr/libexec/virtuoso/isql "DSN=Local Virtuoso;UID=dba;PWD=dba"
"""
def validate_virtuoso_accounts(dict_ttSettings, hostname, suffix):
    my_logger.debug("In validate_virtuoso_aws_accounts()... ")
    print "hostname: %s"%hostname
    
    """
    if suffix == "source":
        my_logger.debug("Collecting ttSettings for %s as source"%hostname )
        dict_ttSettings = dict_source_ttSettings
        #print dict_ttSettings
        
    elif suffix == "target":
        my_logger.debug("Collecting ttSettings for %s as target"%hostname )
        dict_ttSettings = dict_target_ttSettings
    """
    
    h_subname = dict_ttSettings["Environment_Name"]
    #print "h_subname: %s"%h_subname
    h_name = "virtuoso." + h_subname
    #print "h_name:%s"%h_name
    if hostname == h_name:
        if "aws.3top.com" in hostname:
            username = dict_ttSettings["ec2"]["username"]
        elif "nyc.3top.com" in hostname:
            username = raw_input("Enter username for the host: %s: "%hostname)
        #print"username: %s"%username
        my_logger.debug("Validating accounts for host: %s and user: %s"%(hostname, username))
        count_users = dict_ttSettings["virtuoso"]["users"]
        #print count_users
        #if (count_users == "None"): print "None"
        #else: print "Not None"
        if not (count_users == "None"):
            #print "Count_users not None"    
            for k,v in dict_ttSettings["virtuoso"]["users"].items():
                    v_username = v["username"]
                    #print "v_username: %s"%v_username
                    v_password = v["password"]
                    #print "v_password: %s"%v_password
                    
                    #host_chk_cmd = "/usr/bin/isql-vt -S 1111 -U %s -P %s \"EXEC = CONNECT;\"" %(v_username, v_password)
                    host_chk_cmd = "/usr/local/virtuoso-opensource/bin/isql -S 1111 -U %s -P %s \"EXEC = CONNECT;\"" %(v_username, v_password)
                    #print "host_chk_cmd:%s"%host_chk_cmd
                    if "aws.3top.com" in hostname:
                        keyfile=dict_ttSettings["ec2"]["cert_private_key"]
                        
                        try:
                            v_aws_hostname_check(username, hostname, keyfile, host_chk_cmd)
                        except:
                            my_logger.error("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
                            sys.exit("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
                    
                    elif "nyc.3top.com" in hostname:
                        try:
                            v_nyc_hostname_check(username, hostname, host_chk_cmd)
                        except:
                            my_logger.error("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
                            sys.exit("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
                            
                    my_logger.debug("User: %s validated for Host: %s, Account_type: %s"%(username, hostname,k))
    else:
        my_logger.error("Hostname: %s not found. Exiting!"%hostname)
        sys.exit("Hostname: %s not found. Exiting!"%hostname)
            
        

""" validate_aws_accounts ... stop"""
"""********************************************************************************************************************"""
""" validate_nyc_accounts ... start"""

def validate_virtuoso_nyc_accounts(hostname, suffix):
    my_logger.debug("In validate_virtuoso_nyc_accounts()... ")
    print "hostname: %s"%hostname
    
    if suffix == "source":
        my_logger.debug("Collecting ttSettings for %s as source"%hostname )
        dict_ttSettings = dict_source_ttSettings
        #print dict_ttSettings
        
    elif suffix == "target":
        my_logger.debug("Collecting ttSettings for %s as target"%hostname )
        dict_ttSettings = dict_target_ttSettings
    
    
    h_subname = dict_ttSettings["Environment_Name"]
    #print "h_subname: %s"%h_subname
    h_name = "virtuoso." + h_subname
    #print "h_name:%s"%h_name
    if hostname == h_name:
        username = dict_ttSettings["ec2"]["username"]
        #print"username: %s"%username
        my_logger.debug("Validating accounts for host: %s and user: %s"%(hostname, username))
        count_users = dict_ttSettings["virtuoso"]["users"]
        #print count_users
        #if (count_users == "None"): print "None"
        #else: print "Not None"
        if not (count_users == "None"):
            #print "Count_users not None"    
            for k,v in dict_ttSettings["virtuoso"]["users"].items():
                
                    v_username = v["username"]
                    #print "v_username: %s"%v_username
                    v_password = v["password"]
                    #print "v_password: %s"%v_password
                    
                    host_chk_cmd = "/usr/bin/isql-vt -S 1111 -U %s -P %s \"EXEC = CONNECT;\"" %(v_username, v_password)
                    #print "host_chk_cmd:%s"%host_chk_cmd
                    try:
                        v_nyc_hostname_check(username, hostname, host_chk_cmd)
                    except:
                        my_logger.error("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
                        sys.exit("User:%s validation for Host: %s failed. Exiting!"%(username, hostname))
                    my_logger.debug("User: %s validated for Host: %s, Account_type: %s"%(username, hostname,k))
    else:
        my_logger.error("Hostname: %s not found. Exiting!"%hostname)
        sys.exit("Hostname: %s not found. Exiting!"%hostname)
        
""" validate_nyc_accounts ... start"""
"""********************************************************************************************************************"""
def one_graph_backup_aws(v_backup_subpath, isql_path, host, user, keyfile, suffix):
    #backup_subpath = "/usr/local/virtuoso-opensource/share/virtuoso/vad/3t_backup"
    #isql_path = "/usr/bin/isql-vt"
    my_logger.debug("In one_graph_backup_aws(%s, %s, %s, %s, %s)"%(v_backup_subpath, isql_path, host, user, keyfile, suffix))
    if suffix == "source":
        my_logger.debug("Collecting dict_ttSettings for %s and %s"%(host, suffix))
        dict_ttSettings = dict_source_ttSettings        
    elif suffix == "target":
        my_logger.debug("Collecting dict_ttSettings for %s and %s"%(host, suffix))
        dict_ttSettings = dict_target_ttSettings
    
    print dict_ttSettings
    v_user = dict_ttSettings["virtuoso"]["users"]["dba"]["username"]
    my_logger.debug("Extracted virtuoso username as %s for %s"%(v_user, host))
    
    v_password = dict_ttSettings["virtuoso"]["users"]["dba"]["password"]
    my_logger.debug("Extracted virtuoso password for %s"%(host))
    
    g_list = list()
    for k, v in dict_ttSettings["virtuoso"]["graphs"]["rw"].items():
        graph_name = v["graph_name"]
        g_list.append(graph_name)
        my_logger.debug("Initiating backup for graph %s"%graph_name)
        v_backup_path = "%s/%s/3t_%s-"%(v_backup_subpath, timestamp, k)
        v_dump_one_graph_subcmd= "EXEC=dump_one_graph(\'%s\',\'%s\',10000000);"%(graph_name, v_backup_path)
        v_dump_one_graph_cmd = "%s -U %s -P %s \"%s\""%(isql_path, v_user, v_password, v_dump_one_graph_subcmd)
        
        print "Taking backup on VIRTUOSO HOST: %s and VIRTUOSO USER: %s, GRAPH: %s"%(host, v_user, graph_name)
        my_logger.debug("Taking backup on VIRTUOSO HOST: %s and VIRTUOSO USER: %s, GRAPH: %s"%(host, v_user, graph_name))
        my_logger.debug("Calling \"v_dump_one_graph_aws(%s, %s, %s, %s, %s) \" "%(v_backup_subpath, timestamp, v_dump_one_graph_cmd, host, user, keyfile))
        
        try:
            v_dump_one_graph_aws(v_backup_subpath, timestamp, v_dump_one_graph_cmd, host, user, keyfile)
        except:
            my_logger.error("Calling \"v_dump_one_graph_aws() \" failed. Exiting")
            sys.exit("Calling \"v_dump_one_graph_aws() \" failed. Exiting")
        my_logger.debug("\"v_dump_one_graph_aws()\" successful.")
    
    return  g_list            
"""********************************************************************************************************************"""
def one_graph_backup_nyc(v_backup_subpath, isql_path, host, user, suffix):
    #backup_subpath = "/usr/local/virtuoso-opensource/share/virtuoso/vad/3t_backup"
    #isql_path = "/usr/bin/isql-vt"
    my_logger.debug("In one_graph_backup_aws(%s, %s, %s, %s, %s)"%(v_backup_subpath, isql_path, host, user, suffix))
    if suffix == "source":
        my_logger.debug("Collecting dict_ttSettings for %s and %s"%(host, suffix))
        dict_ttSettings = dict_source_ttSettings        
    elif suffix == "target":
        my_logger.debug("Collecting dict_ttSettings for %s and %s"%(host, suffix))
        dict_ttSettings = dict_target_ttSettings
    elif suffix == "backup":
        my_logger.debug("Collecting dict_ttSettings for %s and %s"%(host, suffix))
        dict_ttSettings = dict_source_ttSettings
    
    print dict_ttSettings
    v_user = dict_ttSettings["virtuoso"]["users"]["dba"]["username"]
    my_logger.debug("Extracted virtuoso username as %s for %s"%(v_user, host))
    
    v_password = dict_ttSettings["virtuoso"]["users"]["dba"]["password"]
    my_logger.debug("Extracted virtuoso password for %s"%(host))
     
    g_list = list()
    for k, v in dict_ttSettings["virtuoso"]["graphs"]["rw"].items():
        graph_name = v["graph_name"]
        g_list.append(graph_name)
        my_logger.debug("Initiating backup for graph %s"%graph_name)
        v_backup_path = "%s/%s/3t_%s-"%(v_backup_subpath, timestamp, k)
        v_dump_one_graph_subcmd= "EXEC=dump_one_graph(\'%s\',\'%s\',10000000);"%(graph_name, v_backup_path)
        v_dump_one_graph_cmd = "%s -U %s -P %s \"%s\""%(isql_path, v_user, v_password, v_dump_one_graph_subcmd)
        
        print "Taking backup on VIRTUOSO HOST: %s and VIRTUOSO USER: %s, GRAPH: %s"%(host, v_user, graph_name)
        my_logger.debug("Taking backup on VIRTUOSO HOST: %s and VIRTUOSO USER: %s, GRAPH: %s"%(host, v_user, graph_name))
        my_logger.debug("Calling \"v_dump_one_graph_nyc(%s, %s, %s, %s, %s) \" "%(v_backup_subpath, timestamp, v_dump_one_graph_cmd, host, user))
        
        try:
            v_dump_one_graph_nyc(v_backup_subpath, timestamp, v_dump_one_graph_cmd, host, user)
        except:
            my_logger.error("Calling \"v_dump_one_graph_nyc() \" failed. Exiting")
            sys.exit("Calling \"v_dump_one_graph_nyc() \" failed. Exiting")
        my_logger.debug("\"v_dump_one_graph_nyc()\" successful.")
    
    return  g_list
"""********************************************************************************************************************"""
def virtuoso_operations(s_host, t_host, l_host , l_user):
    my_logger.debug("In virtuoso_operations()...")
    isql_path_new = "/usr/bin/isql-vt"
    #db_dir_old = "/usr/local/virtuoso-opensource/var/lib/virtuoso/db/"
    #backup_sub_path_old = "/usr/local/virtuoso-opensource/share/virtuoso/vad/3t_backup"
    backup_sub_path_new = "/usr/share/virtuoso-opensource-7.1/vad/3t_backup"
    
    backup_sub_path = backup_sub_path_new
    """********************************************************************************************************************"""
    """ Setting source parameters...start"""
    
    my_logger.debug("Setting source host(%s) parameters..."%s_host)
    my_logger.debug("s_host : %s"%s_host)
    if "nyc.3top.com" in s_host:
        s_user = l_user
        my_logger.debug("s_user : %s"%s_user)
        my_logger.debug("Skipping cert as env is NYC...")
    elif "aws.3top.com" in s_host:
        s_user = dict_source_ttSettings["ec2"]["username"]
        my_logger.debug("s_user : %s"%s_user)
        s_dev_key_file=dict_source_ttSettings["ec2"]["cert_private_key"]
        my_logger.debug("s_dev_key_file : %s"%s_dev_key_file)
    
    """ Setting source backup params...start"""
    v_s_isql_path = isql_path_new
    v_s_backup_subpath = backup_sub_path
    """ Setting source backup params...stop"""
    
     
    """ Setting source parameters...stop"""
    """********************************************************************************************************************"""
    """ Setting target parameters...start"""
        
    my_logger.debug("Setting target host(%s) parameters..."%t_host)
    my_logger.debug("t_host : %s"%t_host)
    if "nyc.3top.com" in t_host:
        t_user = l_user
        my_logger.debug("t_user : %s"%t_user)
        my_logger.debug("Skipping cert as env is NYC...")
    elif "aws.3top.com" in t_host:
        t_user = dict_target_ttSettings["ec2"]["username"]
        my_logger.debug("t_user : %s"%t_user)
        t_dev_key_file=dict_target_ttSettings["ec2"]["cert_private_key"]
        my_logger.debug("t_dev_key_file : %s"%t_dev_key_file)
    
    """ Setting target backup params...start"""
    v_t_isql_path = isql_path_new
    v_t_backup_subpath = backup_sub_path
    """ Setting target backup params...stop"""
    
    """ Setting target parameters...stop"""
    """********************************************************************************************************************"""
    """SOURCE OPERATIONS START"""
    """********************************************************************************************************************"""
    """ Take backup on source_host...start"""
    
    if "aws.3top.com" in s_host:
        my_logger.debug("Found \"aws.3top.com\" in %s"%s_host)
        my_logger.debug("Calling one_graph_backup_aws(%s, %s, %s, %s, %s, %s)"%(v_s_backup_subpath, v_s_isql_path, s_host, s_user, s_dev_key_file, "source"))
        try:
            graph_list = one_graph_backup_aws(v_s_backup_subpath, v_s_isql_path, s_host, s_user, s_dev_key_file, "source")
        except:
            my_logger.error("Calling one_graph_backup_aws() failed. Exiting!")
            sys.exit("Calling one_graph_backup_aws() failed. Exiting!")
        my_logger.debug("Calling one_graph_backup_aws() Successful")
   
    elif "nyc.3top.com" in s_host:
        my_logger.debug("Found \"nyc.3top.com\" in %s"%s_host)
        my_logger.debug("Calling one_graph_backup_nyc(%s, %s, %s, %s, %s, %s)"%(v_s_backup_subpath, v_s_isql_path, s_host, s_user, "source"))
        try:
            graph_list = one_graph_backup_nyc(v_s_backup_subpath, v_s_isql_path, s_host, s_user, "source")
        except:
            my_logger.error("Calling one_graph_backup_nyc() failed. Exiting!")
            sys.exit("Calling one_graph_backup_nyc() failed. Exiting!")
        my_logger.debug("Calling one_graph_backup_nyc() Successful")
    
    """ Take backup on source_host...stop"""
    """********************************************************************************************************************"""
    """ Pull backup from source_host to local...start"""
    
    """Create local source backup_before_dir...start"""
    
    #print dirTimestamp
    l_s_backup_before_dir = dirTimestamp + "/virtuoso-backup/s_backup_before"
    v_s_backup_before_dir = backup_sub_path + "/" + timestamp
    
    #print l_s_backup_before_dir
    if not os.path.exists(l_s_backup_before_dir):
        os.makedirs(l_s_backup_before_dir)
        
    """Create local source backup_before_dir...stop"""
    
    try:
        v_pull_backup_to_local(s_user, s_host,  v_s_backup_before_dir, l_s_backup_before_dir)
    except:
        my_logger.error("Calling  v_backup_to_local() failed. Exiting!")
        sys.exit("Calling  v_backup_to_local() failed. Exiting!")
        
    """ Pull backup from source_host to local...stop"""
    """********************************************************************************************************************"""
    """ Delete backup on source_host...start"""
    
    if "aws.3top.com" in s_host:
        my_logger.debug("Found \"aws.3top.com\" in %s"%s_host)
        my_logger.debug("Calling delete_backup_on_aws_virtuoso(%s, %s, %s, %s, %s)"%(v_s_backup_subpath, timestamp, s_host, s_user, s_dev_key_file))
        try:
            delete_backup_on_aws_virtuoso(v_s_backup_subpath, timestamp, s_host, s_user, s_dev_key_file)
        except:
            my_logger.error("Calling delete_backup_on_aws_virtuoso() failed. Exiting!")
            sys.exit("Calling delete_backup_on_aws_virtuoso() failed. Exiting!")
        my_logger.debug("Calling delete_backup_on_aws_virtuoso() Successful")
    
    elif "nyc.3top.com" in s_host:
        my_logger.debug("Found \"nyc.3top.com\" in %s"%s_host)
        my_logger.debug("Calling delete_backup_on_nyc_virtuoso(%s, %s, %s, %s, %s)"%(v_s_backup_subpath, timestamp, s_host, s_user))
        try:
            delete_backup_on_nyc_virtuoso(v_s_backup_subpath, timestamp, s_host, s_user)
        except:
            my_logger.error("Calling delete_backup_on_nyc_virtuoso() failed. Exiting!")
            sys.exit("Calling delete_backup_on_nyc_virtuoso() failed. Exiting!")
        my_logger.debug("Calling delete_backup_on_nyc_virtuoso() Successful")
        
    """ Delete backup on source_host...stop"""
    """********************************************************************************************************************"""
    """SOURCE OPERATIONS STOP"""
    """********************************************************************************************************************"""
    """TARGET OPERATIONS START"""
    """********************************************************************************************************************"""
    """ Take backup on target_host...start"""
    
    if "aws.3top.com" in t_host:
        my_logger.debug("Found \"aws.3top.com\" in %s"%t_host)
        my_logger.debug("Calling one_graph_backup_aws(%s, %s, %s, %s, %s, %s)"%(v_t_backup_subpath, v_t_isql_path, t_host, t_user, t_dev_key_file, "target"))
        try:
            one_graph_backup_aws(v_t_backup_subpath, v_t_isql_path, t_host, t_user, t_dev_key_file, "target")
        except:
            my_logger.error("Calling one_graph_backup_aws() failed. Exiting!")
            sys.exit("Calling one_graph_backup_aws() failed. Exiting!")
        my_logger.debug("Calling one_graph_backup_aws() Successful")
   
    elif "nyc.3top.com" in t_host:
        my_logger.debug("Found \"nyc.3top.com\" in %s"%t_host)
        my_logger.debug("Calling one_graph_backup_nyc(%s, %s, %s, %s, %s, %s)"%(v_t_backup_subpath, v_t_isql_path, t_host, t_user, "target"))
        try:
            one_graph_backup_nyc(v_t_backup_subpath, v_t_isql_path, t_host, t_user, "target")
        except:
            my_logger.error("Calling one_graph_backup_nyc() failed. Exiting!")
            sys.exit("Calling one_graph_backup_nyc() failed. Exiting!")
        my_logger.debug("Calling one_graph_backup_nyc() Successful")
    
    """ Take backup on target_host...stop"""
    """********************************************************************************************************************"""
    """ Pull backup from source_host to local...start"""
    
    """Create local source backup_before_dir...start"""
    
    #print dirTimestamp
    l_t_backup_before_dir = dirTimestamp + "/virtuoso-backup/t_backup_before"
    v_t_backup_before_dir = backup_sub_path + "/" + timestamp
    
    #print l_s_backup_before_dir
    if not os.path.exists(l_t_backup_before_dir):
        os.makedirs(l_t_backup_before_dir)
        
    """Create local source backup_before_dir...stop"""
    
    try:
        v_pull_backup_to_local(t_user, t_host,  v_t_backup_before_dir, l_t_backup_before_dir)
    except:
        my_logger.error("Calling  v_backup_to_local() failed. Exiting!")
        sys.exit("Calling  v_backup_to_local() failed. Exiting!")
    my_logger.debug("v_backup_to_local successful.")
        
    """ Pull backup from source_host to local...stop"""
    """********************************************************************************************************************"""
    """ Delete backup on source_host...start"""
    
    if "aws.3top.com" in t_host:
        my_logger.debug("Found \"aws.3top.com\" in %s"%t_host)
        my_logger.debug("Calling delete_backup_on_aws_virtuoso(%s, %s, %s, %s)"%(v_t_backup_subpath, timestamp, t_host, t_user, t_dev_key_file))
        try:
            delete_backup_on_aws_virtuoso(v_t_backup_subpath, timestamp, t_host, t_user, t_dev_key_file)
        except:
            my_logger.error("Calling delete_backup_on_aws_virtuoso() failed. Exiting!")
            sys.exit("Calling delete_backup_on_aws_virtuoso() failed. Exiting!")
        my_logger.debug("Calling delete_backup_on_aws_virtuoso() Successful")
    
    elif "nyc.3top.com" in t_host:
        my_logger.debug("Found \"nyc.3top.com\" in %s"%t_host)
        my_logger.debug("Calling delete_backup_on_nyc_virtuoso(%s, %s, %s, %s)"%(v_s_backup_subpath, timestamp, t_host, t_user))
        try:
            delete_backup_on_nyc_virtuoso(v_t_backup_subpath, timestamp, t_host, t_user)
        except:
            my_logger.error("Calling delete_backup_on_nyc_virtuoso() failed. Exiting!")
            sys.exit("Calling delete_backup_on_nyc_virtuoso() failed. Exiting!")
        my_logger.debug("Calling delete_backup_on_nyc_virtuoso() Successful")
        
    """ Delete backup on source_host...stop"""
    """********************************************************************************************************************"""
    """ Put backup from local to source_host...start"""
    
    """Create source_host v_t_temp_dir ...start"""
    #print dirTimestamp
    v_t_temp_dir = "/tmp/virtuoso-restore/one_graph/" + timestamp
       
    """Create local source backup_before_dir...stop"""
    if "aws.3top.com" in t_host:
        my_logger.debug("Running v_push_backup_to_aws_target(%s, %s, %s, %s) failed. Exiting!"%(t_user, t_host, l_t_backup_before_dir + "/*", t_dev_key_file, v_t_temp_dir))
        try:
            v_push_backup_to_aws_target(t_user, t_host, l_t_backup_before_dir + "/*", t_dev_key_file, v_t_temp_dir)
        except:
            my_logger.error("v_push_backup_to_aws_target() failed. Exiting!")
            sys.exit("v_push_backup_to_aws_target() failed. Exiting!")
        my_logger.debug("v_push_backup_to_aws_target() Successful")
    elif "nyc.3top.com" in t_host:
        my_logger.debug("Running v_push_backup_to_nyc_target(%s, %s, %s, %s) failed. Exiting!"%(t_user, t_host, l_t_backup_before_dir + "/*", v_t_temp_dir))
        try:
            v_push_backup_to_nyc_target(t_user, t_host, l_t_backup_before_dir + "/*", v_t_temp_dir)
        except:
            my_logger.error("v_push_backup_to_nyc_target() failed. Exiting!")
            sys.exit("v_push_backup_to_nyc_target() failed. Exiting!")
        my_logger.debug("v_push_backup_to_nyc_target() Successful")
        
    """ Put backup from local to source_host...stop"""
    """********************************************************************************************************************"""
    """ Put restore backup...start"""
    
    for graph in graph_list:
        my_logger.debug("Starting restore for graph %s in %s"%(graph, t_host))
        try:
            v_restore_rw(t_user, t_host, t_dev_key_file, graph, v_t_temp_dir)
        except:
            my_logger.error("Restore failed for graph: %s. Exiting!"%graph)
            sys.exit("Restore failed for graph: %s. Exiting!"%graph)
        my_logger.debug("Restore for graph: %s successful!"%graph)
    
    """ Put restore backup...stop"""
    """********************************************************************************************************************"""
    """ Delete backup on ~/tmp...start"""
    
    
    
    """ Delete backup on ~/tmp...stop"""
    """********************************************************************************************************************"""

def virtuoso_backup(dict_ttSettings, s_host, l_host , l_user, backup_dir):
    #global dict_source_ttSettings
    dict_source_ttSettings = dict_ttSettings
    dict_target_ttSettings = 'NA'
    my_logger.debug("In virtuoso_backup()...")
    #isql_path_new = "/usr/bin/isql-vt"
    isql_path_old = "/usr/local/virtuoso-opensource/bin/isql"
    db_dir_old = "/usr/local/virtuoso-opensource/var/lib/virtuoso/db/"
    backup_sub_path_old = "/usr/local/virtuoso-opensource/share/virtuoso/vad/3t_backup"
    #backup_sub_path_new = "/usr/share/virtuoso-opensource-7.1/vad/3t_backup"
    
    #backup_sub_path = backup_sub_path_new
    backup_sub_path = backup_sub_path_old
    """********************************************************************************************************************"""
    """ Setting source parameters...start"""
    
    my_logger.debug("Setting source host(%s) parameters..."%s_host)
    my_logger.debug("s_host : %s"%s_host)
    if "nyc.3top.com" in s_host:
        s_user = l_user
        my_logger.debug("s_user : %s"%s_user)
        my_logger.debug("Skipping cert as env is NYC...")
    elif "aws.3top.com" in s_host:
        s_user = dict_source_ttSettings["ec2"]["username"]
        my_logger.debug("s_user : %s"%s_user)
        s_dev_key_file=dict_source_ttSettings["ec2"]["cert_private_key"]
        my_logger.debug("s_dev_key_file : %s"%s_dev_key_file)
    
    """ Setting source backup params...start"""
    #v_s_isql_path = isql_path_new
    v_s_isql_path = isql_path_old
    v_s_backup_subpath = backup_sub_path
    """ Setting source backup params...stop"""
    
     
    """ Setting source parameters...stop"""
    """********************************************************************************************************************"""
    """********************************************************************************************************************"""
    """SOURCE OPERATIONS START"""
    """********************************************************************************************************************"""
    """ Take backup on source_host...start"""
    
    if "aws.3top.com" in s_host:
        my_logger.debug("Found \"aws.3top.com\" in %s"%s_host)
        my_logger.debug("Calling one_graph_backup_aws(%s, %s, %s, %s, %s, %s)"%(v_s_backup_subpath, v_s_isql_path, s_host, s_user, s_dev_key_file, "backup"))
        try:
            graph_list = one_graph_backup_aws(v_s_backup_subpath, v_s_isql_path, s_host, s_user, s_dev_key_file, "backup")
        except:
            my_logger.error("Calling one_graph_backup_aws() failed. Exiting!")
            sys.exit("Calling one_graph_backup_aws() failed. Exiting!")
        my_logger.debug("Calling one_graph_backup_aws() Successful")
   
    elif "nyc.3top.com" in s_host:
        my_logger.debug("Found \"nyc.3top.com\" in %s"%s_host)
        my_logger.debug("Calling one_graph_backup_nyc(%s, %s, %s, %s, %s)"%(v_s_backup_subpath, v_s_isql_path, s_host, s_user, "backup"))
        try:
            graph_list = one_graph_backup_nyc(v_s_backup_subpath, v_s_isql_path, s_host, s_user, "backup")
        except:
            my_logger.error("Calling one_graph_backup_nyc() failed. Exiting!")
            sys.exit("Calling one_graph_backup_nyc() failed. Exiting!")
        my_logger.debug("Calling one_graph_backup_nyc() Successful")
    
    """ Take backup on source_host...stop"""
    """********************************************************************************************************************"""
    """ Pull backup from source_host to local...start"""
    
    if backup_dir == 'NA':
        """Create local source backup_before_dir...start"""
        #print dirTimestamp
        l_s_backup_before_dir = dirTimestamp + "/virtuoso-backup/s_backup_before"
        v_s_backup_before_dir = backup_sub_path + "/" + timestamp
        
        #print l_s_backup_before_dir
        if not os.path.exists(l_s_backup_before_dir):
            os.makedirs(l_s_backup_before_dir)
            
        """Create local source backup_before_dir...stop"""
        
    elif backup_dir != 'NA':
        l_s_backup_before_dir = backup_dir
        v_s_backup_before_dir = backup_sub_path + "/" + timestamp
    
    try:
        v_pull_backup_to_local(s_user, s_host,  v_s_backup_before_dir, l_s_backup_before_dir)
    except:
        my_logger.error("Calling  v_backup_to_local() failed. Exiting!")
        sys.exit("Calling  v_backup_to_local() failed. Exiting!")
        
    """ Pull backup from source_host to local...stop"""
    """********************************************************************************************************************"""
    """ Delete backup on source_host...start"""
    
    if "aws.3top.com" in s_host:
        my_logger.debug("Found \"aws.3top.com\" in %s"%s_host)
        my_logger.debug("Calling delete_backup_on_aws_virtuoso(%s, %s, %s, %s, %s)"%(v_s_backup_subpath, timestamp, s_host, s_user, s_dev_key_file))
        try:
            delete_backup_on_aws_virtuoso(v_s_backup_subpath, timestamp, s_host, s_user, s_dev_key_file)
        except:
            my_logger.error("Calling delete_backup_on_aws_virtuoso() failed. Exiting!")
            sys.exit("Calling delete_backup_on_aws_virtuoso() failed. Exiting!")
        my_logger.debug("Calling delete_backup_on_aws_virtuoso() Successful")
    
    elif "nyc.3top.com" in s_host:
        my_logger.debug("Found \"nyc.3top.com\" in %s"%s_host)
        my_logger.debug("Calling delete_backup_on_nyc_virtuoso(%s, %s, %s, %s)"%(v_s_backup_subpath, timestamp, s_host, s_user))
        try:
            delete_backup_on_nyc_virtuoso(v_s_backup_subpath, timestamp, s_host, s_user)
        except:
            my_logger.error("Calling delete_backup_on_nyc_virtuoso() failed. Exiting!")
            sys.exit("Calling delete_backup_on_nyc_virtuoso() failed. Exiting!")
        my_logger.debug("Calling delete_backup_on_nyc_virtuoso() Successful")
        
    """ Delete backup on source_host...stop"""
    """********************************************************************************************************************"""
    """SOURCE OPERATIONS STOP"""
    """********************************************************************************************************************"""
    