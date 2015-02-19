from fabric.context_managers import settings
from fabfile_n import start, stop, restart, neo4j_a_dev, localhost, online_backup, backup_to_local, clone_db
import fabfile_n
import sys
import os
import time
from datetime import datetime
from ttLib.ttSys import get_s3_config, dirCreateTimestamp
import logging 
import ttLib.logger as logger
from fabric.api import *
from fabric.utils import abort, error, warn
from fabric.contrib import files
from fabric.contrib.console import confirm
from contextlib import contextmanager as _contextmanager

print "Downloading ttSettings from S3...",
try:
    s3_config_folder = "nyc-sys"
    s3_config_filename = "ttSettings.conf"
    global dict_ttSettings
    dict_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
    #print"complete."  
    #print  dict_ttSettings
except:
    sys.exit("Error Encountered: %s. Download of ttSettings from S3 failed. Exiting!" %str(sys.exc_info()))

PATH_ROOT = dict_ttSettings["PATH_ROOT"]
PATH_LOG = dict_ttSettings["PATH_LOG"]
backup_path = PATH_ROOT + PATH_LOG + "/back-up"
neo4j_install = '/var/lib/neo4j'
timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
log_dir = backup_path + "/" + timestamp

try:
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        print("successful.")
except:
    print "failed.", sys.exc_info()[0]
    sys.exit(-1)


print"Backup and logging directory created successfully..."   
"""    
my_logger = logging.getLogger('neo4j')
for h in my_logger.handlers:
    my_logger.removeHandler(h)
    log_file = dir_backup + '/neo4j.log'
    for h in logger.get_handlers('dnsOps', log_file):
        my_logger.addHandler(h)
        my_logger.setLevel(logging.DEBUG)
        print"Logger created successfully. Continuing..."
        my_logger.info("Logger created successfully. Continuing...")
"""

#fabfile.env.host_string = fabfile.env.hosts[0]
#fabfile.env.user=fabfile.env.user
#fabfile.env.key_filename=fabfile.env.key_filename
"""
print "Taking backup for %s@%s"%(dict_ttSettings["neo4j"]["a_dev"]["user"],dict_ttSettings["neo4j"]["a_dev"]["host"])
source_before=dir_backup + "/" + dict_ttSettings["neo4j"]["a_dev"]["host"] + "-Before" 
online_backup(dict_ttSettings["neo4j"]["a_dev"]["user"], dict_ttSettings["neo4j"]["a_dev"]["host"], dict_ttSettings["aws-dev-cert"], source_before)
print "\nTaking backup for %s@%s"%(dict_ttSettings["neo4j"]["vm"]["user"], dict_ttSettings["neo4j"]["vm"]["host"])
target_before=dir_backup + "/" + dict_ttSettings["neo4j"]["vm"]["host"] + "-Before"
online_backup(dict_ttSettings["neo4j"]["vm"]["user"], dict_ttSettings["neo4j"]["vm"]["host"], dict_ttSettings["aws-dev-cert"], target_before)
"""
source_suffix = dict_ttSettings["neo4j"]["a_dev"]["host"] + "-Before"
source_dir_backup=log_dir + "/" + source_suffix
target_suffix = dict_ttSettings["neo4j"]["vm"]["host"] + "-Before"
target_dir_backup=log_dir + "/" + target_suffix 
print "\nStarting backup operations for %s@%s"%(dict_ttSettings["neo4j"]["a_dev"]["user"],dict_ttSettings["neo4j"]["a_dev"]["host"])
tar_source_file=backup_to_local(dict_ttSettings["neo4j"]["a_dev"]["user"], dict_ttSettings["neo4j"]["a_dev"]["host"], dict_ttSettings["aws-dev-cert"], source_dir_backup, log_dir)
print "\nStarting backup operations for %s@%s"%(dict_ttSettings["neo4j"]["vm"]["user"], dict_ttSettings["neo4j"]["vm"]["host"])
tar_target_file=backup_to_local(dict_ttSettings["neo4j"]["vm"]["user"], dict_ttSettings["neo4j"]["vm"]["host"], dict_ttSettings["aws-dev-cert"], target_dir_backup, log_dir)



#print "\nStarting clone operations for %s@%s"%(dict_ttSettings["neo4j"]["a_dev"]["user"],dict_ttSettings["neo4j"]["a_dev"]["host"])
#clone_db(dict_ttSettings["neo4j"]["a_dev"]["user"], dict_ttSettings["neo4j"]["a_dev"]["host"], dict_ttSettings["aws-dev-cert"], source_dir_backup, log_dir)
print "\nStarting clone operations for %s@%s"%(dict_ttSettings["neo4j"]["vm"]["user"], dict_ttSettings["neo4j"]["vm"]["host"])
#data_dir="/var/lib/neo4j/data/graph.db/"
clone_db(dict_ttSettings["neo4j"]["vm"]["user"], dict_ttSettings["neo4j"]["vm"]["host"], dict_ttSettings["aws-dev-cert"], source_suffix, timestamp)
"""
print "\nCopying backup from %s to Localhost..."%dict_ttSettings["neo4j"]["a_dev"]["host"]
get_from(dict_ttSettings["neo4j"]["a_dev"]["user"], dict_ttSettings["neo4j"]["a_dev"]["host"], dict_ttSettings["aws-dev-cert"], source_before + ".tgz", dir_backup)
print "\nCopying backup from %s to Localhost..."%dict_ttSettings["neo4j"]["vm"]["host"]
get_from(dict_ttSettings["neo4j"]["vm"]["user"], dict_ttSettings["neo4j"]["vm"]["host"], dict_ttSettings["aws-dev-cert"], target_before + ".tgz", dir_backup)
"""
























