from __future__ import with_statement
import os
import sys
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name'])
from fabric.api import *
from fabric.contrib.console import confirm
from ttLib.ttSys import get_s3_config
from ttLib import *
import subprocess
from datetime import datetime
from fabric.utils import abort, error, warn
from fabric.contrib import files
from contextlib import contextmanager as _contextmanager
from fabric.contrib.files import exists
import time
#timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')

"""
from __future__ import with_statement
import os
import sys
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name'])
from fabric.api import *
from fabric.contrib.console import confirm
from ttLib.ttSys import get_s3_config
from ttLib import *
from __future__ import with_statement
import os
import sys
import subprocess
from datetime import datetime
from fabric.api import *
from fabric.utils import abort, error, warn
from fabric.contrib import files
from fabric.contrib.console import confirm
from contextlib import contextmanager as _contextmanager
from fabric.contrib.files import exists
import time
#timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
"""

# globals
env.neo4j_install = '/var/lib/neo4j'
env.service_name = 'neo4j-service'
s3_config_folder = "nyc-sys"
s3_config_filename = "ttSettings.conf"
my_logger.debug("Initiating \"dict_ttSettings = get_s3_config(%s, %s)\""%(s3_config_folder, s3_config_filename))
"""
try:
    global dict_ttSettings
    dict_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
except:
    my_logger.error("Error Encountered in: \"dict_ttSettings = get_s3_config(%s, %s)\". Exiting!"%(s3_config_folder, s3_config_filename))
    sys.exit("Error Encountered in: \"dict_ttSettings = get_s3_config(%s, %s)\" Exiting!"%(s3_config_folder, s3_config_filename))

my_logger.debug("\"dict_ttSettings = get_s3_config(%s, %s)\"...successful!"%(s3_config_folder, s3_config_filename))
"""

@task
def neo4j_a_dev():
    my_logger.debug("Setting env based on neo4j_a_dev()....")
    try:
        env.dev_a_host = 'neo4j.a.dev.aws.3top.com'
        env.dev_a_user = 'ubuntu'
        env.dev_a_key_filename = '/home/3TOP/skanupuru/Home/practice/aws-dev-cert.pem'
        my_logger.debug("Set values for env are dev_a_host: %(dev_a_host)s, dev_a_user: %(dev_a_user)s, dev_a_key_filename: %(dev_a_key_filename)s"%(env))
    except:
        my_logger.error("Error Encountered in : Setting env based on neo4j_a_dev(). Exiting!")
        sys.exit("Error Encountered in: Setting env based on neo4j_a_dev(). Exiting!")
    my_logger.debug("Setting env based on neo4j_a_dev()...successful!")
    
        

@task
def localhost():
    "Use the remote testing server."
    my_logger.debug("Setting env based on localhost()...")
    try:
        env.hosts = ['skanupuru-lxu1.nyc.3top.com']
        env.user="skanupuru"
        my_logger.debug("Set values for env are hosts: %(hosts)s, user: %(user)s"%(env))
    except:
        my_logger.error("Error Encountered. Setting env based on localhost(). Exiting!" )
        sys.exit("Error Encountered. Setting env based on localhost(). Exiting!")
    my_logger.debug("Setting env based on localhost()...successful!")

@task
def neo4j_vm():
    "Use the remote staging server."
    my_logger.debug("Setting env based on neo4j_vm()...")
    try:
        env.hosts = ['skanupuru-lxs3.nyc.3top.com']
        env.user= "skanupuru"
        my_logger.debug("Set values for env are hosts: %(hosts)s, user: %(user)s"%(env))
    except:
        my_logger.error("Error Encountered in:  Setting env based on neo4j_vm(). Exiting!" )
        sys.exit("Error Encountered in:  Setting env based on neo4j_vm(). Exiting!")
    my_logger.debug("Setting env based on neo4j_vm()...successful!")

@task
def n_start_aws(user, host_name, keyfile, log_dir):
    my_logger.debug("Initiating start() neo4j service for %s@%s"%(user, host_name))
    "Start neo4j-service."
    # NB: This doesn't use sudo() directly because it insists on asking
    # for a password, even though we should have NOPASSWD in visudo.
    #with settings(host_string= "%s@%s"%(user, host_name)):
    try:
        with settings(host_string= "%s@%s"%(user, host_name), dst=log_dir):
            env.key_filename= keyfile
            my_logger.debug("Received key_filename: %(key_filename)s "%(env))
            if confirm("Start Neo4j server %s?" %host_name):
                my_logger.debug("Running command \"sudo('sudo service %(service_name)s start\')\" " % env)
                sudo('sudo service %(service_name)s start' % env)
    except:
        my_logger.error("Error Encountered in:  Initiating start() neo4j service for %s@%s. Exiting!"%( user, host_name))
        sys.exit("Error Encountered in:  Initiating start() neo4j service for %s@%s. Exiting!"%( user, host_name))
    my_logger.debug("start() neo4j service for %s@%s...successful!"%(user, host_name))

@task
def n_stop_aws(user, host_name, keyfile, log_dir):
    #my_logger.debug("Initiating stop() neo4j service for %s@%s"%(user, host_name))
    "Stop neo4j-service."
    # NB: This doesn't use sudo() directly because it insists on asking
    # for a password, even though we should have NOPASSWD in visudo.
    my_logger.debug("Initiating \"with settings(host_string= \"%s@%s, dst=%s"%(user, host_name, log_dir))
    try:
        with settings(host_string= "%s@%s"%(user, host_name), dst=log_dir):
            env.key_filename= keyfile
            my_logger.debug("Received key_filename: %(key_filename)s "%(env))
            if confirm("Stop Neo4j server for %s?" %host_name):
                my_logger.debug("Running command \"sudo('sudo service %(service_name)s stop\')\" " % env)
                sudo('sudo service %(service_name)s stop' % env)
    except:
        my_logger.error("Error Encountered in:  \nInitiating stop() neo4j service for %s@%s. Exiting!"%(user, host_name))
        sys.exit("Error Encountered in:  Initiating stop() neo4j service for %s@%s. Exiting!"%(user, host_name))
    my_logger.debug("stop() neo4j service for %s@%s...successful!"%(user, host_name))

@task
def n_stop_nyc(user, host_name, log_dir):
    #my_logger.debug("Initiating stop() neo4j service for %s@%s"%(user, host_name))
    "Stop neo4j-service."
    # NB: This doesn't use sudo() directly because it insists on asking
    # for a password, even though we should have NOPASSWD in visudo.
    my_logger.debug("Initiating \"with settings(host_string= \"%s@%s, dst=%s"%(user, host_name, log_dir))
    try:
        with settings(host_string= "%s@%s"%(user, host_name), dst=log_dir):
            if confirm("Stop Neo4j server for %s?" %host_name):
                my_logger.debug("Running command \"sudo('sudo service %(service_name)s start\')\" " % env)
                sudo('sudo service %(service_name)s stop' % env)
    except:
        my_logger.error("Error Encountered in:  \nInitiating stop() neo4j service for %s@%s. Exiting!"%(user, host_name))
        sys.exit("Error Encountered in:  Initiating stop() neo4j service for %s@%s. Exiting!"%(user, host_name))
    my_logger.debug("stop() neo4j service for %s@%s...successful!"%(user, host_name))
    
    
@task 
def n_restart_aws(user, host_name, keyfile, log_dir):
    #my_logger.debug("Initiating restart() of neo4j service for %s@%s"%(user, host_name))
    "Restart neo4j-service."
    # NB: This doesn't use sudo() directly because it insists on asking
    # for a password, even though we should have NOPASSWD in visudo.
    #with settings(host_string= "%s@%s"%(user, host_name)):
    my_logger.debug("Initiating \"with settings(host_string= \"%s@%s, dst=%s"%(user, host_name, log_dir))
    try:
        with settings(host_string= "%s@%s"%(user, host_name), dst=log_dir):
            env.key_filename= keyfile
            my_logger.debug("Received key_filename: %(key_filename)s "%(env))
            if confirm("Restart Neo4j server for %s?" %host_name):
                my_logger.debug("Running command \"sudo('service %(service_name)s restart\')\" " % env)
                sudo('service %(service_name)s restart' % env)
    except:
        my_logger.error("Error Encountered in:  Initiating restart() neo4j service for %s@%s. Exiting!"%( user, host_name))
        sys.exit("Error Encountered in:  Initiating restart() neo4j service for %s@%s. Exiting!"%( user, host_name))
    my_logger.debug("restart() neo4j service for %s@%s...successful!"%(user, host_name))

    
@task 
def n_restart_nyc(user, host_name, log_dir):
    #my_logger.debug("Initiating restart() of neo4j service for %s@%s"%(user, host_name))
    "Restart neo4j-service."
    # NB: This doesn't use sudo() directly because it insists on asking
    # for a password, even though we should have NOPASSWD in visudo.
    #with settings(host_string= "%s@%s"%(user, host_name)):
    my_logger.debug("Initiating \"with settings(host_string= \"%s@%s, dst=%s"%(user, host_name, log_dir))
    try:
        with settings(host_string= "%s@%s"%(user, host_name), dst=log_dir):
            if confirm("Restart Neo4j server for %s?" %host_name):
                my_logger.debug("Running command \"sudo('service %(service_name)s restart\')\" " % env)
                sudo('service %(service_name)s restart' % env)
    except:
        my_logger.error("Error Encountered in:  Initiating restart() neo4j service for %s@%s. Exiting!"%( user, host_name))
        sys.exit("Error Encountered in:  Initiating restart() neo4j service for %s@%s. Exiting!"%( user, host_name))
    my_logger.debug("restart() neo4j service for %s@%s...successful!"%(user, host_name))
    
    
@task
def n_get_from(user, host_name, keyfile,tar_file, local_dir):
    my_logger.debug("Initiating get_from() for %s@%s, tar_file: %s"%(user, host_name))
    try:
        my_logger.debug("Received tar_file name: %s, local_dir: %s"%(tar_file, local_dir))
        with settings(host_string="%s@%s"%(user, host_name),tmpdst= local_dir):
            my_logger.debug("Running command \"get(%s, os.path.dirname(%s + \"/\"))\""%(tar_file,local_dir) )
            get(tar_file, os.path.dirname(local_dir + "/"))
    except:
        my_logger.error("Error Encountered in:  Initiating get_from() neo4j service for %s@%s. Exiting!"%( user, host_name))
        sys.exit("Error Encountered in:  Initiating get_from()) neo4j service for %s@%s. Exiting!"%( user, host_name))
    my_logger.debug("get_from() neo4j service for %s@%s...successful!"%(user, host_name))
        

@task
def n_online_backup(user, host_name, keyfile, dir_backup):
    #my_logger.debug("Initiating online_backup() for %s@%s"%(user, host_name))
    """
    Do an online backup to a particular directory on the server.
    #online_backup:/path/on/server/backup.graph.db
    """
    try:
        env.key_filename=keyfile
        my_logger.debug("Received key_filename: %(key_filename)s "%(env))
        my_logger.debug("Received dir_backup: %s "%dir_backup)
        my_logger.debug("Running command run(\"/var/lib/neo4j/bin/neo4j-backup -from single://%s@%s:6362 -to %s\") " %(user, host_name, dir_backup))
        run("/var/lib/neo4j/bin/neo4j-backup -from single://%s@%s:6362 -to %s" %(user, host_name, dir_backup))
    except:
        my_logger.error("Error Encountered in:  Initiating online_backup() neo4j service for %s@%s. Exiting!"%( user, host_name))
        sys.exit("Error Encountered in:  Initiating stop() neo4j service for %s@%s. Exiting!"%( user, host_name))
    my_logger.debug("stop() neo4j service for %s@%s...successful!"%(user, host_name))

@task
def n_backup_aws_to_local(user, host_name, local_user, local_host_name, keyfile, dir_backup, log_dir):
    ##my_logger.debug("Initiating backup_to_local() for %s@%s"%(user, host_name))
    """Copy a Neo4j DB from a server using the backup tool.
    This creates a copy of the running DB in /tmp, zips it,
    downloads the zip, extracts it to the specified DB, and
    cleans up.
    
    online_clone_db:/local/path/to/graph.db
    """
    tar_file=dir_backup + ".tgz"
    my_logger.debug("Initiating backup_to_local() of %s@%s from %s@%s"%(user, host_name, local_user, local_host_name))
    my_logger.debug("Running command \" with settings(host_string=\"%s@%s\", dst=%s):\""%(user, host_name, dir_backup))
    #env.key_filename=keyfile
    try:
        with settings(host_string="%s@%s"%(local_user, local_host_name), dst=dir_backup):
            my_logger.info("Taking backup for %s@%s"%(user, host_name))
            env.key_filename=keyfile
            my_logger.debug("Received key_filename: %(key_filename)s "%(env))
            my_logger.debug("Received dir_backup: %s, tar_file: %s "%(dir_backup, tar_file))
            my_logger.debug("Running command run(\"/var/lib/neo4j/bin/neo4j-backup -from single://%s@%s:6362 -to %s\") "%(user, host_name, dir_backup))
            run("/var/lib/neo4j/bin/neo4j-backup -from single://%s@%s:6362 -to %s" %(user, host_name, dir_backup))
            my_logger.debug("\nCreating backup-tar for %s@%s"%(user, host_name))
            my_logger.debug("Running command run(\"tar --create --gzip --file %s -C %s .\") " % (tar_file , dir_backup))
            run("tar --create --gzip --file %s -C %s ." % (tar_file , dir_backup))
            my_logger.debug("Running command run(\"rm -rf %s\")" % dir_backup)
            #run("rm -rf %s" % dir_backup)
    except:
        my_logger.error("Error Encountered in:  Initiating backup_to_local() of %s@%s from %s@%s. Exiting!"%( user, host_name, local_user, local_host_name))
        sys.exit("Error Encountered in:  Initiating backup_to_local() of %s@%s from %s@%s. Exiting!"%( user, host_name, local_user, local_host_name))
    my_logger.debug("Initiating backup_to_local() of %s@%s from %s@%s...successful!"%(user, host_name, local_user, local_host_name))
    return tar_file

@task
def n_backup_nyc_to_local(user, host_name, local_user, local_host_name, dir_backup, log_dir):
    ##my_logger.debug("Initiating backup_to_local() for %s@%s"%(user, host_name))
    """Copy a Neo4j DB from a server using the backup tool.
    This creates a copy of the running DB in /tmp, zips it,
    downloads the zip, extracts it to the specified DB, and
    cleans up.
    
    online_clone_db:/local/path/to/graph.db
    """
    tar_file=dir_backup + ".tgz"
    my_logger.debug("Initiating backup_to_local() of %s@%s from %s@%s"%(user, host_name, local_user, local_host_name))
    my_logger.debug("Running command \" with settings(host_string=\"%s@%s\", dst=%s):\". Exiting!"%(local_user, local_host_name, dir_backup))
    try:
        with settings(host_string="%s@%s"%(local_user, local_host_name), dst=dir_backup):
            my_logger.info("Taking backup for %s@%s"%(user, host_name))
            my_logger.debug("Received key_filename: %(key_filename)s "%(env))
            my_logger.debug("Received dir_backup: %s, tar_file: %s "%(dir_backup, tar_file))
            my_logger.debug("Running command run(\"/var/lib/neo4j/bin/neo4j-backup -from single://%s@%s:6362 -to %s\") "%(user, host_name, dir_backup))
            run("/var/lib/neo4j/bin/neo4j-backup -from single://%s@%s:6362 -to %s" %(user, host_name, dir_backup))
            my_logger.debug("\nCreating backup-tar for %s@%s"%(user, host_name))
            my_logger.debug("Running command run(\"tar --create --gzip --file %s -C %s .\") " % (tar_file , dir_backup))
            run("tar --create --gzip --file %s -C %s ." % (tar_file , dir_backup))
            my_logger.debug("Running command run(\"rm -rf %s\")" % dir_backup)
            #run("rm -rf %s" % dir_backup)
    except:
        my_logger.error("Error Encountered in:  Initiating backup_to_local() of %s@%s from %s@%s. Exiting!"%( user, host_name, local_user, local_host_name))
        sys.exit("Error Encountered in:  Initiating backup_to_local() of %s@%s from %s@%s. Exiting!"%( user, host_name, local_user, local_host_name))
    my_logger.debug("Initiating backup_to_local() of %s@%s from %s@%s...successful!"%(user, host_name, local_user, local_host_name))
    return tar_file

    
@task 
def n_clone_aws_db(user, host_name, keyfile, source_suffix, n_timestamp, restore_dir):
    my_logger.debug("Initializing clone_db() process...")
    
    if restore_dir == 'NA':
        srcdir = "/usr/local/3top/log/dbUtil/" + n_timestamp + "/neo4j_backup"
        srcname = source_suffix + ".tgz" # FIXME: Get this programatically...
    elif restore_dir != 'NA':
        src_find = restore_dir.split("/")
        """first element is null, followed by rest of path"""
        srcname = src_find[len(src_find)-1] + ".tgz"
        """destination dir is removed from the list"""
        src_find.pop(len(src_find)-1)
        
        srcdir = ""
        for i in range(1, len(src_find)):
            srcdir = srcdir + "/" + src_find[i]
        
    dstpath = "/usr/local/3top/log/dbUtil/" + n_timestamp + "/"
    dstfile = os.path.join(dstpath, srcname)
    data_dir="/var/lib/neo4j/data/graph.db/"
    my_logger.debug("Received srcdir: %s, srcname: %s, dstpath: %s, dstfile: %s"%(srcdir, srcname, dstpath, dstfile))
    env.key_filename=keyfile
    my_logger.debug("Received key_filename: %(key_filename)s"%(env))
    my_logger.debug("Running command \"stop(%s, %s, %s, %s)\" " % (user, host_name, keyfile, srcdir))
    try:
        n_stop_aws(user, host_name, keyfile, srcdir)
    except:
        my_logger.error("Error Encountered in:  Running command \"stop(%s, %s, %s, %s)\". Exiting!"%( user, host_name, keyfile, srcdir))
        sys.exit("Error Encountered in:  Running command \"stop(%s, %s, %s, %s)\". Exiting!"%( user, host_name, keyfile, srcdir))
    my_logger.debug("Command \"stop(%s, %s, %s, %s)\" ...successful!"%(user, host_name, keyfile, srcdir))
    my_logger.debug("Initiating put() process for %s@%s" % (user, host_name))
    try:
        path_sub= "/usr/local/3top/log/"
        path = path_sub + "dbUtil"
        my_logger.debug("Received path: %s"%(path))
        my_logger.debug("Running command \" with settings(host_string=%s@%s, dst=%s)\""%(user, host_name, data_dir))
        try:
            with settings(host_string="%s@%s"%(user, host_name)):
                print "In n_clone_aws_db()"
                #print env.path
                if os.path.exists(path_sub):
                    try:
                        sudo("mkdir -p %s"%path_sub)
                    except:
                        sys.exit("mkdir -p %s failed. Exiting!"%path_sub)
                with cd(path_sub):
                    try:
                        sudo("mkdir -p dbUtil")
                        sudo("chown %s dbUtil"%(user))
                        sudo("chmod 777 dbUtil")
                    except:
                        sys.exit("\"mkdir -p dbUtil\" failed. Exiting")
                with cd(path):
                    run("ls -la")
                    my_logger.debug("Running command sudo(\"mkdir %s\")" %n_timestamp)
                    run("mkdir -p %s" %n_timestamp)
                    my_logger.debug("Running command run(\"ls -la\")")
                    run("ls -la")
                    my_logger.debug("Running command put(os.path.join(%s, %s), %s"%(srcdir, srcname, dstfile))
                    put(os.path.join(srcdir, srcname), dstfile)
        except:
            my_logger.error("Error Encountered in:  command put(os.path.join(%s, %s), %s. Restarting service and Exiting!"%(srcdir, srcname, dstfile))
            print("Error Encountered in:  command put(os.path.join(%s, %s), %s. Restarting service and Exiting!"%(srcdir, srcname, dstfile))
            my_logger.debug("Command \"put(os.path.join(%s, %s), %s\" successful!"%(srcdir, srcname, dstfile))
    except:
        my_logger.error("Error Encountered in:  put() process for %s@%s. Restarting service and Exiting!"%(user, host_name))
        print("Error Encountered in:  put() process for %s@%s. Restarting service and Exiting!"%(user, host_name))
        my_logger.info("Running command \" restart(%s, %s, %s, %s)\" "%(user, host_name, keyfile, srcdir))
        try:
            n_restart_aws(user, host_name, keyfile, srcdir)
        except:
            my_logger.error("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
            sys.exit("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
        my_logger.info("Command \" restart(%s, %s, %s, %s)\" successful!. Exiting! "%(user, host_name, keyfile, srcdir))
        sys.exit(-1)
        
    my_logger.debug("put() process for %s@%s...successful!" % (user, host_name))
    my_logger.debug("Initializing process to create .tar for backup")
    try:
        path="/var/lib/neo4j/data/graph.db/"
        my_logger.debug("Initializing \" with settings(host_string=\"%s@%s\", dst= %s"%(user, host_name, data_dir))
        with settings(host_string="%s@%s"%(user, host_name), dst=data_dir):
            my_logger.debug("Initializing \" with cd(%s)\""%(path))
            with cd(path):
                my_logger.debug("Initializing \" run(\"ls -la %s\")\""%path)
                run("ls -la %s"%path)
                if confirm("Delete current data in %s?" %host_name):
                    my_logger.debug("Initializing \" run(\"rm -rf *\")\"")
                    sudo('rm -rf *')
                    my_logger.debug("Initializing \" sudo(\'cp -r /usr/local/3top/log/dbUtil/\' + %s + \'/* \' + \'/var/lib/neo4j/data/graph.db/\')\""%n_timestamp)
                    sudo('cp -r /usr/local/3top/log/dbUtil/' + n_timestamp + '/* ' + '/var/lib/neo4j/data/graph.db/')
                    file_name= srcname
                    my_logger.debug("file_name set to %s"%file_name)
                    my_logger.debug("Initializing \" sudo(\"tar --extract --gzip --file %s -C %s\" "% (file_name, path))
                    sudo("tar --extract --gzip --file %s -C %s" % (file_name, path))
                my_logger.debug("Initializing \" run(\"ls -la %s\")\""%path)
                run("ls -la %s"%path)
    except:
        my_logger.error("Error Encountered in:  process to create .tar for backup. Restarting service and Exiting!")
        print("Error Encountered in:  process to create .tar for backup. Restarting service and Exiting!")
        my_logger.info("Running command \" restart(%s, %s, %s, %s)\" "%(user, host_name, keyfile, srcdir))
        try:
            n_restart_aws(user, host_name, keyfile, srcdir)
        except:
            my_logger.error("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
            sys.exit("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
        my_logger.info("Command \" restart(%s, %s, %s, %s)\"... successful!. Exiting! "%(user, host_name, keyfile, srcdir))
        sys.exit(-1)
    my_logger.debug("Process to create .tar for backup...successful!")
    my_logger.info("Running command \" restart(%s, %s, %s, %s)\" "%(user, host_name, keyfile, srcdir))
    try:
        n_restart_aws(user, host_name, keyfile, srcdir)
    except:
        my_logger.error("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
        sys.exit("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
    my_logger.info("Command \" restart(%s, %s, %s, %s)\"...successful! "%(user, host_name, keyfile, srcdir))
    my_logger.debug("clone_db() process... complete")
    
@task 
def n_clone_nyc_db(user, host_name, source_suffix, n_timestamp, restore_dir):
    my_logger.debug("Initializing clone_db() process...")
    if restore_dir == 'NA':
        srcdir = "/usr/local/3top/log/dbUtil/" + n_timestamp + "/neo4j_backup"
        srcname = source_suffix + ".tgz" # FIXME: Get this programatically...
    elif restore_dir != 'NA':
        src_find = restore_dir.split("/")
        """first element is null, followed by rest of path"""
        srcname = src_find[len(src_find)-1] + ".tgz"
        """destination dir is removed from the list"""
        src_find.pop(len(src_find)-1)
        
        srcdir = ""
        for i in range(1, len(src_find)):
            srcdir = srcdir + "/" + src_find[i]
        
    dstpath = "/usr/local/3top/log/dbUtil/" + n_timestamp + "/"
    dstfile = os.path.join(dstpath, srcname)
    data_dir="/var/lib/neo4j/data/graph.db/"
    my_logger.debug("Received srcdir: %s, srcname: %s, dstpath: %s, dstfile: %s"%(srcdir, srcname, dstpath, dstfile))
    my_logger.debug("Running command \"stop(%s, %s, %s)\" " % (user, host_name, srcdir))
    try:
        n_stop_nyc(user, host_name, srcdir)
    except:
        my_logger.error("Error Encountered in:  Running command \"stop(%s, %s, %s)\". Exiting!"%( user, host_name, srcdir))
        sys.exit("Error Encountered in:  Running command \"stop(%s, %s, %s)\". Exiting!"%( user, host_name, srcdir))
    my_logger.debug("Command \"stop(%s, %s, %s)\" ...successful!"%(user, host_name, srcdir))
    my_logger.debug("Initiating put() process for %s@%s" % (user, host_name))
    try:
        env.path= "/usr/local/3top/log/dbUtil"
        my_logger.debug("Received path: %(path)s"%(env))
        my_logger.debug("Running command \" with settings(host_string=%s@%s, dst=%s)\""%(user, host_name, data_dir))
        try:
            with settings(host_string="%s@%s"%(user, host_name), dst=data_dir):
                with cd(env.path):
                    my_logger.debug("Running command run(\"mkdir -p %s\")" %n_timestamp)
                    run("mkdir -p %s" %n_timestamp)
                    my_logger.debug("Running command run(\"ls -la\")")
                    run("ls -la")
                    my_logger.debug("Running command put(os.path.join(%s, %s), %s"%(srcdir, srcname, dstfile))
                    put(os.path.join(srcdir, srcname), dstfile)
        except:
            my_logger.error("Error Encountered in:  command put(os.path.join(%s, %s), %s. Restarting service and Exiting!"%(srcdir, srcname, dstfile))
            print("Error Encountered in:  command put(os.path.join(%s, %s), %s. Restarting service and Exiting!"%(srcdir, srcname, dstfile))
            my_logger.info("Running command \" restart(%s, %s, %s)\" "%(user, host_name, srcdir))
            try:
                n_restart_nyc(user, host_name, srcdir)
            except:
                my_logger.error("Error encountered: command \" restart(%s, %s, %s)\". Exiting! "%(user, host_name, srcdir))
                sys.exit("Error encountered: command \" restart(%s, %s, %s)\". Exiting! "%(user, host_name, srcdir))
            my_logger.info("Command \" restart(%s, %s, %s)\" successful!. Exiting! "%(user, host_name, srcdir))
            sys.exit(-1)
        my_logger.debug("Command \"put(os.path.join(%s, %s), %s\" successful!"%(srcdir, srcname, dstfile))
    except:
        my_logger.error("Error Encountered in:  put() process for %s@%s. Restarting service and Exiting!"%(user, host_name))
        print("Error Encountered in:  put() process for %s@%s. Restarting service and Exiting!"%(user, host_name))
        my_logger.info("Running command \" restart(%s, %s, %s)\" "%(user, host_name, srcdir))
        try:
            n_restart_nyc(user, host_name, srcdir)
        except:
            my_logger.error("Error encountered: command \" restart(%s, %s, %s)\". Exiting! "%(user, host_name, srcdir))
            sys.exit("Error encountered: command \" restart(%s, %s, %s)\". Exiting! "%(user, host_name, srcdir))
        my_logger.info("Command \" restart(%s, %s, %s)\" successful!. Exiting! "%(user, host_name, srcdir))
        sys.exit(-1)
        
    my_logger.debug("put() process for %s@%s...successful!" % (user, host_name))
    my_logger.debug("Initializing process to create .tar for backup")
    try:
        path="/var/lib/neo4j/data/graph.db/"
        my_logger.debug("Initializing \" with settings(host_string=\"%s@%s\", dst= %s"%(user, host_name, data_dir))
        with settings(host_string="%s@%s"%(user, host_name), dst=data_dir):
            my_logger.debug("Initializing \" with cd(%s)\""%(path))
            with cd(path):
                my_logger.debug("Initializing \" run(\"ls -la %s\")\""%path)
                run("ls -la %s"%path)
                if confirm("Delete current data in %s?" %host_name):
                    my_logger.debug("Initializing \" run(\"rm -rf *\")\"")
                    sudo('rm -rf *')
                    my_logger.debug("Initializing \" sudo(\'cp -r /usr/local/3top/log/dbUtil/\' + %s + \'/* \' + \'/var/lib/neo4j/data/graph.db/\')\""%n_timestamp)
                    sudo('cp -r /usr/local/3top/log/dbUtil/' + n_timestamp + '/* ' + '/var/lib/neo4j/data/graph.db/')
                    file_name= srcname
                    my_logger.debug("file_name set to %s"%file_name)
                    my_logger.debug("Initializing \" sudo(\"tar --extract --gzip --file %s -C %s\" "% (file_name, path))
                    sudo("tar --extract --gzip --file %s -C %s" % (file_name, path))
                my_logger.debug("Initializing \" run(\"ls -la %s\")\""%path)
                run("ls -la %s"%path)
    except:
        my_logger.error("Error Encountered in:  process to create .tar for backup. Restarting service and Exiting!")
        print("Error Encountered in:  process to create .tar for backup. Restarting service and Exiting!")
        my_logger.info("Running command \" restart(%s, %s, %s)\" "%(user, host_name, srcdir))
        try:
            n_restart_nyc(user, host_name, srcdir)
        except:
            my_logger.error("Error encountered: command \" restart(%s, %s,%s)\". Exiting! "%(user, host_name, srcdir))
            sys.exit("Error encountered: command \" restart(%s, %s, %s)\". Exiting! "%(user, host_name, srcdir))
        my_logger.info("Command \" restart(%s, %s, %s)\"... successful!. Exiting! "%(user, host_name, srcdir))
        sys.exit(-1)
    my_logger.debug("Process to create .tar for backup...successful!")
    my_logger.info("Running command \" restart(%s, %s, %s)\" "%(user, host_name, srcdir))
    try:
        n_restart_nyc(user, host_name, srcdir)
    except:
        my_logger.error("Error encountered: command \" restart(%s, %s, %s)\". Exiting! "%(user, host_name, srcdir))
        sys.exit("Error encountered: command \" restart(%s, %s, %s)\". Exiting! "%(user, host_name, srcdir))
    my_logger.info("Command \" restart(%s, %s, %s)\"...successful! "%(user, host_name, srcdir))
    my_logger.debug("clone_db() process... complete")
    
    

@task
def n_nyc_hostname_check(user, host_name):
    my_logger.debug("Initializing nyc_hostname_check() process...")
    my_logger.debug("Running command \"with settings(host_string=\"%s@%s\""%(user, host_name))
    with settings(host_string="%s@%s"%(user, host_name)):
        try:
            my_logger.debug("Running command \"run(\"hostname\")\"")
            run("hostname")
        except:
            my_logger.error("Error encountered in: command \"run(\"hostname\")\". Exiting!")
            sys.exit("Error encountered in: command \"run(\"hostname\")\". Exiting!")
        my_logger.debug("command \"run(\"hostname\")\"...successful!")
        
@task
def n_aws_hostname_check(user, host_name, keyfile):
    my_logger.debug("Initializing aws_hostname_check() process...")
    env.key_filename=keyfile
    my_logger.debug("Received keyfile %(key_filename)s"%(env))
    my_logger.debug("Running command \"with settings(host_string=\"%s@%s)\""%(user, host_name))
    with settings(host_string="%s@%s"%(user, host_name)):
        try:
            my_logger.debug("Running command \"run(\"hostname\")\"")
            run("hostname")
        except:
            my_logger.error("Error encountered in: command \"run(\"hostname\")\". Exiting!")
            sys.exit("Error encountered in: command \"run(\"hostname\")\". Exiting!")
        my_logger.debug("command \"run(\"hostname\")\"...successful!")
    
"""***************************************************************************************************************************"""

@task
def v_dump_one_graph_nyc(backup_subpath, timestamp, cmd, host, user):
    my_logger.debug("In v_dump_one_graph_nyc()...")
    my_logger.debug("\nCreating backup on user:%s, host:%s"%(user,host))
    with settings(host_string="%s@%s"%(user, host)):
        my_logger.debug("backup_subpath:%s"%backup_subpath)
        if not os.path.exists(backup_subpath):
            my_logger.debug("Calling sudo(\"mkdir -p %s\")"%backup_subpath)
            sudo("mkdir -p %s"%backup_subpath)
            my_logger.debug("Calling sudo(\"chmod 777 %s\")"%backup_subpath)
            sudo("chmod 777 %s"%backup_subpath)
            my_logger.debug("Calling sudo(\"chown %s %s\")"%(user, backup_subpath))
            sudo("chown %s %s"%(user, backup_subpath))
        with cd(backup_subpath):
            my_logger.debug("In directory: %s"%backup_subpath)
            if not exists(timestamp):
                my_logger.debug("Dir: %s does not exist. Creating..."%timestamp)
                run("mkdir %s"%timestamp)
            my_logger.debug("Running command...%s"%cmd)
            run(cmd)
            

@task
def v_dump_one_graph_aws(backup_subpath, timestamp, cmd, host, user, keyfile):
    my_logger.debug("In v_dump_one_graph_aws()...")
    my_logger.debug("\nCreating backup on user:%s, host:%s"%(user,host))
    with settings(host_string="%s@%s"%(user, host)):
        env.key_filename = keyfile
        my_logger.debug("backup_subpath:%s"%backup_subpath)
        if not os.path.exists(backup_subpath):
            my_logger.debug("Calling sudo(\"mkdir -p %s\")"%backup_subpath)
            sudo("mkdir -p %s"%backup_subpath)
            my_logger.debug("Calling sudo(\"chmod 777 %s\")"%backup_subpath)
            sudo("chmod 777 %s"%backup_subpath)
            my_logger.debug("Calling sudo(\"chown %s %s\")"%(user, backup_subpath))
            sudo("chown %s %s"%(user, backup_subpath))
        with cd(backup_subpath):
            my_logger.debug("In directory: %s"%backup_subpath)
            if not exists(timestamp):
                my_logger.debug("Dir: %s does not exist. Creating..."%timestamp)
                run("mkdir %s"%timestamp)
            my_logger.debug("Running command...%s"%cmd)
            run(cmd)

@task        
def v_pull_backup_to_local(user, host, srcdir, dir_backup):
    #srcdir = '/usr/local/virtuoso-opensource/share/virtuoso/vad/3t_backup/*'
    print "\nCreating backup_to_local for %s@%s"%(user, host)
    with settings(host_string="%s@%s"%(user, host)):
        run("ls -la")
        run("pwd")
        #sudo("mkdir %s" %dir_backup)
        print("Running get")
        get(srcdir + "/*", dir_backup)
        print "get success"
    #return srcdir        

@task    
def v_push_backup_to_aws_target(user, host, dir_backup, keyfile, dstpath):
    env.key_filename = keyfile
    my_logger.debug("Running \"with settings(host_string=\"%s@%s\", dst = %s)\""%(user, host, dir_backup))
    print "\nRunning v_push_backup_to_aws_target for %s@%s"%(user, host)
    with settings(host_string="%s@%s"%(user, host), dst=dir_backup):
        if not exists(dstpath):
            my_logger.debug("Running \"sudo(\"mkdir -p %s\""%dstpath)
            sudo("mkdir -p %s"%dstpath)
        print "Source: %s, Target: %s"%(dir_backup, dstpath)
        my_logger.debug("Running \" put(%s, %s, use_sudo = True\" "%(dir_backup, dstpath))
        try:
            put(dir_backup, dstpath, use_sudo=True)
        except:
            my_logger.error("Put failed. Exiting!")
            sys.exit("Put failed. Exiting!")
        my_logger.debug("put successful")
    #return dstpath

@task    
def v_push_backup_to_nyc_target(user, host, dir_backup, dstpath):
    print "\nRunning v_push_backup_to_nyc_target for %s@%s"%(user, host)
    my_logger.debug("Running \"with settings(host_string=\"%s@%s\", dst = %s)\""%(user, host, dir_backup))
    with settings(host_string="%s@%s"%(user, host), dst=dir_backup):
        if not exists(dstpath):
            my_logger.debug("Running \"sudo(\"mkdir -p %s\""%dstpath)
            sudo("mkdir -p %s"%dstpath)
        print "Source: %s, Target: %s"%(dir_backup, dstpath)
        my_logger.debug("Running \" put(%s, %s, use_sudo = True\" "%(dir_backup, dstpath))
        try:
            put(dir_backup, dstpath, use_sudo=True)
        except:
            my_logger.error("Put failed. Exiting!")
            sys.exit("Put failed. Exiting!")
        my_logger.debug("put successful")
    #return dstpath
        
"""
Restore rw 1
    1. /usr/bin/isql-vt "EXEC=SPARQL DROP SILENT GRAPH '<http://3top.com>';"
    2. /usr/bin/isql-vt "EXEC= DB.DBA.TTLP_MT (file_to_string_output ('/tmp/virtuoso-restore/one_graph/3t-000001.ttl'), '', 'http://3top.com');"
    3. TEST:/usr/bin/isql-vt "EXEC=SPARQL select * from <http://3top.com> where {?s ?p ?o} LIMIT 100;"

"""
@task
def v_restore_rw(user, host_name, keyfile, graph_name, temp_path):
    drop_graph_cmd= "/usr/bin/isql-vt \"EXEC=SPARQL DROP SILENT GRAPH '<%s>';\""%graph_name
    restore_graph_cmd ="/usr/bin/isql-vt \"EXEC= DB.DBA.TTLP_MT (file_to_string_output (\'%s/*.ttl\'), \'\', \'%s\');\""%(temp_path, graph_name)
    test_restore_cmd = "/usr/bin/isql-vt \"EXEC=SPARQL select * from <%s> where {?s ?p ?o} LIMIT 100;\""%graph_name
    #my_logger.debug("drop_graph_cmd: %s"%drop_graph_cmd)
    #print drop_graph_cmd
    #my_logger.debug("restore_graph_cmd: %s"%restore_graph_cmd)
    #print restore_graph_cmd
    #my_logger.debug("test_restore_cmd: %s"%test_restore_cmd)
    #print test_restore_cmd
    
    with settings(host_string = "%s@%s"%(user, host_name)):
        env.key_filename = keyfile
        
        my_logger.debug("RUNNING:: drop_graph_cmd: %s"%drop_graph_cmd)
        print "Running drop_graph_cmd: %s"%drop_graph_cmd
        run(drop_graph_cmd)
        print "Drop_graph_cmd successful!"
        my_logger.debug("Drop_graph_cmd successful!")
        my_logger.debug("RUNNING::restore_graph_cmd: %s"%restore_graph_cmd)
        print "Running restore_graph_cmd: %s"%restore_graph_cmd
        run(restore_graph_cmd)
        print "Restore_graph_cmd successful!"
        my_logger.debug("Restore_graph_cmd successful!")
        my_logger.debug("Running test_restore_cmd: %s"%test_restore_cmd)
        print "Running test_restore_cmd: %s"%test_restore_cmd
        run(test_restore_cmd)
        print "Test_restore_cmd successful!"
        my_logger.debug("Test_restore_cmd successful!")

@task
def v_aws_hostname_check(user, host, keyfile, host_chk_cmd):
    my_logger.debug("Initializing v_nyc_hostname_check() process...")
    my_logger.debug("Running command \"with settings(host_string=\"%s@%s\""%(user, host))
    env.key_filename = keyfile
    with settings(host_string="%s@%s"%(user, host)):
        #env.key_filename = keyfile
        my_logger.debug("Running command \"run(%s)\""%host_chk_cmd)
        try:
            run(host_chk_cmd)
        except:
            my_logger.error("Error encountered in: command \"run(%s)\". Exiting!"%host_chk_cmd)
            sys.exit("Error encountered in: command \"run(%s)\". Exiting!"%host_chk_cmd)
        my_logger.debug("command \"run(%s)\"....successful!"%host_chk_cmd)

@task
def delete_backup_on_aws_virtuoso(delete_path, timestamp, host, user, keyfile): 
    my_logger.debug("Initializing delete_backup_on_aws_virtuoso() process...")
    my_logger.debug("Running command \"with settings(host_string=\"%s@%s\""%(user, host))
    with settings(host_string="%s@%s"%(user, host)):
        env.key_filename = keyfile
        del_backup_cmd = "rm -rf %s"%timestamp
        my_logger.debug("Running command \"run(%s)\""%del_backup_cmd)
        with cd(delete_path):
            try:
                run(del_backup_cmd)
            except:
                my_logger.error("Error encountered in: command \"run(%s)\". Exiting!"%del_backup_cmd)
                sys.exit("Error encountered in: command \"run(%s)\". Exiting!"%del_backup_cmd)
        my_logger.debug("command \"run(%s)\"....successful!"%del_backup_cmd)  

@task
def delete_backup_on_nyc_virtuoso(delete_path, timestamp, host, user): 
    my_logger.debug("Initializing delete_backup_on_nyc_virtuoso() process...")
    my_logger.debug("Running command \"with settings(host_string=\"%s@%s\""%(user, host))
    with settings(host_string="%s@%s"%(user, host)):
        del_backup_cmd = "rm -rf %s"%timestamp
        my_logger.debug("Running command \"run(%s)\""%del_backup_cmd)
        with cd(delete_path):
            try:
                run(del_backup_cmd)
            except:
                my_logger.error("Error encountered in: command \"run(%s)\". Exiting!"%del_backup_cmd)
                sys.exit("Error encountered in: command \"run(%s)\". Exiting!"%del_backup_cmd)
        my_logger.debug("command \"run(%s)\"....successful!"%del_backup_cmd)  
                
@task
def v_nyc_hostname_check(user, host, host_chk_cmd):
    my_logger.debug("Initializing v_nyc_hostname_check() process...")
    my_logger.debug("Running command \"with settings(host_string=\"%s@%s\""%(user, host))
    with settings(host_string="%s@%s"%(user, host)):
        my_logger.debug("Running command \"run(%s)\""%host_chk_cmd)
        try:
            run(host_chk_cmd)
        except:
            my_logger.error("Error encountered in: command \"run(%s)\". Exiting!"%host_chk_cmd)
            sys.exit("Error encountered in: command \"run(%s)\". Exiting!"%host_chk_cmd)
        my_logger.debug("command \"run(%s)\"....successful!"%host_chk_cmd)