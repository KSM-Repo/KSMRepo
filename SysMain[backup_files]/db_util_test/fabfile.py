
from __future__ import with_statement
from ttLib import *
import os
import sys
import subprocess
from datetime import datetime

from fabric.api import *
from fabric.utils import abort, error, warn
from fabric.contrib import files
from fabric.contrib.console import confirm
from contextlib import contextmanager as _contextmanager
from ttLib.ttSys import dirCreateTimestamp
from ttLib.ttAWS import get_s3_config
import time

# globals
try:
    s3_config_folder = "nyc-sys"
    s3_config_filename = "ttSettings.conf"
    global dict_ttSettings
    dict_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
    print"complete."  
    #print  dict_ttSettings
except:
    sys.exit("Error Encountered: %s. Download of ttSettings from S3 failed. Exiting!" %str(sys.exc_info()))
"""
PATH_ROOT = dict_ttSettings["PATH_ROOT"]
PATH_LOG = dict_ttSettings["PATH_LOG"]
env.backup_path = PATH_ROOT + PATH_LOG + "/back-up"
env.neo4j_install = '/var/lib/neo4j'
env.timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
env.dir_backup = path + "/" + env.timestamp
"""
env.neo4j_install = '/var/lib/neo4j'
env.service_name = 'neo4j-service'

@task
def neo4j_a_dev():
    env.dev_a_host = 'neo4j.a.dev.aws.3top.com'
    env.dev_a_user = 'ubuntu'
    env.dev_a_key_filename = '/home/3TOP/skanupuru/Home/practice/aws-dev-cert.pem'

@task
def localhost():
    "Use the remote testing server."
    env.hosts = ['skanupuru-lxu1.nyc.3top.com']
    env.user="skanupuru"

@task
def neo4j_vm():
    "Use the remote staging server."
    env.hosts = ['skanupuru-lxs3.nyc.3top.com']
    env.user= "skanupuru"

@task
def start(user, host_name, keyfile, log_dir):
    "Start neo4j-service."
    # NB: This doesn't use sudo() directly because it insists on asking
    # for a password, even though we should have NOPASSWD in visudo.
    #with settings(host_string= "%s@%s"%(user, host_name)):
    with settings(host_string= "%s@%s"%(user, host_name), dst=log_dir):
        env.key_filename= keyfile
        if confirm("Start Neo4j server %s?" %host_name):
            sudo('sudo service %(service_name)s start' % env)

@task
def stop(user, host_name, keyfile, log_dir):
    "Stop neo4j-service."
    # NB: This doesn't use sudo() directly because it insists on asking
    # for a password, even though we should have NOPASSWD in visudo.
    with settings(host_string= "%s@%s"%(user, host_name), dst=log_dir):
        env.key_filename= keyfile
        if confirm("Stop Neo4j server for %s?" %host_name):
            sudo('sudo service %(service_name)s stop' % env)

@task 
def restart(user, host_name, keyfile, log_dir):
    "Restart neo4j-service."
    # NB: This doesn't use sudo() directly because it insists on asking
    # for a password, even though we should have NOPASSWD in visudo.
    #with settings(host_string= "%s@%s"%(user, host_name)):
    with settings(host_string= "%s@%s"%(user, host_name), dst=log_dir):
        env.key_filename= keyfile
        if confirm("Restart Neo4j server for %s?" %host_name):
            sudo('service %(service_name)s restart' % env)

@task
def get_from(user, host_name, keyfile,tar_file, local_dir):
    with settings(host_string="%s@%s"%(user, host_name),tmpdst= local_dir):
        get(tar_file, os.path.dirname(local_dir + "/"))


@task
def online_backup(user, host_name, keyfile, dir_backup):
    """
    Do an online backup to a particular directory on the server.
    #online_backup:/path/on/server/backup.graph.db
    """
    env.key_filename=keyfile
    run("/var/lib/neo4j/bin/neo4j-backup -from single://%s@%s:6362 -to %s" %(user, host_name, dir_backup))   


@task
def backup_aws_to_local(user, host_name, keyfile, dir_backup, log_dir):
    """Copy a Neo4j DB from a server using the backup tool.
    This creates a copy of the running DB in /tmp, zips it,
    downloads the zip, extracts it to the specified DB, and
    cleans up.
    
    online_clone_db:/local/path/to/graph.db
    """
    print "\nCreating backup-tar for %s@%s"%(user, host_name)
    tar_file=log_dir + "/" + (host_name + "-Before.tgz")
    local_user="skanupuru"
    local_host_name="skanupuru-lxu1.nyc.3top.com"
    with settings(host_string="%s@%s"%(local_user, local_host_name), dst=dir_backup):
        print "Taking backup for %s@%s"%(user, host_name)
        env.key_filename=keyfile
        run("/var/lib/neo4j/bin/neo4j-backup -from single://%s@%s:6362 -to %s" %(user, host_name, dir_backup))
        print "\nCreating backup-tar for %s@%s"%(user, host_name)
        run("tar --create --gzip --file %s -C %s ." % (tar_file , dir_backup))
        run("rm -rf %s" % dir_backup)
    return tar_file

@task
def backup_nyc_to_local(user, host_name, dir_backup, log_dir):
    """Copy a Neo4j DB from a server using the backup tool.
    This creates a copy of the running DB in /tmp, zips it,
    downloads the zip, extracts it to the specified DB, and
    cleans up.
    
    online_clone_db:/local/path/to/graph.db
    """
    print "\nCreating backup-tar for %s@%s"%(user, host_name)
    tar_file=log_dir + "/" + (host_name + "-Before.tgz")
    local_user="skanupuru"
    local_host_name="skanupuru-lxu1.nyc.3top.com"
    with settings(host_string="%s@%s"%(local_user, local_host_name), dst=dir_backup):
        print "Taking backup for %s@%s"%(user, host_name)
        run("/var/lib/neo4j/bin/neo4j-backup -from single://%s@%s:6362 -to %s" %(user, host_name, dir_backup))
        print "\nCreating backup-tar for %s@%s"%(user, host_name)
        run("tar --create --gzip --file %s -C %s ." % (tar_file , dir_backup))
        run("rm -rf %s" % dir_backup)
    return tar_file


    
@task 
def clone_db(user, host_name, keyfile, target_suffix, timestamp):
    srcdir = "/usr/local/3top/log/back-up/" + timestamp + "/"
    srcname = target_suffix + ".tgz" # FIXME: Get this programatically...
    dstpath = "/usr/local/3top/log/back-up/" + timestamp + "/"
    dstfile = os.path.join(dstpath, srcname)
    local_user="skanupuru"
    local_host_name="skanupuru-lxu1.nyc.3top.com"
    remote_db_dir = "%(neo4j_install)s/data/graph.db" % env
    data_dir="/var/lib/neo4j/data/graph.db/"
    env.key_filename=keyfile
    stop(user, host_name, keyfile, srcdir)
    try:
        print "in try"
        #with cd(srcdir):
        #print "cd success"
        env.path= "/usr/local/3top/log/back-up"
        with cd(env.path):
            with settings(host_string="%s@%s"%(user, host_name), dst=data_dir):
                run("mkdir -p %s" %timestamp)
                run("ls -la")
                print("Running put")
                put(os.path.join(srcdir, srcname), dstfile)
                print "put success"
    except:
        print "Error. Restarting service!"
    try:
        print "in try"
        path="/var/lib/neo4j/data/graph.db/"
        #path="/var/lib/neo4j/data/"
       
        with settings(host_string="%s@%s"%(user, host_name), dst=data_dir):
            with cd(path):
                run("ls -la %s"%path)
                if confirm("Delete current data in %s?" %host_name):
                    sudo('rm -rf *')
                    sudo('cp -r /usr/local/3top/log/back-up/' + timestamp + '/* ' + '/var/lib/neo4j/data/graph.db/')
                    file_name= target_suffix + ".tgz"
                    sudo("tar --extract --gzip --file %s -C %s" % (file_name, path))
                
                #sudo('cp -r /usr/local/3top/log/back-up/' + timestamp + '/* ' + '/var/lib/neo4j/data/graph.db/')
                #sudo("tar --extract --gzip --file %s -C %s" % (file_name, path))   
                run("ls -la %s"%path)
    except:
        print "Error. Restarting service!"
                    
                
            
    restart(user, host_name, keyfile, srcdir)
    
        

def copy_to_server():
    "Upload the app to a versioned path."
    # Ensure the deployment directory is there...

    with cd(env.path):
        srcdir = "assembly/target"
        srcname = "assembly-0.1.tar.gz" # FIXME: Get this programatically...
        dstpath = "deploys/%(version)s" % env
        dstfile = os.path.join(dstpath, srcname)

        # make the deploy dir
        run("mkdir -p deploys/%(version)s" % env)
        # upload the assembly gzip
        print("Running put")
        put(os.path.join(srcdir, srcname), dstfile)
        # extract it
        with cd(dstpath):
            run("tar --extract --gzip --file %s" % srcname)
        # delete the zip
        run("rm %s" % dstfile)


