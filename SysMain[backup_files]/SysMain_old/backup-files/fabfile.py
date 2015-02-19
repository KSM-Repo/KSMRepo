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



# globals
env.neo4j_install = '/var/lib/neo4j'
env.service_name = 'neo4j-service'
s3_config_folder = "nyc-sys"
s3_config_filename = "ttSettings.conf"
my_logger.debug("Initiating \"dict_ttSettings = get_s3_config(%s, %s)\""%(s3_config_folder, s3_config_filename))

try:
    global dict_ttSettings
    dict_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
except:
    my_logger.error("Error Encountered in: \"dict_ttSettings = get_s3_config(%s, %s)\". Exiting!"%(s3_config_folder, s3_config_filename))
    sys.exit("Error Encountered in: \"dict_ttSettings = get_s3_config(%s, %s)\" Exiting!"%(s3_config_folder, s3_config_filename))

my_logger.debug("\"dict_ttSettings = get_s3_config(%s, %s)\"...successful!"%(s3_config_folder, s3_config_filename))


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
def start(user, host_name, keyfile, log_dir):
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
def stop(user, host_name, keyfile, log_dir):
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
                my_logger.debug("Running command \"sudo('sudo service %(service_name)s start\')\" " % env)
                sudo('sudo service %(service_name)s stop' % env)
    except:
        my_logger.error("Error Encountered in:  \nInitiating stop() neo4j service for %s@%s. Exiting!"%(user, host_name))
        sys.exit("Error Encountered in:  Initiating stop() neo4j service for %s@%s. Exiting!"%(user, host_name))
    my_logger.debug("stop() neo4j service for %s@%s...successful!"%(user, host_name))

@task 
def restart(user, host_name, keyfile, log_dir):
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
def get_from(user, host_name, keyfile,tar_file, local_dir):
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
def online_backup(user, host_name, keyfile, dir_backup):
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
def backup_to_local(user, host_name, keyfile, dir_backup, log_dir):
    ##my_logger.debug("Initiating backup_to_local() for %s@%s"%(user, host_name))
    """Copy a Neo4j DB from a server using the backup tool.
    This creates a copy of the running DB in /tmp, zips it,
    downloads the zip, extracts it to the specified DB, and
    cleans up.
    
    online_clone_db:/local/path/to/graph.db
    """
    tar_file=dir_backup + ".tgz"
    local_user="skanupuru"
    local_host_name="skanupuru-lxu1.nyc.3top.com"
    my_logger.debug("Initiating backup_to_local() of %s@%s from %s@%s"%(user, host_name, local_user, local_host_name))
    my_logger.debug("Running command \" with settings(host_string=\"%s@%s\", dst=%s):\". Exiting!"%(local_user, local_host_name, dir_backup))
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
            run("rm -rf %s" % dir_backup)
    except:
        my_logger.error("Error Encountered in:  Initiating backup_to_local() of %s@%s from %s@%s. Exiting!"%( user, host_name, local_user, local_host_name))
        sys.exit("Error Encountered in:  Initiating backup_to_local() of %s@%s from %s@%s. Exiting!"%( user, host_name, local_user, local_host_name))
    my_logger.debug("Initiating backup_to_local() of %s@%s from %s@%s...successful!"%(user, host_name, local_user, local_host_name))
    return tar_file
    
@task 
def clone_db(user, host_name, keyfile, target_suffix, timestamp):
    my_logger.debug("Initializing clone_db() process...")
    srcdir = "/usr/local/3top/log/swap/" + timestamp + "/neo4j_backup"
    srcname = target_suffix + ".tgz" # FIXME: Get this programatically...
    dstpath = "/usr/local/3top/log/swap/" + timestamp + "/"
    dstfile = os.path.join(dstpath, srcname)
    data_dir="/var/lib/neo4j/data/graph.db/"
    my_logger.debug("Received srcdir: %s, srcname: %s, dstpath: %s, dstfile: %s"%(srcdir, srcname, dstpath, dstfile))
    env.key_filename=keyfile
    my_logger.debug("Received key_filename: %(key_filename)s"%(env))
    my_logger.debug("Running command \"stop(%s, %s, %s, %s)\" " % (user, host_name, keyfile, srcdir))
    try:
        stop(user, host_name, keyfile, srcdir)
    except:
        my_logger.error("Error Encountered in:  Running command \"stop(%s, %s, %s, %s)\". Exiting!"%( user, host_name, keyfile, srcdir))
        sys.exit("Error Encountered in:  Running command \"stop(%s, %s, %s, %s)\". Exiting!"%( user, host_name, keyfile, srcdir))
    my_logger.debug("Command \"stop(%s, %s, %s, %s)\" ...successful!"%(user, host_name, keyfile, srcdir))
    my_logger.debug("Initiating put() process for %s@%s" % (user, host_name))
    try:
        env.path= "/usr/local/3top/log/swap"
        my_logger.debug("Received path: %(path)s"%(env))
        my_logger.debug("Running command \" with settings(host_string=%s@%s, dst=%s)\""%(user, host_name, data_dir))
        try:
            with settings(host_string="%s@%s"%(user, host_name), dst=data_dir):
                with cd(env.path):
                    my_logger.debug("Running command run(\"mkdir -p %s\")" %timestamp)
                    run("mkdir -p %s" %timestamp)
                    my_logger.debug("Running command run(\"ls -la\")")
                    run("ls -la")
                    my_logger.debug("Running command put(os.path.join(%s, %s), %s"%(srcdir, srcname, dstfile))
                    put(os.path.join(srcdir, srcname), dstfile)
        except:
            my_logger.error("Error Encountered in:  command put(os.path.join(%s, %s), %s. Restarting service and Exiting!"%(srcdir, srcname, dstfile))
            print("Error Encountered in:  command put(os.path.join(%s, %s), %s. Restarting service and Exiting!"%(srcdir, srcname, dstfile))
            my_logger.info("Running command \" restart(%s, %s, %s, %s)\" "%(user, host_name, keyfile, srcdir))
            try:
                restart(user, host_name, keyfile, srcdir)
            except:
                my_logger.error("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
                sys.exit("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
            my_logger.info("Command \" restart(%s, %s, %s, %s)\" successful!. Exiting! "%(user, host_name, keyfile, srcdir))
            sys.exit(-1)
        my_logger.debug("Command \"put(os.path.join(%s, %s), %s\" successful!"%(srcdir, srcname, dstfile))
    except:
        my_logger.error("Error Encountered in:  put() process for %s@%s. Restarting service and Exiting!"%(user, host_name))
        print("Error Encountered in:  put() process for %s@%s. Restarting service and Exiting!"%(user, host_name))
        my_logger.info("Running command \" restart(%s, %s, %s, %s)\" "%(user, host_name, keyfile, srcdir))
        try:
            restart(user, host_name, keyfile, srcdir)
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
                    my_logger.debug("Initializing \" sudo(\'cp -r /usr/local/3top/log/swap/\' + %s + \'/* \' + \'/var/lib/neo4j/data/graph.db/\')\""%timestamp)
                    sudo('cp -r /usr/local/3top/log/swap/' + timestamp + '/* ' + '/var/lib/neo4j/data/graph.db/')
                    file_name= target_suffix + ".tgz"
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
            restart(user, host_name, keyfile, srcdir)
        except:
            my_logger.error("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
            sys.exit("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
        my_logger.info("Command \" restart(%s, %s, %s, %s)\"... successful!. Exiting! "%(user, host_name, keyfile, srcdir))
        sys.exit(-1)
    my_logger.debug("Process to create .tar for backup...successful!")
    my_logger.info("Running command \" restart(%s, %s, %s, %s)\" "%(user, host_name, keyfile, srcdir))
    try:
        restart(user, host_name, keyfile, srcdir)
    except:
        my_logger.error("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
        sys.exit("Error encountered: command \" restart(%s, %s, %s, %s)\". Exiting! "%(user, host_name, keyfile, srcdir))
    my_logger.info("Command \" restart(%s, %s, %s, %s)\"...successful! "%(user, host_name, keyfile, srcdir))
    my_logger.debug("clone_db() process... complete")

@task
def nyc_hostname_check(user, host_name):
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
def aws_hostname_check(user, host_name, keyfile):
    my_logger.debug("Initializing aws_hostname_check() process...")
    env.key_filename=keyfile
    my_logger.debug("Received keyfile %(key_filename)s"%(env))
    my_logger.debug("Running command \"with settings(host_string=\"%s@%s\""%(user, host_name))
    with settings(host_string="%s@%s"%(user, host_name)):
        try:
            my_logger.debug("Running command \"run(\"hostname\")\"")
            run("hostname")
        except:
            my_logger.error("Error encountered in: command \"run(\"hostname\")\". Exiting!")
            sys.exit("Error encountered in: command \"run(\"hostname\")\". Exiting!")
        my_logger.debug("command \"run(\"hostname\")\"...successful!")