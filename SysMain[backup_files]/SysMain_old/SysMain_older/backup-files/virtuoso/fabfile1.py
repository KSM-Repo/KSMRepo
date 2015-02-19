"""
Fabric deployment script for EHRI rest backend.
"""

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
from ttLib.ttSys import get_s3_config, dirCreateTimestamp

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

PATH_ROOT = dict_ttSettings["PATH_ROOT"]
PATH_LOG = dict_ttSettings["PATH_LOG"]
path = PATH_ROOT + PATH_LOG + "/back-up"

env.backup_path = PATH_ROOT + PATH_LOG + "/back-up"
env.neo4j_install = '/var/lib/neo4j'
local_folder = path
env.tmpdst = path

TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"

#now = datetime.now(time.time())
env.timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
env.dir_backup = path + "/" + env.timestamp


#environments

@task
def neo4j_a_dev():
    env.hosts = ['neo4j.a.dev.aws.3top.com']
    env.user = 'ubuntu'
    env.key_filename = '/home/3TOP/skanupuru/Home/practice/aws-dev-cert.pem'

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
def start():
    "Start neo4j-service."
    # NB: This doesn't use sudo() directly because it insists on asking
    # for a password, even though we should have NOPASSWD in visudo.
    if confirm("Start Neo4j server?"):
        sudo('sudo service %(service_name)s start' % env)

@task
def stop():
    "Stop neo4j-service."
    # NB: This doesn't use sudo() directly because it insists on asking
    # for a password, even though we should have NOPASSWD in visudo.
    if confirm("Stop Neo4j server?"):
        sudo('sudo service %(service_name)s stop' % env)

@task 
def restart():
    "Restart neo4j-service."
    # NB: This doesn't use sudo() directly because it insists on asking
    # for a password, even though we should have NOPASSWD in visudo.
    if confirm("Restart Neo4j server?"):
        sudo('service %(service_name)s restart' % env)

@task
def get_from():
    get("/usr/local/3top/log/back-up/", os.path.dirname(local_folder))
    
@task
def online_backup(a_dev_dir):
    """
    Do an online backup to a particular directory on the server.

    online_backup:/path/on/server/backup.graph.db
    """
    #a_dev_dir="skanupuru-lxu1.nyc.3top.com"
    with settings(dst=a_dev_dir):
            run("%(neo4j_install)s/bin/neo4j-backup -from single://localhost:6362 -to %(dst)s" %(env))


@task
def online_clone_db():
    """Copy a Neo4j DB from a server using the backup tool.
    This creates a copy of the running DB in /tmp, zips it,
    downloads the zip, extracts it to the specified DB, and
    cleans up.
    
    online_clone_db:/local/path/to/graph.db
    """
    with settings(tmpdst = env.dir_backup):
        print "Calling online_backup()..."
        online_backup(env.tmpdst)
        print "Creating tar file for ..."
        run("tar --create --gzip --file %(tmpdst)s.tgz -C %(tmpdst)s ." % env)
        get(env.tmpdst + ".tgz", "abc.tgz")
        #run("rm -rf %(tmpdst)s %(tmpdst)s.tgz" % env)
        #local("mkdir -p " + env.dir_backup)
        local("tar xf /tmp/%s.tgz -C %s" % (env.timestamp, env.dir_backup))
        #local("rm " + env.tmpdst + ".tgz")

@task
def copy_db():
    """Copy a (not running) DB from the remote server.
    
    copy_db:/local/path/to/graph.db
    """
    #path = PATH_ROOT + PATH_LOG + "/back-up"
    #now = datetime.datetime.fromtimestamp(time.time())
    #s = now.strftime('%Y_%m_%d_%H_%M_%S')
    #local_dir = path + "/" + env.timestamp
    run("mkdir -p %s" %env.dir_backup)
    #if confirm("Stop Neo4j server?"):
    stop()

    remote_db_dir = "%(neo4j_install)s/data/graph.db" % env
    print remote_db_dir
    #temp_file = our_temp_file = run("mktemp")
    temp_file = run("mktemp")
    #if not os.path.exists(local_dir):
        #os.mkdir(local_dir)

    run("tar --create --gzip --file %s -C %s ." % (temp_file, remote_db_dir))
    #get(temp_file, os.path.dirname(our_temp_file))
    #local("tar --extract --gzip --file %s -C %s" % (our_temp_file, local_dir))
    run("rm %s" % temp_file)
    #os.unlink(our_temp_file)

    #if confirm("Restart Neo4j server?"):
    start()


#@task
def update_db(local_dir):
    """Update a Neo4j DB on a server.    
    Tar the input dir for upload, upload it, stop the server,
    move the current DB out of the way, and unzip it.
    
    update_db:/local/path/to/graph.db
    """
    # Check we have a reasonable path...
    if not os.path.exists(os.path.join(local_dir, "index.db")):
        raise Exception("This doesn't look like a Neo4j DB folder!: " + local_dir)

    remote_db_dir = "%(neo4j_install)s/data/graph.db" % env
    timestamp = get_timestamp()
    import tempfile
    tf = tempfile.NamedTemporaryFile(suffix=".tgz")
    name = tf.name
    tf.close()

    local("tar --create --gzip --file %s -C %s ." % (name, local_dir))
    remote_name = os.path.join("/tmp", os.path.basename(name))
    put(name, remote_name)

    if confirm("Stop Neo4j server?"):
        stop()
        run("mv %s %s.%s" % (remote_db_dir, remote_db_dir, timestamp))
        run("mkdir " + remote_db_dir)
        run("tar zxf %s -C %s" % (remote_name, remote_db_dir))
        run("chown %s.webadm -R %s" % (env.user, remote_db_dir))
        run("chmod -R g+w " + remote_db_dir)
        start()
        
@task
def update_db_test():
    """Update a Neo4j DB on the TEST server.    
    Tar the input dir for upload, upload it, stop the server,
    move the current DB out of the way, and unzip it.
    
    update_db_test:/local/path/to/graph.db
    """
    # Check we have a reasonable path...
    local_dir="%(neo4j_install)s/data/graph.db" % env
    if not os.path.exists(os.path.join(local_dir, "index.db")):
        raise Exception("This doesn't look like a Neo4j DB folder!: " + local_dir)

    remote_db_dir = "%(neo4j_install)s/data/graph.db" % env
    timestamp = get_timestamp()
    import tempfile
    tf = tempfile.NamedTemporaryFile(suffix=".tgz")
    name = tf.name
    tf.close()

    local("tar --create --gzip --file %s -C %s ." % (name, local_dir))
    remote_name = os.path.join("/tmp", os.path.basename(name))
    put(name, remote_name)

    
    run("mv %s %s.%s" % (remote_db_dir, remote_db_dir, timestamp))
    run("mkdir " + remote_db_dir)
    run("tar zxf %s -C %s" % (remote_name, remote_db_dir))
    run("chown %s.webadm -R %s" % (env.user, remote_db_dir))
    run("chmod -R g+w " + remote_db_dir)
    

#@task
def reindex_repository(repo_id):
    """Reindex items held by a repository.
    NB: Bit specific but very useful."""
    indexer_cmd = [
        "java", "-jar", env.index_helper,
        "--clear-key-value", "holderId=" + repo_id,
        "--index",
        "-H", "Authorization=admin",
        "--stats",
        "--solr", "http://localhost:8080/ehri/portal",
        "--rest", "http://localhost:7474/ehri",
        "'repository|%s'" % repo_id,
    ]
    run(" ".join(indexer_cmd))

#@task
def reindex_all():
    "Run a full reindex of Neo4j -> Solr data"
    all_types = ["documentaryUnit", "repository", "country",
            "historicalAgent", "cvocVocabulary", "cvocConcept",
            "authoritativeSet", "userProfile", "group"]
    indexer_cmd = [
        "java", "-jar", env.index_helper,
        "--clear-all",
        "--index",
        "-H", "Authorization=admin",
        "--stats",
        "--solr", "http://localhost:8080/ehri/portal",
        "--rest", "http://localhost:7474/ehri",
    ] + all_types
    run(" ".join(indexer_cmd))

#@task
def current_version():
    "Show the current date/revision"
    with cd(env.path):
        path = run("readlink -f current")
        deploy = os.path.split(path)
        timestamp, revision = os.path.basename(deploy[-1]).split("_")
        date = datetime.strptime(timestamp, TIMESTAMP_FORMAT)
        print("Timestamp: %s, revision: %s" % (date, revision))
        return date, revision

#@task
def current_version_log():
    "Output git log between HEAD and the current deployed version."
    _, revision = current_version()
    local("git log %s..HEAD" % revision)

def get_version_stamp():
    "Get a dated and revision stamped version string"
    rev = subprocess.check_output(["git","rev-parse", "--short", "HEAD"]).strip()
    return "%s_%s" % (get_timestamp(), rev)

def get_timestamp():
    return datetime.now().strftime(TIMESTAMP_FORMAT)    

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


