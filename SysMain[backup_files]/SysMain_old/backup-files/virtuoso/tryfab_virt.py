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

@task
def virt_dump_one_graph_nyc(sub_path, timestamp, cmd, host, user):
    print "\nCreating backup on user:%s, host:%s"%(user,host)
    print "In virt_dump_one_graph..."
    with settings(host_string="%s@%s"%(user, host)):
        with cd(sub_path):
            run("ls -la")
            if not exists(timestamp):
                sudo("mkdir %s"%timestamp)
            print "Running command..."
            run(cmd)
            

@task
def virt_dump_one_graph_aws(sub_path, timestamp, cmd, host, user, keyfile):
    print "\nCreating backup on user:%s, host:%s"%(user,host)
    print "In virt_dump_one_graph..."
    with settings(host_string="%s@%s"%(user, host)):
        env.key_filename = keyfile
        with cd(sub_path):
            run("ls -la")
            if not exists(timestamp):
                sudo("mkdir %s"%timestamp)
            print "Running command..."
            run(cmd)


@task        
def backup_to_local(user, host, srcdir, dir_backup):
    #srcdir = '/usr/local/virtuoso-opensource/share/virtuoso/vad/3t_backup/*'
    print "\nCreating backup_to_local for %s@%s"%(user, host)
    with settings(host_string="%s@%s"%(user, host)):
        run("ls -la")
        run("pwd")
        #sudo("mkdir %s" %dir_backup)
        print("Running get")
        get(srcdir, dir_backup)
        print "get success"
    #return srcdir        


@task
def remove_nyc_server_backup(sub_path, timestamp, host, user):
    print "In remove_server_backup..."
    print "\nRemoving backup on user:%s, host:%s"%(user,host)
    with settings(host_string="%s@%s"%(user, host)):
        with cd(sub_path):
            run("ls -la")
            sudo("rm -rf %s" %timestamp)
     
@task
def remove_aws_server_backup(sub_path, timestamp, host, user, keyfile):
    print "In remove_server_backup..."
    print "\nRemoving backup on user:%s, host:%s"%(user,host)
    with settings(host_string="%s@%s"%(user, host)):
        env.key_filename = keyfile
        with cd(sub_path):
            run("ls -la")
            run("rm -rf %s" %timestamp)


@task    
def backup_to_aws(user, host, dir_backup, keyfile, dstpath):
    env.key_filename = keyfile
    print "\nCreating backup_to_aws for %s@%s"%(user, host)
    with settings(host_string="%s@%s"%(user, host), dst=dir_backup):
        if not exists(dstpath):
                sudo("mkdir -p %s"%dstpath)
        print "Source: %s, Target: %s"%(dir_backup, dstpath)
        print("Running put")
        put(dir_backup, dstpath, use_sudo=True)
        print "put success"
    #return dstpath

    
"""
Restore rw 1
    1. /usr/bin/isql-vt "EXEC=SPARQL DROP SILENT GRAPH '<http://3top.com>';"
    2. /usr/bin/isql-vt "EXEC= DB.DBA.TTLP_MT (file_to_string_output ('/tmp/virtuoso-restore/one_graph/3t-000001.ttl'), '', 'http://3top.com');"
    3. TEST:/usr/bin/isql-vt "EXEC=SPARQL select * from <http://3top.com> where {?s ?p ?o} LIMIT 100;"

"""


@task
def restore_rw(user, host_name, keyfile, graph_name, temp_path):
    drop_graph_cmd= "/usr/bin/isql-vt \"EXEC=SPARQL DROP SILENT GRAPH '<%s>';\""%graph_name
    restore_graph_cmd ="/usr/bin/isql-vt \"EXEC= DB.DBA.TTLP_MT (file_to_string_output (\'%s/*.ttl\'), \'\', \'%s\');\""%(temp_path, graph_name)
    test_restore_cmd = "/usr/bin/isql-vt \"EXEC=SPARQL select * from <%s> where {?s ?p ?o} LIMIT 100;\""%graph_name
    print drop_graph_cmd
    print restore_graph_cmd
    print test_restore_cmd
    with settings(host_string = "%s@%s"%(user, host_name)):
        env.key_filename = keyfile
        
        print "Running drop_graph_cmd: %s"%drop_graph_cmd
        run(drop_graph_cmd)
        print "Drop_graph_cmd successful!"
        print "Running restore_graph_cmd: %s"%restore_graph_cmd
        run(restore_graph_cmd)
        print "Restore_graph_cmd successful!"
        print "Running test_restore_cmd: %s"%test_restore_cmd
        run(test_restore_cmd)
        print "Test_restore_cmd successful!"
    
@task
def remove_temp_restore(user, host_name, keyfile, temp_path):
    with settings(host_string = "%s@%s"%(user, host_name)):
        env.key_filename = keyfile
        
        print "Running remove_temp_restore()..."
        sudo("rm -rf %s"%temp_path)
        
    
    
    
    




