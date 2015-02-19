import sys
import os
import time
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name'])
from ttLx import callSubprocess
global swapTimeStampDirPath
from ttLib.ttAWS import findSourceSuffix

"""********************************************************************************************************************"""
""" set host name based on env ... start"""

def set_mysql_host(service, s_env,t_env):
    my_logger.debug("Setting mysql_host()...")
    try:
        if (service=='mysql') or (service=='all'):
            mysql_s_host='fp-rds-1.' + s_env
            if ("aws.3top.com" in s_env) and ("aws.3top.com" in t_env):
                mysql_t_host='fp-rds-1.' + t_env 
            elif "nyc.3top.com" in t_env:
                mysql_t_host=t_env
    except:
        my_logger.error("Setting mysql_host() failed. Exiting!")
        sys.exit("Setting mysql_host() failed. Exiting!")
        
    my_logger.debug("Setting mysql_host() successful! Returning to main() with Mysql_Source_Name: %s, Mysql_Target_Name: %s" %(mysql_s_host, mysql_t_host))
    return mysql_s_host, mysql_t_host

""" set host name based on env ... stop"""
"""********************************************************************************************************************"""
""" validate_nyc_accounts ... start"""

def validate_mysql_nyc_accounts(dict_ttSettings, target_hostname):
    my_logger.debug("validating mysql_nyc_accounts()...")
    for k,v in dict_ttSettings["mysql"]["users"].items():
        try:
            my_logger.debug("Validating mysql nyc %s account for %s"%(target_hostname, k))
            conn_cmd="mysql --user=" + v["username"] + " -p" + v["password"] + " --host=" + target_hostname + " --port=3306 -e \"CONNECT;\""
            conn_out=callSubprocess(conn_cmd)
        except:
            my_logger.error("Validation of mysql_nyc_accounts() failed for service:%s, username: %s, hostname: %s"%(k, v["username"], target_hostname))
            sys.exit("Validation of mysql_nyc_accounts() failed for service:%s, username: %s, hostname: %s"%(k, v["username"], target_hostname))
        my_logger.debug("Validation of mysql_nyc_accounts() was successful for service:%s, username: %s, hostname: %s"%(k, v["username"], target_hostname))
    my_logger.debug("Accounts validation successful for service: MySQL !")
    
""" validate_nyc_accounts ... start"""
"""********************************************************************************************************************"""    
""" validate_aws_accounts ... start"""

def validate_mysql_aws_accounts(dict_ttSettings, target_hostname):
    aws_cert = dict_ttSettings["mysql"]["certs"]["public"]
    for k,v in dict_ttSettings["mysql"]["users"].items():
        try:
            my_logger.debug("Validating mysql aws %s account for %s"%(target_hostname, k))
            conn_cmd="mysql --user=" + v["username"] + " -p" + v["password"] + " --ssl-ca=" + aws_cert + " --host=" + target_hostname + " --port=3306 -e \"CONNECT;\""
            conn_out=callSubprocess(conn_cmd)
        except:
            my_logger.error("Error Encountered in validate_mysql_aws_accounts(): %s. Exiting!")
            sys.exit("Error Encountered in validate_mysql_aws_accounts(): %s. Exiting!")
        my_logger.debug("mysql aws account for %s validated successfully for %s!"%(k, target_hostname))

""" validate_aws_accounts ... start"""
"""********************************************************************************************************************"""
""" ...start"""
    
def validate_mysql_source(dict_ttSettings, source_hostname):
    try:
        aws_cert = dict_ttSettings["mysql"]["certs"]["public"]
        echo_OP=callSubprocess("echo $USER")
        val_s_cmd="mysql --user=" + dict_ttSettings["mysql"]["users"]["r"]["username"] + " -p" + dict_ttSettings["mysql"]["users"]["r"]["password"] \
        + " --ssl-ca=" + aws_cert + " --host=" + source_hostname + " --port=3306 -e \"CONNECT;\""
        val_s_cmd_out=callSubprocess(val_s_cmd)
    except:
        my_logger.error("Encountered Error in validate_mysql_source(): %s. Exiting!")
        sys.exit("Encountered Error in validate_mysql_source(): %s. Exiting!")
    my_logger.debug("Accounts validation successful for service: MySQL source with output: %s"%val_s_cmd_out)        
    return("Accounts validation successful for service: MySQL source with output: %s"%val_s_cmd_out)

""" ...stop"""
"""********************************************************************************************************************""" 
""" ...start"""

def mysql_operations(operation, service, dict_source_ttSettings, dict_target_ttSettings, source_hostname, target_hostname, my_logger, log_file):
    my_logger.debug("Initiating mysql_Operations()...")
    region_source = dict_target_ttSettings["aws"]["REGION_NAME"]
    region_target = dict_target_ttSettings["aws"]["REGION_NAME"]
    
    if region_source == region_target:
        Region = region_target
    else:
        my_logger.error("Source and Target Regions Mismatch. Exiting!")
        sys.exit("Source and Target Regions Mismatch. Exiting!")
    my_logger.debug("Setting Region successful! Region: %s"%Region)
    
    timestamp= time.strftime("%Y%m%d-%H%M%S")
    my_logger.debug("Calling mysqldump_source() from mysql_operations()... for host: %s"%source_hostname)
    dump_source_before=mysqldump_source("source-beforeUpdate", dict_source_ttSettings, source_hostname, my_logger, log_file)
    my_logger.debug("dump_source_before: %s"%dump_source_before)
    print"Backup before update successful for source..."
    
    my_logger.debug("Calling mysqldump_source() from mysql_operations()... for host: %s"%target_hostname)
    dump_target_before=mysqldump_target("target-beforeUpdate", dict_target_ttSettings, target_hostname, my_logger, log_file)
    my_logger.debug("dump_target_before: %s"%dump_target_before)
    print"Backup before update successful for target..."
    
    print"Starting snapshot for source before clone..."
    my_logger.debug("Starting snapshot for source before clone for host: %s..."%source_hostname)
    snapshot_source_before= takesnapshot(source_hostname, my_logger, 'source', 'before-copy', timestamp, log_file, Region)
    log_str=str("Started clone: "+snapshot_source_before)
    my_logger.info(log_str)
        
    if "aws.3top.com" in target_hostname:
        print"Starting snapshot for target before clone..."
        my_logger.debug("Starting snapshot for target before clone for host: %s..."%target_hostname)
        snapshot_target_before=takesnapshot(target_hostname, my_logger, 'target', 'before-copy', timestamp, log_file, Region)
        log_str=str("Started clone: "+snapshot_target_before)
        my_logger.info(log_str)
    
    my_logger.debug("Calling snapstatuscheck()...")
    statusCheck=snapStatusCheck(snapshot_source_before,snapshot_target_before, log_file, region_target)
    my_logger.info(statusCheck)
    
    clone_OP=awsmysqlclone(dict_source_ttSettings, dict_target_ttSettings, source_hostname, target_hostname, my_logger, log_file)
    my_logger.info(clone_OP)
    print "Mysql clone complete..."
    
    dump_source_after=mysqldump_source("source-afterUpdate", dict_source_ttSettings, source_hostname, my_logger, log_file)
    my_logger.info(dump_source_after)
    print"Backup after update successful for source..."
    
    dump_target_after=mysqldump_target("target-afterUpdate", dict_target_ttSettings, target_hostname, my_logger, log_file)
    my_logger.info(dump_target_after)
    print"Backup after update successful for target..."
    
    print"Starting snapshot for source after clone..."
    snapshot_source_after=takesnapshot(source_hostname, my_logger,'source', 'after-copy',timestamp, log_file, Region)
    log_str=str("Started clone: "+snapshot_source_after)
    my_logger.info(log_str)
    
    if "aws.3top.com" in target_hostname:
        print"Starting snapshot for target after clone..."
        snapshot_target_after=takesnapshot(target_hostname, my_logger,'target', 'after-copy',timestamp, log_file, Region)
        log_str=str("Started clone: "+snapshot_target_after)
        my_logger.info(log_str)
        
    statusCheck=snapStatusCheck(snapshot_source_after, snapshot_target_after, log_file, Region)
    my_logger.info(statusCheck)

"""********************************************************************************************************************"""
""" ...start"""
    
def mysqldump_source(suffix, dict_ttSettings, source_hostname, my_logger, log_file):
    try:
        my_logger.info("In mysqldump_source()...")
        dump_cmd="mysqldump --user=" + dict_ttSettings["mysql"]["users"]["r"]["username"] + " -p" + dict_ttSettings["mysql"]["users"]["r"]["password"] \
        + " --ssl-ca=" + os.path.expanduser(dict_ttSettings["mysql"]["certs"]["public"]) + " --host=" + source_hostname + " --port=3306 --single-transaction=TRUE --routines --events ebdb"
        filename=suffix + "-ebdb.sql"
        filepath=os.path.join(swapTimeStampDirPath,filename)
        my_logger.info("Create file success! %s " % filename)
        dump_out=callSubprocess(dump_cmd)
        f=open(filepath, "wb")
        f.write(dump_out)
        f.close()
        my_logger.info("Backup successful!")
        
    except:
        my_logger.info("Encountered Error in mysqldump_source(). Exiting!")
        sys.exit("Encountered Error in mysqldump_source(). Exiting!")
    my_logger.debug("mysqldump_source() %s for %s successful! Backup path: %s"%(suffix, source_hostname, filepath))
    return("Returning to mysql_operations")

""" ...stop"""
"""********************************************************************************************************************"""   
""" ...start"""

def mysqldump_target(suffix, dict_ttSettings, target_hostname, my_logger, log_file):
    try:
        my_logger.info("In mysqldump()...")
        dump_cmd="mysqldump --user=" + dict_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" + dict_ttSettings["mysql"]["users"]["dba"]["password"] \
        + " --ssl-ca=" + os.path.expanduser(dict_ttSettings["mysql"]["certs"]["public"]) + " --host=" + target_hostname + " --port=3306 --single-transaction=TRUE --routines --events ebdb"
        filename=suffix + "-ebdb.sql"
        filepath=os.path.join(swapTimeStampDirPath,filename)
        my_logger.info("Create file success! %s " % filename)
        dump_out=callSubprocess(dump_cmd)
        f=open(filepath, "wb")
        f.write(dump_out)
        f.close()
        my_logger.info("Backup successful!")
    except:
        my_logger.info("Encountered Error in mysqldump_target(). Exiting!")
        sys.exit("Encountered Error in mysqldump_target(). Exiting!")
    my_logger.debug("mysqldump_target() %s for %s successful! Backup path: %s"%(suffix, target_hostname, filepath))
    return("Returning to mysql_operations")

""" ...stop"""
"""********************************************************************************************************************"""   
""" ...start"""

def takesnapshot(host, my_logger,suffix1, suffix2,timestamp, log_file, Region):
    my_logger.debug("Starting takesnapshot() process for host: %s"%host)
    import boto.rds2
    #Region = 'us-east-1'
    my_logger.debug("Connecting to RDS for Region: %s"%Region)
    try:
        conn = boto.rds2.connect_to_region(Region)
        _dbinstances = conn.describe_db_instances()
        for dbi in _dbinstances['DescribeDBInstancesResponse']['DescribeDBInstancesResult']['DBInstances']:
            DbinstanceId=dbi['DBInstanceIdentifier']
            _tags = conn.list_tags_for_resource("arn:aws:rds:us-east-1:671788406991:db:" + dbi['DBInstanceIdentifier'])
            DbinstanceName= _tags['ListTagsForResourceResponse']['ListTagsForResourceResult']['TagList'][0]["Value"]
            InstSplit=DbinstanceName.split("-")
            env_name=InstSplit[2]+"."+InstSplit[1]
            if env_name in host:
                snapshotId="fp-"+timestamp+"-"+suffix1+"-"+InstSplit[1]+"-"+InstSplit[2]+"-"+suffix2
                instId= DbinstanceId
        print "Creating snapshot for : ",host
        my_logger.debug("Creating DB snapshot with snapshotId: %s and InstId:%s"%(snapshotId, instId))
        try:
            conn.create_db_snapshot(snapshotId, instId)
        except:
            my_logger.error("create_db_snapshot() failed for snapshotId: %s, instId: %s. Exiting!"%(snapshotId, instId))
            sys.exit("create_db_snapshot() failed for snapshotId: %s, instId: %s. Exiting!"%(snapshotId, instId))
    except:
        my_logger.error("takesnapshot() failed for host: %s"% host)
        sys.exit("takesnapshot() failed for host: %s"% host)
    my_logger.debug("takesnapshot() process was successful for host: %s. Retuning to main() with snapshotId: %s"%(host, snapshotId))
    return snapshotId

""" ...stop"""
"""********************************************************************************************************************"""   
""" ...start"""

def snapStatusCheck(snapshotId1, snapshotId2, log_file, Region):
    try:
        import boto.rds2
        #Region="us-east-1"
        conn = boto.rds2.connect_to_region(Region)
        _snapshots = conn.describe_db_snapshots()
        len_snap=len(_snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'])
        
        for i in range(len_snap):
            if _snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'][i]['DBSnapshotIdentifier']== snapshotId1:
                #print i, _snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'][i]['DBSnapshotIdentifier']
                loc1=i
            elif _snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'][i]['DBSnapshotIdentifier']== snapshotId2:
                #print i, _snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'][i]['DBSnapshotIdentifier']
                loc2=i
            
        status1=callStatus(loc1, Region)
        status2=callStatus(loc2, Region)
        while (status1 != 'available') or (status2 != 'available'):
            status1=callStatus(loc1, Region)
            print "Status of %s: %s"%(snapshotId1, status1)
            #time.sleep(10)
            status2=callStatus(loc2, Region)
            print "Status of %s: %s"%(snapshotId2, status2)
            time.sleep(10)
    except:
        sys.exit("Error encountered during snapStatusCheck. Exiting! Logfile location:%s" %(log_file))
        
               
        
    ret_str=str("Snapshot created successfully for %s and %s"%(snapshotId1, snapshotId2))
    return ret_str

""" ...stop"""
"""********************************************************************************************************************"""
""" ...start"""

def callStatus(loc, Region):
    import boto.rds2
    #Region="us-east-1"
    conn = boto.rds2.connect_to_region(Region)
    _snapshots = conn.describe_db_snapshots()
    return str(_snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'][loc]["Status"])

""" ...stop"""
"""********************************************************************************************************************"""   
""" ...start"""

def awsmysqlclone(dict_source_ttSettings, dict_target_ttSettings, source_hostname, target_hostname, my_logger, log_file):
    my_logger.info("In awsmysqlclone()...")
    #print "awsmysqlclone start"
    drop_args="mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" + dict_target_ttSettings["mysql"]["users"]["dba"]["password"] \
    + " --ssl-ca=" + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"]) + " --host=" + target_hostname + " --port=3306 -e \"DROP DATABASE IF EXISTS ebdb\""
    #mysql --user=" + dict_ttSettings["mysql"]["dba"]["username"] + " -p" + dict_ttSettings["mysql"]["dba"]["password"] + " --force --ssl-ca=" + aws_cert+ " --host=" + target_hostname + " --port=3306 -e \"DROP DATABASE IF EXISTS ebdb\""
    #print drop_args
    try:
        clone_drop_out=callSubprocess(drop_args)        
    except:
        e = str(sys.exc_info())
        my_logger.info("Error encountered in awsmysqlclone_drop table: %s. Exiting!" %e)
        print("Error encountered. Exiting! Logfile location: %s" %log_file)
        sys.exit(-1)
    #print "drop success"
    my_logger.info("Drop database success!")

    create_args="mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" + dict_target_ttSettings["mysql"]["users"]["dba"]["password"] \
    + " --ssl-ca=" + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"]) + " --host=" + target_hostname + " --port=3306 -e \"CREATE DATABASE IF NOT EXISTS ebdb\""
                                                                                                                                                                
    #print create_args
    try:
        clone_create_out=callSubprocess(create_args)        
    except:
        e = str(sys.exc_info())
        my_logger.info("Error encountered in awsmysqlclone_create table: %s. Exiting!" %e)
        print("Error encountered. Exiting! Logfile location: %s" %log_file)
        sys.exit(-1)
    #print "create success"
    my_logger.info("Create database success! Output: %s"%clone_create_out)
    
    priv_args1 = "mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" + dict_target_ttSettings["mysql"]["users"]["dba"]["password"] + " --ssl-ca=" \
    + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"])  + " --host=" + target_hostname + \
    " --port=3306 -e \"USE ebdb; GRANT SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO 'replnyc'@'%' IDENTIFIED BY \'" + dict_source_ttSettings["mysql"]["users"]["r"]["password"] + "\'; FLUSH PRIVILEGES;\" "
    #print priv_args1
    try:
        clone_priv_out = callSubprocess(priv_args1)            
    except:
        e = str(sys.exc_info())
        my_logger.info("Error encountered in awsmysqlclone_grant privileges: %s. Exiting!" %e)
        print("Error encountered. Exiting! Logfile location: %s" %log_file)
        sys.exit(-1)  
    #print "grant success"
    my_logger.info( "Grant privileges success!")
    final_clone="mysqldump --user=replnyc -p" + dict_source_ttSettings["mysql"]["users"]["r"]["password"] + " --ssl-ca=" + os.path.expanduser(dict_source_ttSettings["mysql"]["certs"]["public"]) \
    + " --host=" + source_hostname + " --port=3306 --single-transaction=TRUE ebdb | mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" \
    + dict_target_ttSettings["mysql"]["users"]["dba"]["password"]  + " --ssl-ca=" + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"]) + " --host=" + target_hostname + " --port=3306 ebdb"
    #print final_clone
    #final_clone_out=callSubprocess(final_clone)
    try:
        os.system(final_clone)   
    except:
        e = str(sys.exc_info())
        my_logger.info("Error Encountered %s. Exiting...."%e)
        print("Error encountered awsmysqlclone_final_clone. Exiting! Logfile location: %s" %log_file)
        sys.exit(-1)
    #print "clone success"
    my_logger.info("MySQL Clone success!")
    return("Returning to mysql_operations()")

""" ...stop"""
"""********************************************************************************************************************"""   
