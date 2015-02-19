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
"""validate_mysql_accounts() ...start"""
    
def validate_mysql_accounts(dict_ttSettings, host):
    print host
    aws_cert = dict_ttSettings["mysql"]["certs"]["public"]
    for k,v in dict_ttSettings["mysql"]["users"].items():
        my_logger.debug("Validating mysql aws %s account for %s"%(host, k))
        conn_cmd="mysql --user=" + v["username"] + " -p" + v["password"] + " --ssl-ca=" + aws_cert + " --host=" + host + " --port=3306 -e \"CONNECT;\""
        my_logger.debug("Conn_cmd: %s" %conn_cmd)
        print v['username'], 
        conn_out=callSubprocess(conn_cmd)
        print "validated!"
        my_logger.debug("mysql aws account for %s validated successfully for %s!"%(k, host))
    
"""validate_mysql_accounts() ...stop"""
"""********************************************************************************************************************""" 
"""mysql_operations() ...start"""

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
    dump_source_before=mysqldump("source-beforeUpdate", dict_source_ttSettings, source_hostname, my_logger, log_file, 'NA')
    my_logger.debug("dump_source_before: %s"%dump_source_before)
    print"Backup before update successful for source..."
    
    my_logger.debug("Calling mysqldump_source() from mysql_operations()... for host: %s"%target_hostname)
    dump_target_before=mysqldump("target-beforeUpdate", dict_target_ttSettings, target_hostname, my_logger, log_file, 'NA')
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
    
    dump_source_after=mysqldump("source-afterUpdate", dict_source_ttSettings, source_hostname, my_logger, log_file, 'NA')
    my_logger.info(dump_source_after)
    print"Backup after update successful for source..."
    
    print"Collecting checksums for source..."
    source_checksum = collect_checksum(dict_source_ttSettings, source_hostname)
    
    dump_target_after=mysqldump("target-afterUpdate", dict_target_ttSettings, target_hostname, my_logger, log_file, 'NA')
    my_logger.info(dump_target_after)
    print"Backup after update successful for target..."
    
    print"Collecting checksums for target..."
    target_checksum = collect_checksum(dict_target_ttSettings, target_hostname)
    
    validate_checksum(source_checksum, target_checksum)
    """
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
    """
"""mysql_operations()...stop"""
"""********************************************************************************************************************"""
"""mysqldump_source() ...start"""
    
def mysqldump(suffix, dict_ttSettings, hostname, my_logger, log_file, backup_dir):
    my_logger.debug("Received suffix: %s"%suffix)
    try:
        my_logger.info("In mysqldump_source()...")
        dump_cmd="mysqldump --user=" + dict_ttSettings["mysql"]["users"]["r"]["username"] + " -p" + dict_ttSettings["mysql"]["users"]["r"]["password"] \
        + " --ssl-ca=" + os.path.expanduser(dict_ttSettings["mysql"]["certs"]["public"]) + " --host=" + hostname + " --port=3306 --single-transaction=TRUE --routines --events ebdb"
        if ('source' in suffix) or ('target' in suffix):
            filename = suffix + "-ebdb"
            filepath=os.path.join(swapTimeStampDirPath,filename)
        elif ('backup' in suffix):
            filename = "ebdb.sql"
            filepath = backup_dir + "/" + filename
        my_logger.info("Create file success! %s " % filename)
        my_logger.info("Backup file path: %s " % filepath)
        dump_out=callSubprocess(dump_cmd)
        f=open(filepath, "wb")
        f.write(dump_out)
        f.close()
        my_logger.info("Backup successful!")
        
    except:
        my_logger.info("Encountered Error in mysqldump(). Exiting!")
        sys.exit("Encountered Error in mysqldump(). Exiting!")
    my_logger.debug("mysqldump() %s for %s successful! Backup path: %s"%(suffix, hostname, filepath))
    

"""mysqldump_source() ...stop"""
"""********************************************************************************************************************"""   
"""mysqldump_target() ...start"""

def mysqldump_target(suffix, dict_ttSettings, hostname, my_logger, log_file):
    try:
        my_logger.info("In mysqldump()...")
        dump_cmd="mysqldump --user=" + dict_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" + dict_ttSettings["mysql"]["users"]["dba"]["password"] \
        + " --ssl-ca=" + os.path.expanduser(dict_ttSettings["mysql"]["certs"]["public"]) + " --host=" + hostname + " --port=3306 --single-transaction=TRUE --routines --events ebdb"
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
    my_logger.debug("mysqldump_target() %s for %s successful! Backup path: %s"%(suffix, hostname, filepath))
    return("Returning to mysql_operations")

"""mysqldump_target() ...stop"""
"""********************************************************************************************************************"""   
"""validate_checksum() ...start"""

def validate_checksum(source_checksum, target_checksum):
    """Checks the total elements"""
    count = 1
    if len(source_checksum) == len(target_checksum):
        my_logger.debug("Number of tables in source and target matched. Continuing!")
        for k,v in source_checksum.items():
            for k1, v1 in target_checksum.items():
                if (k == k1):
                    if (v == v1):
                        count_new = 1
                        
                    else:
                        count_new = 0
                        
                    count = count * count_new
                        
    if count == 1:
        my_logger.debug("Source and target checksums match. Mysql clone successful!")
        print("Source and target checksums match. Mysql clone successful!")
    elif count == 0:
        my_logger.error("Source and target checksums mismatch. Exiting!")
        sys.exit("Source and target checksums mismatch. Exiting!")
    else:
        my_logger.error("Received count = %s. Exiting!"%count)
        sys.exit("Received count = %s. Exiting!"%count)
                    
    

"""validate_checksum() ...stop"""
"""********************************************************************************************************************"""
"""collect_checksum() ...start"""    
def collect_checksum(dict_ttSettings, hostname):
    
    aws_cert = dict_ttSettings["mysql"]["certs"]["public"]
    """GET LIST OF TABLES"""
    list_tables_cmd ="mysql --user=" + dict_ttSettings["mysql"]["users"]["r"]["username"] + " -p" + dict_ttSettings["mysql"]["users"]["r"]["password"] + " --ssl-ca=" + os.path.expanduser(dict_ttSettings["mysql"]["certs"]["public"]) + " --host=" + hostname + " --port=3306 -e \"show tables from ebdb;\""
    """
    conn_cmd="mysql --user=" + v["username"] + " -p" + v["password"] + " --ssl-ca=" + aws_cert + " --host=" + host + " --port=3306 -e \"CONNECT;\""
    """
    list_tables_out=callSubprocess(list_tables_cmd)
    #print list_tables_out
    list_tables = list_tables_out.split("\n")
    checksum_list = list()
    len_tables = len(list_tables)
    dict_checksum = dict()
    for i in range(len_tables-1):
        if len(list_tables[i+1]) != 0:
            table_name = list_tables[i+1]
            #print "%s. Table_name: %s"%(i+1, table_name)
            checksum_cmd = "mysql --user=" + dict_ttSettings["mysql"]["users"]["r"]["username"] + " -p" + dict_ttSettings["mysql"]["users"]["r"]["password"] + " --ssl-ca=" + os.path.expanduser(dict_ttSettings["mysql"]["certs"]["public"]) + " --host=" + hostname + " --port=3306 -e \"checksum table ebdb." + table_name + ";\""
            checksum_out = callSubprocess(checksum_cmd)
            checksum1 = (checksum_out.split("\t"))
            checksum2 = list()
            #ict_checksum = dict()
            for k in checksum1:
                #print "k: %s"%k
                if "\n" in k:
                    for l in k.split("\n"):
                        if len(l) != 0:
                            checksum2.append(l)
                else:
                    checksum2.append(k)
            for j in checksum2:
                if ("Table" not in j) and ("Checksum" not in j) and (table_name not in j):
                    checksum = int(j)
                    dict_checksum[table_name] = checksum

    
    return dict_checksum
             
    
"""collect_checksum() ...stop"""
"""********************************************************************************************************************"""
"""mysql_backup() ...start"""

def mysql_backup(operation, service, dict_ttSettings, hostname, my_logger, log_file, backup_dir):
    
    my_logger.debug("Initiating mysql_backup()...")
    Region = dict_ttSettings["aws"]["REGION_NAME"]
    
    my_logger.debug("Setting Region successful! Region: %s"%Region)
    
    my_logger.debug("Calling mysqldump() from mysql_operations()... for host: %s"%hostname)
    mysqldump("backup", dict_ttSettings, hostname, my_logger, log_file, backup_dir)
    print("mysql backup successful for %s..."%hostname)
    
"""mysql_backup()...stop"""
"""********************************************************************************************************************"""
"""takesnapshot() ...start"""

def takesnapshot(host, my_logger,suffix1, suffix2, timestamp, log_file, Region):
    my_logger.debug("Starting takesnapshot() process for host: %s"%host)
    my_logger.debug("finding envName...")
    envName_test = host.split(".aws.3top.com")[0]
    envName = envName_test.split(".")[1] + "." + envName_test.split(".")[2]
    import boto.rds2
    my_logger.debug("Connecting to RDS for Region: %s"%Region)
    my_logger.debug("Received suffix1: %s, suffix2: %s"%(suffix1, suffix2))
    my_logger.debug("Received timestamp: %s"%timestamp)
    try:
        my_logger.debug("Connecting to region: %s"%Region)
        conn = boto.rds2.connect_to_region(Region)
        my_logger.debug("Connection to region successful. conn: %s"%conn)
        my_logger.debug("Collecting db instances...")
        _dbinstances = conn.describe_db_instances()
        my_logger.debug("Received db instances: %s"%_dbinstances)
        for dbi in _dbinstances['DescribeDBInstancesResponse']['DescribeDBInstancesResult']['DBInstances']:
            my_logger.debug("dbi: %s"%dbi)
            DbinstanceId=dbi['DBInstanceIdentifier']
            my_logger.debug("Received DbinstanceId: %s"%DbinstanceId)
            my_logger.debug("Collecting tags...")
            _tags = conn.list_tags_for_resource("arn:aws:rds:us-east-1:671788406991:db:" + dbi['DBInstanceIdentifier'])
            my_logger.debug("Received tags: %s"%_tags)
            my_logger.debug("Collecting tags...")
            for tag in _tags['ListTagsForResourceResponse']['ListTagsForResourceResult']['TagList']:
                my_logger.debug("received tag: %s"%tag)
                my_logger.debug("Checking if tag has \'Key\'...")
                if tag['Key']:
                    my_logger.debug("Found \'Key\' in tag. Continuing!")
                    my_logger.debug("Checking if tag[\'Key\'] = \"3t:environment\"")
                    if tag['Key'] == '3t:environment':
                        my_logger.debug("Found \'3t:environment\' in tag[\'Key\']. Continuing!")
                        my_logger.debug("Checking if envName: %s in tag[\'Value\']: %s"%(envName, tag['Value']))
                        if (envName in tag['Value']):
                            my_logger.debug("Found envName: %s in tag[\'Value\']: %s"%(envName, tag['Value']))
                            
                            """                    
                            DbinstanceName= _tags['ListTagsForResourceResponse']['ListTagsForResourceResult']['TagList'][0]["Value"]
                            my_logger.debug("received DbinstanceName: %s"%DbinstanceName)
                            my_logger.debug("Splitting instancename...")
                            InstSplit=DbinstanceName.split("-")
                            my_logger.debug("Finding envName...")
                            env_name=InstSplit[2]+"."+InstSplit[1]
                            my_logger.debug("Received envName: %s"%env_name)
                            my_logger.debug("Checking if env_name: %s in host: %s"%(env_name, host))
                            if env_name in host:
                            """
                            
                            my_logger.debug("Determining snapshotId...")
                            snapshotId="fp-"+timestamp+"-"+suffix1+"-"+ envName.split(".")[0] + "-" + envName.split(".")[1] +"-"+suffix2
                            my_logger.debug("snapshotId: %s"%snapshotId)
                            instId= DbinstanceId
                            my_logger.debug("Setting instId as %s"%instId)
                            print "Creating snapshot for : ",host
                            my_logger.debug("Creating DB snapshot with snapshotId: %s and InstId:%s"%(snapshotId, instId))
                            try:
                                conn.create_db_snapshot(snapshotId, instId)
                            except:
                                my_logger.error("create_db_snapshot() failed for snapshotId: %s, instId: %s. Exiting!"%(snapshotId, instId))
                                sys.exit("create_db_snapshot() failed for snapshotId: %s, instId: %s. Exiting!"%(snapshotId, instId))
                            
                            my_logger.debug("takesnapshot() process was successful for host: %s. Retuning to main() with snapshotId: %s"%(host, snapshotId))
                            return snapshotId
                        
                        else:
                            my_logger.debug("envName: %s not found in tag[\'Value\']: %s. Skipping!"%(envName, tag['Value']))
                    else:
                        my_logger.debug("\'3t:environment\' not found in tag[\'Key\']. Skipping!")
                else:
                    my_logger.debug("\'Key\' not found in tag. Skipping!")
                
    except:
        my_logger.error("Unexpected error in takesnapshot() for host: %s. Exiting"% host)
        sys.exit("Unexpected error in takesnapshot() for host: %s. Exiting"% host)
    

"""takesnapshot() ...stop"""
"""********************************************************************************************************************"""   
"""snapStatusCheck() ...start"""

def snapStatusCheck(snapshotId1, snapshotId2, log_file, Region):
    try:
        import boto.rds2
        conn = boto.rds2.connect_to_region(Region)
        _snapshots = conn.describe_db_snapshots()
        len_snap=len(_snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'])
        
        for i in range(len_snap):
            if _snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'][i]['DBSnapshotIdentifier']== snapshotId1:
                loc1=i
            elif _snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'][i]['DBSnapshotIdentifier']== snapshotId2:
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

"""snapStatusCheck() ...stop"""
"""********************************************************************************************************************"""
"""callStatus() ...start"""

def callStatus(loc, Region):
    import boto.rds2
    #Region="us-east-1"
    conn = boto.rds2.connect_to_region(Region)
    _snapshots = conn.describe_db_snapshots()
    return str(_snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'][loc]["Status"])

"""callStatus() ...stop"""
"""********************************************************************************************************************"""   
""" awsmysqlclone()...start"""

def awsmysqlclone(dict_source_ttSettings, dict_target_ttSettings, source_hostname, target_hostname, my_logger, log_file):
    my_logger.info("In awsmysqlclone()...")
    drop_args="mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" + dict_target_ttSettings["mysql"]["users"]["dba"]["password"] \
    + " --ssl-ca=" + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"]) + " --host=" + target_hostname + " --port=3306 -e \"DROP DATABASE IF EXISTS ebdb\""
    try:
        clone_drop_out=callSubprocess(drop_args)        
    except:
        e = str(sys.exc_info())
        my_logger.info("Error encountered in awsmysqlclone_drop table: %s. Exiting!" %e)
        print("Error encountered. Exiting! Logfile location: %s" %log_file)
        sys.exit(-1)
    my_logger.info("Drop database success!")

    create_args="mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" + dict_target_ttSettings["mysql"]["users"]["dba"]["password"] \
    + " --ssl-ca=" + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"]) + " --host=" + target_hostname + " --port=3306 -e \"CREATE DATABASE IF NOT EXISTS ebdb\""
    try:
        clone_create_out=callSubprocess(create_args)        
    except:
        my_logger.info("Error encountered in awsmysqlclone_create table. Exiting!")
        print("Error encountered. Exiting! Logfile location: %s" %log_file)
        sys.exit(-1)
    my_logger.info("Create database success! Output: %s"%clone_create_out)
    
    priv_args1 = "mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" + dict_target_ttSettings["mysql"]["users"]["dba"]["password"] + " --ssl-ca=" \
    + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"])  + " --host=" + target_hostname + \
    " --port=3306 -e \"USE ebdb; GRANT SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO 'replnyc'@'%' IDENTIFIED BY \'" + dict_source_ttSettings["mysql"]["users"]["r"]["password"] + "\'; FLUSH PRIVILEGES;\" "
    try:
        clone_priv_out = callSubprocess(priv_args1)            
    except:
        my_logger.error("Error encountered in awsmysqlclone_grant privileges. Exiting!")
        sys.exit("Error encountered in awsmysqlclone_grant privileges. Exiting! Logfile location: %s" %log_file)
    my_logger.info( "Grant privileges success!")
    
    final_clone="mysqldump --user=replnyc -p" + dict_source_ttSettings["mysql"]["users"]["r"]["password"] + " --ssl-ca=" + os.path.expanduser(dict_source_ttSettings["mysql"]["certs"]["public"]) \
    + " --host=" + source_hostname + " --port=3306 --single-transaction=TRUE ebdb | mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" \
    + dict_target_ttSettings["mysql"]["users"]["dba"]["password"]  + " --ssl-ca=" + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"]) + " --host=" + target_hostname + " --port=3306 ebdb"
    try:
        os.system(final_clone)   
    except:
        my_logger.error("Error encountered awsmysqlclone_final_clone. Exiting! Logfile location: %s" %log_file)
        sys.exit("Error encountered awsmysqlclone_final_clone. Exiting! Logfile location: %s" %log_file)
    my_logger.info("MySQL Clone success!")
    
    
    return("Returning to mysql_operations()")

""" awsmysqlclone()...stop"""
"""********************************************************************************************************************"""   
""" mysql_restore()...start"""

def mysql_restore(dict_target_ttSettings, target_hostname, my_logger, log_file, restore_dir):
    """
    Check if the folder with database ebdb.sql exists
    """
    my_logger.debug("Received restore_dir: %s"%restore_dir)
    print "path: %s"%os.path.join(restore_dir + "/ebdb.sql")
    if os.path.exists(os.path.join(restore_dir + "/ebdb.sql")):
        my_logger.info("%s/ebdb.sql database location confirmed on local host...continuing"%restore_dir)
        print ("%s/ebdb.sql database location confirmed on local host...continuing"%restore_dir)
        
    else:
        my_logger.error("%s+/ebdb.sql database location confirmed on local host...continuing"%restore_dir)
        sys.exit("%s+/ebdb.sql does not exist on local host. Exiting!")
    
    confirm_restore = raw_input("Confirm if you want to continue with database restore [y/n]: ")
    
    if (confirm_restore == 'y') or (confirm_restore == 'Y'):
        my_logger.info("In mysql_restore()...")
        
        """
        DROP DATABASE ebdb
        """
        
        drop_args="mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" + dict_target_ttSettings["mysql"]["users"]["dba"]["password"] \
        + " --ssl-ca=" + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"]) + " --host=" + target_hostname + " --port=3306 -e \"DROP DATABASE IF EXISTS ebdb\""
        print"Dropping database ebdb.sql on %s"%target_hostname
        try:
            clone_drop_out=callSubprocess(drop_args)        
        except:
            my_logger.error("Unexpected error encountered in dropping ebdb.sql from %s. Exiting!" %target_hostname)
            sys.exit("Unexpected error encountered in dropping ebdb.sql from %s. Exiting!" %target_hostname)
            
        my_logger.info("Drop database ebdb.sql from %s successful!"%target_hostname)
        print("Drop database ebdb.sql from %s successful!"%target_hostname)
        
        print("Creating database ebdb.sql on %s"%target_hostname)
        create_args="mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" + dict_target_ttSettings["mysql"]["users"]["dba"]["password"] \
        + " --ssl-ca=" + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"]) + " --host=" + target_hostname + " --port=3306 -e \"CREATE DATABASE IF NOT EXISTS ebdb\""
        try:
            clone_create_out=callSubprocess(create_args)        
        except:
            my_logger.error("Unexpected error encountered in Creating database ebdb.sql on %s. Exiting!"%target_hostname)
            sys.exit("Unexpected error encountered in Creating database ebdb.sql on %s. Exiting!"%target_hostname)
            
        my_logger.info("Create database ebdb.sql successful!")
        print("Create database ebdb.sql successful!")
        
        print"Creating read-only user \'replnyc\' on %s"%target_hostname
        priv_args1 = "mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" + dict_target_ttSettings["mysql"]["users"]["dba"]["password"] + " --ssl-ca=" \
        + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"])  + " --host=" + target_hostname + \
        " --port=3306 -e \"USE ebdb; GRANT SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO 'replnyc'@'%' IDENTIFIED BY \'" + dict_target_ttSettings["mysql"]["users"]["r"]["password"] + "\'; FLUSH PRIVILEGES;\" "
        try:
            clone_priv_out = callSubprocess(priv_args1)            
        except:
            my_logger.error("Unexpected error encountered in Creating read-only user \'replnyc\' on %s. Exiting!"%target_hostname)
            sys.exit("Unexpected error encountered in Creating read-only user \'replnyc\' on %s. Exiting!"%target_hostname)
        
        my_logger.info("Creating read-only user \'replnyc\' on %s successful!"%target_hostname)
        print ("Creating read-only user \'replnyc\' on %s successful!"%target_hostname)
        
        print("Restoring data from %s/ebdb.sql to %s"%(restore_dir, target_hostname))
        final_clone = "mysql --user=" + dict_target_ttSettings["mysql"]["users"]["dba"]["username"] + " -p" \
        + dict_target_ttSettings["mysql"]["users"]["dba"]["password"]  + " --ssl-ca=" + os.path.expanduser(dict_target_ttSettings["mysql"]["certs"]["public"]) + " --host=" + target_hostname + " --port=3306 ebdb < " + restore_dir + "/ebdb.sql"
        try:
            os.system(final_clone)   
        except:
            my_logger.error("Unexpected encountered in Restoring data from %s/ebdb.sql to %s. Exiting!"%(restore_dir, target_hostname))
            sys.exit("Unexpected encountered in Restoring data from %s/ebdb.sql to %s. Exiting!"%(restore_dir, target_hostname))
        my_logger.info("Restoring data from %s/ebdb.sql to %s successful!!"%(restore_dir, target_hostname))
        print("Restoring data from %s/ebdb.sql to %s successful!"%(restore_dir, target_hostname))
    
    elif (confirm_restore == 'n') or (confirm_restore == 'N'):
        my_logger.info("Received restore confirmation: %s. Exiting!"%confirm_restore)
        sys.exit("Received restore confirmation: %s. Exiting!"%confirm_restore)
    
    else:
        my_logger.error("Unidentified response. Received restore confirmation: %s. Exiting!"%confirm_restore)
        sys.exit("Unidentified response. Received restore confirmation: %s. Exiting!"%confirm_restore)
    
""" mysql_restore()...stop"""
"""********************************************************************************************************************""" 
