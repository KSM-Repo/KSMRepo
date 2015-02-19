#!/usr/bin/env python
#
#Author: Sudha Kanupuru
#Description: Database Copy and Swap for MongoDB

"""********************************************************************************************************************""" 
print "Initializing setup..."
"""Import sys, re, os...start"""
#print "Importing python libraries..."
try:
    #print "Importing \'sys\'...",
    import sys
except:
    raise SystemExit("sys import failed. Exiting!")
#print "successful."
try:
    #print "Importing \'os\'...",
    import os
except:
    sys.exit("os import failed. Exiting!")
#print "successful."

try:
    #print "Importing \'re\'...",
    import re
except:
    sys.exit("re import failed. Exiting!")
#print "successful."

print "Importing python libraries successful!"

"""Import sys, re...stop"""
"""********************************************************************************************************************"""
"""Download ttSettings.conf from S3...start"""

from ttLib.ttSys import get_s3_config 
#print "Downloading ttSettings from S3...",
try:
    s3_config_foldername="aws-prod"
    s3_config_filename = "ttSettings.conf"
    global dict_ttSettings
    dict_ttSettings = get_s3_config(s3_config_foldername, s3_config_filename)
    #global dict_ttSettings
    #dict_ttSettings = get_s3_config(s3_config_filename)
    #print"complete."  
    #print  dict_ttSettings
except:
    sys.exit("Error Encountered: %s. Download of ttSettings from S3 failed. Exiting!" %str(sys.exc_info()))

print "Downloading ttSettings from S3 successful!"
PATH_ROOT = dict_ttSettings["PATH_ROOT"]

"""Download ttSettings.conf from S3...stop"""
"""********************************************************************************************************************"""
""" Create a timestamp directory to store log files...start"""
    
print "Creating timestamp directory...",
from ttLib.ttSys import dirCreateTimestamp
_aws_swap_dir = PATH_ROOT + dict_ttSettings["PATH_LOG"] + "/swap"
#print"_aws_swap_dir:", _aws_swap_dir
#print "Creating timestamp directory...",
global swapTimeStampDirPath
swapTimeStampDirPath = dirCreateTimestamp(_aws_swap_dir)
#print "swapTimeStampDirPath: ",swapTimeStampDirPath

""" Create a timestamp directory to store log files...stop"""
"""********************************************************************************************************************"""
""" Implement Logging...start"""

print "Setting up logging...",
import logging
from ttLib.logger import get_handlers 
log_handler_name='dbUtil'
my_logger = logging.getLogger(log_handler_name)
for h in my_logger.handlers:
    my_logger.removeHandler(h)
log_file_name='dbUtil.log'
log_file_abs_path = swapTimeStampDirPath + "/" + log_file_name
#print"log_file_abs_path: ",log_file_abs_path
for h in get_handlers(log_handler_name, log_file_abs_path):
    my_logger.addHandler(h)
my_logger.setLevel(logging.DEBUG)
#print "complete." 
#print "\nLog and backup location: %s"%swapTimeStampDirPath
my_logger.info("Logging configured successfully!")

""" Implement Logging...stop"""
"""********************************************************************************************************************"""    

length_of_args=len(sys.argv)
if length_of_args<3:
    print "Argument format mismatch. Use: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target_env (for 'copy' only)]"
    sys.exit("Exiting!")
    
"""********************************************************************************************************************"""
""" Find ttLib and import its libraries...start """

#print("Importing 3top libraries (ttLib)...")
try:
    from ttLib.ttLx import callSubprocess
except:
    sys.exit("Error encountered during import of callSubprocess from ttLib.ttLx. Exiting!%s" %str(sys.exc_info()))
    #sys.exit("Logfile location: %s" %swapTimeStampDirPath)
#print("Import callSubprocess...successful")

try:
    from ttLib.ttAWS import findSourceSuffix
except:
    sys.exit("Error encountered during import of findSourceSuffix from ttLib.ttAWS. Exiting! %s" %str(sys.exc_info()))
    #sys.exit("Logfile location: %s" %swapTimeStampDirPath)
#print("Import findSourceSuffix...successful")

print("Importing 3top libraries (ttLib) successful!") 
    
"""Find ttLib and import its libraries...stop"""
"""********************************************************************************************************************"""

print "Initial setup complete."

"""********************************************************************************************************************"""

"""main() Module definition... start"""

def main():
    print("In main()...")
    
    """****************************************************************************************************************"""
    """ Collecting arguments into variables based on the format:
        db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix (for 'copy' only)]
        start
        """
    #print("Collecting arguments into variables...")
    if length_of_args == 4:
        target_env = sys.argv[3]
    elif length_of_args==3:
        target_env = " "    
    else:
        sys.exit("Arguments format mismatch.  Exiting! \nUse: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target_env (for 'copy' only)].")
        #sys.exit("Arguments mismatch.  Exiting!")
        #sys.exit(-1)
    print("Received target_env: %s"%target_env)
    #print "target_env: ", target_env
    
    db_operation = sys.argv[1]
    print("Received db_operation: %s"%db_operation)
    #print"db_operation: ", db_operation
    
    db_service = sys.argv[2]
    print("Received db_service: %s"%db_service)
    #print"db_service: ",db_service
    
    """Collecting arguments into variables...stop"""
    """****************************************************************************************************************"""
    """ Validate certificate location...start"""
    
    #print("Calling \"validate_cert_loc()\"...")
    #AWS_MYSQL_CERT = validate_cert_loc()
    
    """ Validate certificate location...start"""
    """****************************************************************************************************************"""
    """ calling validate_args()...start"""
    
    #print("Calling \"validate_args()\"...")
    validate_args(db_operation, db_service, target_env)
    print("Validate args successful. Back in main()...")
    
    """ calling validate_args()...stop"""
    """****************************************************************************************************************"""
    """ calling set_env()...start"""
    
    #print("Calling \"set_env()\"...")
    (source_env, target_env)=set_env(db_operation, target_env)
    print("set_env successful. Back in main()... Received:: source_env: %s, target_env: %s. "%(source_env, target_env))
    
    """ calling set_env()...stop"""
    """****************************************************************************************************************"""
    """Start download of ttSettings based on source env"""
    
    #dict_source_ttSettings = {}
    source_s3_folder = "aws-" + source_env.split(".")[1] + "/" + source_env.split(".")[0]
    print "Downloading ttSettings for %s from %s folder on S3..."%(source_env,source_s3_folder)
    try:
        s3_config_folder = source_s3_folder
        s3_config_filename = "ttSettings.conf"
        global dict_source_ttSettings
        dict_source_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
        print"complete."  
        print  "dict_source_ttSettings: %s"%dict_source_ttSettings
    except:
        sys.exit("Download of ttSettings from S3 failed. Exiting! Error Encountered: %s." %str(sys.exc_info()))
        
    """Stop download of ttSettings based on source env""" 
    """****************************************************************************************************************"""    
    """Start download of ttSettings based on target env"""
    
    if "aws.3top.com" in target_env:
        #dict_source_ttSettings = {}
        target_s3_folder = "aws-" + target_env.split(".")[1] + "/" + target_env.split(".")[0]
    elif "nyc.3top.com" in target_env:
        #dict_source_ttSettings = {}
        target_s3_folder = "nyc-sys"
    print "Downloading ttSettings for %s from %s folder on S3..."%(target_env, target_s3_folder)
    try:
        s3_config_folder = target_s3_folder
        s3_config_filename = "ttSettings.conf"
        global dict_target_ttSettings
        dict_target_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
        print"complete."  
        print  "dict_target_ttSettings: %s"%dict_target_ttSettings
    except:
        sys.exit("Download of ttSettings from S3 failed. Exiting! Error Encountered: %s." %str(sys.exc_info()))         
       
    """Stop download of ttSettings based on target env""" 
    """****************************************************************************************************************"""
    
"""main() Module definition... stop"""
"""********************************************************************************************************************"""
"""mongo_operations()... start"""

def mongo_operations(operation, service, s_host, t_host, my_logger, log_file):
    print"Starting mongo operations..."
    my_logger.info("In mongo_operations...")
    my_logger.info("Taking backup before update. Calling mongodump()...")
    mongodump_source("beforeUpdate-source", s_host, my_logger, log_file)
    print"Source backup before update successful for source..."
    my_logger.info("Back in mongo_operations. Backup before update successful for source.")
    mongodump_target("beforeUpdate-target", t_host, my_logger, log_file)
    print"Target backup before update successful for source..."
    my_logger.info("Back in mongo_operations. Backup before update successful for target.")
    my_logger.info("Starting clone process. Calling mongoclone()...")
    mongoclone(s_host, t_host, my_logger, log_file)
    print "Mongo clone successful..."
    #my_logger.info(clone_OP)
    mongodump_source("afterUpdate-source", s_host, my_logger, log_file)
    print"Source backup after update successful for source..."
    my_logger.info("Back in mongo_operations. Backup after update successful for source.")
    mongodump_target("afterUpdate-target", t_host, my_logger, log_file)
    print"Target backup after update successful for source..."
    my_logger.info("Back in mongo_operations. Backup after update successful for target.")
    
"""mongo_operations()... start"""
"""********************************************************************************************************************"""

"""********************************************************************************************************************"""
"""mongodump_source()... start"""

def mongodump_source(suffix, s_host, my_logger, log_file):
    my_logger.info("In mongodump()...")
    try: #print("Calling \"validate_args()\"...")
        #validate_args(db_operation, db_service, target_env)
        filename="mongodb-" + suffix
        filepath=os.path.join(swapTimeStampDirPath,filename)
        my_logger.info("Create file success! %s " % filepath)
        dump_cmd="mongodump --host " + s_host + " --port 27017 --username "+ dict_ttSettings["aws_mongo_dbUser"] + " --password " + dict_ttSettings["aws_mongo_dbUser_password"] + " --db apis_cache --out " + filepath
        #print dump_cmd
        """
         mongodump --host mongodb.b.prod.aws.3top.com:27017 --username superuser1 --password HnFBg67tZJ0NZvPC33YvM2hy5H1Sh48mwEe --out mongodump/
        """
        dump_OP=callSubprocess(dump_cmd)
    except:
        my_logger.info("Error encountered. Exiting! %s"%str(sys.exc_info()))
        sys.exit("Arguments mismatch. Exiting! \nLogfile location: %s" %log_file)
    my_logger.info("Mongodump successful. \nOutput Received: %s"%dump_OP)
 
"""mongodump_source()... stop"""
"""********************************************************************************************************************"""

"""********************************************************************************************************************"""
"""mongodump_target()... start"""

def mongodump_target(suffix, t_host, my_logger, log_file):
    my_logger.info("In mongodump()...")
    try:
        filename="mongodb-" + suffix 
        filepath=os.path.join(swapTimeStampDirPath,filename)
        my_logger.info("Create file success! %s " % filepath)
        #dump_cmd="mongodump --host " + t_host + " --port 27017 --username "+ dict_ttSettings["mongodb"]["dba"]["username"] + " --password " + dict_ttSettings["mongodb"]["dba"]["password"] + " --db admin --out " + filepath
        dump_cmd="mongodump --host " + t_host + " --port 27017 --username "+ dict_ttSettings["mongodb"]["dba"]["username"] + " --password " + dict_ttSettings["mongodb"]["dba"]["password"] + " --out " + filepath
        #print dump_cmd
        """
         mongodump --host mongodb.b.prod.aws.3top.com:27017 --username superuser1 --password HnFBg67tZJ0NZvPC33YvM2hy5H1Sh48mwEe --out mongodump/
        """
        dump_OP=callSubprocess(dump_cmd)
    except:
        my_logger.info("Error encountered. Exiting! %s"%str(sys.exc_info()))
        sys.exit("Arguments mismatch. Exiting! \nLogfile location: %s" %log_file)
    my_logger.info("Mongodump successful. \nOutput Received: %s"%dump_OP)
 
"""mongodump_target()... stop"""
"""********************************************************************************************************************"""
"""mongoclone()... start"""

def mongoclone(s_host, t_host, my_logger,log_file):
    my_logger.info(" In mongoclone()...")
    try:
        filename="mongoclone.js"
        filepath=os.path.join(swapTimeStampDirPath,filename)
        my_logger.info("Create file success! %s " % filename)
        f=open(filepath, "wb")
        f.write("printjson(db = db.getSiblingDB('apis_cache'));")
        f.write("\nprintjson(db.getCollectionNames());")
        f.write("\nprintjson(db.dropDatabase());")
        f.write("\nprintjson(db.copyDatabase (\"apis_cache\", \"apis_cache\", \"%s\", \"%s\", \"%s\"));"%(s_host, dict_ttSettings["aws_mongo_dbUser"],dict_ttSettings["aws_mongo_dbUser_password"]))
        f.write("\nprintjson(db.getCollectionNames());")
        f.close()
        clone_cmd="mongo admin -u " + dict_ttSettings["mongodb"]["dba"]["username"] + " -p " + dict_ttSettings["mongodb"]["dba"]["password"] + " --host " + t_host + " --verbose --port 27017 " + filepath
        #print clone_cmd
        clone_OP=callSubprocess(clone_cmd)
    except:
        my_logger.info("Error encountered. Exiting! %s"%str(sys.exc_info()))
        sys.exit("Arguments mismatch. Exiting! \nLogfile location: %s" %log_file)
    my_logger.info("Mongodump successful. \nOutput Received: %s"%clone_OP)
    
    
"""mongoclone()...start"""
"""********************************************************************************************************************"""
"""validate_mongodb_target_accounts()... start"""

def validate_mongodb_target_accounts(t_host):
    for k, v in dict_ttSettings["mongodb"].items():
        try:
            #print v["username"] , v["password"]
            conn_cmd="mongo " + t_host + ":27017/admin -u " + v["username"] + " -p " + v["password"] + " --eval \"printjson(db.getCollectionNames());\""
            conn_out=callSubprocess(conn_cmd)
        except:
            sys.exit("NYC account for %s cannot be validated. Error Encountered: %s. Exiting!" %(v["username"] , str(sys.exc_info())))
            #sys.exit(-1)
        #print("Connection Successful for %s user: %s. \nConnection Output: \n%s!"%(k, v["username"] ,conn_out))
    #print("Accounts validation successful for service: MongoDB !")
        
"""validate_mongodb_target_accounts()... stop"""
"""********************************************************************************************************************"""
"""validate_mongodb_source_accounts()... start"""

def validate_mongodb_source_accounts(s_host):
    try:
        conn_cmd="mongo " + s_host + ":27017/apis_cache -u "+ dict_ttSettings["aws_mongo_dbUser"] + " -p " + dict_ttSettings["aws_mongo_dbUser_password"] + " --eval \"printjson(db.getCollectionNames());\""
        conn_out=callSubprocess(conn_cmd)
    except:
        sys.exit("NYC account for %s cannot be validated. Error Encountered: %s. Exiting!" %(dict_ttSettings["aws_mongo_dbUser"] , str(sys.exc_info())))
        #sys.exit(-1)
    #print("Connection Successful for user: %s. \nConnection Output: \n%s!"%(dict_ttSettings["aws_mongo_dbUser"] ,conn_out))
    #print("Accounts validation successful for service: MongoDB !")
        
"""validate_mongodb_source_accounts()... stop"""
"""********************************************************************************************************************"""
""" Validate if the AWS certificate is located in path provided in dict_ttSettings... start """
    
def validate_cert_loc():
    try:
        PATH_SRC = dict_ttSettings["PATH_SRC"]
        path_ssl= PATH_ROOT + PATH_SRC + dict_ttSettings["AWS_MYSQL_CERT"]
        if (os.access(path_ssl, os.R_OK)):
            print("SSL Validated. Continuing!")
        else:
            sys.exit("Validate SSL failed. Exiting! %s" %str(sys.exc_info()))
            #sys.exit("Program terminated on error.")
    except:
        sys.exit("Certificate location cannot be validated. Error Encountered: %s. Exiting!" %str(sys.exc_info()))
    return path_ssl

""" Validate if the AWS certificate is located in path provided in dict_ttSettings... stop  """
"""********************************************************************************************************************"""
""" Validate arguments...start    """

def validate_args(operation, service, t_env):
    print("In validate_args()")
    
    try:
        """ Validate database operation and target environment arguments start"""
    
        #print("Validating db_operation and target_env...")
        if operation=="":
            print("No db_operation argument found. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]")
            sys.exit(-1)
        elif operation not in ['swap', 'copy']:
            print("db_operation argument invalid. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]")
            sys.exit(-1)
        elif (operation=='swap') and (t_env != " "):
            #print("db_operation valid! Received db_operation: %s. Continuing..."%operation)
            print("Incorrect format. target_env not required for swap operation. \nUse: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only)]")
            sys.exit(-1)
        elif (operation=='copy') and (t_env == " "):
            #print("db_operation valid! Received db_operation: %s. Continuing..."%operation)
            print("Incorrect format. target_env not found. Exiting! \nUse: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only)]")
            sys.exit(-1)
        elif operation == 'copy':
            print("db_operation valid!  Received db_operation: %s. Continuing..."%operation)
            
            
            """ Validate target_env format start  """
            
            if "aws.3top.com" in t_env:
                if re.match('^[ab]\.(dev)(\.(aws)\.(3top)\.(com))$', t_env):
                    print("target_env valid! Received target_env: %s. Continuing..."%t_env)
                    
                else:
                    print("Target_env format mismatch. Exiting! \nUse format: [a/b].(prod/dev).(nyc/aws).(3top).(com)")
                    sys.exit(-1)
            elif "nyc.3top.com" in t_env:
                print("target_env valid! Received target_env: %s. Continuing..."%t_env)
                
            else:
                print("Target host format mismatch. Exiting! \nUse format: [a/b].(prod/dev).(nyc/aws).(3top).(com)")
                sys.exit(-1)
            
            """ Validate target_env format stop  """
        
        """ Validate database operation and target environment arguments stop"""
        
        """ Validate database service argument... start"""
    
        print("Validating db_service...")
        if service=="":
            print("No db_service arguments found. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]")
            sys.exit(-1)
        elif service not in ['mysql', 'mongodb', 'all']:
            print("Incorrect Database chosen: %s. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]" %service)
            sys.exit(-1)
        else:
            print("db_service validation successful!")
            
    
        """ Validate database service argument stop"""
                        
    except:
        print("Error encountered in validate_args. Exiting! %s" %str(sys.exc_info()))
        sys.exit(-1)
    #print("Validate arguments complete. Returning to main()...")
       
""" Validate arguments...stop    """
"""********************************************************************************************************************"""
""" Set env based on the arguments... start"""

def set_env(operation, t_env):
    #print("In set_env()...")
    try:
        #print("Calling findSourceSuffix()...")
        s_env = findSourceSuffix()
        #print("Current active Environment (source environment): %s"%s_env)
        
        if operation == 'swap':
            if s_env == "a.prod.aws.3top.com":
                #global target_env
                t_env = "b.prod.aws.3top.com"
            else:
                #global target_env
                t_env="a.prod.aws.3top.com"
            #print("Target Environment set as: %s"%(t_env))
        elif operation == 'copy':
            print("Target Environment set as: %s"%(t_env))
            
    except:
        print("Error encountered in set_env. Exiting! %s" %str(sys.exc_info()))
        sys.exit(-1)
        #print source_env
    #print("set_env call successful. Returning to main()")
    return s_env, t_env

""" Set env based on the arguments... stop"""
"""********************************************************************************************************************"""
""" Validate if the AWS certificate is located in path provided in dict_ttSettings...start"""

#def validate_cert_loc():
    #try:
        #PATH_SRC = dict_ttSettings["PATH_SRC"]
        #path_ssl= PATH_ROOT + PATH_SRC + dict_ttSettings["AWS_MYSQL_CERT"]
        #if (os.access(path_ssl, os.R_OK)):
            #print("SSL Validated. Continuing!")
        #else:
            #print("Validate SSL failed. Exiting!")
            #print("Logfile location: %s" %swapTimeStampDirPath)
            #sys.exit(-1)
    #except:
        #e = str(sys.exc_info())
        #print("Certificate location cannot be validated. Error Encountered: %s. Exiting!" %e)
        #print("Error encountered. Exiting! Logfile location: %s" %swapTimeStampDirPath)
        #sys.exit(-1)
    #return path_ssl

""" Validate if the AWS certificate is located in path provided in dict_ttSettings...stop"""
"""********************************************************************************************************************"""
""" set host based on env ... start"""

def set_mongodb_host(service, s_env, t_env):
    #print("In set_host()...")
    if (service == "mongodb") or (service=='all'):
        mongo_s_host= 'mongodb.' + s_env
        #print("Source Hostname set as:: mongodb_source_host: %s"%(mongo_s_host))
        if ("aws.3top.com" in t_env):
            mongo_t_host='mongodb.' + t_env
        elif "nyc.3top.com" in t_env:
            mongo_t_host= t_env
        #print("Target Hostname set as:: mongodb_target_host: %s"%(mongo_t_host))
    #print("set_host() operations complete. Returning to main()...")
    return mongo_s_host, mongo_t_host

""" set host based on env ... stop """
"""********************************************************************************************************************"""
""" set host name based on env ... start"""

def set_mysql_host(service, s_env,t_env):
    if (service=='mysql') or (service=='all'):
        mysql_s_host='fp-rds-1.' + s_env
        if ("aws.3top.com" in s_env) and ("aws.3top.com" in t_env):
            mysql_t_host='fp-rds-1.' + t_env 
        elif "nyc.3top.com" in t_env:
            mysql_t_host=t_env
        
    #print("Mysql_Source_Name: %s, Mysql_Target_Name: %s" %(mysql_s_host, mysql_t_host))
    return mysql_s_host, mysql_t_host

""" set host name based on env ... stop"""
"""********************************************************************************************************************"""
""" validate_nyc_accounts ... start"""

def validate_mysql_nyc_accounts(target_hostname):
    #import MySQLdb
    for k,v in dict_ttSettings["mysql"].items():
        try:
            conn_cmd="mysql --user=" + v["username"] + " -p" + v["password"] + " --host=" + target_hostname + " --port=3306 -e \"CONNECT;\""
            conn_out=callSubprocess(conn_cmd)
        except:
            e = str(sys.exc_info())
            print("Error Encountered: %s. Exiting!" %e)
            sys.exit(-1)
        #conn.close()
        #print(conn_out)
        #print("Connection Successful for user: %s!"%k)
    #print("Accounts validation successful for service: MySQL !")
    
""" validate_nyc_accounts ... start"""
"""********************************************************************************************************************"""    
""" validate_aws_accounts ... start"""

def validate_mysql_aws_accounts(aws_cert, target_hostname):
    #import MySQLdb
    for k,v in dict_ttSettings["mysql"].items():
        try:
            #print v["username"]
            #print v["password"]
            #print target_hostname
            #print aws_cert
            #conn_cmd= MySQLdb.Connection(user= v["username"], passwd= v["password"], host= target_hostname, ssl= aws_cert)
            conn_cmd="mysql --user=" + v["username"] + " -p" + v["password"] + " --ssl-ca=" + aws_cert + " --host=" + target_hostname + " --port=3306 -e \"CONNECT;\""
            conn_out=callSubprocess(conn_cmd)
        except:
            e = str(sys.exc_info())
            print("Error Encountered: %s. Exiting!" %e)
            sys.exit(-1)
        #conn.close()
        #print(conn_out)
        #conn_kill=callSubprocess(conn_cmd)
        #print("Connection Successful for user: %s!"%k)
    #print("Accounts validation successful for service: MySQL !")
    #print "validate complete"

""" validate_aws_accounts ... start"""
"""********************************************************************************************************************"""
""" ...start"""
    
def validate_mysql_source(aws_cert, source_hostname):
    try:
        #print("In validate_mysql_source()...")
        val_s_cmd="mysql --user=replnyc -p" + dict_ttSettings["_aws_rds_replpass"] + " --ssl-ca=" + aws_cert + " --host=" + source_hostname + " --port=3306 -e \"CONNECT;\""
        val_s_cmd_out=callSubprocess(val_s_cmd)
    except:
        e = str(sys.exc_info())
        sys.exit("Encountered Error in mysqldump(): %s. Exiting!" %e)
        
    return("Accounts validation successful for service: MySQL source with output: %s"%val_s_cmd_out)

""" ...stop"""
"""********************************************************************************************************************""" 
""" ...start"""

def mysql_operations(operation, service, aws_cert, source_hostname, target_hostname, my_logger, log_file):
    my_logger.info("In mysql_Operations...")
    #print("Source Host: %s " %mysql_source_host)
    #print("Target Host: %s " %mysql_target_host)
    import time
    timestamp= time.strftime("%Y%m%d-%H%M%S")
    dump_source_before=mysqldump_source("source-beforeUpdate", aws_cert, source_hostname, my_logger, log_file)
    my_logger.info(dump_source_before)
    print"Backup before update successful for source..."
    
    dump_target_before=mysqldump_target("target-beforeUpdate", aws_cert, target_hostname, my_logger, log_file)
    my_logger.info(dump_target_before)
    print"Backup before update successful for target..."
    
    print"Starting snapshot for source before clone..."
    snapshot_source_before= takesnapshot(source_hostname,my_logger,'source','before-copy',timestamp, log_file)
    log_str=str("Started clone: "+snapshot_source_before)
    my_logger.info(log_str)
        
    if "aws.3top.com" in target_hostname:
        print"Starting snapshot for target before clone..."
        snapshot_target_before=takesnapshot(target_hostname,my_logger,'target','before-copy',timestamp, log_file)
        log_str=str("Started clone: "+snapshot_target_before)
        my_logger.info(log_str)
    
    statusCheck=snapStatusCheck(snapshot_source_before,snapshot_target_before, log_file)
    #statusCheck_source=snapStatusCheck(snapshot_source_before)
    #statusCheck_target=snapStatusCheck(snapshot_target_before)
    #my_logger.info(statusCheck_source)
    #my_logger.info(statusCheck_target)
    my_logger.info(statusCheck)
    
    clone_OP=awsmysqlclone(aws_cert, source_hostname, target_hostname, my_logger, log_file)
    my_logger.info(clone_OP)
    print "Mysql clone complete..."
    
    dump_source_after=mysqldump_source("source-afterUpdate", aws_cert, source_hostname, my_logger, log_file)
    my_logger.info(dump_source_after)
    print"Backup after update successful for source..."
    
    dump_target_after=mysqldump_target("target-afterUpdate", aws_cert, target_hostname, my_logger, log_file)
    my_logger.info(dump_target_after)
    print"Backup after update successful for target..."
    
    print"Starting snapshot for source after clone..."
    snapshot_source_after=takesnapshot(source_hostname,my_logger,'source', 'after-copy',timestamp, log_file)
    log_str=str("Started clone: "+snapshot_source_after)
    my_logger.info(log_str)
    
    if "aws.3top.com" in target_hostname:
        print"Starting snapshot for target after clone..."
        snapshot_target_after=takesnapshot(target_hostname,my_logger,'target', 'after-copy',timestamp, log_file)
        log_str=str("Started clone: "+snapshot_target_after)
        my_logger.info(log_str)
        
    statusCheck=snapStatusCheck(snapshot_source_after,snapshot_target_after, log_file)
    #statusCheck_source=snapStatusCheck(snapshot_source_after)
    #statusCheck_target=snapStatusCheck(snapshot_target_after)
    #my_logger.info(statusCheck_source)
    #my_logger.info(statusCheck_target)
    my_logger.info(statusCheck)

"""********************************************************************************************************************"""
""" ...start"""
    
def mysqldump_source(suffix, aws_cert, source_hostname, my_logger, log_file):
    try:
        my_logger.info("In mysqldump()...")
        dump_cmd="mysqldump --user=replnyc -p" + dict_ttSettings["_aws_rds_replpass"] + " --ssl-ca=" + aws_cert + " --host=" + source_hostname + " --port=3306 --single-transaction=TRUE --routines --events ebdb"
        #dump_cmd="mysqldump -u mysql_rw -p" + ttSettings["mysql_rw"] + " -ssl-ca " + ttSettings["AWS_MYSQL_CERT"] + " -h " + mysql_source_host + " -P 3306 --single-transaction=TRUE --routines --events ebdb"
        #print dump_cmd
        #print("command for dump: ",dump_cmd)
        #dump_cmd="mysqldump -u replnyc -p" + _aws_rds_replpass + " --host=" + source_host + " --port=3306 --single-transaction=TRUE ebdb"
        filename=suffix + "-ebdb.sql"
        filepath=os.path.join(swapTimeStampDirPath,filename)
        my_logger.info("Create file success! %s " % filename)
        dump_out=callSubprocess(dump_cmd)
        f=open(filepath, "wb")
        f.write(dump_out)
        f.close()
        my_logger.info("Backup successful!")
        
    except:
        e = str(sys.exc_info())
        my_logger.info("Encountered Error in mysqldump(): %s. Exiting!" %e)
        print("Error encountered. Exiting! Logfile location: %s" %log_file)
        sys.exit(-1)
    return("Returning to mysql_operations")

""" ...stop"""
"""********************************************************************************************************************"""   
""" ...start"""

def mysqldump_target(suffix, aws_cert, target_hostname, my_logger, log_file):
    try:
        my_logger.info("In mysqldump()...")
        dump_cmd="mysqldump --user=" + dict_ttSettings["mysql"]["dba"]["username"] + " -p" + dict_ttSettings["mysql"]["dba"]["password"] + " --ssl-ca=" + aws_cert + " --host=" + target_hostname + " --port=3306 --single-transaction=TRUE --routines --events ebdb"
        #dump_cmd="mysqldump -u mysql_rw -p" + ttSettings["mysql_rw"] + " -ssl-ca " + ttSettings["AWS_MYSQL_CERT"] + " -h " + mysql_source_host + " -P 3306 --single-transaction=TRUE --routines --events ebdb"
        #print dump_cmd
        #print("command for dump: ",dump_cmd)
        #dump_cmd="mysqldump -u replnyc -p" + _aws_rds_replpass + " --host=" + source_host + " --port=3306 --single-transaction=TRUE ebdb"
        filename=suffix + "-ebdb.sql"
        filepath=os.path.join(swapTimeStampDirPath,filename)
        my_logger.info("Create file success! %s " % filename)
        dump_out=callSubprocess(dump_cmd)
        f=open(filepath, "wb")
        f.write(dump_out)
        f.close()
        my_logger.info("Backup successful!")
    except:
        e = str(sys.exc_info())
        my_logger.info("Encountered Error in mysqldump(): %s. Exiting!" %e)
        print("Error encountered. Exiting! Logfile location: %s" %log_file)
        sys.exit(-1)
    return("Returning to mysql_operations")

""" ...stop"""
"""********************************************************************************************************************"""   
""" ...start"""

def takesnapshot(host, my_logger,suffix1, suffix2,timestamp, log_file):
    import boto.rds2
    Region = 'us-east-1'
    #dict_Dbinstance={}
    #print"started"
    try:
        conn = boto.rds2.connect_to_region(Region)
        _dbinstances = conn.describe_db_instances()
        #print "dbInstances: ", _dbinstances
        for dbi in _dbinstances['DescribeDBInstancesResponse']['DescribeDBInstancesResult']['DBInstances']:
            DbinstanceId=dbi['DBInstanceIdentifier']
            #print "DbinstanceId: %s"%(DbinstanceId)
            _tags = conn.list_tags_for_resource("arn:aws:rds:us-east-1:671788406991:db:" + dbi['DBInstanceIdentifier'])
            #print "tags: ",_tags
            DbinstanceName= _tags['ListTagsForResourceResponse']['ListTagsForResourceResult']['TagList'][0]["Value"]
            #print "DbinstanceName: ",DbinstanceName
            InstSplit=DbinstanceName.split("-")
            env_name=InstSplit[2]+"."+InstSplit[1]
            #print "env_name: ",env_name
            if env_name in host:
                #print "%s in %s" %(env_name,host)
                snapshotId="fp-"+timestamp+"-"+suffix1+"-"+InstSplit[1]+"-"+InstSplit[2]+"-"+suffix2
                instId= DbinstanceId
        #print "SnapshotId= ",snapshotId
        #print "DBInstanceId= ",instId
        print "Creating snapshot for : ",host
        #print "DBInstance information: ", dict_Dbinstance
        try:
            conn.create_db_snapshot(snapshotId, instId)
        except:
            print("Error Encountered %s. Exiting...."%str(sys.exc_info()))
            print("Error encountered takesnapshot for %s. Exiting! Logfile location:%s" %(host, log_file))
            sys.exit(-1)
    except:
        e = str(sys.exc_info())
        print("Error Encountered %s. Exiting...."%e)
        sys.exit(-1)
    return snapshotId

""" ...stop"""
"""********************************************************************************************************************"""   
""" ...start"""

def snapStatusCheck(snapshotId1, snapshotId2, log_file):
    try:
        import time
        import boto.rds2
        Region="us-east-1"
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
            
        status1=callStatus(loc1)
        status2=callStatus(loc2)
        while (status1 != 'available') or (status2 != 'available'):
            status1=callStatus(loc1)
            print "Status of %s: %s"%(snapshotId1, status1)
            #time.sleep(10)
            status2=callStatus(loc2)
            print "Status of %s: %s"%(snapshotId2, status2)
            time.sleep(10)
    except:
        sys.exit("Error encountered during snapStatusCheck. Exiting! Logfile location:%s" %(log_file))
        
               
        
    ret_str=str("Snapshot created successfully for %s and %s"%(snapshotId1, snapshotId2))
    return ret_str

""" ...stop"""
"""********************************************************************************************************************"""
""" ...start"""

def callStatus(loc):
    import boto.rds2
    Region="us-east-1"
    conn = boto.rds2.connect_to_region(Region)
    _snapshots = conn.describe_db_snapshots()
    return str(_snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'][loc]["Status"])

""" ...stop"""
"""********************************************************************************************************************"""   
""" ...start"""

def awsmysqlclone(aws_cert, source_hostname, target_hostname, my_logger, log_file):
    my_logger.info("In awsmysqlclone()...")
    #print "awsmysqlclone start"
    drop_args="mysql --user=" + dict_ttSettings["mysql"]["dba"]["username"] + " -p" + dict_ttSettings["mysql"]["dba"]["password"] + " --ssl-ca=" + aws_cert + " --host=" + target_hostname + " --port=3306 -e \"DROP DATABASE IF EXISTS ebdb\""
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

    create_args="mysql --user=" + dict_ttSettings["mysql"]["dba"]["username"] + " -p" + dict_ttSettings["mysql"]["dba"]["password"] + " --ssl-ca=" + aws_cert + " --host=" + target_hostname + " --port=3306 -e \"CREATE DATABASE IF NOT EXISTS ebdb\""
                                                                                                                                                                
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
    
    priv_args1="mysql --user=" + dict_ttSettings["mysql"]["dba"]["username"] + " -p" + dict_ttSettings["mysql"]["dba"]["password"] + " --ssl-ca=" + aws_cert + " --host=" + target_hostname + " --port=3306 -e \"USE ebdb; GRANT SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO 'replnyc'@'%' IDENTIFIED BY \'" + dict_ttSettings["_aws_rds_replpass"] + "\'; FLUSH PRIVILEGES;\" "
    #print priv_args1
    try:
        clone_priv_out=callSubprocess(priv_args1)            
    except:
        e = str(sys.exc_info())
        my_logger.info("Error encountered in awsmysqlclone_grant privileges: %s. Exiting!" %e)
        print("Error encountered. Exiting! Logfile location: %s" %log_file)
        sys.exit(-1)  
    #print "grant success"
    my_logger.info( "Grant privileges success!")
    final_clone="mysqldump --user=replnyc -p" + dict_ttSettings["_aws_rds_replpass"] + " --ssl-ca=" + aws_cert + " --host=" + source_hostname + " --port=3306 --single-transaction=TRUE ebdb | mysql --user=" + dict_ttSettings["mysql"]["dba"]["username"] + " -p" + dict_ttSettings["mysql"]["dba"]["password"]  + " --ssl-ca=" + aws_cert + " --host=" + target_hostname + " --port=3306 ebdb"
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
   
        
print "Initializing program...calling main()"
main()


