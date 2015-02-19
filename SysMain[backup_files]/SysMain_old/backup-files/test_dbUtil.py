 
#!/usr/bin/env python
#
#Author: Sudha Kanupuru
#Description: Datpython_root=os.path.dirname(os.path.realpath(__file__)) + "/"abase Copy and Swap for MongoDB

"""********************************************************************************************************************""" 
print "Initializing setup..."
"""Import sys, re, os...start"""
#print "Importing python libraries..."
try:
    print "Importing \'sys\'...",
    import sys
except:
    raise SystemExit("sys import failed. Exiting!")
print "successful."
try:
    print "Importing \'os\'...",
    import os
except:
    sys.exit("os import failed. Exiting!")
print "successful."

print "Importing python libraries successful!"

"""Import sys, re...stop"""
"""********************************************************************************************************************"""
"""Download ttSettings.conf from S3...start"""

python_root=os.path.dirname(os.path.realpath(__file__)) + "/"
sys.path.append(python_root + "ttLib")

from ttLib.ttSys import get_s3_config
#print "Downloading ttSettings from S3...",
try:
    s3_config_foldername="nyc-sys"
    s3_config_filename = "ttSettings.conf"
    global dict_ttSettings
    dict_ttSettings = get_s3_config(s3_config_foldername, s3_config_filename)
except:
    sys.exit("Error Encountered: %s. Download of ttSettings from S3 failed. Exiting!" %str(sys.exc_info()))

print "Downloading ttSettings from S3 successful!"
PATH_ROOT = dict_ttSettings["PATH_ROOT"]


"""Download ttSettings.conf from S3...stop"""
"""********************************************************************************************************************"""
""" Create a timestamp directory to store log files...start"""
    
#print "Creating timestamp directory..."
try:
    from ttLib.ttSys import dirCreateTimestamp
except:
    sys.exit("Error Encountered: %s. \"from ttLib.ttSys import dirCreateTimestamp\"...failed. Exiting!" %str(sys.exc_info()))
     
_aws_swap_dir = PATH_ROOT + dict_ttSettings["PATH_LOG"] + "/swap"
#print"_aws_swap_dir:", _aws_swap_dir
#print "Creating timestamp directory...",
global swapTimeStampDirPath
print("Calling swapTimeStampDirPath = dirCreateTimestamp(%s)"%_aws_swap_dir)
(swapTimeStampDirPath, timestamp) = dirCreateTimestamp(_aws_swap_dir)
print "swapTimeStampDirPath: ",swapTimeStampDirPath

""" Create a timestamp directory to store log files...stop"""
"""********************************************************************************************************************"""
""" Implement Logging...start"""

print "Setting up logging..."
try:
    import logging
except:
    sys.exit("Error Encountered: %s. \"import logging\"...failed. Exiting!" %str(sys.exc_info()))
try:
    from ttLib.logger import get_handlers 
except:
    sys.exit("Error Encountered: %s. \"from ttLib.logger import get_handlers\"...failed. Exiting!" %str(sys.exc_info()))

    
log_handler_name='dbUtil'
my_logger = logging.getLogger(log_handler_name)
for h in my_logger.handlers:
    my_logger.removeHandler(h)
log_file_name='dbUtil.log'
log_file_abs_path = swapTimeStampDirPath + "/" + log_file_name
#print"log_file_abs_path: ",log_file_abs_path
loglevel = dict_ttSettings["log_level"]

try:
    confirm_loglevel = raw_input("\nCurrent loglevel is %s. Press [c/ C] to continue. To change the loglevel to 'DEBUG' enter [debug/ DEBUG]: "%loglevel)
    if confirm_loglevel.upper() == 'C':
        loglevel = "INFO"
    elif confirm_loglevel.upper() == "DEBUG":
        loglevel = "DEBUG"
    else:
        sys.exit("Incorrect entry received: %s. Exiting!"%confirm_loglevel)
except:
    sys.exit("Unexpected error encountered. Exiting!")
print"Received loglevel: %s"%loglevel

loglevel = getattr(logging, loglevel.upper())
for h in get_handlers(log_handler_name, log_file_abs_path):
    my_logger.addHandler(h)
my_logger.setLevel(loglevel)
my_logger.debug("Logger created successfully. Continuing...")
my_logger.debug("dict_ttSettings: "%dict_ttSettings)
print("Logging configured successfully!")
my_logger.info("Logging configured successfully. Initiating logging...")

try:
    from ttLib.ttAWS import env_ttSettings
except:
    my_logger.error("\'from ttLib.ttAWS import env_ttSettings\'...failed. Exiting!")
    sys.exit("\'from ttLib.ttAWS import env_ttSettings\' failed. Exiting!")
my_logger.info("\'from ttLib.ttAWS import env_ttSettings\' successful!")

""" Implement Logging...stop"""
"""********************************************************************************************************************"""
"""Mysql Imports Start"""

try:
    from ttLib.ttDB import validate_args  
except:
    my_logger.error("\'from ttLib.ttDB import validate_args\'...failed. Exiting!")
    sys.exit("\'from ttLib.ttDB import validate_args\'...failed. Exiting!")
my_logger.info("\'from ttLib.ttDB import validate_args\' successful!")
    
  

try:
    from ttLib import ttMysql
except:
    my_logger.error("\'from ttLib import ttMysql\'...failed. Exiting!")
    sys.exit("\'from ttLib import ttMysql\'...failed. Exiting!")
my_logger.info("\'from ttLib import ttMysql\' successful!")

try:
    from ttLib.ttMysql import set_mysql_host, validate_mysql_nyc_accounts, validate_mysql_aws_accounts, validate_mysql_source, mysql_operations
except:
    my_logger.error("\'from ttLib.ttMysql import set_mysql_host, validate_mysql_nyc_accounts, validate_mysql_aws_accounts, validate_mysql_source, mysql_operations\'...failed. Exiting!")
    sys.exit("\'from ttLib.ttMysql import set_mysql_host, validate_mysql_nyc_accounts, validate_mysql_aws_accounts, validate_mysql_source, mysql_operations\'...failed. Exiting!")
my_logger.info("\'from ttLib.ttMysql import set_mysql_host, validate_mysql_nyc_accounts, validate_mysql_aws_accounts, validate_mysql_source, mysql_operations\' successful!")

my_logger.info("Setting swapTimeStampDirPath: %s in ttMysql.py"%swapTimeStampDirPath)
ttMysql.swapTimeStampDirPath= swapTimeStampDirPath

"""Mysql Imports Stop"""
"""********************************************************************************************************************"""
"""MongoDB Imports Start"""
try:
    from ttLib import ttMongoDB
except:
    my_logger.error("\'from ttLib import ttMongoDB\'...failed. Exiting!")
    sys.exit("\'from ttLib import ttMongoDB\'...failed. Exiting!")
my_logger.info("\'from ttLib import ttMongoDB\' successful!")

try:
    from ttLib.ttMongoDB import mongo_operations, validate_mongodb_target_accounts, validate_mongodb_source_accounts, set_mongodb_host
except:
    my_logger.error("\'from ttLib.ttMongoDB import mongo_operations, validate_mongodb_target_accounts, validate_mongodb_source_accounts, set_mongodb_host\'...failed. Exiting!")
    sys.exit("\'from ttLib.ttMongoDB import mongo_operations, validate_mongodb_target_accounts, validate_mongodb_source_accounts, set_mongodb_host\'...failed. Exiting!")
my_logger.info("\'from ttLib.ttMongoDB import mongo_operations, validate_mongodb_target_accounts, validate_mongodb_source_accounts, set_mongodb_host\' successful!")

my_logger.info("Setting swapTimeStampDirPath: %s in ttMongoDB.py"%swapTimeStampDirPath)
ttMongoDB.swapTimeStampDirPath= swapTimeStampDirPath

"""MongoDB Imports Stop"""
"""********************************************************************************************************************"""
"""Neo4j Imports Start"""
try:
    from ttLib import ttNeo
except:
    my_logger.error("\'from ttLib import ttNeo\'...failed. Exiting!")
    sys.exit("\'from ttLib import ttNeo\'...failed. Exiting!")
my_logger.info("\'from ttLib import ttNeo\' successful!")

try:
    from ttLib.ttNeo import set_neo4j_host, validate_neo4j_nyc_target, validate_neo4j_aws_accounts, neo4j_operations
except:
    my_logger.error("\'from ttLib.ttNeo import set_neo4j_host, validate_neo4j_nyc_target, validate_neo4j_aws_accounts, neo4j_operations\'...failed. Exiting!")
    sys.exit("\'from ttLib.ttNeo import set_neo4j_host, validate_neo4j_nyc_target, validate_neo4j_aws_accounts, neo4j_operations\'...failed. Exiting!")
my_logger.info("\'from ttLib.ttNeo import set_neo4j_host, validate_neo4j_nyc_target, validate_neo4j_aws_accounts, neo4j_operations\' successful!")

my_logger.info("Setting swapTimeStampDirPath: %s in ttNeo.py"%swapTimeStampDirPath)
ttNeo.swapTimeStampDirPath= swapTimeStampDirPath

"""Neo4j Imports Stop"""
"""********************************************************************************************************************"""
"""Initial arguments check Start"""

length_of_args=len(sys.argv)
my_logger.info("Number of arguments received: %s"%length_of_args)
if length_of_args<3:
    my_logger.error("Argument format mismatch. Use: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target_env (for 'copy' only)]")
    sys.exit("Argument format mismatch. Use: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target_env (for 'copy' only)]")
else:
    my_logger.info("Number of arguments not <3. Continuing!")
    
"""Initial arguments check Stop"""   
"""********************************************************************************************************************"""
""" Find ttLib and import its libraries...start """

try:
    from ttLib.ttLx import callSubprocess
except:
    my_logger.error("Error encountered: %s. \"from ttLib.ttLx import callSubprocess\"...failed. Exiting!" %str(sys.exc_info()))
    sys.exit("Error encountered: %s. \"from ttLib.ttLx import callSubprocess\"...failed. Exiting!" %str(sys.exc_info()))
my_logger.info("\"from ttLib.ttLx import callSubprocess\"...successful")

try:
    from ttLib.ttAWS import findSourceSuffix, validate_cert_loc
except:
    my_logger.error("Error encountered: %s. \"from ttLib.ttAWS import findSourceSuffix, validate_cert_loc\"...failed. Exiting!" %str(sys.exc_info()))
    sys.exit("Error encountered: %s. \"from ttLib.ttAWS import findSourceSuffix, validate_cert_loc\"...failed. Exiting!" %str(sys.exc_info()))
my_logger.info("\"from ttLib.ttAWS import findSourceSuffix, validate_cert_loc\"...successful")

my_logger.info("Importing 3top libraries (ttLib) successful!") 
    
"""Find ttLib and import its libraries...stop"""
"""********************************************************************************************************************"""

print "Initial setup complete..."

"""********************************************************************************************************************"""
""" Set env based on the arguments... start"""

def set_env(operation, t_env):
    my_logger.debug("In set_env()...")
    try:
        my_logger.debug("Calling findSourceSuffix()...")
        s_env = findSourceSuffix()
        my_logger.info("Current active Environment (source environment): %s"%s_env)
        
        if operation == 'swap':
            if s_env == "a.prod.aws.3top.com":
                t_env = "b.prod.aws.3top.com"
            else:
                t_env="a.prod.aws.3top.com"
            my_logger.info("Target Environment set as: %s"%(t_env))
        elif operation == 'copy':
            my_logger.info("Target Environment set as: %s"%(t_env))
            
    except:
        my_logger.error("Error encountered in set_env. Exiting! %s" %str(sys.exc_info()))
        sys.exit("Error encountered in set_env. Exiting! %s" %str(sys.exc_info()))
    my_logger.info("set_env call successful. Returning to main()")
    return s_env, t_env

""" Set env based on the arguments... stop"""
"""********************************************************************************************************************"""
"""main() Module definition... start"""

def main():
    my_logger.info("\nInitial setup complete...")
    my_logger.info("In main()...")
    
    """****************************************************************************************************************"""
    """ Collecting arguments into variables based on the format:
        db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix (for 'copy' only)]
        start
        """
    my_logger.info("Collecting arguments into variables...")
    
    if length_of_args == 4:
        target_env = sys.argv[3]
    elif length_of_args==3:
        target_env = " "    
    else:
        my_logger.error("Arguments format mismatch.  Exiting! \nUse: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target_env (for 'copy' only)].")
        sys.exit("Arguments format mismatch.  Exiting! \nUse: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target_env (for 'copy' only)].")
    
    my_logger.info("Received target_env: %s"%target_env)
    
    db_operation = sys.argv[1]
    my_logger.info("Received db_operation: %s"%db_operation)
    
    db_service = sys.argv[2]
    my_logger.info("Received db_service: %s"%db_service)
    
    """Collecting arguments into variables...stop"""
    """****************************************************************************************************************"""
    """ calling validate_args()...start"""
    
    my_logger.info("Calling \"validate_args(%s, %s, %s)"%(db_operation, db_service, target_env))
    try:
        validate_args(db_operation, db_service, target_env)
    except:
        my_logger.error("Error encountered: %s. Calling \"validate_args(%s, %s, %s)\"...failed. Exiting!"%(str(sys.exc_info()), db_operation, db_service, target_env))
        sys.exit("Error encountered: %s. Calling \"validate_args(%s, %s, %s)\"...failed. Exiting!"%(str(sys.exc_info()), db_operation, db_service, target_env))
    my_logger.info("validate_args(%s, %s, %s)... successful!"%(db_operation, db_service, target_env))
    
    """ calling validate_args()...stop"""
    """****************************************************************************************************************"""
    """ calling set_env()...start"""
    
    my_logger.info("Calling \"(source_env, target_env)=set_env(%s, %s)\" "%(db_operation, target_env))
    try:
        (source_env, target_env)=set_env(db_operation, target_env)
    except:
        my_logger.error("Error encountered: %s. Calling \"(source_env, target_env)=set_env(%s, %s)\" ...failed. Exiting!"%(str(sys.exc_info()), db_operation, target_env))
        sys.exit("Error encountered: %s. Calling \"(source_env, target_env)=set_env(%s, %s)\" ...failed. Exiting!"%(str(sys.exc_info()), db_operation, target_env))
    my_logger.info("(source_env, target_env) = set_env(%s, %s) successful."%(db_operation, target_env))
    
    """ calling set_env()...stop"""
    """****************************************************************************************************************"""
    """Start download of ttSettings based on source and target env start"""
    
    my_logger.info("Calling \"dict_source_ttSettings = env_ttSettings(%s)"%(source_env))
    try:
        dict_source_ttSettings = env_ttSettings(source_env)
    except:
        my_logger.error("Error encountered: %s. Calling \"dict_source_ttSettings = env_ttSettings(%s)...failed. Exiting!"%(str(sys.exc_info()),source_env))
        sys.exit("Error encountered: %s. Calling \"dict_source_ttSettings = env_ttSettings(%s)...failed. Exiting!"%(str(sys.exc_info()),source_env))
    my_logger.info("\"dict_source_ttSettings = env_ttSettings(%s) successful!"%(source_env))
    
    my_logger.info("Calling \"dict_source_ttSettings = env_ttSettings(%s)"%(target_env))
    try:
        dict_target_ttSettings = env_ttSettings(target_env)
    except:
        my_logger.error("Error encountered: %s. Calling \"dict_source_ttSettings = env_ttSettings(%s)...failed. Exiting!"%(str(sys.exc_info()),target_env))
        sys.exit("Error encountered: %s. Calling \"dict_source_ttSettings = env_ttSettings(%s)...failed. Exiting!"%(str(sys.exc_info()),target_env))
    my_logger.info("\"dict_source_ttSettings = env_ttSettings(%s) successful!"%(target_env))
    
    """Stop download of ttSettings based on source and target env stop""" 
    """****************************************************************************************************************"""
    """ validate AWS MYSQL CERT location...start"""
    
    if db_service == "mysql":
        if ("aws.3top.com" in source_env):
            my_logger.info("Received \"aws.3top.com\" in %s"%source_env)
            mysql_source_cert = dict_source_ttSettings["mysql"]["certs"]["public"]
            #print mysql_source_cert
            my_logger.debug("Calling validate_cert_loc() for mysql_source_cert: %s" %(mysql_source_cert))
            try:
                """The location of cert in target has to be verified as MySQL does direct clone from source to target without need of admin"""
                validate_cert_loc(mysql_source_cert)
            except:
                my_logger.error("Error encountered: %s. \"Calling validate_cert_loc() with mysql_source_cert: %s\"...failed. Exiting!"%(str(sys.exc_info()),mysql_source_cert))
                sys.exit("Error encountered: %s. \"Calling validate_cert_loc() with mysql_source_cert: %s\"...failed. Exiting!"%(str(sys.exc_info()),mysql_source_cert))
            my_logger.info("validate_cert_loc() with mysql_source_cert: %s successful for %s!"%(mysql_source_cert, source_env))
        if ("aws.3top.com" in target_env):
            my_logger.info("Received \"aws.3top.com\" in %s"%target_env)
            mysql_target_cert = dict_target_ttSettings["mysql"]["certs"]["public"]
            my_logger.debug("Calling validate_cert_loc() for mysql_target_cert: %s" %(mysql_target_cert))
            try:
                validate_cert_loc(mysql_target_cert)
            except:
                my_logger.error("Error encountered: %s. \"Calling validate_cert_loc() with mysql_target_cert: %s\"...failed. Exiting!"%(str(sys.exc_info()),mysql_target_cert))
                sys.exit("Error encountered: %s. \"Calling validate_cert_loc() with mysql_target_cert: %s\"...failed. Exiting!"%(str(sys.exc_info()),mysql_target_cert))
            my_logger.info("validate_cert_loc() with mysql_target_cert: %s successful for %s!"%(mysql_target_cert, target_env))
    
    """ validate AWS MYSQL CERT location...stop"""
    """****************************************************************************************************************"""
    """ validate AWS CERT location...start"""
    
    """ SOURCE_ENV"""
    if ("aws.3top.com" in source_env):
        aws_source_cert = os.path.expanduser(dict_source_ttSettings["ec2"]["cert_private_key"])
        my_logger.info("Received \"aws.3top.com\" in %s"%source_env)
        my_logger.info("Calling validate_cert_loc() for aws_source_cert: %s"%(aws_source_cert))
        try:
            validate_cert_loc(aws_source_cert)
        except:
            my_logger.error("Error encountered: %s. \"validate_cert_loc() with cert_name: %s\"...failed. Exiting!"%(str(sys.exc_info()),aws_source_cert))
            sys.exit("Error encountered: %s. \"validate_cert_loc() with cert_name: %s\"...failed. Exiting!"%(str(sys.exc_info()),aws_source_cert))
        my_logger.info("validate_cert_loc() with cert_name: %s successful for %s!"%(aws_source_cert, source_env))
    elif("aws.3top.com" not in source_env):
        my_logger.error("\"aws.3top.com\" not in %s. Exiting!"%source_env)
        sys.exit("\"aws.3top.com\" not in %s. Exiting!"%source_env)
    
    """ TARGET_ENV"""   
    if ("aws.3top.com" in target_env):
        #print("Calling \"validate_cert_loc()\"...")
        aws_target_cert = os.path.expanduser(dict_target_ttSettings["ec2"]["cert_private_key"])
        my_logger.info("Received \"aws.3top.com\" in %s"%target_env)
        my_logger.info("Calling validate_cert_loc() for aws_cert_name: %s"%(aws_target_cert))
        try:
            validate_cert_loc(aws_target_cert)
        except:
            my_logger.error("Error encountered: %s. \"validate_cert_loc() with cert_name: %s\"...failed. Exiting!"%(str(sys.exc_info()),aws_target_cert))
            sys.exit("Error encountered: %s. \"validate_cert_loc() with cert_name: %s\"...failed. Exiting!"%(str(sys.exc_info()),aws_target_cert))
        my_logger.info("validate_cert_loc() with cert_name: %s successful for %s!"%(aws_target_cert, target_env))
    elif ("nyc.3top.com" in target_env):
        my_logger.info("Received \"nyc.3top.com\" in %s"%target_env)
    
       
    
    """ calling validate_cert_loc()...stop"""
    """****************************************************************************************************************"""
    """ calling set_host()...start"""
    
    if db_service == 'mysql':
        my_logger.info("Setting hosts for db_service %s"%db_service)
        my_logger.info("Calling \"(mysql_source_host, mysql_target_host) = set_mysql_host(%s, %s, %s)..."%(db_service, source_env,target_env))
        try:
            (mysql_source_host, mysql_target_host) = set_mysql_host(db_service, source_env,target_env)
        except:
            my_logger.error("Error encountered: %s. \"(mysql_source_host, mysql_target_host) = set_mysql_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service, source_env,target_env))
            sys.exit("Error encountered: %s. \"(mysql_source_host, mysql_target_host) = set_mysql_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service, source_env,target_env))
        my_logger.info("\"(mysql_source_host, mysql_target_host) = set_mysql_host(%s, %s, %s)...successful!"%(db_service, source_env,target_env))
    
    elif db_service == 'mongodb':
        my_logger.info("Setting hosts for db_service %s"%db_service)
        my_logger.info("Calling \"mongodb_source_host, mongodb_target_host)=set_mongodb_host(%s, %s, %s)..."%(db_service, source_env,target_env))
        try:
            (mongodb_source_host, mongodb_target_host)=set_mongodb_host(db_service, source_env, target_env)
        except:
            my_logger.error("Error encountered: %s. \"mongodb_source_host, mongodb_target_host)=set_mongodb_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service, source_env,target_env))
            sys.exit("Error encountered: %s. \"mongodb_source_host, mongodb_target_host)=set_mongodb_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service, source_env,target_env))
        my_logger.info("\"mongodb_source_host, mongodb_target_host)=set_mongodb_host(%s, %s, %s)...successful!"%(db_service, source_env,target_env))
    
    elif db_service == 'neo4j':
        my_logger.info("Setting hosts for db_service %s"%db_service)
        my_logger.info("Calling \"(neo4j_source_host, neo4j_target_host)=set_neo4j_host(%s, %s, %s)..."%(db_service, source_env,target_env))
        try:
            (neo4j_source_host, neo4j_target_host)=set_neo4j_host(db_service, source_env, target_env)
        except:
            my_logger.error("Error encountered: %s. \"neo4j_source_host, neo4j_target_host)=set_neo4j_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service, source_env,target_env))
            sys.exit("Error encountered: %s. \"neo4j_source_host, neo4j_target_host)=set_neo4j_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service, source_env,target_env))
        my_logger.info("\"neo4j_source_host, neo4j_target_host)=set_neo4j_host(%s, %s, %s)...successful!"%(db_service, source_env,target_env))
    
    elif db_service == 'all':
        
        """MYSQL"""
        db_service1 = "mysql"
        my_logger.info("Setting hosts for db_service %s"%db_service1)
        my_logger.info("Calling \"(mysql_source_host, mysql_target_host) = set_mysql_host(%s, %s, %s)..."%(db_service1, source_env,target_env))
        try:
            (mysql_source_host, mysql_target_host) = set_mysql_host(db_service1, source_env,target_env)
        except:
            my_logger.error("Error encountered: %s. \"(mysql_source_host, mysql_target_host) = set_mysql_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service1, source_env,target_env))
            sys.exit("Error encountered: %s. \"(mysql_source_host, mysql_target_host) = set_mysql_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service1, source_env,target_env))
        my_logger.info("\"(mysql_source_host, mysql_target_host) = set_mysql_host(%s, %s, %s)...successful!"%(db_service1, source_env,target_env))
        
        """MONGODB"""
        db_service2 = "mongodb"
        my_logger.info("Setting hosts for db_service %s"%db_service2)
        my_logger.info("Calling \"mongodb_source_host, mongodb_target_host)=set_mongodb_host(%s, %s, %s)..."%(db_service2, source_env,target_env))
        try:
            (mongodb_source_host, mongodb_target_host)=set_mongodb_host(db_service2, source_env, target_env)
        except:
            my_logger.error("Error encountered: %s. \"mongodb_source_host, mongodb_target_host)=set_mongodb_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service2, source_env,target_env))
            sys.exit("Error encountered: %s. \"mongodb_source_host, mongodb_target_host)=set_mongodb_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service2, source_env,target_env))
        my_logger.info("\"mongodb_source_host, mongodb_target_host)=set_mongodb_host(%s, %s, %s)...successful!"%(db_service2, source_env,target_env))
        
        """NEO4J"""
        db_service3 = "neo4j"
        my_logger.info("Setting hosts for db_service %s"%db_service3)
        my_logger.info("Calling \"(neo4j_source_host, neo4j_target_host)=set_neo4j_host(%s, %s, %s)..."%(db_service3, source_env,target_env))
        try:
            (neo4j_source_host, neo4j_target_host)=set_neo4j_host(db_service3, source_env, target_env)
        except:
            my_logger.error("Error encountered: %s. \"neo4j_source_host, neo4j_target_host)=set_neo4j_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service3, source_env,target_env))
            sys.exit("Error encountered: %s. \"neo4j_source_host, neo4j_target_host)=set_neo4j_host(%s, %s, %s)...failed. Exiting!"%(str(sys.exc_info()),db_service3, source_env,target_env))
        my_logger.info("\"neo4j_source_host, neo4j_target_host)=set_neo4j_host(%s, %s, %s)...successful!"%(db_service3, source_env,target_env))
        
    """ calling set_host()...stop"""
    """****************************************************************************************************************"""
    """ Validate DB Accounts... start"""
    
    """ Validate Source Accounts... start"""
    print("Validating source host..."),
    if db_service == 'mysql':
        my_logger.info("Validating source host for db_service %s"%db_service)
        my_logger.info("Calling \"validate_mysql_source(dict_source_ttSettings, %s)..."%(mysql_source_host))
        try:
            validate_mysql_source(dict_source_ttSettings, mysql_source_host)
        except:
            my_logger.error("Error encountered: %s. \"validate_mysql_source(dict_source_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_source_host))
            sys.exit("Error encountered: %s. \"validate_mysql_source(dict_source_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_source_host))
        my_logger.info("\"validate_mysql_source(dict_source_ttSettings, %s)...successful!"%(mysql_source_host))
        
    elif db_service == 'mongodb':
        my_logger.info("Validating source host for db_service %s"%db_service)
        my_logger.info("Calling \"validate_mongodb_source_accounts(%s, dict_source_ttSettings)..."%(mongodb_source_host))
        try:
            validate_mongodb_source_accounts(mongodb_source_host, dict_source_ttSettings)
        except:
            my_logger.error("Error encountered: %s. \"validate_mongodb_source_accounts(%s, dict_source_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),mongodb_source_host))
            sys.exit("Error encountered: %s. \"validate_mongodb_source_accounts(%s, dict_source_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),mongodb_source_host))
        my_logger.info("\"validate_mongodb_source_accounts(%s, dict_source_ttSettings)...successful!"%(mongodb_source_host))
        
    elif db_service == 'neo4j':
        my_logger.info("Validating source host for db_service %s"%db_service)
        my_logger.info("Calling \"validate_neo4j_aws_accounts(%s, dict_source_ttSettings)..."%(neo4j_source_host))
        try:
            validate_neo4j_aws_accounts(neo4j_source_host, dict_source_ttSettings)
        except:
            my_logger.error("Error encountered: %s. \"validate_neo4j_aws_accounts(%s, dict_source_ttSettings))...failed. Exiting!"%(str(sys.exc_info()),neo4j_source_host))
            sys.exit("Error encountered: %s. \"validate_neo4j_aws_accounts(%s, dict_source_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),neo4j_source_host))
        my_logger.info("\"validate_neo4j_aws_accounts(%s, dict_source_ttSettings)...successful!"%(neo4j_source_host))
        
    elif db_service == 'all':
        db_service1 = 'mysql'
        my_logger.info("Validating source host for db_service %s"%db_service1)
        my_logger.info("Calling \"validate_mysql_source(dict_source_ttSettings, %s)..."%(mysql_source_host))
        try:
            validate_mysql_source(dict_source_ttSettings, mysql_source_host)
        except:
            my_logger.error("Error encountered: %s. \"validate_mysql_source(dict_source_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_source_host))
            sys.exit("Error encountered: %s. \"validate_mysql_source(dict_source_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_source_host))
        my_logger.info("\"validate_mysql_source(dict_source_ttSettings, %s)...successful!"%(mysql_source_host))
        
        db_service2 = 'mongodb'
        my_logger.info("Validating source host for db_service %s"%db_service2)
        my_logger.info("Calling \"validate_mongodb_source_accounts(%s, dict_source_ttSettings)..."%(mongodb_source_host))
        try:
            validate_mongodb_source_accounts(mongodb_source_host, dict_source_ttSettings)
        except:
            my_logger.error("Error encountered: %s. \"validate_mongodb_source_accounts(%s, dict_source_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),mongodb_source_host))
            sys.exit("Error encountered: %s. \"validate_mongodb_source_accounts(%s, dict_source_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),mongodb_source_host))
        my_logger.info("\"validate_mongodb_source_accounts(%s, dict_source_ttSettings)...successful!"%(mongodb_source_host))
        
        db_service2 = 'neo4j'
        my_logger.info("Validating source host for db_service %s"%db_service3)
        my_logger.info("Calling \"validate_neo4j_aws_accounts(%s, dict_source_ttSettings)..."%(neo4j_source_host))
        try:
            validate_neo4j_aws_accounts(neo4j_source_host, dict_source_ttSettings)
        except:
            my_logger.error("Error encountered: %s. \"validate_neo4j_aws_accounts(%s, dict_source_ttSettings))...failed. Exiting!"%(str(sys.exc_info()),neo4j_source_host))
            sys.exit("Error encountered: %s. \"validate_neo4j_aws_accounts(%s_host, dict_source_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),neo4j_source_host))
        my_logger.info("\"validate_neo4j_aws_accounts(%s, dict_source_ttSettings)...successful!"%(neo4j_source_host))
    print "Complete!"
    """ Validate Source Accounts... stop"""
    
    """ Validate Target Accounts... start"""
    #print("Validating target host..."),
    if db_service == 'mysql':
        if "aws.3top.com" in mysql_target_host:
            my_logger.info("Found \"aws.3top.com\" in target host")
            my_logger.info("Validating target host for %s"%(db_service))
            my_logger.info("Calling \"validate_mysql_aws_accounts(dict_target_ttSettings, %s)..."%(mysql_target_host))
            try:
                validate_mysql_aws_accounts(dict_target_ttSettings, mysql_target_host)
            except:
                my_logger.error("Error encountered: %s. \"validate_mysql_aws_accounts(dict_target_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_target_host))
                sys.exit("Error encountered: %s. \"validate_mysql_aws_accounts(dict_target_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_target_host))
            my_logger.info("\"validate_mysql_aws_accounts(dict_target_ttSettings, %s)...successful!"%(mysql_target_host))
        
        elif "nyc.3top.com" in mysql_target_host:
            my_logger.info("Found \"nyc.3top.com\" in target host")
            my_logger.info("Validating target host for db_service %s"%db_service)
            my_logger.info("Calling \"validate_mysql_nyc_accounts(dict_target_ttSettings, %s)..."%(mysql_target_host))
            try:
                validate_mysql_nyc_accounts(dict_target_ttSettings, mysql_target_host)
            except:
                my_logger.error("Error encountered: %s. \"validate_mysql_nyc_accounts(dict_target_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_target_host))
                sys.exit("Error encountered: %s. \"validate_mysql_nyc_accounts(dict_target_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_target_host))
            my_logger.info("\"validate_mysql_nyc_accounts(dict_target_ttSettings, %s)...successful!"%(mysql_target_host))
        
    elif db_service == 'mongodb':
        my_logger.info("Validating source host for db_service %s"%db_service)
        my_logger.info("Calling \"validate_mongodb_target_accounts(%s, dict_target_ttSettings)..."%(mongodb_target_host))
        try:
            validate_mongodb_target_accounts(mongodb_target_host, dict_target_ttSettings)
        except:
            my_logger.error("Error encountered: %s. \"validate_mongodb_target_accounts(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),mongodb_target_host))
            sys.exit("Error encountered: %s. \"validate_mongodb_target_accounts(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),mongodb_target_host))
        my_logger.info("\"validate_mongodb_target_accounts(%s, dict_target_ttSettings)...successful!"%(mongodb_target_host))
        
    elif db_service == 'neo4j':
        if "nyc.3top.com" in neo4j_target_host:
            my_logger.info("Found \"nyc.3top.com\" in target host")
            my_logger.info("calling \"validate_neo4j_nyc_target(%s, dict_target_ttSettings)\"..."%(neo4j_target_host))
            try:
                validate_neo4j_nyc_target(neo4j_target_host, dict_target_ttSettings)
            except:
                my_logger.error("Error encountered: %s. \"validate_neo4j_nyc_target(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),neo4j_target_host))
                sys.exit("Error encountered: %s. \"validate_neo4j_nyc_target(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),neo4j_target_host))
            my_logger.info("\"validate_neo4j_nyc_target(%s, dict_target_ttSettings)\"...successful!")
        
        elif "aws.3top.com" in neo4j_target_host:
            my_logger.info("Found \"aws.3top.com\" in target host")
            my_logger.info("calling \"validate_neo4j_aws_accounts(%s, dict_target_ttSettings)\"..."%(neo4j_target_host))
            try:
                validate_neo4j_aws_accounts(neo4j_target_host, dict_target_ttSettings)
            except:
                my_logger.error("Error encountered: %s. \"validate_neo4j_aws_accounts(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),neo4j_target_host))
                sys.exit("Error encountered: %s. \"validate_neo4j_aws_accounts(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),neo4j_target_host))
            my_logger.info("\"validate_neo4j_aws_accounts(%s, dict_target_ttSettings)\"...successful!")

    elif db_service == 'all':
        if "aws.3top.com" in mysql_target_host:
            my_logger.info("Found \"aws.3top.com\" in target host")
            my_logger.info("Validating target host for %s"%(mysql_target_host))
            my_logger.info("Calling \"validate_mysql_aws_accounts(dict_target_ttSettings, %s)..."%(mysql_target_host))
            try:
                validate_mysql_aws_accounts(dict_target_ttSettings, mysql_target_host)
            except:
                my_logger.error("Error encountered: %s. \"validate_mysql_aws_accounts(dict_target_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_target_host))
                sys.exit("Error encountered: %s. \"validate_mysql_aws_accounts(dict_target_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_target_host))
            my_logger.info("\"validate_mysql_aws_accounts(dict_target_ttSettings, %s)...successful!"%(mysql_target_host))
        
        elif "nyc.3top.com" in mysql_target_host:
            my_logger.info("Found \"nyc.3top.com\" in target host")
            my_logger.info("Validating target host for db_service %s"%db_service)
            my_logger.info("Calling \"validate_mysql_nyc_accounts(dict_target_ttSettings, %s)..."%(mysql_target_host))
            try:
                validate_mysql_nyc_accounts(dict_target_ttSettings, mysql_target_host)
            except:
                my_logger.error("Error encountered: %s. \"validate_mysql_nyc_accounts(dict_target_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_target_host))
                sys.exit("Error encountered: %s. \"validate_mysql_nyc_accounts(dict_target_ttSettings, %s)...failed. Exiting!"%(str(sys.exc_info()),mysql_target_host))
            my_logger.info("\"validate_mysql_nyc_accounts(dict_target_ttSettings, %s)...successful!"%(mysql_target_host))
        
    
        my_logger.info("Validating source host for db_service %s"%db_service)
        my_logger.info("Calling \"validate_mongodb_target_accounts(%s, dict_target_ttSettings)..."%(mongodb_target_host))
        try:
            validate_mongodb_target_accounts(mongodb_target_host, dict_target_ttSettings)
        except:
            my_logger.error("Error encountered: %s. \"validate_mongodb_target_accounts(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),mongodb_target_host))
            sys.exit("Error encountered: %s. \"validate_mongodb_target_accounts(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),mongodb_target_host))
        my_logger.info("\"validate_mongodb_target_accounts(%s, dict_target_ttSettings)...successful!"%(mongodb_target_host))
        
   
        if "nyc.3top.com" in neo4j_target_host:
            my_logger.info("Found \"nyc.3top.com\" in target host")
            my_logger.info("calling \"validate_neo4j_nyc_target(%s, dict_target_ttSettings)\"..."%(neo4j_target_host))
            try:
                validate_neo4j_nyc_target(neo4j_target_host, dict_target_ttSettings)
            except:
                my_logger.error("Error encountered: %s. \"validate_neo4j_nyc_target(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),neo4j_target_host))
                sys.exit("Error encountered: %s. \"validate_neo4j_nyc_target(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),neo4j_target_host))
            my_logger.info("\"validate_neo4j_nyc_target(%s, dict_target_ttSettings)\"...successful!")
        
        elif "aws.3top.com" in neo4j_target_host:
            my_logger.info("Found \"aws.3top.com\" in target host")
            my_logger.info("calling \"validate_neo4j_aws_accounts(%s, dict_target_ttSettings)\"..."%(neo4j_target_host))
            try:
                validate_neo4j_aws_accounts(neo4j_target_host, dict_target_ttSettings)
            except:
                my_logger.error("Error encountered: %s. \"validate_neo4j_aws_accounts(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),neo4j_target_host))
                sys.exit("Error encountered: %s. \"validate_neo4j_aws_accounts(%s, dict_target_ttSettings)...failed. Exiting!"%(str(sys.exc_info()),neo4j_target_host))
            my_logger.info("\"validate_neo4j_aws_accounts(%s, dict_target_ttSettings)\"...successful!")
            
    """ Validate Target Accounts... stop"""
    
    """ Validate DB Accounts... stop"""    
    """****************************************************************************************************************"""
    """ Find current working directory path: python_root...start"""
    
    global python_root
    my_logger.debug("Finding real path (working directory)...")
    python_root = os.path.dirname(os.path.realpath(__file__)) + "/"
    my_logger.debug("python_root:%s"%python_root)
    #print "found."
    
    """ Find current working directory path: python_root...stop"""
    """********************************************************************************************************************"""
    """ Executing db_operation"""
    #Region = dict_ttSettings["aws"]["REGION_NAME"]
    if db_service == 'mysql':
        print "Executing mysql_operations..."
        my_logger.debug("calling mysql_operations() with source: %s, target: %s..."%(mysql_source_host, mysql_target_host))
        mysql_operations(db_operation, db_service, dict_source_ttSettings, dict_target_ttSettings, mysql_source_host, mysql_target_host, my_logger,log_file_abs_path)
        my_logger.info("MySQL operations completed successfully")
    elif db_service== "mongodb":
        print "Executing mongo_operations..."
        my_logger.debug("calling mongo_operations() with source: %s, target: %s..."%(mongodb_source_host, mongodb_target_host))
        mongo_operations(db_operation, db_service, dict_source_ttSettings, dict_target_ttSettings, mongodb_source_host, mongodb_target_host, my_logger,log_file_abs_path)
        my_logger.info("MongoDB operations completed successfully")
    elif db_service == 'neo4j':
        print "Executing neo4j_operations..."
        my_logger.debug("calling neo4j_operations() with source: %ss , target: %s..."%(neo4j_source_host, neo4j_target_host))
        neo4j_operations(dict_source_ttSettings, dict_target_ttSettings)
        my_logger.info("Neo4j operations completed successfully")
    elif db_service == 'all':
        print "Executing mysql_operations..."
        my_logger.debug("calling mysql_operations() with source: %s, target: %s..."%(mysql_source_host, mysql_target_host))
        mysql_operations(db_operation, db_service, dict_source_ttSettings, dict_target_ttSettings, mysql_source_host, mysql_target_host, my_logger,log_file_abs_path)
        my_logger.info("MySQL operations completed successfully")
        print "Executing mongo_operations..."
        my_logger.debug("calling mongo_operations() with source: %s, target: %s..."%(mongodb_source_host, mongodb_target_host))
        mongo_operations(db_operation, db_service, dict_source_ttSettings, dict_target_ttSettings, mongodb_source_host, mongodb_target_host, my_logger,log_file_abs_path)
        my_logger.info("MongoDB operations completed successfully")
        print "Executing neo4j_operations..."
        my_logger.debug("calling neo4j_operations() with source: %ss , target: %s..."%(neo4j_source_host, neo4j_target_host))
        neo4j_operations(dict_source_ttSettings, dict_target_ttSettings)
        my_logger.info("Neo4j operations completed successfully")
    """main() Module definition... start"""
    """main() Module definition... stop"""
    
    my_logger.info("Program execution complete. Exiting!")
    print "Program execution complete. Exiting!\nLogfile location: %s" %log_file_abs_path
   

"""main() Module definition... stop"""

print "Initializing program...calling main()"
main()


