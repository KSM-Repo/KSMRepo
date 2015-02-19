import sys
import os
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name'])
from ttLx import callSubprocess
global swapTimeStampDirPath
from ttLib.ttAWS import findSourceSuffix

"""********************************************************************************************************************"""
"""mongo_operations()... start"""

def mongodb_operations(operation, service, dict_source_ttSettings, dict_target_ttSettings, s_host, t_host, my_logger, log_file):
    my_logger.info("In mongo_operations...")
    my_logger.info("Taking backup before update. Calling mongodump_source()...")
    mongodb_dump("beforeUpdate-source", dict_source_ttSettings, s_host, my_logger, log_file)
    print"Source backup before update successful for source..."
    my_logger.info("Back in mongo_operations. Backup before update successful for source.")
    mongodb_dump("beforeUpdate-target", dict_target_ttSettings, t_host, my_logger, log_file)
    print"Target backup before update successful for source..."
    my_logger.info("Back in mongo_operations. Backup before update successful for target.")
    my_logger.info("Starting clone process. Calling mongoclone()...")
    mongoclone(s_host, t_host, dict_source_ttSettings, dict_target_ttSettings, my_logger, log_file)
    print "Mongo clone successful..."
    mongodump_source("afterUpdate-source", dict_source_ttSettings, s_host, my_logger, log_file)
    print"Source backup after update successful for source..."
    my_logger.info("Back in mongo_operations. Backup after update successful for source.")
    mongodb_dump("afterUpdate-target", dict_target_ttSettings, t_host, my_logger, log_file)
    print"Target backup after update successful for source..."
    my_logger.info("Back in mongo_operations. Backup after update successful for target.")
    
"""mongo_operations()... start"""
"""********************************************************************************************************************"""

"""********************************************************************************************************************"""
"""mongodump_source()... start"""

def mongodump_source(suffix, dict_source_ttSettings, s_host, my_logger, log_file):
    my_logger.info("In mongodump_source()...")
    try: 
        filename="mongodb-" + suffix
        filepath=os.path.join(swapTimeStampDirPath,filename)
        my_logger.debug("Create file success! %s " % filepath)
        dump_cmd="mongodump --host " + s_host + " --port 27017 --username "+ dict_source_ttSettings["mongodb"]["users"]["rw"]["username"] + " --password " \
        + dict_source_ttSettings["mongodb"]["users"]["rw"]["password"] + " --db apis_cache --out " + filepath
        """
         mongodump --host mongodb.b.prod.aws.3top.com:27017 --username superuser1 --password HnFBg67tZJ0NZvPC33YvM2hy5H1Sh48mwEe --out mongodump/
        """
        dump_OP=callSubprocess(dump_cmd)
    except:
        my_logger.error("Error encountered in mongodump_source(). Exiting! ")
        sys.exit("Error encountered in mongodump_source(). Exiting!")
    my_logger.info("Mongodump process completed successfully!")
     
"""mongodump_source()... stop"""
"""********************************************************************************************************************"""

"""********************************************************************************************************************"""
"""mongodump_target()... start"""

def mongodb_dump(suffix, dict_ttSettings, host, my_logger, log_file, backup_dir):
    my_logger.debug("In mongodump()...")
    try:
        if ('source' in suffix) or ('target' in suffix):
            filename="mongodb-" + suffix 
            filepath=os.path.join(swapTimeStampDirPath,filename)
        elif 'backup' in suffix:
            filepath=os.path.join(backup_dir)
            
        my_logger.debug("Create file success! %s " % filepath)
        dump_cmd="mongodump --host " + host + " --port 27017 --username "+ dict_ttSettings["mongodb"]["users"]["dba"]["username"] + " --password " \
        + dict_ttSettings["mongodb"]["users"]["dba"]["password"] + " --out " + filepath
        """
         mongodump --host mongodb.b.prod.aws.3top.com:27017 --username superuser1 --password HnFBg67tZJ0NZvPC33YvM2hy5H1Sh48mwEe --out mongodump/
        """
        dump_OP=callSubprocess(dump_cmd)
    except:
        my_logger.error("Error encountered in mongodump_target(). Exiting! ")
        sys.exit("Error encountered in mongodump_target(). Exiting! ")
    my_logger.info("Mongodump() process completed successfully!")
 
"""mongodump_target()... stop"""
"""********************************************************************************************************************"""
"""mongodb_backup()... start"""

def mongodb_backup(host, dict_ttSettings, my_logger,log_file, backup_dir):
    my_logger.info("In mongodb_backup()...")
    my_logger.info("Taking backup for host: %s Calling mongodump_source()..."%host)
    mongodb_dump("backup", dict_ttSettings, host, my_logger, log_file, backup_dir)
    print"Taking backup for host: %s successful!"%host
    my_logger.info("Back in mongodb_backup(). Backup successful for host: %s"%host)
    
"""mongodb_backup()...start"""
"""********************************************************************************************************************"""
"""mongoclone()... start"""

def mongoclone(s_host, t_host, dict_source_ttSettings, dict_target_ttSettings, my_logger,log_file):
    my_logger.debug(" In mongoclone()...")
    try:
        filename="mongoclone.js"
        filepath=os.path.join(swapTimeStampDirPath,filename)
        my_logger.debug("Create file success! %s " % filename)
        f=open(filepath, "wb")
        f.write("printjson(db = db.getSiblingDB('apis_cache'));")
        f.write("\nprintjson(db.getCollectionNames());")
        f.write("\nprintjson(db.dropDatabase());")
        f.write("\nprintjson(db.copyDatabase (\"apis_cache\", \"apis_cache\", \"%s\", \"%s\", \"%s\"));"\
                %(s_host, dict_source_ttSettings["mongodb"]["users"]["rw"]["username"],dict_source_ttSettings["mongodb"]["users"]["rw"]["password"]))
        f.write("\nprintjson(db.getCollectionNames());")
        f.close()
        clone_cmd="mongo admin -u " + dict_target_ttSettings["mongodb"]["users"]["dba"]["username"] + " -p " + dict_target_ttSettings["mongodb"]["users"]["dba"]["password"] \
        + " --host " + t_host + " --verbose --port 27017 " + filepath
        print clone_cmd
        clone_OP=callSubprocess(clone_cmd)
    except:
        my_logger.error("Error encountered in mongoclone(). Exiting! ")
        sys.exit("Error encountered in mongoclone(). Exiting! ")
    my_logger.info("Mongoclone() process completed successfully!")
    
    
"""mongoclone()...start"""
"""********************************************************************************************************************"""
"""mongo_restore()... start"""

def mongo_restore(t_host, dict_target_ttSettings, my_logger,log_file, restore_dir):
    l_host = "skanupuru-lxu1.nyc.3top.com"
    my_logger.debug(" In mongoclone()...")
    try:
        filename="apis_cache"
        filepath=os.path.join(restore_dir, filename)
        #my_logger.debug("Create file success! %s " % filename)
        #f=open(filepath, "wb")
        #f.write("printjson(db = db.getSiblingDB('apis_cache'));")
        #f.write("\nprintjson(db.getCollectionNames());")
        #f.write("\nprintjson(db.dropDatabase());")
        #f.write("\nprintjson(db.copyDatabase (\"%s\", \"apis_cache\"));"\
        #       %(filepath+"/apis_cache"))
        #f.write("\nprintjson(db.getCollectionNames());")
        #f.close()
        clone_cmd="mongorestore --username " + dict_target_ttSettings["mongodb"]["users"]["dba"]["username"] + " -p " + dict_target_ttSettings["mongodb"]["users"]["dba"]["password"] \
        + " --host " + t_host + " --verbose --port 27017 " + filepath
        
        print clone_cmd
        clone_OP2=callSubprocess(clone_cmd)
        
    except:
        my_logger.error("Error encountered in mongoclone(). Exiting! ")
        sys.exit("Error encountered in mongoclone(). Exiting! ")
    my_logger.info("Mongoclone() process completed successfully!")
    
    
"""mongo_restore()...stop"""
"""********************************************************************************************************************"""
"""validate_mongodb_accounts()... start"""

def validate_mongodb_accounts(dict_ttSettings, hostname):
    my_logger.debug("validating mongodb_accounts()...")
    for k, v in dict_ttSettings["mongodb"]["users"].items():
        my_logger.debug("Validating %s account for host: %s"%(k, hostname))
        try:
            print("Mongodb account: %s for host: %s"%(k,hostname)),
            conn_cmd="mongo " + hostname + ":27017/" + v["database"] + " -u " + v["username"] + " -p " \
            + v["password"] + " --eval \"printjson(db.getCollectionNames());\""
            #print "Running command: %s"%conn_cmd,
            conn_OP=callSubprocess(conn_cmd)
            my_logger.debug("Received output: %s"%conn_OP)
            my_logger.debug("Account: %s for host: %s...validated"%(k, hostname))
            print "...Validated!"
        except:
            my_logger.error("Validating mongodb_accounts failed. Exiting!")
            sys.exit("Validating mongodb_accounts failed. Exiting!")
    my_logger.debug("Accounts validation successful for service: MongoDB !")
        
"""validate_mongodb_accounts()... stop"""
"""********************************************************************************************************************"""
""" Set env based on the arguments... start"""

def set_env(operation, t_env):
    my_logger.debug("Calling findSourceSuffix()...")
    try:
        s_env = findSourceSuffix()
        my_logger.debug("Received current active Environment (source environment): %s"%s_env)
        
        if operation == 'swap':
            if s_env == "a.prod.aws.3top.com":
                t_env = "b.prod.aws.3top.com"
            else:
                t_env="a.prod.aws.3top.com"
        elif operation == 'copy':
            print("Target Environment set as: %s"%(t_env))
            
    except:
        my_logger.error("Set environment failed. Exiting!")
        sys.exit("Set environment failed. Exiting!")
    my_logger.debug("Set environment process completed successfully. Returning to main() with source environment: %s and target_environment: %s"%(s_env, t_env))
    return s_env, t_env

""" Set env based on the arguments... stop"""
"""********************************************************************************************************************"""
""" set host based on env ... start"""

def set_mongodb_host(service, s_env, t_env):
    my_logger.debug(" Setting mongodb_host()...")
    try:
        if (service == "mongodb") or (service=='all'):
            mongo_s_host= 'mongodb.' + s_env
            if ("aws.3top.com" in t_env):
                mongo_t_host='mongodb.' + t_env
            elif "nyc.3top.com" in t_env:
                mongo_t_host= t_env
    except:
        my_logger.error("Setting mongodb_host() failed. Exiting!")
        sys.exit("Setting mongodb_host() failed. Exiting!")
    my_logger.debug("Setting mongodb_host() successful. returning to main() with source_host: %s and target_host: %s"%(mongo_s_host, mongo_t_host))
    return mongo_s_host, mongo_t_host

""" set host based on env ... stop """
"""********************************************************************************************************************"""
