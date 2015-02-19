import sys
import os
import re
import platform
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name'])
from ttAWS import findActiveEnv


"""********************************************************************************************************************"""
"""validate_args()...START"""

def validate_args(operation, service, t_env):
    my_logger.debug("Validating arguments passed to dbUtil...")
    try:
        """ Validate database operation and target environment arguments start"""
        my_logger.debug("Validating db_operation and target_environment...")
        if operation=="":
            my_logger.error("No db_operation argument found. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]")
            sys.exit("No db_operation argument found. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]")
        elif operation not in ['swap', 'copy']:
            my_logger.error("db_operation argument invalid. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]")
            sys.exit("db_operation argument invalid. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]")
        elif (operation=='swap') and (t_env != " "):
            my_logger.error("Incorrect format. target_env not required for swap operation. \nUse: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only)]")
            sys.exit("Incorrect format. target_env not required for swap operation. \nUse: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only)]")
        elif (operation=='copy') and (t_env == " "):
            my_logger.error("Incorrect format. target_env not found. Exiting! \nUse: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only)]")
            sys.exit("Incorrect format. target_env not found. Exiting! \nUse: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only)]")
        elif operation == 'copy':
            my_logger.debug("db_operation valid!  Received db_operation: %s. Continuing..."%operation)
            
            """ Validate target_env format start  """
            
            if "aws.3top.com" in t_env:
                if re.match('^[abc]\.(dev)(\.(aws)\.(3top)\.(com))$', t_env):
                    my_logger.debug("target_env valid! Received target_env: %s. Continuing..."%t_env)
                    
                else:
                    my_logger.error("Target_env format mismatch. Exiting! \nUse format: [a/b].(prod/dev).(nyc/aws).(3top).(com)")
                    sys.exit("Target_env format mismatch. Exiting! \nUse format: [a/b].(prod/dev).(nyc/aws).(3top).(com)")
            elif "nyc.3top.com" in t_env:
                my_logger.debug("target_env valid! Received target_env: %s. Continuing..."%t_env)
                
            else:
                my_logger.error("Target host format mismatch. Exiting! \nUse format: [a/b].(prod/dev).(nyc/aws).(3top).(com)")
                sys.exit("Target host format mismatch. Exiting! \nUse format: [a/b].(prod/dev).(nyc/aws).(3top).(com)")
            
            """ Validate target_env format stop  """
        
        """ Validate database operation and target environment arguments stop"""
        
        """ Validate database service argument... start"""
    
        my_logger.debug("Validating db_service...")
        if service=="":
            my_logger.error("No db_service arguments found. Exiting! \nUse format: db_util.py [ swap |copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]")
            sys.exit("No db_service arguments found. Exiting! \nUse format: db_util.py [ swap | copy ][ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]")
        elif service not in ['mysql', 'mongodb', 'neo4j', 'virtuoso', 'all']:
            my_logger.error("Incorrect Database chosen: %s. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]" %service)
            sys.exit("Incorrect Database chosen: %s. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]" %service)
        else:
            my_logger.debug("db_service validated!")
            
    
        """ Validate database service argument stop"""
                        
    except:
        my_logger.error("Error encountered in validate_args. Exiting!")
        sys.exit("Error encountered in validate_args. Exiting!")
    #print("Validate arguments complete. Returning to main()...")
       
"""validate_args()...STOP"""
"""********************************************************************************************************************"""
"""set_params()...START"""

def set_params(param_list):
    my_logger.debug("Received arguments: %s"%param_list)
    
    ttdb_dict = dict()
    
    s_count, t_count, o_count, d_count, bdir_count, rdir_count, p_count =\
     0, 0, 0, 0, 0, 0, 0
    
    my_logger.debug("Extracting the parameters from command line arguments...")
        
    for i in range(0, len(param_list)-1):
        if (param_list[i] == "-s"):
            s_count = s_count + 1
            source_loc = i + 1
        elif (param_list[i] == "-t"):
            t_count = t_count + 1
            target_loc = i + 1
        elif (param_list[i] == "-o"):
            o_count = o_count + 1
            operation_loc = i + 1
        elif (param_list[i] == "-d"):
            d_count = d_count + 1
            service_loc = i + 1
        elif (param_list[i] == "-bdir"):
            bdir_count = bdir_count + 1
            bdir_loc = i + 1
        elif (param_list[i] == "-rdir"):
            rdir_count = rdir_count + 1
            rdir_loc = i + 1
        elif (param_list[i] == "-p"):
            p_count = p_count + 1
            p_loc = i + 1
            
    my_logger.debug("Received s_count: %s, t_count: %s, o_count: %s, p_count: %s"%(s_count, t_count, o_count, p_count))
    my_logger.debug("Received d_count: %s, bdir_count: %s, rdir_count: %s"%(d_count, bdir_count, rdir_count))
    
    
    if (s_count> 1) or (t_count > 1) or (o_count > 1 ) or (d_count > 1) or (bdir_count > 1) or (rdir_count > 1) or (p_count > 1):
        my_logger.error("Multiple flags of same type detected. Exiting!") 
        sys.exit("Multiple flags of same type detected. Exiting!")
    elif o_count == 0:
        my_logger.error("No db_operation found. Exiting!") 
        print("No db_operation found. Required format: python dbUtil.py -s <SOURCE_ENV> -t <TARGET_ENV> -o <DB_OPERATION> -d <DB_SERVICE> [optional: -p <PROD_OVERRIDE>] \n or python dbUtil.py -o swap -d <DB_SERVICE> [optional: -p <PROD_ENV>].")
        sys.exit("Please use correct format and run again. Exiting!")
    elif d_count == 0:
        my_logger.error("No db_service found. Exiting!") 
        print("No db_service found. Required format: python dbUtil.py -s <SOURCE_ENV> -t <TARGET_ENV> -o <DB_OPERATION> -d <DB_SERVICE> [optional: -p <PROD_OVERRIDE>] \n or python dbUtil.py -o swap -d <DB_SERVICE> [optional: -p <PROD_ENV>].")
        sys.exit("Please use correct format and run again. Exiting!")
        
        
    """*********************************************************************************************************************************
    FIND DB_OPERATION...START
    *********************************************************************************************************************************"""
    my_logger.debug("Calling operation_validate(%s, %s, %s)"%(o_count, param_list, operation_loc))    
    db_operation = db_operation_validate(o_count, param_list, operation_loc)
    my_logger.debug("operation_validate() call successful! Received db_operation: %s"%db_operation)
    my_logger.debug("Inserting db_operation into ttdb_dict...")
    ttdb_dict['db_operation'] = db_operation
    
    """*********************************************************************************************************************************
    FIND DB_OPERATION...STOP
    *********************************************************************************************************************************"""
    
    """*********************************************************************************************************************************
    VALIDATE NUMBER OF ARGUMENTS...START
    *********************************************************************************************************************************"""
    my_logger.debug("Calling arg_valid_bool = arg_count_validate(%s, %s)" %(len(param_list), db_operation))
    arg_valid_bool = arg_count_validate(len(param_list), db_operation)
    my_logger.debug("arg_count_validate() call successful. Received arg_valid_bool: %s"%arg_valid_bool)
    
    if arg_valid_bool:
        my_logger.debug("Argument count is valid. Continuing!")
    else:
        my_logger.error("Argument count is invalid. Exiting!")
        sys.exit("Argument count is invalid. Exiting!")
    
    """*********************************************************************************************************************************
    VALIDATE NUMBER OF ARGUMENTS...STOP
    *********************************************************************************************************************************"""
    
    """*********************************************************************************************************************************
    FIND ACTIVE ENVIRONMENT...START
    *********************************************************************************************************************************"""
    my_logger.debug("Determining active environment...")
    if p_count == 0:
        active_env = findActiveEnv()
        my_logger.debug("Active (production) environment determined as: %s"%active_env)
    elif p_count == 1:
        active_env = param_list[p_loc]
        my_logger.debug("Active (production) environment determined as: %s"%active_env)
    else:
        my_logger.error("Unexpected error encountered in setting active environment. Exiting!")
        sys.exit("Unexpected error encountered in setting active environment. Exiting!")
    
    """*********************************************************************************************************************************
    FIND ACTIVE ENVIRONMENT...STOP
    *********************************************************************************************************************************"""
    
    """*********************************************************************************************************************************
    FIND OS...START
    *********************************************************************************************************************************"""
    my_logger.debug("Determining OS Platform information...")
    platform_info = platform.platform()
    my_logger.debug("Received platform_info: %s"%platform_info)
    if "Windows" in platform_info:
        os_platform = 'Windows'
        my_logger.debug("Platform determined as %s"%os_platform)
    elif "Linux" in platform_info:
        os_platform = 'Linux'
        my_logger.debug("Platform determined as %s"%os_platform)
    else:
        my_logger.debug("Cannot determine platform. Exiting!")
        sys.exit("Cannot determine platform. Exiting!")
    
    """*********************************************************************************************************************************
    FIND OS...STOP
    *********************************************************************************************************************************"""
    
    """*********************************************************************************************************************************
    FIND DB_SERVICE...START
    *********************************************************************************************************************************"""
    my_logger.debug("Calling db_service_validate(%s, %s, %s, %s)" %(d_count, param_list, service_loc, os_platform))
    db_service = db_service_validate(d_count, param_list, service_loc, os_platform)
    my_logger.debug("Updating ttdb_dict with db_service")
    ttdb_dict['db_service'] = db_service
    
    """*********************************************************************************************************************************
    FIND DB_SERVICE...STOP
    *********************************************************************************************************************************"""
    
    """*********************************************************************************************************************************
    FIND SOURCE_ENV ...START
    *********************************************************************************************************************************"""
    """*******************
    SOURCE_ENV: COPY
    *******************"""
    if db_operation == 'copy':
        my_logger.debug("Setting source_env for db_operation: %s."%db_operation)
        if (bdir_count != 0) or (rdir_count != 0):
            my_logger.error("Directory arguments [bdir/rdir] found. Use format [python dbUtil.py -s <source_env> -t <target_env> -o copy -d <db_service>] for copy operation. Exiting!")
            sys.exit("Directory arguments [bdir/rdir] found. Use format [python dbUtil.py -s <source_env> -t <target_env> -o copy -d <db_service>] for copy operation. Exiting!")
        elif not ((bdir_count != 0) or (rdir_count != 0)):
            if s_count == 0:
                print("Received no source flag. Setting active (production) environment as source")
                my_logger.debug("Received no source flag. Setting active (production) environment as source")
                source_env = active_env.split("-")[1] + "." + active_env.split("-")[0] + ".aws.3top.com"
                my_logger.debug("source_env: %s"%source_env)
            elif s_count == 1: 
                my_logger.debug("Source flag found! Extracting source_env from list of arguments.")
                source_env = param_list[source_loc]
                my_logger.debug("source_env: %s"%source_env)
            else:
                my_logger.error("Unexpected error encountered in determining source_env. Exiting!")
                sys.exit("Unexpected error encountered in determining source_env. Exiting!")
            
            """*******************
            SOURCE_ENV: VALIDATE
            *******************"""
            source_env = env_validate(source_env)
            my_logger.info("The source_env has been validated!")
            
            if t_count == 0:
                my_logger.debug("Received no target flag! Exiting!")
                sys.exit("Received no target flag! Exiting!")
            elif t_count == 1: 
                my_logger.debug("Target flag found! Extracting target_env from list of arguments.")
                target_env = param_list[target_loc]
                my_logger.debug("target_env: %s"%target_env)
            else:
                my_logger.error("Unexpected error encountered in determining target_env. Exiting!")
                sys.exit("Unexpected error encountered in determining target_env. Exiting!")
            
            my_logger.info("Received target: %s"%target_env)
            
            """*******************
            TARGET_ENV: VALIDATE
            *******************"""
            target_env = env_validate(target_env)
            my_logger.info("The target_env has been validated!")
            
        else:
            my_logger.error("Unexpected error encountered in determining flags/ arguments for copy. Exiting!")
            sys.exit("Unexpected error encountered in determining flags/ arguments for copy. Exiting!")
        
        my_logger.info("Updating ttdb_dict with source: %s"%source_env)
        ttdb_dict['source_env'] = source_env
        
        my_logger.info("Updating ttdb_dict with target: %s"%target_env)
        ttdb_dict['target_env'] = target_env
        
    
    elif db_operation == 'swap':
        my_logger.debug("Setting source_env for db_operation: %s."%db_operation)
        if (s_count != 0) or (t_count != 0) or (bdir_count != 0) or (rdir_count != 0):
            my_logger.error("Improper format. Use [python dbUtil.py -o swap -d <db_service>] for swap operation. Exiting!")
            sys.exit("Improper format. Use [python dbUtil.py -o swap -d <db_service>] for swap operation. Exiting!")
        elif not ((s_count != 0) or (t_count != 0) or (bdir_count != 0) or (rdir_count != 0)):
            my_logger.debug("Setting active (production) environment as source")
            source_env = active_env.split("-")[1] + "." + active_env.split("-")[0] + ".aws.3top.com"
            my_logger.debug("source_env: %s"%source_env)
            if source_env == 'a.prod.aws.3top.com':
                target_env = 'b.prod.aws.3top.com'
            elif source_env == 'b.prod.aws.3top.com':
                target_env = 'a.prod.aws.3top.com'
            else:
                my_logger.error("Cannot recognize source_env. Received: %s. Exiting!"%source_env)
                sys.exit("Cannot recognize source_env. Received: %s. Exiting!"%source_env)
        else:
            my_logger.error("Unexpected error encountered in determining flags/ arguments for swap-. Exiting!")
            sys.exit("Unexpected error encountered in determining flags/ arguments for swap. Exiting!")
        
        my_logger.info("Updating ttdb_dict with source: %s"%source_env)
        ttdb_dict['source_env'] = source_env
        
        my_logger.info("Updating ttdb_dict with target: %s"%target_env)
        ttdb_dict['target_env'] = target_env
    
    elif db_operation == 'backup':
        my_logger.debug("Setting source_env for db_operation: %s."%db_operation)
        if ((t_count != 0) or (rdir_count != 0) or (bdir_count == 0)):
            my_logger.error("Improper format found. Use [python dbUtil.py -s <source_env> -bdir <backup target directory> -o backup -d <db_service>] for backup operation. Exiting!")
            sys.exit("Improper format found. Use [python dbUtil.py -s <source_env> -bdir <backup target directory> -o backup -d <db_service>] for backup operation. Exiting!")
        elif not ((t_count != 0) or (rdir_count != 0)):
            if (s_count == 1):
                my_logger.debug("Source flag found! Extracting source_env from list of arguments.")
                source_env = param_list[source_loc]
                my_logger.debug("source_env: %s"%source_env)
            elif (s_count == 0):
                my_logger.debug("Received no source flag. Setting active (production) environment as source")
                source_env = active_env.split("-")[1] + "." + active_env.split("-")[0] + ".aws.3top.com"
                my_logger.debug("source_env: %s"%source_env)
                
            my_logger.info("Received source: %s"%source_env)
            
            """*******************
            SOURCE_ENV: VALIDATE
            *******************"""
            source_env = env_validate(source_env)
            my_logger.info("The source_env has been validated!")
            
            
            if bdir_count == 1:
                bdir = param_list[bdir_loc]
                my_logger.debug("Backup dir found as : %s"%bdir)
        
                if bdir == '.':
                    pathname = os.getcwd()
                    bdir = os.path.abspath(pathname)
                    my_logger.debug("Backup dir will be: %s"%bdir)
                
                if not os.path.isdir(os.path.expanduser(bdir)):
                    my_logger.debug("No backup dir found. Creating %s"%bdir)
                    os.makedirs(bdir)
                    my_logger.debug("%s created successfully!"%bdir)
            else:
                my_logger.error("Unexpected error encountered in determining source_env. Exiting!")
                sys.exit("Unexpected error encountered in determining source_env. Exiting!")    
        else:
            my_logger.error("Unexpected error encountered in determining flags/ arguments for backup. Exiting!")
            sys.exit("Unexpected error encountered in determining flags/ arguments for backup. Exiting!")
        
        my_logger.info("Updating ttdb_dict with source: %s"%source_env)
        ttdb_dict['source_env'] = source_env
        my_logger.info("Updating ttdb_dict with target_dir: %s"%bdir)
        ttdb_dict['target_dir'] = bdir
        
    elif db_operation == 'restore':
        my_logger.debug("Setting source_env for db_operation: %s."%db_operation)
        if (s_count != 0) or (bdir_count != 0) or (rdir_count == 0):
            my_logger.error("Improper format found. Use [python dbUtil.py -rdir <restore source directory> -t <target_env> -o restore -d <db_service>] for restore operation. Exiting!")
            sys.exit("Improper format found. Use [python dbUtil.py -rdir <restore source directory> -t <target_env> -o restore -d <db_service>] for restore operation. Exiting!")
        elif not ((s_count != 0) or (bdir_count != 0)):
            if (t_count == 1):
                my_logger.debug("Target flag found! Extracting target_env from list of arguments.")
                target_env = param_list[target_loc]
                my_logger.debug("target_env: %s"%target_env)
            elif (t_count == 0):
                my_logger.error("No target flag found. Exiting!")
                sys.exit("No target flag found. Exiting!")
            
            my_logger.info("Received target: %s"%target_env)
            
            """*******************
            TARGET_ENV: VALIDATE
            *******************"""
            target_env = env_validate(target_env)
            my_logger.info("The target_env has been validated!")
            
            if (rdir_count == 1):
                rdir = param_list[rdir_loc]
                my_logger.debug("Restore dir found as : %s"%rdir)
    
                if rdir == '.':
                    pathname = os.getcwd()
                    rdir = os.path.abspath(pathname)
                    my_logger.debug("Restore source dir will be: %s"%rdir)
            
                if not os.path.isdir(os.path.expanduser(rdir)):
                    my_logger.error("No restore dir: %s found. Exiting!"%rdir)
                    sys.exit("No restore dir: %s found. Exiting!"%rdir)
            else:
                my_logger.error("Unexpected error encountered in determining target_env. Exiting!")
                sys.exit("Unexpected error encountered in determining target_env. Exiting!")    
        else:
            my_logger.error("Unexpected error encountered in determining flags/ arguments for restore. Exiting!")
            sys.exit("Unexpected error encountered in determining flags/ arguments for restore. Exiting!")
        
        my_logger.info("Updating ttdb_dict with source_dir: %s"%rdir)
        ttdb_dict['source_dir'] = rdir
        
        my_logger.info("Updating ttdb_dict with target: %s"%target_env)
        ttdb_dict['target_env'] = target_env
        
        
    my_logger.info("Returning to main() with ttdb_dict: %s"%ttdb_dict)
    return(ttdb_dict)
        
    """*********************************************************************************************************************************
    FIND SOURCE_ENV...STOP
    *********************************************************************************************************************************"""
    
"""set_params()...STOP"""
"""********************************************************************************************************************"""
"""arg_count_validate()...START"""

def arg_count_validate(count, operation):
    if operation == 'copy':
        if count != 9:
            my_logger.error("Format mismatch. Exiting!")
            print ("Format mismatch for \'copy\' operation. \nUse format: python dbUtil.py -s <SOURCE_ENV> -t <TARGET_ENV> -o copy -d <DB_SERVICE>")
            sys.exit("Please use correct format and run again. Exiting!")
        else:
            count_bool = True
            
    elif operation == 'swap':
        if (count != 5) and (count != 7):
            my_logger.error("Format mismatch. Exiting!")
            print("Format mismatch for \'swap\' operation. \nUse: python dbUtil.py -o swap -d <DB_SERVICE> (OR) python dbUtil.py -o swap -d <DB_SERVICE> -p <PROD_ENV>")
            sys.exit("Please use correct format and run again. Exiting!")
        else:
            count_bool = True
            
    elif operation == 'backup':
        if (count != 9):
            my_logger.error("Format mismatch. Exiting!")
            print("Format mismatch for \'backup\' operation. \nUse: python dbUtil.py -s <SOURCE_ENV> -bdir <DIR_LOC> -o backup -d <DB_SERVICE>")
            sys.exit("Please use correct format and run again. Exiting!")
        else:
            count_bool = True
            
    elif operation == 'restore':
        if (count != 9):
            my_logger.error("Format mismatch. Exiting!")
            print("Format mismatch for \'restore\' operation. \nUse: python dbUtil.py -rdir <DIR_LOC> -t <TARGET_ENV> -o backup -d <DB_SERVICE>")
            sys.exit("Please use correct format and run again. Exiting!")
        else:
            count_bool = True
    
    else:
        count_bool = False
        
    return(count_bool)

"""arg_count_validate()...STOP"""
"""********************************************************************************************************************"""
"""env_validate()...START"""

def env_validate(env_rxd):
    if "aws.3top.com" in env_rxd:
        source_env_list = env_rxd.split(".")
        if (source_env_list[0] in ['a', 'b', 'c']) and (source_env_list[1] in ['dev', 'prod']):
            my_logger.debug("Received env: %s. VALID!"%env_rxd)
    elif "nyc.3top.com" in env_rxd:
        source_env_list = env_rxd.split(".")
        if (source_env_list[0] == 'dev'):
            my_logger.debug("Received env: %s. VALID!"%env_rxd)
    else:
        my_logger.error("Received env: %s format is invalid. Exiting!"%env_rxd)
        print("Received env: %s format is invalid. Use: [a/b/c] . [dev/prod] . [aws/nyc] . [3top.com]"%env_rxd)
        sys.exit("Please use correct format and run again. Exiting!")
        
    return env_rxd
        

"""env_validate()...STOP"""
"""********************************************************************************************************************"""
"""swap_env_find()...START"""
        
def swap_env_find(p_count, p_loc, param_list):
    if (p_count == 0) and (p_loc == 'NA'):
        my_logger.debug("No '-p' found in arguments. Finding active environment")
        try:
            s_suffix = findActiveEnv()
        except:
            my_logger.error("Failed in finding active environment. If in maintenance mode use: python dbUtil.py -o swap -d <DB_SERVICE> -p <PROD_ENV>. Exiting!")
            sys.exit("Failed in finding active environment. If in maintenance mode use: python dbUtil.py -o swap -d <DB_SERVICE> -p <PROD_ENV>. Exiting!")
    elif p_count ==1:
        s_suffix = param_list[p_loc]
    my_logger.debug("Received s_suffix: %s"%s_suffix)
    s_env = s_suffix.split("-")[1] + "." + s_suffix.split("-")[0] + ".aws.3top.com"
    if s_env == 'a.prod.aws.3top.com':
        t_env = "b.prod.aws.3top.com"
    elif s_env == 'b.prod.aws.3top.com':
        t_env = "a.prod.aws.3top.com"
    else:
        my_logger.error("Cannot recognize source_environment. Received s_env: %s. Exiting!"%s_env)
        sys.exit("Cannot recognize source_environment. Received s_env: %s. Exiting!"%s_env)
    
    my_logger.debug("Returning to set_params() with s_env: %s and t_env: %s."%(s_env, t_env))
    
    return (s_env, t_env)

"""swap_env_find()...STOP"""
"""********************************************************************************************************************"""
"""backup_env_validate()...START"""
        
def backup_env_validate(s_loc, bdir_loc, param_list):
    my_logger.debug("In backup_env_validate(%s, %s, %s)"%(s_loc, bdir_loc, param_list))
    source_env = param_list[s_loc]
    if "aws.3top.com" in source_env:
        source_env_list = source_env.split(".")
        if (source_env_list[0] in ['a', 'b', 'c']) and (source_env_list[1] in ['dev', 'prod']):
            print("Received source_env: %s."%source_env)
    elif "nyc.3top.com" in source_env:
        source_env_list = source_env.split(".")
        if (source_env_list[0] == 'dev'):
            print("Received source_env: %s."%source_env)
    else:
        sys.exit("Received source_env: %s. Status: Invalid [Use Format: [a/b/c] . [dev/prod] . [aws/nyc] . [3top.com]] Exiting!"%source_env)
            
    bdir = param_list[bdir_loc]
    my_logger.debug("Backup dir found as : %s"%bdir)
    
    if bdir == '.':
        bdir = os.getcwd()
        my_logger.debug("Backup dir will be: %s"%bdir)
            
    if not os.path.isdir(os.path.expanduser(bdir)):
        my_logger.debug("No backup dir found. Creating %s"%bdir)
        os.makedirs(bdir)
        my_logger.debug("%s created successfully!"%bdir)
       
    return (source_env, bdir)

"""backup_env_validate()...STOP"""
"""********************************************************************************************************************"""
"""restore_env_validate()...START"""
        
def restore_env_validate(t_loc, rdir_loc, param_list):
    my_logger.debug("In backup_env_validate(%s, %s, %s)"%(t_loc, rdir_loc, param_list))
    target_env = param_list[t_loc]
    if "aws.3top.com" in target_env:
        target_env_list = target_env.split(".")
        if (target_env_list[0] in ['a', 'b', 'c']) and (target_env_list[1] in ['dev', 'prod']):
            print("Received target_env: %s."%target_env)
    elif "nyc.3top.com" in target_env:
        target_env_list = target_env.split(".")
        if (target_env_list[0] == 'dev'):
            print("Received target_env: %s."%target_env)
    else:
        sys.exit("Received target_env: %s. Status: Invalid [Use Format: [a/b/c] . [dev/prod] . [aws/nyc] . [3top.com]] Exiting!"%target_env)
        
    rdir = param_list[rdir_loc]
    my_logger.debug("Restore dir found as : %s"%rdir)
    
    if rdir == '.':
        rdir = os.getcwd()
        my_logger.debug("Restore source dir will be: %s"%rdir)
            
    if not os.path.isdir(os.path.expanduser(rdir)):
        my_logger.error("No restore dir: %s found. Exiting!"%rdir)
        sys.exit("No restore dir: %s found. Exiting!"%rdir)
    
    return (target_env, rdir)

"""restore_env_validate()...STOP"""
"""********************************************************************************************************************"""
"""db_service_validate()...START"""
        
def db_service_validate(d_cnt, p_list, s_loc, os_p):
    if d_cnt == 0:
        my_logger.error("DB Service not found! Exiting.")
        sys.exit("DB Service not found! Exiting.")
    elif d_cnt == 1:
        db_service = p_list[s_loc].lower()
        if db_service in ['all', 'mysql', 'mongodb', 'neo4j', 'virtuoso']:
            my_logger.debug("DB_Service received: %s." %db_service)
            if os_p == 'Windows':
                if (db_service == 'all') or (db_service == 'neo4j') or (db_service == 'virtuoso'):
                    my_logger.error("Virtuoso and Neo4j operations cannot be performed on Windows platform. Exiting!")
                    sys.exit("Virtuoso and Neo4j operations cannot be performed on Windows platform. Exiting!")
                if (db_service == 'mysql')  or (db_service == 'mongodb'):
                    my_logger.debug("Received db_service: %s operations can be performed on os_platform: %s"%(db_service, os_p))
            elif os_p == 'Linux':
                my_logger.debug("Received db_service: %s operations can be performed on os_platform: %s"%(db_service, os_p))
        else:
            my_logger.error("The db_service cannot be identified. DB_Service received: %s. \n [Choose: all/mysql/mongodb/neo4j/virtuoso]. Exiting!"%db_service)
            sys.exit("The db_service cannot be identified. DB_Service received: %s. \n [Choose: all/mysql/mongodb/neo4j/virtuoso]. Exiting!"%db_service)
    
    return db_service

"""db_service_validate()...STOP"""
"""********************************************************************************************************************"""
"""operation_validate()...START"""
        
def db_operation_validate(o_cnt, p_list, o_loc):
    if o_cnt == 0:
        my_logger.error("DB Operation not found! Exiting.")
        sys.exit("DB Operation not found! Exiting.")
    elif o_cnt == 1:
        db_operation = p_list[o_loc].lower()
        if db_operation in ['swap', 'copy', 'backup', 'restore']:
            my_logger.debug("DB_Operation received: %s." %db_operation)
        else:
            my_logger.error("Received DB_Operation: %s is invalid. Exiting!"%db_operation)
            print("Received DB_Operation: %s is invalid. [Choose: swap/copy]"%db_operation)
            sys.exit("DB_Operation received: %s. Status: Invalid \n [Choose: swap/copy]. Exiting!"%db_operation)
    
    return db_operation
    
"""operation_validate()...STOP"""
"""********************************************************************************************************************"""
"""********************************************************************************************************************"""
"""set_host()...START"""
        
def set_host(local_dict):
    my_logger.debug("Setting hosts...")
    if (local_dict['db_operation'] == 'copy') or (local_dict['db_operation'] == 'swap') or (local_dict['db_operation'] == 'backup'):
        s_env = local_dict['source_env']
        for k, v in local_dict.items():
            if type(v) is dict:
                if v['service_name'] != 'NA':
                    if v['service_name'] == 'mysql':
                        s_host_suffix = 'fp-rds-1.'
                    elif v['service_name'] == 'mongodb':
                        s_host_suffix = 'mongodb.'
                    elif v['service_name'] == 'neo4j':
                        s_host_suffix = 'neo4j.'
                    elif v['service_name'] == 'virtuoso':
                        s_host_suffix = 'virtuoso.'
                        
                    s_host_key = v['service_name'] + "_source_host"
                    if ("aws.3top.com" in s_env):
                        s_host_value = s_host_suffix + s_env 
                    elif "nyc.3top.com" in s_env:
                        s_host_value = s_env
                    v[s_host_key] = s_host_value
                    my_logger.info("Inserted record {%s: %s}"%(s_host_key, s_host_value))
                     
    if (local_dict['db_operation'] == 'copy') or (local_dict['db_operation'] == 'swap') or (local_dict['db_operation'] == 'restore'):
        t_env = local_dict['target_env']
        for k, v in local_dict.items():
            if type(v) is dict:
                if v['service_name'] != 'NA':
                    if v['service_name'] == 'mysql':
                        t_host_suffix = 'fp-rds-1.'
                    elif v['service_name'] == 'mongodb':
                        t_host_suffix = 'mongodb.'
                    elif v['service_name'] == 'neo4j':
                        t_host_suffix = 'neo4j.'
                    elif v['service_name'] == 'virtuoso':
                        t_host_suffix = 'virtuoso.'
                    t_host_key = v['service_name'] + "_target_host"
                    if ("aws.3top.com" in t_env):
                        t_host_value = t_host_suffix + t_env 
                    elif "nyc.3top.com" in t_env:
                        t_host_value = t_env
                    v[t_host_key] = t_host_value
                    my_logger.info("Inserted record {%s: %s}"%(t_host_key, t_host_value))
    
    my_logger.debug("Setting host successful. Returning to main() with %s" %(local_dict))
    return local_dict
    
"""set_host()...STOP"""
"""********************************************************************************************************************"""