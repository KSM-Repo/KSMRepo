import sys
import os
import re
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name'])


"""********************************************************************************************************************"""
""" Validate arguments...start    """

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
                if re.match('^[ab]\.(dev)(\.(aws)\.(3top)\.(com))$', t_env):
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
            my_logger.error("No db_service arguments found. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]")
            sys.exit("No db_service arguments found. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]")
        elif service not in ['mysql', 'mongodb', 'neo4j', 'all']:
            my_logger.error("Incorrect Database chosen: %s. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]" %service)
            sys.exit("Incorrect Database chosen: %s. Exiting! \nUse format: db_util.py [ swap | copy ] [ ALL | mysql | mongodb | virtuoso | neo4j ] [ target.suffix(for 'copy' only) ]" %service)
        else:
            my_logger.debug("Validating db_service...")
            
    
        """ Validate database service argument stop"""
                        
    except:
        my_logger.error("Error encountered in validate_args. Exiting!")
        sys.exit("Error encountered in validate_args. Exiting!")
    #print("Validate arguments complete. Returning to main()...")
       
""" Validate arguments...stop    """
"""********************************************************************************************************************"""