#!/usr/bin/python
#
# Author: Sudha Kanupuru
# Description: DB Authenticate



"""

_aws_rds_dba_usr="root"
_aws_rds_dba_pass="password"

_mysql_dba_usr="root"
_mysql_dba_pass="password"

_aws_rds_rw_usr="replnyc"
_aws_rds_rw_pass="password"

_mysql_rw_usr="replnyc"
_mysql_rw_pass="password"
"""db_name="ebdb"""

"""*** HOST DETAILS*** """
_aws_rds_host="skanupuru-lxs1.nyc.3top.com"
_mysql_host="skanupuru-lxs1.nyc.3top.com"
"""db_name="ebdb"""


"""*** CERTIFICATE DETAILS*** """
AWS_MYSQL_CERT="/usr/local/3top/src/aws/mysql-ssl-ca-cert.pem"

"""database=MySQLdb.connect(db_host, db_user, db_pass, db_name)"""

"""************************DBA USER VALIDATION START************************************"""

try:
    database=MySQLdb.connect(_aws_rds_host, _aws_rds_dba_usr, _aws_rds_dba_pass)
    """
    command="mysql --host=" + _aws_rds_host + " -u " + _aws_rds_dba_usr + " -p" + _aws_rds_dba_pass 
    proc1=subprocess.Popen([command], stdout=subprocess.PIPE, shell=True)
    out, err= proc1.communicate()
    print out
    print err
    """
    print "Success!"
except:
     e = str(sys.exc_info())
     print("Unable to initialize database, check settings! %s" %e)
     sys.exit(-1)
     
try:
    database=MySQLdb.connect(_mysql_host, _mysql_dba_usr, _mysql_dba_pass)
    print "Success!"
except:
     e = str(sys.exc_info())
     print("Unable to initialize database, check settings! %s" %e)
     sys.exit(-1)
"""************************DBA USER VALIDATION STOP*************************************"""


"""*******************READ-WRITE USER VALIDATION START *********************************"""
try:
    database=MySQLdb.connect(_aws_rds_host, _aws_rds_rw_usr, _aws_rds_rw_pass)
    print "Success!"
except:
     e = str(sys.exc_info())
     print("Unable to initialize database, check settings! %s" %e)
     sys.exit(-1)
     
try:
    database=MySQLdb.connect(_mysql_host, _mysql_rw_usr, _mysql_rw_pass)
    print "Success!"
except:
     e = str(sys.exc_info())
     print("Unable to initialize database, check settings! %s" %e)
     sys.exit(-1)
"""*****************READ-WRITE USER VALIDATION STOP**************************************"""

"""*******************CERTIFICATE VALIDATION START **************************************"""
try:
    path_ssl=AWS_MYSQL_CERT
    if (os.access(path_ssl, os.R_OK)):
        print "SSL Validated. Continuing!"
    else:
        print "Validate SSL failed. Exiting!"
        sys.exit(-1)
except:
    e = str(sys.exc_info())
    print("Error Encountered: %s. Exiting!" %e)
    sys.exit(-1)
    """*******************CERTIFICATE VALIDATION STOP************************************"""