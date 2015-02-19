import os
import sys


AWS_MYSQL_CERT="/usr/local/3top/src/aws/mysql-ssl-ca-cert.pem"

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
"""********************CERTIFICATE VALIDATION STOP***************************************"""
