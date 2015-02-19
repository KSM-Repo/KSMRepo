#!/usr/bin/python 
import os
import subprocess
import shlex
import datetime
import time
import sys
import logging
import ttLib.logger as logger
my_logger = logging.getLogger(os.environ['logger_name'])

def callSubprocess(cmd):
    my_logger.debug("In callSubprocess...")
    try:
        proc = subprocess.Popen(shlex.split(cmd), stdout = subprocess.PIPE, close_fds=True)
        out, err = proc.communicate()
        my_logger.debug("out:%s, err:%s"%(out, err))
        #if(err == None): print "hey"
        if (err == None) and (proc.returncode == 0):
            my_logger.debug("callSubprocess operation successful! Returning to main()")
            return out
        elif ("ERROR" in err.upper()) or ("FAILED" in err.upper()):
            print "Error: ", err
            sys.exit(-1)
        elif ("Error" in out) or ("ERROR" in out) or ("Failed" in out):
            print "Error: ", out  
            sys.exit(-1)
        """else:
            sys.exit("Error: %s, \nReturn Code: %s"%(err,proc.returncode))"""
    except:
        e = sys.exc_info()[0]
        print("Error Encountered %s. Exiting...."%e)
        sys.exit(-1)
    

def check_files(paths):
        dict1 = {}
        status = ""
        for p in paths:
            if not os.access(p, os.F_OK):
                dict[p] = status = status + "NOEXISTS"
            if(os.access(p,os.R_OK)):
                dict[p] = status = status + "READ," 
            if(os.access(p, os.W_OK)):
               dict[p] = status = status + "WRITE," 
            if (os.access(p, os.X_OK)):
                dict[p] = status = status + "EXECUTE"
            elif os.access(p, os.F_OK) and not (os.access(p,os.R_OK)) and not (os.access(p, os.W_OK)) and not (os.access(p, os.X_OK)):
               dict[p] = status = status = "NOACCESS"
            status = "" # Set blank before we enter the loop again
        return dict1