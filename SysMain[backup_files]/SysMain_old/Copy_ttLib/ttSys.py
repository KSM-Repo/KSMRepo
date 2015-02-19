#!/usr/bin/python 
"""
This module checks for the dns directory in the /usr/local/3top/log path
and creates one if it is not already present. In the dns directory, it creates the log file
with the current timestamp
""" 
import os
import datetime
import time
import sys
import re



def dirCreateTimestamp(path):
    print("Initiating create_timestamp_directory to store logfile and backups")
    if not os.path.exists(path):
        os.makedirs(path)
    now = datetime.datetime.fromtimestamp(time.time())
    s = now.strftime('%Y_%m_%d_%H_%M_%S')
    #print s
    try:
        dir_name = path + "/" + s
        #print dir_name
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print("Directory creation successful!")
    except:
        print("Directory creation failed. Exiting!")
    return dir_name


import boto
import ast
from boto.s3.key import Key

""" Get ttSettings configuration file from S3
    """

def get_s3_config(s3_folder, s3_filename):
    conn = boto.connect_s3()
    buck = conn.get_bucket('app-env')
    key = Key(buck)
    #print"S3_filename: ",s3_filename
    #key.key = "/aws-prod/" + s3_filename
    key.key = "/" + s3_folder + "/" + s3_filename

    """ Download the values in a file on S3 into a string
        """
    s3_file_contents = key.get_contents_as_string()
    #print"s3_file_contents: ", s3_file_contents

    """ Convert the s3 file contents into a dictionary
        """
    dict_s3_settings = {}
    #import ast
    dict_s3_settings = ast.literal_eval(s3_file_contents)
    #print type(dict_s3_settings)
    
    """ Return dictionary of settings to calling function
        """
    return dict_s3_settings
