import boto.s3
import sys
from boto.s3.key import Key
import os
import inspect

python_root=os.path.dirname(os.path.realpath(__file__)) + "/"

try:
    option=input("1. Upload New File \n2. Update Existing File \n3. Download File \nSelect an option: ")
    if option==1:
        fileLocOK=raw_input("Is your ttSettings.conf file located in the path %s \n [y/n]: " %python_root)
        if fileLocOK=='y' or fileLocOK=='Y':
            print "Received \"Upload New File\" Option. Validating file location..."
            confFilePath=python_root+ "ttSettings.conf"
            if (os.access(confFilePath, os.R_OK)):
                print "File location verified. Continuing!"
            else:
                print "File not found. Exiting!"
                sys.exit(-1)
            conn = boto.connect_s3()
            bucket=conn.get_bucket('app-env')
            bucket_dir=raw_input("Enter folder name inside the bucket:")
            file_name=raw_input("Enter File Name: ")
            pathFileLoc=bucket_dir + "/" + file_name
            print pathFileLoc
            k = bucket.new_key(pathFileLoc)
            k.metadata["Content-Type"]='text/plain'
            k.set_contents_from_filename(python_root+'ttSettings.conf')
            print("File upload successful. Location: %s.\nPlease remember to delete the file: %s Exiting!" %(pathFileLoc,confFilePath))
            
        elif fileLocOK=='n' or fileLocOK=='N':
            print "Exiting!"
            
        else:
            print "Unexpected value entered: %s \n Exiting!" %fileLocOK
    elif option==2:
        fileLocOK=raw_input("Is your ttSettings.conf file located in the path %s \n [y/n]: " %python_root)
        if fileLocOK=='y' or fileLocOK=='Y':
            print "Received \"Update File\" Option. Validating file location..."
            confFilePath=python_root+ "ttSettings.conf"
            if (os.access(confFilePath, os.R_OK)):
                print "File location verified. Continuing!"
            else:
                print "File not found. Exiting!"
                sys.exit(-1)
            conn = boto.connect_s3()
            bucket=conn.get_bucket('app-env')
            k=Key(bucket)
            bucket_dir=raw_input("Enter folder name inside the bucket:")
            file_name=raw_input("Enter File Name: ")
            pathFileLoc=bucket_dir + "/" + file_name
            #print pathFileLoc
            k.key = pathFileLoc
            k.metadata["Content-Type"]='text/plain'
            #k.content_type='text/plain'
            k.set_contents_from_filename(python_root+'ttSettings.conf')
            print("File upload successful. Location: %s.\nPlease remember to delete the file: %s Exiting!" %(pathFileLoc,confFilePath))
            
        elif fileLocOK=='n' or fileLocOK=='N':
            print "Exiting!"
            
        else:
            print "Unexpected value entered: %s \n Exiting!" %fileLocOK
     
    elif option==3:
        print("Downloading file \'ttSettings.conf\' from \'app-env/aws-prod/\'to the path %s" %python_root)
        conn = boto.connect_s3()
        bucket=conn.get_bucket('app-env')
        k=Key(bucket)
        k.key="aws-prod/ttSettings.conf"
        k.get_contents_to_filename('ttSettings.conf')
        print("Download complete. Please update 'ttSettings.conf' and run the script with option 1. \nExiting!")
        
    else:
        print "Selected option incorrect. Exiting!"
        
        
except:
    e = str(sys.exc_info())
    print("Unexpected Error: %s \n Exiting!" %e)
    sys.exit(-1) 
