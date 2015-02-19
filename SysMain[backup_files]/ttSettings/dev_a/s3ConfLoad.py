import boto.s3
import sys
from boto.s3.key import Key
import os
import inspect
python_root=os.path.dirname(os.path.realpath(__file__)) + "/"

"""
bucket ="app-env"
folders:
1. aws-dev/a
2. aws-dev/b
3. aws-prod/a
4. aws-prod/b

"""

try:
    option=input("1. Upload New File \n2. Update Existing File \n3. Download File \nSelect an option: ")
    if option==1:
        s3Continue= raw_input("You are about to make changes to the S3 bucket. Confirm to continue [y/n]:")
        if s3Continue == 'y' or s3Continue == 'Y':       
            fileLocOK=raw_input("Is your ttSettings.conf file located in the path %s \n [y/n]: " %python_root)
            if fileLocOK=='y' or fileLocOK=='Y':
                print "Received \"Upload New File\" Option. Validating file location..."
                confFilePath=python_root+ "ttSettings.conf"
                if (os.access(confFilePath, os.R_OK)):
                    print "File location verified. Continuing!"
                else:
                    print "File not found. Exiting!"
                    sys.exit(-1)
                
                print"Existing folders are as follows: \n1. aws-dev/a \n2. aws-dev/b\n3. aws-dev/c\n4. aws-prod/a\n5. aws-prod/b\n6. nyc-sys\n7. nyc-dev\n8. nyc-sys-dev"
                conn = boto.connect_s3()
                bucket=conn.get_bucket('app-env')
                k=Key(bucket)
                folder_option=input("Choose a folder: ")
                if folder_option == 1:
                    folder_path="aws-dev/a"
                elif folder_option == 2:
                    folder_path="aws-dev/b"
                elif folder_option == 3:
                    folder_path="aws-dev/c"
                elif folder_option == 4:
                    folder_path="aws-prod/a"
                elif folder_option == 5:
                    folder_path="aws-prod/b"
                elif folder_option == 6:
                    folder_path="nyc-sys"
                elif folder_option == 7:
                    folder_path = "nyc-dev"
                elif folder_option == 8:
                    folder_path = "nyc-sys-dev"
                else: 
                    sys.exit("Wrong folder choice. Exiting...")
                pathFileLoc = folder_path + "/ttSettings.conf"
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
        s3Continue= raw_input("You are about to make changes to the S3 bucket. Confirm to continue [y/n]:")
        if s3Continue == 'y' or s3Continue == 'Y':       
            fileLocOK=raw_input("Is your ttSettings.conf file located in the path %s \n [y/n]: " %python_root)
            if fileLocOK=='y' or fileLocOK=='Y':
                print "Received \"Update File\" Option. Validating file location..."
                confFilePath=python_root+ "ttSettings.conf"
                if (os.access(confFilePath, os.R_OK)):
                    print "File location verified. Continuing!"
                else:
                    print "File not found. Exiting!"
                    sys.exit(-1)
                print"Existing folders are as follows: \n1. aws-dev/a \n2. aws-dev/b\n3. aws-dev/c\n4. aws-prod/a\n5. aws-prod/b\n6. nyc-sys\n7. nyc-dev\n8. nyc-sys-dev"
                conn = boto.connect_s3()
                bucket=conn.get_bucket('app-env')
                k=Key(bucket)
                folder_option=input("Choose a folder: ")
                if folder_option == 1:
                    folder_path="aws-dev/a"
                elif folder_option == 2:
                    folder_path="aws-dev/b"
                elif folder_option == 3:
                    folder_path="aws-dev/c"
                elif folder_option == 4:
                    folder_path="aws-prod/a"
                elif folder_option == 5:
                    folder_path="aws-prod/b"
                elif folder_option == 6:
                    folder_path="nyc-sys"
                elif folder_option == 7:
                    folder_path = "nyc-dev"
                elif folder_option == 8:
                    folder_path = "nyc-sys-dev"
                else: 
                    sys.exit("Wrong folder choice. Exiting...")
                pathFileLoc = folder_path + "/ttSettings.conf"
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
        print"Existing folders are as follows: \n1. aws-dev/a \n2. aws-dev/b\n3. aws-dev/c\n4. aws-prod/a\n5. aws-prod/b\n6. nyc-sys\n7. nyc-dev\n8. nyc-sys-dev"
        conn = boto.connect_s3()
        bucket=conn.get_bucket('app-env')
        k=Key(bucket)
        folder_option=input("Choose a folder: ")
        if folder_option == 1:
            folder_path="aws-dev/a"
        elif folder_option == 2:
            folder_path="aws-dev/b"
        elif folder_option == 3:
            folder_path="aws-dev/c"
        elif folder_option == 4:
            folder_path="aws-prod/a"
        elif folder_option == 5:
            folder_path="aws-prod/b"
        elif folder_option == 6:
            folder_path="nyc-sys"
        elif folder_option == 7:
            folder_path = "nyc-dev"
        elif folder_option == 8:
            folder_path = "nyc-sys-dev"
        else: 
            sys.exit("Wrong folder choice. Exiting...")
        k.key= folder_path + "/ttSettings.conf"
        k.get_contents_to_filename('ttSettings.conf')
        print("Download complete. Please update 'ttSettings.conf' and run the script with option 1. \nExiting!")
            
    else:
        print "Selected option incorrect. Exiting!"
        sys.exit("Exiting...")
        
        
except:
    sys.exit("Unexpected Error encoutered. Exiting!")
     
