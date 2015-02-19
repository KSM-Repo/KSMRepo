import os
import db_usr_creat
dictDB_Users=db_usr_creat.dictDB_Users
import ast
import boto
from boto.s3.key import Key
import unittest
import time


print"Storing user details on S3." 

conn=boto.connect_s3()
buck=conn.get_bucket('app-env')
key=Key(buck)
key.key='/aws-prod/credentials.txt'
s1=str(dictDB_Users)
#key.set_contents_from_string(s1)
time.sleep(5)

#o=key.get_contents_as_string()
#assert o == s1
        
key.set_contents_from_string(s1, encrypt_key=True)
time.sleep(5)

o=key.get_contents_as_string()
assert o ==s1




>>> os.environ['mysql-usr']='usr'
>>> print os.environ.get['mysql_usr']
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: 'instancemethod' object has no attribute '__getitem__'
>>> print os.environ.get['mysql-usr']
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: 'instancemethod' object has no attribute '__getitem__'
>>> somevar=str(os.environ['mysql-usr'])
>>> somevar
'usr'
