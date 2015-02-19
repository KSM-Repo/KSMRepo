#!/usr/bin/python
import logging
import ttLib.logger as logger
import os
import subprocess
import sys
my_logger = logging.getLogger(os.environ['logger_name'])
from ttLx import callSubprocess
from ttSys import get_s3_config

try:
    my_logger.debug("Trying to import Boto AWS APIs....")
    import boto.ec2
    import boto.rds2
    import boto.beanstalk
    import boto.route53
except:
    my_logger.error("Error Encountered in importing Boto APIs. Please run the requirements file to install boto and DNSPython. Exiting...." )
    sys.exit("Error Encountered in importing Boto APIs. Please run the requirements file to install boto and DNSPython. Exiting...." )
my_logger.debug("Import of Boto-EC2, RDS2, Beanstalk, Route53 successful. Boto connection validated!")

dictNyc = dict()
dictAws = dict()
currAws = dict()

"""
Define Global Constants
"""
_aws_http_root = "www.3top.com"
_aws_dns_suffix = "aws.3top.com"

_fp_live_cmd = "dig +short " + _aws_http_root + " | grep " + _aws_dns_suffix
_fp_live_dns_cmd = "dig +short " + _aws_http_root + " | grep " + _aws_dns_suffix
_fp_dns_internal_cmd = "dig " + _aws_http_root + " +short"
_fp_dns_external_cmd = "dig @8.8.8.8 " + _aws_http_root + " +short"


"""****************************************************************************************************************"""
"""auth_func()...START"""

def auth_func(region):
    """#The auth_func in ttAWS checks if the import of boto packages
    (ec2, rds2 etc) is successful by checking
    #if the required packages have been installed
    and authentication information is accurately stored"""
    my_logger.debug(" Initializing auth_func()....")
    try:
        my_logger.debug(" Executing boto.ec2.connect_to_region(%s)...."%region)
        boto.ec2.connect_to_region(region)
    except:
        my_logger.error("Error Encountered in: auth_func(). Exiting!" )
        sys.exit("Error Encountered in:auth_func(). Exiting!")
    my_logger.debug("auth_func()....successful!")

"""auth_func()...STOP """
"""****************************************************************************************************************"""
"""connBotoEc2()...START"""

"""
Here the connections to boto EC2 API are made. When an error, system prompts the user to correctly configure the Credentials in the ~/.boto file
This will allow the user to be authenticated
"""
def connBotoEc2(region):
    my_logger.debug("Initializing boto.ec2.connect_to_region(%s)...."%region)
    try:
        connEc2 = boto.ec2.connect_to_region(region)
    except:
        my_logger.error("Error Encountered in: Boto EC2 connection. Exiting!")     
        sys.exit("Error Encountered in: Boto EC2 connection. Exiting!")
    my_logger.debug("Boto EC2 connection successful! Returning to main() with EC2 connection")
    return connEc2

"""connBotoEc2()...STOP """
"""****************************************************************************************************************"""
"""connBotoBeanstalk()...START"""
    
def connBotoBeanstalk(region):
    my_logger.debug("Initializing boto.beanstalk.connect_to_region(%s)...."%region)
    try:
        boto.beanstalk.connect_to_region(region)
    except:
        my_logger.error("Error Encountered in: Boto Beanstalk connection. Exiting!")
        sys.exit("Error Encountered in: Boto Beanstalk connection. Exiting!")
    l1 = boto.beanstalk.layer1.Layer1()
    my_logger.debug("Boto Beanstalk connection successful! Returning to main() with Beanstalk layer1")
    return l1

"""connBotoBeanstalk()...STOP """
"""****************************************************************************************************************"""
"""connBotoRds2()...START"""
    
def connBotoRds2(region):
    my_logger.debug("Initializing boto.rds2.connect_to_region(%s)...."%region)
    try:
        connRds2 = boto.rds2.connect_to_region(region)
    except:
        my_logger.error("Error Encountered in:  Boto RDS connection. Exiting!" )
        sys.exit("Error Encountered in:  Boto RDS connection. Exiting!" )
    my_logger.debug("Boto RDS connection successful! Returning to main() with RDS connection")
    return connRds2

"""connBotoRds2()...STOP """
"""****************************************************************************************************************"""
"""connRoute53()...START"""
    
def connRoute53():
    my_logger.debug("Initializing boto.connect_route53()....")
    try:
        connR53 = boto.connect_route53()
    except:
        my_logger.error("Error Encountered. Boto Route53 connection failed! Exiting....")
        sys.exit("Error Encountered. Boto Route53 connection failed! Exiting....")
    my_logger.debug("Boto Route53 connection successful! Returning to main() with Route53 connection")
    return connR53 

"""connRoute53o()...STOP """
"""****************************************************************************************************************"""
"""connELB()...START"""

def connELB():
    my_logger.debug("Initializing boto.ElasticLoadBalancer.connect()....")
    try:
        conn_elb = boto.connect_elb()
    except:
        my_logger.error("Error Encountered in: Boto ElasticLoadBalancer connection. Exiting!")
        sys.exit("Error Encountered in: Boto ElasticLoadBalancer connection. Exiting!")
    my_logger.debug("Boto ElasticLoadBalancer connection successful! Returning with ElasticLoadBalancer")
    return conn_elb

"""connELB()...STOP """
"""****************************************************************************************************************"""
"""selectEnviron()...START"""

"""
This part finds the number of environments available and allows the user 
to choose the env. This finds the number of environments and collects the environments
list and allocates and index. The user can input the index to choose the environment
whose dns records need to be exported. The Environment name will be a.prod, b.prod, a.dev etc...
"""
def selectEnviron(region, m_arg):
    try:
        l1 = connBotoBeanstalk(region)
        des_env = l1.describe_environments()
        my_logger.debug("Output for describe environments : %s" % des_env)
        
        app_env = list()
        
        for de in des_env["DescribeEnvironmentsResponse"]["DescribeEnvironmentsResult"]["Environments"]:
            if de['Status'] == 'Ready':
                i_app = de['ApplicationName'].split("-")[1]
                i_env = de['EnvironmentName'].split("-")[1] + "-" + de['EnvironmentName'].split("-")[2]
                app_env.append(i_app + "_" + i_env)
    
        my_logger.debug("Application environments determined as %s."%app_env)
    
        if m_arg[0] == "-p":
            envActive = m_arg[1]
            print envActive
        else:
                my_logger.debug("Calling findActiveEnv()...")
                envActive = findActiveEnv()
                my_logger.debug("Received active environment information as : %s" %envActive)
    except:
        my_logger.error("Error Encountered in determining the application environments list. Exiting...." )
        sys.exit("Error Encountered in determining the application environments list. Exiting...." )
            
    print("Received active environment information as : %s" %envActive)
    try:
        for app in app_env:
            #print app
            if app.split("_")[1] == envActive:
                app_env.remove(app)
            if (app.split("_")[1][:4] == "dev-") or (app.split("_")[1][:5] == "prod-"):
                if (app.split("_")[1][:4] == "dev-"):
                    app_split = app.split("_")[1][:4]
                    my_logger.debug("Received %s"%app_split)
                elif (app.split("_")[1][:5] == "prod-"):
                    app_split = app.split("_")[1][:5]
                    my_logger.debug("Received %s"%app_split)
            else:
                app_env.remove(app)
        my_logger.debug("Removed the active environment: %s from the list of environments!"%envActive)
        my_logger.debug("APP_ENV: %s"%app_env)
        index_list = range(1,len(app_env)+1)
        env_list = list()
        for env in app_env:
            env_list.append(env.split("_")[1])
        my_logger.debug("Index List: %s"%index_list)
        my_logger.debug("New list of environments after removing active environment Env_list: %s"%env_list)
        dict_env = dict(zip(index_list, sorted(env_list)))
        my_logger.debug("Dict ENV: %s" %dict_env)
        my_logger.debug("Environments available for update are:%s " %dict_env)
            
        #print("Select AWS environment to update: ")
        print ("\n")
        for k, v in dict_env.items():
            print '{:4s} {:7s}'.format(str(k), str(v))
        #print ("\n") 
        envIndex = input("Select AWS environment to update: ")
        my_logger.debug("Environment Index entered by user: %s" % envIndex)
        envName = dict_env.get(envIndex).split("-")[1] + "." + dict_env.get(envIndex).split("-")[0] 
        print "\nAnalyzing existing %s DNS Records..."%envName
        my_logger.debug("\nAnalyzing existing %s DNS Records..."%envName)
    except:
        my_logger.error("Unexpected error encountered in creating a dictionary of environments (with index). Exiting!")
        sys.exit("Unexpected error encountered in creating a dictionary of environments (with index). Exiting!")
    my_logger.debug("Returning to main() with selected envName: %s"%envName)       
    return str(envName)
    
"""
This module maps the EC2 Instance name to the public and private DNS names
for mq,search,mongodb
Gets information of all the instances (primarily prints the reservations)
"""

"""
for resv in ec2Reservations:
...     for ec2Inst in resv.instances:
...             if 'Name' in ec2Inst.tags:
...                     if "b.prod" in ec2Inst.tags["3t:environment"]:
...                             print "%s, %s, [%s]"%(ec2Inst.tags["3t:environment"], ec2Inst.tags["3t:service"], ec2Inst.state)

"""

"""selectEnviron()...STOP """
"""****************************************************************************************************************"""
"""dnsEc2ToDict()...START"""

def dnsEc2ToDict(region, envName,currZone):
    my_logger.debug("In ttAWS.dnsEc2ToDict....")
    #print region
    #print "envName: %s"%envName
    
    try:
        my_logger.debug("Connecting to EC2")
        connEc2 = connBotoEc2(region)
        my_logger.debug(connEc2)
        my_logger.debug("Collecting EC2 reservations...")
        ec2Reservations = connEc2.get_all_instances()
        my_logger.debug("List of reservations are: %s" %ec2Reservations)
        for resv in ec2Reservations:
            my_logger.debug("Reservation: %s" %resv)
            my_logger.debug("Reservation Instances: %s" %resv.instances)
            for ec2Inst in resv.instances:
                my_logger.debug("EC2 Instance: %s" %ec2Inst)
                if ec2Inst.state == "terminated":
                    my_logger.debug("ec2Inst.state: %s"% ec2Inst.state) 
                elif ec2Inst.state == "running":
                    my_logger.debug("ec2Inst.state: %s"% ec2Inst.state)
                    if 'Name' in ec2Inst.tags:
                        my_logger.debug("EC2 Instance tags: %s" %ec2Inst.tags)
                        my_logger.debug("envName: %s"%envName)
                        my_logger.debug("Checking if environment name matches with %s" %(envName))
                        for t in ec2Inst.tags:
                            if "3t:environment" in t:
                                if envName in ec2Inst.tags["3t:environment"]:
                                    #print 1
                                    my_logger.debug("%s, %s, [%s]"%(ec2Inst.tags["3t:environment"], ec2Inst.tags["3t:service"], ec2Inst.state))
                                    name = ec2Inst.tags["3t:service"]
                                    #sys.exit(-1)           
                                    ##name = ec2Inst.tags['Name']
                                    my_logger.debug("Name: %s"% name)
                                    #print 4
                                    nameSpl = name.split(":")#Split name 'prod-a:mq:search'-store in list
                                    my_logger.debug("nameSpl= %s"%nameSpl)
                                    #print 5
                                    ##tag = nameSpl[0].split("-")    #split 'prod-a' part of name
                                    lenNameSpl = len(nameSpl)    #find len. of list to define range
                                    my_logger.debug("lenNameSpl: %s"%lenNameSpl)
                                    servNames = list()        #define list to store the new name format
                                    dns_public_lst = list()        #dns_list is used to append to name
                                    dns_private_lst = list()
                                    my_logger.debug("Populating NYC Dictionary with EC2 records.....")
                                    for i in range(1, lenNameSpl+1): #dns list size = no. of services (mq,search
                                        dns_public_lst.append(ec2Inst.dns_name)
                                    my_logger.debug("Populating AWS Dictionary with EC2 records....") 
                                    for i in range(1, lenNameSpl+1):
                                        dns_private_lst.append(ec2Inst.private_dns_name)
                                   
                                    for i in range(1, lenNameSpl+1): #rearrange names in required format
                                        servNames.append(nameSpl[i-1]+ "." + envName + ".aws." + currZone)
                                    dict_NYC_temp = dict(zip(servNames, dns_public_lst)) #merge 2 lists dns,service
                                    dict_AWS_temp = dict(zip(servNames,dns_private_lst))
                                    for k, v in dict_NYC_temp.items():
                                        dictNyc.update(dict_NYC_temp) #merge dict. for all reservation IDs
                                        dictAws.update(dict_AWS_temp)
                            else:
                                my_logger.debug("No ec2Inst.tags[\"3t:environment\"] found. Skipping.")
                        else:
                            my_logger.debug("Environment name does not match with %s. Skipping." %(envName))
                    else:
                        my_logger.debug("Cannot find 'Name' in ec2Inst.tags. Skipping.")
                else:
                    my_logger.debug("Cannot recognize ec2Inst state. Received: %s. Skipping."%ec2Inst.state)
                        
    except:
        my_logger.error("dnsEc2ToDict failed. Exiting!")
        sys.exit("dnsEc2ToDict failed. Exiting!")
    #print "new_name : %s\n old_name:%s"%(new_name, nameSpl[i]+ "." +tag[1]+ "." +tag[0]+".aws."+currZone)
    #print "dns_public_lst:%s"%dns_public_lst
    #print "dns_private_lst:%s"%dns_private_lst
    #print "servNames: %s"%servNames
    #print "dict_NYC_temp: %s"%dict_NYC_temp
    #print "dict_AWS_temp: %s"%dict_AWS_temp
    my_logger.debug("Collected NYC records dictNyc: %s" % str(dictNyc))
    my_logger.debug("Collected AWS records dictAws: %s" % str(dictAws))
    my_logger.debug("Returning to main() with dictNyc and dictAws")
    return (dictNyc, dictAws)

"""dnsEc2ToDict()...STOP """
"""****************************************************************************************************************"""
"""dnsBeanstalkToDict()...START"""

"""
BEANSTALK fp, fp-lb-1
"""
def dnsBeanstalkToDict(region, envName,currZone):
    my_logger.debug("In ttAWS.dnsBeanstalkToDict....")
    dictBean = dict()
    beanList_key = []
    beanList_value = []
    try:
        l1 = connBotoBeanstalk(region)
        des_env = l1.describe_environments()
        my_logger.debug("Output for describe environments : %s" % des_env)
        for de in des_env["DescribeEnvironmentsResponse"]["DescribeEnvironmentsResult"]["Environments"]:
            my_logger.debug("de (in des_env): %s"%de)
            if de['Health'] == 'Green':
                EnvironmentName = de['EnvironmentName']
                my_logger.debug("EnvironmentName: %s"%EnvironmentName)
                EnvName_temp = EnvironmentName.split("-")[2] + "." + EnvironmentName.split("-")[1]
                my_logger.debug("EnvName_temp: %s, received envName: %s"%(EnvName_temp, envName))
                if EnvName_temp == envName:
                    EnvironmentId = de['EnvironmentId']
                    my_logger.debug("EnvironmentId: %s"%EnvironmentId)
                    des_env_res = l1.describe_environment_resources(EnvironmentId)
                    my_logger.debug("Describe_Environment_Resources: %s"%des_env_res)
                    load_balancer = str(des_env_res['DescribeEnvironmentResourcesResponse']['DescribeEnvironmentResourcesResult']['EnvironmentResources']['LoadBalancers'][0]['Name'])
                    elb_conn = connELB()
                    elb = elb_conn.get_all_load_balancers()
                    my_logger.debug("ELBs available: %s"%elb)
                    elb_list = list()
                    for e in elb:
                        elb_list.append(str(e).split(":")[1])
                    my_logger.debug("Checking if %s in %s"%(load_balancer,elb_list))
                    if load_balancer in elb_list:
                        my_logger.debug("Describe Resources:: LoadBalancer: %s in elb_list: %s. VERIFIED!"%(load_balancer,elb_list))
                        CNAME = de['CNAME']
                        my_logger.debug("CNAME:%s"%CNAME)
                        beanList_value.append(CNAME)
                        my_logger.debug("beanList_value: %s"%beanList_value)
                        CNAME_key = "fp"+"."+ EnvironmentName.split("-")[2] + "." + EnvironmentName.split("-")[1] + "."+ "aws."+currZone
                        beanList_key.append(CNAME_key)
                        my_logger.debug("CNAME_key: %s"% CNAME_key)
                        EndpointURL = de['EndpointURL']
                        my_logger.debug("EndpointURL: %s"%EndpointURL)
                        beanList_value.append(EndpointURL)  # this one is for fp-lb-1.b.dev
                        URL_key = "fp-lb-1"+"."+ EnvironmentName.split("-")[2] +"."+ EnvironmentName.split("-")[1] +"."+"aws."+currZone
                        beanList_key.append(URL_key)
                        my_logger.debug("URL_key: %s"%URL_key)
                        beanList_value.append(EndpointURL)  # this one is for fp-lb-1-b-dev
                        URL_key2 = "fp-lb-1"+"-"+ EnvironmentName.split("-")[2] +"-"+ EnvironmentName.split("-")[1] +"-"+"aws."+currZone
                        beanList_key.append(URL_key2)
                        my_logger.debug("URL_key2: %s"%URL_key2)
                        my_logger.debug("beanList_key: %s"%beanList_key)
                        my_logger.debug("beanList_value: %s"%beanList_value)
                        dictBean = dict(zip(beanList_key, beanList_value))
                        for k,v in dictBean.items():
                            dictNyc[k] = v
                            dictAws[k] = v
                    else:
                        my_logger.error("EndpointURL not in Load Balancer list. Exiting!")
                        sys.exit("EndpointURL not in Load Balancer list. Exiting!")
                else:
                    my_logger.debug("EnvName_temp: %s not in envName:%s"%(EnvName_temp, envName))
            else:
                my_logger.debug("Received Environment Health as %s"%de['Health'])
                    
    except:
        my_logger.error("dnsBeanstalkToDict failed. Exiting!")
        sys.exit("dnsBeanstalkToDict failed. Exiting!")
        
    my_logger.debug("Collected NYC records dictNyc: %s" % str(dictNyc))
    my_logger.debug("Collected AWS records dictAws: %s" % str(dictAws))
    my_logger.debug("Returning to main() with dictNyc and dictAws...")
    return dictNyc, dictAws

"""dnsBeanstalkToDict()...STOP """
"""****************************************************************************************************************"""
"""dnsRdsToDict()...START"""

"""
RDS
"""
def dnsRdsToDict(region, envName,currZone):
    my_logger.debug("In ttAWS.dnsRdsToDict....")
    rds_dict = dict()
    rds_key_list = []
    rds_value_list = []
    try:
        connRds2 = connBotoRds2(region)
        rds_dict_temp1 = dict()
        rds_list_temp = []
        rds_list_temp1 = []
        _dbinstances = connRds2.describe_db_instances()
        #print _dbinstances
        my_logger.debug("Received _dbinstances: %s"%_dbinstances)
        for dbi in _dbinstances['DescribeDBInstancesResponse']['DescribeDBInstancesResult']['DBInstances']:
            my_logger.debug("Checking dbi: %s"%dbi)
            my_logger.debug("Checking DBInstanceStatus... ")
            if dbi['DBInstanceStatus'] =='available':
                my_logger.debug("Received DBInstanceStatus: %s...continuing"%dbi['DBInstanceStatus'])
                _tags = connRds2.list_tags_for_resource("arn:aws:rds:us-east-1:671788406991:db:" + dbi['DBInstanceIdentifier'])
                my_logger.debug("Received _tags: %s"%_tags)
                for _itags in _tags['ListTagsForResourceResponse']['ListTagsForResourceResult']['TagList']:
                    my_logger.debug("In taglist: %s"%_itags)
                    my_logger.debug("Checking if _itags[\'Key\'] is \'3t:environment\'")
                    if '3t:environment' in str(_itags['Key']):
                        my_logger.debug("Received _itags[\'Key\']: %s...continuing"%_itags['Key'])
                        EnvName_spl = _itags['Value'].split(".")
                        EnvName_temp = EnvName_spl[0] + "." + EnvName_spl[1]
                        my_logger.debug("Received EnvName_temp: %s"%EnvName_temp)
                        my_logger.debug("Checking if EnvName_temp: %s == envName:%s..."%(EnvName_temp, envName))
                        if EnvName_temp == envName:
                            my_logger.debug("VERIFIED: EnvName_temp: %s == envName:%s..."%(EnvName_temp, envName))
                            rds_key = "fp-rds-1"+"." + EnvName_temp + "." + "aws."+currZone
                            my_logger.debug("rds_key: %s"%rds_key)
                            rds_key_list.append(rds_key)
                            my_logger.debug("rds_key_list: %s"%rds_key_list)
                            rds_value = dbi["Endpoint"]["Address"]
                            my_logger.debug("RDS Value: %s"%rds_value)
                            rds_value_list.append(rds_value)
                            my_logger.debug("rds_value_list: %s"%rds_value_list)
                        else:
                            my_logger.debug("FAILED: EnvName_temp: %s != envName:%s...skipping"%(EnvName_temp, envName))
                    else:
                        my_logger.debug("Received _itags[\'Key\']: %s...skipping"%_itags['Key'])
            else:
                my_logger.debug("Received DBInstanceStatus: %s...skipping"%dbi['DBInstanceStatus'])
        rds_dict = dict(zip(rds_key_list, rds_value_list))
        my_logger.debug("RDS_dict: %s"%rds_dict)
        for k,v in rds_dict.items():
                dictNyc[k] = v
                dictAws[k] = v    
    except:
        my_logger.error("dnsRdsToDict failed. Exiting....")
        sys.exit("dnsRdsToDict failed. Exiting....")
    my_logger.debug("Collected NYC records: %s" % str(dictNyc))
    my_logger.debug("Collected AWS records: %s" % str(dictAws))   
    my_logger.debug("Returning to main()")
    return (dictNyc, dictAws) 

"""dnsRdsToDict()...STOP """
"""****************************************************************************************************************"""
"""zoneR53()...START""" 
"""
                FIND ZONE ID FOR ZONE NAME
"""
def zoneR53(currZone):
    my_logger.debug("In ttAWS.zoneR53....")
    try:
        connR53 = connRoute53()
        hosted_zones = connR53.get_all_hosted_zones()
        z_Name_Id_dict = dict()
        z_Name_count_dict = dict()
        hz_Name = []
        hz_Id = []
        c_z = 1
        count_zone = []

        for hz in hosted_zones["ListHostedZonesResponse"]["HostedZones"]:
            count_zone.append(c_z)
            hz_Name.append(hz["Name"])
            temp_Id = hz["Id"].split("/")
            hz_Id.append(temp_Id[2])
            c_z = c_z+1

        z_Name_Id_dict = dict(zip(hz_Name,hz_Id))
        Id = z_Name_Id_dict[currZone]
    except:
        my_logger.error("Error during AWS zone validation. Check ttSettings file. Exiting!")
        sys.exit("Error during AWS zone validation. Check ttSettings file. Exiting!")
    my_logger.debug("Returning to main() with ZoneId: %s"%Id)
    return Id

"""zoneR53()...STOP """
"""****************************************************************************************************************"""
"""dnsUpdate()...START""" 
"""
DNS Update/ Upsert boto using new env data
"""
def dnsUpdate(envName, zone_Id, dict_AWS, zone_name):
    print envName
    my_logger.debug("In ttAWS.dnsUpdate....")
    my_logger.debug("Received envName: %s, zone_Id: %s, dict_AWS: %s, zone_name: %s"%(envName, zone_Id, dict_AWS, zone_name))
    try:
        connR53 = connRoute53()
        changes = boto.route53.record.ResourceRecordSets(connR53, zone_Id)
        if dict_AWS:
            my_logger.debug("Found elements to be added in dict_AWS. Continuing!")
            my_logger.debug("dict_AWS: %s"%dict_AWS)
            for k,v in dict_AWS.items():
                my_logger.debug("Checking if envName: %s in k: %s"%(envName, k))
                envName_oth = str(envName.split(".")[0] + "-" + envName.split(".")[1])
                if (envName in k) or (envName_oth in k):
                    my_logger.debug("Checking of 'fp-lb-1 in k: %s"%k)
                    if "fp-lb-1" not in k:
                        my_logger.debug("fp-lb-1 not in k: %s"%k)
                        rr_type = "CNAME"
                        my_logger.debug("Setting rr_type as %s"%rr_type)
                        try:
                            my_logger.debug("Executing changes.addchange(\"UPSERT\", k=%s, rr_type =%s)"%(k, rr_type))
                            change = changes.add_change("UPSERT",k, rr_type)
                            my_logger.debug("Executing changes.addchange() successful!")
                            my_logger.debug("Executing change.add_value(v=%s"%(v))
                            change.add_value(v)
                            my_logger.debug("change.add_value() successful!")
                            my_logger.debug("Executing changes.commit()")
                            result = changes.commit()
                            my_logger.debug("changes.commit() successful!")
                        except:
                            my_logger.error("add_change/add_value/changes.commit() failed for %s record: %s. Exiting!"%(rr_type, k))
                            sys.exit("add_change/add_value/changes.commit() failed for %s record: %s. Exiting!"%(rr_type, k))
                            
                        my_logger.debug("Result: %s"%result)
                        my_logger.debug("%s %s %s inserted successfully"%(rr_type, k,v))
                        ret_str = str("Inserted route53 record: record type: %s, name: %s /%s/ CNAME " % (rr_type, k, v))
                        my_logger.debug("Returning to main() with %s"%ret_str)
                        return ret_str
                    elif "fp-lb-1" in k:
                        my_logger.debug("fp-lb-1 in k: %s"%k)
                        rr_type = "ALIAS"
                        my_logger.debug("Setting rr_type as %s"%rr_type)
                        my_logger.debug("Calling elb_hz_id(v=%s)"%v)
                        try:
                            elb_id = elb_hz_id(v)
                        except:
                            my_logger.error("Calling elb_hz_id() failed. Exiting!")
                            sys.exit("Calling elb_hz_id() failed. Exiting!")
                        my_logger.debug("Calling elb_hz_id() successful! Received elb_id: %s"%elb_id)
                        try:
                            cli53_rrcreate(zone_name, elb_id, k, v, rr_type)
                        except:
                            my_logger.error("cli53_rrcreate() failed for %s record: %s. Exiting!"%(rr_type, k))
                            sys.exit("cli53_rrcreate() failed for %s record: %s. Exiting!"%(rr_type, k))
                            
                        my_logger.debug("%s %s %s inserted successfully"%(rr_type, k,v))
                        ret_str = str("Inserted route53 record: record type: %s, name: %s /%s/ CNAME " % (rr_type, k, v))
                        my_logger.debug("Returning to main() with %s"%ret_str)
                        return ret_str
                else:
                    my_logger.debug("%s or %s not found in k: %s"%(envName, envName_oth, k))
        else:
            my_logger.debug("No elements to be added in dict_AWS. Skipping!")
    except:
        my_logger.error("Route53 update failed for %s record: %s. Exiting!"%(rr_type, k))
        sys.exit("Route53 update failed for %s record: %s. Exiting!"%(rr_type, k))
    
    

"""dnsUpdate()...STOP """
"""****************************************************************************************************************"""
"""elb_hz_id()...START""" 

"""Get ELB hosted zone ID        
"""
def elb_hz_id(v):
    #print v
    elb_conn = connELB()
    elb = elb_conn.get_all_load_balancers()
    #print elb
    for name in elb:
        #print name.dns_name
        if name.dns_name == v:
            hz_id = name.canonical_hosted_zone_name_id
    return hz_id

"""elb_hz_id()...STOP """
"""****************************************************************************************************************"""
"""cli53_rrcreate()...START"""     
"""
cli53 rrcreate
"""    
def cli53_rrcreate(zone_name, zone_Id, rr_name, rr_value, rr_type):
    my_logger.debug("Running export command for cli53 module")
    try:
        path_curr = os.getcwd()
        print "path_curr: %s"%path_curr
        command = path_curr + '/cli53 rrcreate ' + zone_name + " " + rr_name + " " + rr_type + " \'%s %s\'"%(zone_Id, rr_value)
        print "command: %s"%command
        proc1 = subprocess.Popen([command], stdout = subprocess.PIPE, shell = True)
        (out1, err1) = proc1.communicate()
    except:
        my_logger.error("Cli53 export failed! Exiting....")
        sys.exit("Cli53 export failed! Exiting....")
    my_logger.info("Output: %s, Error: %s"%(out1, err1))        

"""cli53_rrcreate()...STOP """
"""****************************************************************************************************************"""
"""cli53_export()...START""" 
        
def route53Records(zone_id, ENV_NAME):
    my_logger.debug("Updating currAws dict based on values on rrsets(%s)"%zone_id)
    try:
        connR53 = connRoute53()
        sets = connR53.get_all_rrsets(zone_id)
        for rset in sets:
            if ENV_NAME in str(rset):
                str_rset = str(rset)
                if "ALIAS" in str_rset:
                    rset_sub_value = str(rset).split("ALIAS")
                    for rval in rset_sub_value:
                        if "elb.amazonaws.com" in rval:
                            rval_spl = rval.split(" ")
                            for rv in rval_spl:
                                if "elb.amazonaws.com" in rv:
                                    rset_value = [rv, ]
                                    currAws.update({rset.name: rset_value})
                    
                elif "ALIAS" not in str_rset:
                    currAws.update({rset.name: rset.resource_records})
    except:
        my_logger.error("Route53 records collection failed! Exiting!")
        sys.exit("Route53 records collection failed! Exiting!")
    my_logger.debug("Returning to main() with currAws as %s"%currAws)
    return currAws

"""route53Records()...STOP """
"""****************************************************************************************************************"""
"""cli53_export()...START""" 

def cli53_export(zone, dir_name, suffix):
    my_logger.debug("Running export command for cli53 module")
    try:
        path_curr = os.getcwd()
        command = path_curr + '/cli53 export ' + zone + ' > ' + dir_name + suffix
        proc1 = subprocess.Popen([command], stdout = subprocess.PIPE, shell = True)
        (out1, err1) = proc1.communicate()
    except:
        my_logger.error("Cli53 export failed! Exiting....")
        sys.exit("Cli53 export failed! Exiting....")
    my_logger.info("Cli53 export successful! Find the backup in %s" %dir_name)
    
"""cli53_export()...STOP """
"""****************************************************************************************************************"""
"""find_rrtype()...START"""     
"""
find rr_type: cli53 export
"""
def find_rrtype(zone, lb_rec_name):
    my_logger.debug("Running find rr_type command for cli53 module")
    #print lb_rec_name
    #print zone
    lb_rec_name = str(lb_rec_name).strip(zone)
    #print "New lb_rec_name: %s"%lb_rec_name
    my_logger.debug("Calling Cli53 export...")
    try:
        path_curr = os.getcwd()
        command = path_curr + '/cli53 export ' + zone
        proc1 = subprocess.Popen([command], stdout = subprocess.PIPE, shell = True)
        (out1, err1) = proc1.communicate()
    except:
        my_logger.error("Cli53 export failed. Exiting!")
        sys.exit("Cli53 export failed. Exiting!")
    my_logger.info("Cli53 export Successful!")
    
    rec_list = out1.split("\n")
    #print rec_list
    for rec in rec_list:        
        if (lb_rec_name in rec) and ("CNAME" in rec):
            #print rec
            rr_type = "CNAME"
            #print "rr_type: %s"%rr_type
            my_logger.debug("rr_type determined as %s"%rr_type)
            return rr_type
        elif (lb_rec_name in rec) and ("ALIAS" in rec):
            #print rec
            rr_type = "ALIAS"
            #print "rr_type: %s"%rr_type
            my_logger.debug("rr_type determined as %s"%rr_type)
            return rr_type
        #my_logger.debug("rr_type determined as %s"%rr_type)
    #return rr_type
    
"""find_rrtype()...STOP """
"""****************************************************************************************************************"""
"""del_cname()...START"""      

def del_cname(zone, rr_name, rr_type):
    my_logger.debug("Running del_cname command for cli53 module")
    try:
        path_curr = os.getcwd()
        command = path_curr + '/cli53 rrdelete ' + zone + " " + rr_name + " " + rr_type
        #print command
        proc1 = subprocess.Popen([command], stdout = subprocess.PIPE, shell = True)
        (out1, err1) = proc1.communicate()
    except:
        my_logger.error("Cli53 del_cname failed! Exiting....")
        sys.exit("Cli53 del_cname failed! Exiting....")
    my_logger.info("Output: %s, Error: %s"%(out1, err1))
       
"""del_cname()...STOP """
"""****************************************************************************************************************"""
"""findActiveEnv()...START"""    
    
def findActiveEnv():
    my_logger.debug("Finding active environment...")
    try:
        my_logger.debug("Evaluating fp_internal_dns....")
        dns_int = callSubprocess(_fp_dns_internal_cmd)
        _fp_dns_internal = dns_int.split("\n")[2]
        my_logger.debug("fp_dns_internal: %s" % _fp_dns_internal)
        my_logger.debug("Evaluating fp_external_dns....")
        dns_ext = callSubprocess(_fp_dns_external_cmd)
        _fp_dns_external = dns_ext.split("\n")[1]
        my_logger.debug("fp_dns_external: %s" % _fp_dns_external)
        my_logger.debug("Evaluating _fp_live.....")
        live = callSubprocess(_fp_live_cmd)
        _fp_live = live.split("\n")[0][3]
        my_logger.debug("fp_live: %s" % _fp_live)
        my_logger.debug("Evaluating _fp_live_dns.....")
        live_dns = callSubprocess(_fp_live_dns_cmd)
        _fp_live_dns = live_dns.split("\n")[0]
        my_logger.debug("live_dns: %s" % _fp_live_dns)
        
        """
        ORIGINAL CODE WITH EXTERNAL AND INTERNAL CHECK
        **********************************************
        if _fp_dns_internal != _fp_dns_external:
            my_logger.error("The Internal Active Directory DNS and external AWS Route 53 DNS servers think different environments are live!  Internal DNS says %s and external says %s. EXITING!" %(_fp_dns_internal, _fp_dns_external))
            sys.exit("ERROR: The Internal Active Directory DNS and external AWS Route 53 DNS servers think different environments are live!  Internal DNS says %s and external says %s. EXITING!" %(_fp_dns_internal, _fp_dns_external))
        else:
            if _fp_live != "a" and _fp_live != "b":
                my_logger.error("ERROR: Could not resolve which prod environment is live, A or B. Script got %s from %s. EXITING!" %(_fp_live,_fp_live_dns))
                sys.exit("ERROR: Could not resolve which prod environment is live, A or B. Script got %s from %s. EXITING!" %(_fp_live,_fp_live_dns))
            else:
                if _fp_live == "a":
                    _copy_source_prefix = "prod-a"
                elif _fp_live == "b": 
                    _copy_source_prefix = "prod-b"
        """
        
        """
        #NEW CODE W/O EXTERNAL AND INTERNAL CHECK START
        **********************************************"""
    
        if _fp_live != "a" and _fp_live != "b":
            my_logger.error("Could not resolve which prod environment is live, A or B. Script got %s from %s. Run with option \"-p\" if you are in maintenance mode (to set production environment). EXITING!" %(_fp_live,_fp_live_dns))
            sys.exit("ERROR: Could not resolve which prod environment is live, A or B. Script got %s from %s. Run with option \"-p\" if you are in maintenance mode (to set production environment). EXITING! " %(_fp_live,_fp_live_dns)) 
        else:
            if _fp_live == "a":
                _copy_source_prefix = "prod-a"
            elif _fp_live == "b": 
                _copy_source_prefix = "prod-b"
    
        """
        #NEW CODE W/O EXTERNAL AND INTERNAL CHECK STOP
        **********************************************"""
    
        
    except:
        my_logger.error("Unexpected Error Encountered. Cannot deter Exiting!")
        sys.exit("Unexpected Error Encountered. Exiting!")
    
    
    """
    #new insert start
    
    _copy_source_prefix = "prod-b"
    
    #new insert stop
    """
    
    
    my_logger.debug("Returning to main() with _copy_source_prefix: %s" % _copy_source_prefix)
    return _copy_source_prefix
    
"""findActiveEnv()...STOP """
"""****************************************************************************************************************"""
"""findSourceSuffix()...START"""        

def findSourceSuffix():
    try:
        my_logger.debug("Calling findActiveEnv()...") 
        activeEnv = findActiveEnv()
        my_logger.debug("Received Active Environment: %s" % activeEnv)
        activeEnvSplit = activeEnv.split("-")
        if activeEnvSplit[0] != "prod":
            my_logger.error("Error encountered in active env determination. Exiting!")
            sys.exit("Error encountered in active env determination. Exiting!")
        else:
            source_suffix = activeEnvSplit[1] + "." + activeEnvSplit[0] + ".aws.3top.com"
            my_logger.debug("Env_name: %s" %source_suffix)
        my_logger.debug("Returning to main()")
    except:
        my_logger.error("Unexpected Error Encountered. Exiting!")
        sys.exit("Unexpected Error Encountered. Exiting!")
    my_logger.debug("Returning to main() with Source suffix as %s"%source_suffix)
    return source_suffix    

"""findSourceSuffix()...START""" 
"""****************************************************************************************************************"""
""" Validate if the AWS certificate is located in path provided in dict_ttSettings... start """
    
def validate_cert_loc(mysql_cert):
    #print mysql_cert
    try:
        my_logger.debug("Validating location of certificate in %s"%(mysql_cert))
        if os.path.isfile(mysql_cert):
            my_logger.debug("Location verified. Continuing!")
        else:
            my_logger.error("MySQL Certificate Location cannot be verified. Exiting!")
            sys.exit("MySQL Certificate Location cannot be verified. Exiting!")
            #sys.exit("Program terminated on error.")
    except:
        my_logger.error("Error Encountered during verification of certificate in %s  Exiting!" %(mysql_cert))
        sys.exit("Error Encountered during verification of certificate in %s  Exiting!" %(mysql_cert))
    
""" Validate if the AWS certificate is located in path provided in dict_ttSettings... stop  """
"""****************************************************************************************************************"""
"""env_ttSettings()...START"""

def env_ttSettings(env_name):
    my_logger.debug("Downloading ttSettings using env_ttSettings...")
    if "aws.3top.com" in env_name:
        s3_config_folder = "aws-" + env_name.split(".")[1] + "/" + env_name.split(".")[0]
        my_logger.debug("Found \"aws.3top.com\" in env_name: %s. Setting s3_config_folder as %s"%(env_name, s3_config_folder))
    elif "nyc.3top.com" in env_name:
        s3_config_folder = "nyc-dev"
        my_logger.debug("Found \"nyc.3top.com\" in env_name: %s. Setting s3_config_folder as %s"%(env_name, s3_config_folder))
    try:
        s3_config_filename = "ttSettings.conf"
        my_logger.debug("Pulling dict_ttSettings from %s"%s3_config_filename)
        global dict_source_ttSettings
        dict_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
    except:
        my_logger.error("Download of ttSettings from S3 failed. Exiting! ")
        sys.exit("Download of ttSettings from S3 failed. Exiting! ")
    my_logger.debug("Download of s3 settings complete! Returning to main() with dict_ttSettings")   
    return dict_ttSettings

"""env_ttSettings()...STOP """
"""****************************************************************************************************************"""
"""collect_security_groups_info()...START"""
      
def collect_security_groups_info(region, s_host, t_host):
    my_logger.debug("Calling EC2 connection...")
    conn = connBotoEc2(region)
    
    s_host_list = s_host.split(".")
    s_env = s_host_list[1] + "." + s_host_list[2] + "." + s_host_list[3] + "." + s_host_list[4] + "." + s_host_list[5]
    s_service = s_host_list[0]
    my_logger.debug("s_service: %s"%s_service)
    my_logger.debug("s_env: %s"%s_env)
    
    t_host_list = t_host.split(".")
    t_env = t_host_list[1] + "." + t_host_list[2] + "." + t_host_list[3] + "." + t_host_list[4] + "." + t_host_list[5]
    t_service = t_host_list[0]
    my_logger.debug("t_service: %s"%t_service)
    my_logger.debug("t_env: %s"%t_env)
    
    sg_list = conn.get_all_security_groups()
    my_logger.debug("List of security groups: %s"%sg_list)
    
    for sg in sg_list:
        sg_tags = sg.tags
        if ("3t:environment" in sg_tags) and ("3t:service" in sg_tags):
                if (sg_tags["3t:environment"] == s_env) and (sg_tags["3t:service"] == s_service):
                                group_id = sg.id
                                #s_owner_id = sg.owner_id
                                group_name = sg.name
                                s_rules = sg.rules
                                #s_tag = s_env + "." + s_service
                                my_logger.debug("group_id: %s, group_name: %s, s_rules: %s"%(group_id, group_name, s_rules))
                                
                elif (sg_tags["3t:environment"] == t_env) and (sg_tags["3t:service"] == t_service):
                                src_security_group_name = sg.name
                                src_security_group_owner_id = sg.owner_id
                                src_security_group_group_id = sg.id
                                t_rules = sg.rules
                                #t_tag = t_env + "." + t_service
              changes.add_change                  my_logger.debug("src_security_group_name: %s, src_security_group_owner_id: %s, src_security_group_group_id: %s,\
                                 t_rules: %s"%(src_security_group_name, src_security_group_owner_id, src_security_group_group_id, t_rules))
    
    for rule in s_rules:
            s_grants = rule.grants
            my_logger.debug("s_grants: %s"%(s_grants))
            
    for rule in t_rules:
            from_port = rule.from_port
            to_port = rule.to_port
            ip_protocol = rule.ip_protocol 
            my_logger.debug("ip_protocol: %s, from_port: %s, to_port: %s"%(ip_protocol, from_port, to_port))
    #print conn, group_name, src_security_group_name, src_security_group_owner_id, ip_protocol, from_port, to_port, group_id, src_security_group_group_id, s_grants
    return (conn, group_name, src_security_group_name, src_security_group_owner_id, ip_protocol, from_port, to_port, s_grants)

"""collect_security_groups_info()...STOP """
"""****************************************************************************************************************"""
"""test_grants()...START"""

def test_grants(grants, t_name):
    #print t_tag
    for g in grants:
        #print g
        g_exist_list = list()
        if (str(t_name) in str(g)):
            my_logger.debug("Grants for this security group already exist.")
            print ("Grants for this security group already exist.")
            g_exist_list.append(True)
        else:
            g_exist_list.append(False)
    #print g_exist_list
    g_exist = False
    for g in g_exist_list:
        g_exist = g_exist or g 
    
    print"g_exist: %s"%g_exist   
    return g_exist

"""test_grants()...STOP """
"""****************************************************************************************************************"""
"""authorize_security_groups()...START"""
    
def authorize_security_groups(conn, g_name, src_s_g_name, src_s_g_owner_id, ip_prot, f_port, t_port):
    my_loggerchanges.add_change.debug("Performing authorize_security_group()...") 
    conn.authorize_security_group(group_name = g_name, src_security_group_name = src_s_g_name, \
                                src_security_group_owner_id = src_s_g_owner_id, ip_protocol = ip_prot,\
                                 from_port = f_port, to_port = t_port)
    

"""authorize_security_groups()...STOP """
"""****************************************************************************************************************"""
"""revoke_security_groups()...START"""
    
def revoke_security_groups(conn, g_name, src_s_g_name, src_s_g_owner_id, ip_prot, f_port, t_port):
    my_logger.debug("Performing revoke_security_group()...") 
    conn.revoke_security_group(group_name = g_name, src_security_group_name = src_s_g_name, \
                                src_security_group_owner_id = src_s_g_owner_id, ip_protocol = ip_prot,\
                                 from_port = f_port, to_port = t_port)
    
"""revoke_security_groups()...STOP"""
"""****************************************************************************************************************"""    
        
        
    
    

