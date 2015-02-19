#!/usr/bin/python
import logging
import ttLib.logger as logger
import os
import subprocess
import sys
my_logger = logging.getLogger(os.environ['logger_name'])
import ttLib.ttLx as ttLx
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
    
def connBotoRds2(region):
    my_logger.debug("Initializing boto.rds2.connect_to_region(%s)...."%region)
    try:
        connRds2 = boto.rds2.connect_to_region(region)
    except:
        my_logger.error("Error Encountered in:  Boto RDS connection. Exiting!" )
        sys.exit("Error Encountered in:  Boto RDS connection. Exiting!" )
    my_logger.debug("Boto RDS connection successful! Returning to main() with RDS connection")
    return connRds2
    
def connRoute53():
    my_logger.debug("Initializing boto.connect_route53()....")
    try:
        connR53 = boto.connect_route53()
    except:
        my_logger.error("Error Encountered. Boto Route53 connection failed! Exiting....")
        sys.exit("Error Encountered. Boto Route53 connection failed! Exiting....")
    my_logger.debug("Boto Route53 connection successful! Returning to main() with Route53 connection")
    return connR53 

def connELB():
    my_logger.debug("Initializing boto.ElasticLoadBalancer.connect()....")
    try:
        conn_elb = boto.connect_elb()
    except:
        my_logger.error("Error Encountered in: Boto ElasticLoadBalancer connection. Exiting!")
        sys.exit("Error Encountered in: Boto ElasticLoadBalancer connection. Exiting!")
    l1 = boto.beanstalk.layer1.Layer1()
    my_logger.debug("Boto ElasticLoadBalancer connection successful! Returning with ElasticLoadBalancer")
    return conn_elb

"""
This part finds the number of environments available and allows the user 
to choose the env. This finds the number of environments and collects the environments
list and allocates and index. The user can input the index to choose the environment
whose dns records need to be exported. The Environment name will be a.prod, b.prod, a.dev etc...
"""
def selectEnviron(region):
    try:
        l1 = connBotoBeanstalk(region)
        des_env = l1.describe_environments()
        my_logger.debug("Output for describe environments : %s" % des_env)
        
        app_env = list()
        
        for de in des_env["DescribeEnvironmentsResponse"]["DescribeEnvironmentsResult"]["Environments"]:
            
            i_app = de['ApplicationName'].split("-")[1]
            i_env = de['EnvironmentName'].split("-")[1] + "-" + de['EnvironmentName'].split("-")[2]
            app_env.append(i_app + "_" + i_env)
    except:
        my_logger.error("Error Encountered in determining the application environments list. Exiting...." )
        sys.exit("Error Encountered in determining the application environments list. Exiting...." )
    my_logger.debug("Application environments determined as %s."%app_env)
    try:
        my_logger.debug("Calling findActiveEnv()...")
        envActive = findActiveEnv()
        my_logger.debug("Received active environment information as : %s" %envActive)
        print("Received active environment information as : %s" %envActive)
        for app in app_env:
            if app.split("_")[1] == envActive:
                app_env.remove(app)
            if (app.split("_")[1][:4] == "dev-") or (app.split("_")[1][:5] == "prod-"):
                    continue
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
def dnsEc2ToDict(region, envName,currZone):
    my_logger.debug("In ttAWS.dnsEc2ToDict....")
    try:
        my_logger.debug("Connecting to EC2")
        connEc2 = connBotoEc2(region)
        my_logger.debug(connEc2)
        my_logger.debug("Collecting EC2 reservations...")
        ec2Reservations = connEc2.get_all_instances()
        my_logger.debug("List of reservations are: %s" %ec2Reservations)
        for resv in ec2Reservations:    #parsing through information in reservations
            my_logger.debug("Resv: %s"%resv)
            for ec2Inst in resv.instances:    #parses through instances for each reservation
                my_logger.debug("EC2Inst: %s"%ec2Inst)
                name = ec2Inst.tags['Name']
                my_logger.debug("Name: %s"% name)
                nameSpl = name.split(":")#Split name 'prod-a:mq:search'-store in list
                tag = nameSpl[0].split("-")    #split 'prod-a' part of name
                lenNameSpl = len(nameSpl)    #find len. of list to define range
                servNames = list()        #define list to store the new name format
                dns_public_lst = list()        #dns_list is used to append to name
                dns_private_lst = list()
                my_logger.debug("Populating NYC Dictionary with EC2 records.....")
                for i in range(1, lenNameSpl): #dns list size = no. of services (mq,search
                    dns_public_lst.append(ec2Inst.dns_name)
                my_logger.debug("Populating AWS Dictionary with EC2 records....") 
                for i in range(1, lenNameSpl):
                    dns_private_lst.append(ec2Inst.private_dns_name)
                for i in range(1, lenNameSpl): #rearrange names in required format
                    servNames.append(nameSpl[i]+ "." +tag[1]+ "." +tag[0]+".aws."+currZone)
                dict_NYC_temp = dict(zip(servNames, dns_public_lst)) #merge 2 lists dns,service
                dict_AWS_temp = dict(zip(servNames,dns_private_lst))
                for k, v in dict_NYC_temp.items():
                    if envName in k:
                        dictNyc.update(dict_NYC_temp) #merge dict. for all reservation IDs
                        dictAws.update(dict_AWS_temp)
    except:
        my_logger.error("dnsEc2ToDict failed. Exiting!")
        sys.exit("dnsEc2ToDict failed. Exiting!")
    my_logger.debug("Collected NYC records dictNyc: %s" % str(dictNyc))
    my_logger.debug("Collected AWS records dictAws: %s" % str(dictAws))
    my_logger.debug("Returning to main() with dictNyc and dictAws")
    return (dictNyc, dictAws)

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
            EnvironmentName = de['EnvironmentName']
            CNAME = de['CNAME']
            beanList_value.append(CNAME)
            EndpointURL = de['EndpointURL']
            beanList_value.append(EndpointURL)
            EnvName_temp = EnvironmentName.split("-")
            CNAME_key = "fp"+"."+ EnvName_temp[2]+ "." +EnvName_temp[1]+ "."+ "aws."+currZone
            URL_key = "fp-lb-1"+"."+EnvName_temp[2]+"."+EnvName_temp[1]+"."+"aws."+currZone
            beanList_key.append(CNAME_key)
            beanList_key.append(URL_key)
            my_logger.debug("CNAME_key: %s"% CNAME_key)
            my_logger.debug("URL_key: %s"%URL_key)
            dictBean = dict(zip(beanList_key, beanList_value))
            for k,v in dictBean.items():
                if envName in URL_key:
                    dictNyc[k] = v
                    dictAws[k] = v
    except:
        my_logger.error("dnsBeanstalkToDict failed. Exiting!")
        sys.exit("dnsBeanstalkToDict failed. Exiting!")
        
    my_logger.debug("Collected NYC records dictNyc: %s" % str(dictNyc))
    my_logger.debug("Collected AWS records dictAws: %s" % str(dictAws))
    my_logger.debug("Returning to main() with dictNyc and dictAws...")
    return dictNyc, dictAws

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
        for dbi in _dbinstances['DescribeDBInstancesResponse']['DescribeDBInstancesResult']['DBInstances']:
            _tags = connRds2.list_tags_for_resource("arn:aws:rds:us-east-1:671788406991:db:" + dbi['DBInstanceIdentifier'])
            for _itags in _tags['ListTagsForResourceResponse']['ListTagsForResourceResult']['TagList']:
                rds_list_temp.append(_itags["Key"])
                rds_list_temp1.append(_itags["Value"])
            rds_dict_temp1 = dict(zip(rds_list_temp, rds_list_temp1))
            my_logger.debug("rds_dict_temp1: %s"%rds_dict_temp1)
            name_rds = rds_dict_temp1.get("elasticbeanstalk:environment-name")
            name_rds_new = name_rds.split("-")
            rds_key = "fp-rds-1"+"." + name_rds_new[2]+ "." + name_rds_new[1] + "." + "aws."+currZone
            my_logger.debug("rds_key: %s"%rds_key)
            rds_key_list.append(rds_key)
        for dbi_key in  _dbinstances['DescribeDBInstancesResponse']['DescribeDBInstancesResult']['DBInstances']:
            rds_value = dbi_key["Endpoint"]["Address"]
            my_logger.debug("RDS Value: %s"%rds_value)
            rds_value_list.append(rds_value)
        rds_dict = dict(zip(rds_key_list, rds_value_list))
        my_logger.debug("RDS_dict: %s"%rds_dict)
        
        for k,v in rds_dict.items():
            if envName in k:
                dictNyc[k] = v
                dictAws[k] = v    
    except:
        my_logger.error("dnsRdsToDict failed. Exiting....")
        sys.exit("dnsRdsToDict failed. Exiting....")
    my_logger.debug("Collected NYC records: %s" % str(dictNyc))
    my_logger.debug("Collected AWS records: %s" % str(dictAws))   
    my_logger.debug("Returning to main()")
    return (dictNyc, dictAws) 

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

"""
DNS Update/ Upsert boto using new env data
"""
def dnsUpdate(envName, zone_Id, dict_AWS, zone_name):
    my_logger.debug("In ttAWS.dnsUpdate....")
    try:
        connR53 = connRoute53()
        changes = boto.route53.record.ResourceRecordSets(connR53, zone_Id)
        for k,v in dict_AWS.items():
            if envName in k:
                #print k,v 
                if "fp-lb-1" not in k:
                    #print "fp-lb-1 not in k"
                    rr_type = "CNAME"
                    change = changes.add_change("UPSERT",k, rr_type)
                    change.add_value(v)
                    result = changes.commit()
                    my_logger.debug("Result: %s"%result)
                    my_logger.debug("CNAME %s %s inserted successfully"%(k,v))
                elif "fp-lb-1" in k:
                    #print "fp-lb-1 in k"
                    rr_type = "ALIAS"
                    elb_id = elb_hz_id(v)
                    #print elb_id
                    cli53_rrcreate(zone_name, elb_id, k, v, rr_type)
                    my_logger.debug("%s %s %s inserted successfully"%(rr_type, k,v))
    except:
        my_logger.error("Route53 update failed for %s record: %s. Exiting!"%(rr_type, k))
        sys.exit("Route53 update failed for %s record: %s. Exiting!"%(rr_type, k))
    
    ret_str = str("Inserted route53 record: record type: %s, name: %s /%s/ CNAME " % (rr_type, k, v))
    my_logger.debug("Returning to main() with %s"%ret_str)
    return ret_str

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
    
"""
cli53 rrcreate
"""    
def cli53_rrcreate(zone_name, zone_Id, rr_name, rr_value, rr_type):
    my_logger.debug("Running export command for cli53 module")
    try:
        path_curr = os.getcwd()
        command = path_curr + '/cli53 rrcreate ' + zone_name + " " + rr_name + " " + rr_type + " \'%s %s\'"%(zone_Id, rr_value)
        #print command
        proc1 = subprocess.Popen([command], stdout = subprocess.PIPE, shell = True)
        (out1, err1) = proc1.communicate()
    except:
        my_logger.error("Cli53 export failed! Exiting....")
        sys.exit("Cli53 export failed! Exiting....")
    my_logger.info("Output: %s, Error: %s"%(out1, err1))        
        
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

        
"""
cli53 export
"""
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
    my_logger.info("Output: %s, Error: %s"%(out1, err1))
    
"""
find rr_type: cli53 export
"""
def find_rrtype(zone, lb_rec_name):
    my_logger.debug("Running find rr_type command for cli53 module")
    #print lb_rec_name
    #print zone
    lb_rec_name = str(lb_rec_name).strip(zone)
    #print "New lb_rec_name: %s"%lb_rec_name
    try:
        path_curr = os.getcwd()
        command = path_curr + '/cli53 export ' + zone
        proc1 = subprocess.Popen([command], stdout = subprocess.PIPE, shell = True)
        (out1, err1) = proc1.communicate()
        my_logger.info("Output: %s, Error: %s"%(out1, err1))
        rec_list = out1.split("\n")
        #print rec_list
        for rec in rec_list:
                if (lb_rec_name in rec) and ("CNAME" in rec):
                    #print rec
                    rr_type = "CNAME"
                    #print "rr_type: %s"%rr_type
                    return rr_type
                elif (lb_rec_name in rec) and ("ALIAS" in rec):
                    #print rec
                    rr_type = "ALIAS"
                    #print "rr_type: %s"%rr_type
                    return rr_type
    except:
        my_logger.error("Cli53 find rr_type failed! Exiting....")
        sys.exit("Cli53 find rr_type failed! Exiting....")
    
    

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
       
    
    
def findActiveEnv():
    my_logger.debug("Finding active environment...")
    try:
        my_logger.debug("Evaluating fp_internal_dns....")
        dns_int = ttLx.callSubprocess(_fp_dns_internal_cmd)
        _fp_dns_internal = dns_int.split("\n")[2]
        my_logger.debug("fp_dns_internal: %s" % _fp_dns_internal)
        my_logger.debug("Evaluating fp_external_dns....")
        dns_ext = ttLx.callSubprocess(_fp_dns_external_cmd)
        _fp_dns_external = dns_ext.split("\n")[1]
        my_logger.debug("fp_dns_external: %s" % _fp_dns_external)
        my_logger.debug("Evaluating _fp_live.....")
        live = ttLx.callSubprocess(_fp_live_cmd)
        _fp_live = live.split("\n")[0][3]
        my_logger.debug("fp_live: %s" % _fp_live)
        my_logger.debug("Evaluating _fp_live_dns.....")
        live_dns = ttLx.callSubprocess(_fp_live_dns_cmd)
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
        NEW CODE W/O EXTERNAL AND INTERNAL CHECK START
        **********************************************
        """
        if _fp_live != "a" and _fp_live != "b":
            my_logger.error("Could not resolve which prod environment is live, A or B. Script got %s from %s. EXITING!" %(_fp_live,_fp_live_dns))
            sys.exit("ERROR: Could not resolve which prod environment is live, A or B. Script got %s from %s. EXITING!" %(_fp_live,_fp_live_dns))
        else:
            if _fp_live == "a":
                _copy_source_prefix = "prod-a"
            elif _fp_live == "b": 
                _copy_source_prefix = "prod-b"
        """
        NEW CODE W/O EXTERNAL AND INTERNAL CHECK STOP
        **********************************************
        """
        
    except:
        my_logger.error("Unexpected Error Encountered. Exiting!")
        sys.exit("Unexpected Error Encountered. Exiting!")
    my_logger.debug("Returning to main() with _copy_source_prefix: %s" % _copy_source_prefix)
    return _copy_source_prefix
        

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

def env_ttSettings(env_name):
    my_logger.debug("Downloading ttSettings using env_ttSettings...")
    if "aws.3top.com" in env_name:
        s3_config_folder = "aws-" + env_name.split(".")[1] + "/" + env_name.split(".")[0]
        my_logger.debug("Found \"aws.3top.com\" in env_name: %s. Setting s3_config_folder as %s"%(env_name, s3_config_folder))
    elif "nyc.3top.com" in env_name:
        s3_config_folder = "nyc-dev"
        my_logger.debug("Found \"aws.3top.com\" in env_name: %s. Setting s3_config_folder as %s"%(env_name, s3_config_folder))
    try:
        s3_config_filename = "ttSettings.conf"
        my_logger.debug("Pulling dict_ttSettings from %s"%s3_config_filename)
        global dict_source_ttSettings
        dict_ttSettings = get_s3_config(s3_config_folder, s3_config_filename)
    except:
        my_logger.debug("Download of ttSettings from S3 failed. Exiting! ")
        sys.exit("Download of ttSettings from S3 failed. Exiting! ")
    my_logger.debug("Download of s3 settings complete! Returning to main() with dict_ttSettings")   
    return dict_ttSettings
        
        

