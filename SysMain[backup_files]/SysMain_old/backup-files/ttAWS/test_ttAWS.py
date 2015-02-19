#!/usr/bin/python

class ttAWS():
    
    import logging
    import ttLib.logger as logger
    import os
    my_logger = logging.getLogger(os.environ['logger_name'])
    import subprocess
    import shlex
    import sys
    from pprint import pprint
    import subprocess
    import json
    import ast
    import ttLib.ttLx as ttLx
    from ttLib.ttLx import check_files
    
    """
    my_logger = logging.getLogger('ttAWS')
    for h in my_logger.handlers:
        my_logger.removeHandler(h)
    my_logger.addHandler(logger.get_handler())
    my_logger.setLevel(logging.DEBUG)
    """
    
    try:
        my_logger.info("Trying to import Boto AWS APIs....")
        import boto
        import boto.ec2
        import boto.rds2
        import boto.beanstalk
        import boto.route53
        my_logger.debug("Import of Boto-EC2, RDS2, Beanstalk, Route53, S3 successful. Boto connection validated!")
    except:
        e = sys.exc_info()[0]
        my_logger.error("Error Encountered: %s. Please run the requirements file to install boto and DNSPython. Exiting...." % e)
        sys.exit(-1)
    dictNyc=dict()
    dictAws=dict()
    currAws=dict()
    
    """
    Define Global Constants
    """
    _aws_http_root="www.3top.com"
    _aws_dns_suffix="aws.3top.com"
    
    _fp_live_cmd="dig +short " + _aws_http_root + " | grep " + _aws_dns_suffix
    _fp_live_dns_cmd="dig +short " + _aws_http_root + " | grep " + _aws_dns_suffix
    _fp_dns_internal_cmd="dig " + _aws_http_root + " +short"
    _fp_dns_external_cmd="dig @8.8.8.8 " + _aws_http_root + " +short"
    
    def auth_func(self, region):
        my_logger.info(" In ttAWS.authFunc....")
        try:
            boto.ec2.connect_to_region(region)
            my_logger.info("Boto authentication success!")
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered: %s. Boto authentication failed! Configure credentials in the ~/.boto file to allow authentication. Exiting...." % e)
            sys.exit(-1)
        return
    
    """
    Here the connections to boto EC2 API are made. When an error, system prompts the user to correctly configure the Credentials in the ~/.boto file
    This will allow the user to be authenticated
    """
    def connBotoEc2(region):
        my_logger.info("In ttAWS.connBotoEc2....")
        try:
            connEc2=boto.ec2.connect_to_region(region)
            my_logger.info("Boto EC2 connection success!")
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered: %s. Boto EC2 connection failed! Exiting...." % e)
            sys.exit(-1)       
        return connEc2
        
    def connBotoBeanstalk(region):
        my_logger.info("In ttAWS.connBoto2Beanstalk....")
        try:
            boto.beanstalk.connect_to_region(region)
            my_logger.info("Boto Beanstalk connection success!")
            
        except:
            my_logger.error("Error Encountered. Boto Beanstalk connection failed! Exiting....")
            sys.exit(-1)
        l1=boto.beanstalk.layer1.Layer1()
        return l1
        
    def connBotoRds2(region):
        my_logger.info("In ttAWS.connBotoRds2....")
        try:
            connRds2=boto.rds2.connect_to_region(region)
            my_logger.info("Boto RDS connection success!")
        except:
            my_logger.error("Error Encountered. Boto RDS connection failed! Exiting...." )
            sys.exit(-1)
        return connRds2
        
    def connRoute53():
        my_logger.info("In ttAWS.connBotoRoute53....")
        try:
            connR53=boto.connect_route53()
            my_logger.info("Boto Route53 connection success!")
        except:
            my_logger.error("Error Encountered. Boto Route53 connection failed! Exiting....")
            sys.exit(-1)
        return connR53 
    
    """
    This part finds the number of environments available and allows the user 
    to choose the env. This finds the number of environments and collects the environments
    list and allocates and index. The user can input the index to choose the environment
    whose dns records need to be exported. The Environment name will be a.prod, b.prod, a.dev etc...
    """
    def selectEnviron(region):
        my_logger.info("In ttAWS.selectEnviron....")
        try:
            connEc2=connBotoEc2(region)
            my_logger.info("Connection to EC2 success!")
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered: %s. Connection to EC2 failed!Exiting...." % e)
            sys.exit(-1)
        try:
            ec2Reservations=connEc2.get_all_instances()
            my_logger.info("EC2 Reservations collection complete!")
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered %s. EC2 Reservations collection failed! Exiting...." % e)
            sys.exit(-1)
        try:
            appCount=[]
            tempApp=[]
            appDict=dict()
            for countReserv in ec2Reservations:
                for reservInstance in countReserv.instances:
                    tempEnvName=reservInstance.tags['Name']
                    tempApp.append(tempEnvName.split(":")[0])
            appCount=list(set(tempApp))
            my_logger.info("Current environments: %s " %str(appCount))
            envActive=findActiveEnv()
            my_logger.info("Received active environment information as : %s" %envActive)
            appCount.remove(envActive)
            my_logger.info("List after removing active environment: %s" %str(appCount))
            #print "Old list", appCount
            for env_accurate in appCount:
                if (env_accurate[:4] == "dev-") or (env_accurate[:5] == "prod-"):
                    continue
                else:
                    appCount.remove(env_accurate) 
            appIndex=range(1,len(appCount)+1)
            appDict=dict(zip(appIndex,appCount))
            my_logger.info("Environments available for update are: %s" % str(appDict))
            for k, v in appDict.items():
                print '{:4s} {:7s}'.format(str(k), str(v))
                    
            envIndex=input("Select an environment to update: ")
            my_logger.info("Environment Index entered by user: %s" % envIndex)
            EnvName_temp=appDict.get(envIndex).split("-")
            my_logger.info("Environment chosen for update: %s"%EnvName_temp)
            envName=EnvName_temp[1]+ "." + EnvName_temp[0]
            # print envName
            env_temp= str(appDict.get(envIndex))
            print "Analyzing existing %s DNS Records"%env_temp
                
            return str(envName)
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered %s. Exiting...." % e)
            sys.exit(-1)
    """
    This module maps the EC2 Instance name to the public and private DNS names
    for mq,search,mongodb
    Gets information of all the instances (primarily prints the reservations)
    """
    def dnsEc2ToDict(region, envName,currZone):
        my_logger.info("In ttAWS.dnsEc2ToDict....")
        try:
            my_logger.info("Connecting to EC2")
            connEc2=connBotoEc2(region)
            my_logger.info(connEc2)
            my_logger.info("Collecting EC2 reservations...")
            ec2Reservations=connEc2.get_all_instances()
            my_logger.info("List of reservations are: %s" %ec2Reservations)
            for resv in ec2Reservations:    #parsing through information in reservations
                #print resv
                for ec2Inst in resv.instances:    #parses through instances for each reservation
                    #print ec2Inst
                    name= ec2Inst.tags['Name']
                    #print name
                    nameSpl=name.split(":")#Split name 'prod-a:mq:search'-store in list
                    tag= nameSpl[0].split("-")    #split 'prod-a' part of name
                    lenNameSpl= len(nameSpl)    #find len. of list to define range
                    servNames=list()        #define list to store the new name format
                    dns_public_lst=list()        #dns_list is used to append to name
                    dns_private_lst=list()
                    my_logger.info("Populating NYC Dictionary with EC2 records.....")
                    for i in range(1, lenNameSpl): #dns list size=no. of services (mq,search
                        dns_public_lst.append(ec2Inst.dns_name)
                    my_logger.info("Populating AWS Dictionary with EC2 records....") 
                    for i in range(1, lenNameSpl):
                        dns_private_lst.append(ec2Inst.private_dns_name)
                    for i in range(1, lenNameSpl): #rearrange names in required format
                        servNames.append(nameSpl[i]+ "." +tag[1]+ "." +tag[0]+".aws."+currZone)
                    dict_NYC_temp=dict(zip(servNames, dns_public_lst)) #merge 2 lists dns,service
                    dict_AWS_temp=dict(zip(servNames,dns_private_lst))
                    for k, v in dict_NYC_temp.items():
                        if envName in k:
                            dictNyc.update(dict_NYC_temp) #merge dict. for all reservation IDs
                            dictAws.update(dict_AWS_temp)
            my_logger.info("Collected NYC records: %s" % str(dictNyc))
            my_logger.info("Collected AWS records: %s" % str(dictAws))
            #print "In ttAWS:EC2:"
            #print "\ndictNyc:"
            #print dictNyc
            #print "\ndictAws:"
            #print dictAws
            return (dictNyc, dictAws)
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered %s. Exiting...." % e)
            sys.exit(-1)
    """
    BEANSTALK fp, fp-lb-1
    """
    def dnsBeanstalkToDict(region, envName,currZone):
        my_logger.info("In ttAWS.dnsBeanstalkToDict....")
        dictBean=dict()
        beanList_key=[]
        beanList_value=[]
        try:
            l1= connBotoBeanstalk(region)
            des_env=l1.describe_environments()
            my_logger.info("Output for describe environments : %s" % des_env)
            for de in des_env["DescribeEnvironmentsResponse"]["DescribeEnvironmentsResult"]["Environments"]:
                EnvironmentName=de['EnvironmentName']
                CNAME=de['CNAME']
                beanList_value.append(CNAME)
                EndpointURL=de['EndpointURL']
                beanList_value.append(EndpointURL)
                EnvName_temp= EnvironmentName.split("-")
                CNAME_key="fp"+"."+ EnvName_temp[2]+ "." +EnvName_temp[1]+ "."+ "aws."+currZone
                URL_key="fp-lb-1"+"."+EnvName_temp[2]+"."+EnvName_temp[1]+"."+"aws."+currZone
                beanList_key.append(CNAME_key)
                beanList_key.append(URL_key)
                #print CNAME_key
                #print URL_key
                dictBean= dict(zip(beanList_key, beanList_value))
                for k,v in dictBean.items():
                    if envName in URL_key:
                        dictNyc[k]=v
                        dictAws[k]=v
            #print "In ttAWS:Beanstalk:"
            #print "\ndictNyc:"
            #print dictNyc
            #print "\ndictAws:"
            #print dictAws
            return dictNyc, dictAws
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered %s. Exiting...." % e)
            sys.exit(-1)
    """
    RDS
    """
    def dnsRdsToDict(region, envName,currZone):
        my_logger.info("In ttAWS.dnsRdsToDict....")
        rds_dict=dict()
        rds_key_list=[]
        rds_value_list=[]
        try:
            connRds2=connBotoRds2(region)
            rds_dict_temp1=dict()
            rds_list_temp=[]
            rds_list_temp1=[]
            _dbinstances = connRds2.describe_db_instances()
            for dbi in _dbinstances['DescribeDBInstancesResponse']['DescribeDBInstancesResult']['DBInstances']:
                _tags = connRds2.list_tags_for_resource("arn:aws:rds:us-east-1:671788406991:db:" + dbi['DBInstanceIdentifier'])
                for _itags in _tags['ListTagsForResourceResponse']['ListTagsForResourceResult']['TagList']:
                    rds_list_temp.append(_itags["Key"])
                    rds_list_temp1.append(_itags["Value"])
                rds_dict_temp1= dict(zip(rds_list_temp, rds_list_temp1))
                #print rds_dict_temp1
                name_rds= rds_dict_temp1.get("elasticbeanstalk:environment-name")
                name_rds_new= name_rds.split("-")
                rds_key="fp-rds-1"+"." + name_rds_new[2]+ "." + name_rds_new[1] + "." + "aws."+currZone
                #print rds_key
                rds_key_list.append(rds_key)
            for dbi_key in  _dbinstances['DescribeDBInstancesResponse']['DescribeDBInstancesResult']['DBInstances']:
                rds_value= dbi_key["Endpoint"]["Address"]
                #print rds_value
                rds_value_list.append(rds_value)
            rds_dict= dict(zip(rds_key_list, rds_value_list))
            #print rds_dict
            
            for k,v in rds_dict.items():
                if envName in k:
                    dictNyc[k]=v
                    dictAws[k]=v
            my_logger.info("RDS Records for NYC are: \n%s" %dictNyc)
            my_logger.info("RDS Records for AWS are: \n%s" %dictAws)
            #print "In ttAWS:RDS:"
            #print "\ndictNyc:"
            #print dictNyc
            #print "\ndictAws:"
            #print dictAws   
            return (dictNyc, dictAws)
            
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered %s. Exiting...." % e)
            sys.exit(-1)
    """
                    FIND ZONE ID FOR ZONE NAME
    """
    def zoneR53(currZone):
        my_logger.info("In ttAWS.zoneR53....")
        try:
            connR53=connRoute53()
            hosted_zones=connR53.get_all_hosted_zones()
            z_Name_Id_dict=dict()
            z_Name_count_dict=dict()
            hz_Name=[]
            hz_Id=[]
            c_z=1
            count_zone=[]
    
            for hz in hosted_zones["ListHostedZonesResponse"]["HostedZones"]:
                count_zone.append(c_z)
                hz_Name.append(hz["Name"])
                temp_Id=hz["Id"].split("/")
                hz_Id.append(temp_Id[2])
                c_z=c_z+1
    
            z_Name_Id_dict=dict(zip(hz_Name,hz_Id))
            #print ("z_Name_Id_dict", z_Name_Id_dict)
            #z_Name_count_dict = dict(zip(count_zone,hz_Name))
            #print("z_Name_count_dict",z_Name_count_dict)
            Id =z_Name_Id_dict[currZone]
            """
            print "In ttAWS: zone53"
            print("Id:",Id)
            """
            return Id
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered %s. EC2 Reservations collection failed! Exiting...." % e)
            sys.exit(-1)
    """
    DNS Update/ Upsert boto using new env data
    """
    def dnsUpdate(envName, zone_Id, dict_AWS):
        my_logger.info("In ttAWS.dnsUpdate....")
        try:
            connR53 = connRoute53()
            changes=boto.route53.record.ResourceRecordSets(connR53, zone_Id)
            for k,v in dict_AWS.items():
                if envName in k:
                    change=changes.add_change("UPSERT",k,"CNAME")
                    change.add_value(v)
                    result=changes.commit()
                print "Inserted record: %s /%s/ CNAME " % (k, v)
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered %s. EC2 Reservations collection failed! Exiting...." % e)
            sys.exit(-1)
            
            
            
    def currRecords(zone_id):
        my_logger.info("In ttAWS.currRecords....")
        try:
            connR53=connRoute53()
            sets = connR53.get_all_rrsets(zone_id)
            for rset in sets:
                currAws.update({rset.name: rset.resource_records})
                
            """
            print "In ttAWS:currRecords:"
            print currAws
            """
            
            return currAws
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered %s. EC2 Reservations collection failed! Exiting...." % e)
            sys.exit(-1)
            
    """
    cli53 export
    """
    def cli53_export(zone, dir, suffix):
        my_logger.info("In ttAWS.cli53_export....")
        try:
            path_curr=os.getcwd()
            command=path_curr + '/cli53 export ' + zone + ' > ' + dir + suffix
            proc1=subprocess.Popen([command], stdout=subprocess.PIPE, shell=True)
            (out1, err1)= proc1.communicate()
        except:
            e = sys.exc_info()[0]
            my_logger.error("Error Encountered %s. EC2 Reservations collection failed! Exiting...." % e)
            sys.exit(-1)
        print out1
        
    def findActiveEnv():
        my_logger.info("In ttAWS.findActiveEnv....")
        try:
            my_logger.info("Evaluating fp_internal_dns....")
            dns_int=ttLx.callSubprocess(_fp_dns_internal_cmd)
            _fp_dns_internal= dns_int.split("\n")[2]
            my_logger.info("fp_dns_internal: %s" % _fp_dns_internal)
            my_logger.info("Evaluating fp_external_dns....")
            dns_ext=ttLx.callSubprocess(_fp_dns_external_cmd)
            _fp_dns_external=dns_ext.split("\n")[1]
            my_logger.info("fp_dns_external: %s" % _fp_dns_external)
            my_logger.info("Evaluating _fp_live.....")
            live=ttLx.callSubprocess(_fp_live_cmd)
            _fp_live= live.split("\n")[0][3]
            my_logger.info("fp_live: %s" % _fp_live)
            my_logger.info("Evaluating _fp_live_dns.....")
            live_dns=ttLx.callSubprocess(_fp_live_dns_cmd)
            _fp_live_dns=live_dns.split("\n")[0]
            my_logger.info("live_dns: %s" % _fp_live_dns)
        
            if _fp_dns_internal != _fp_dns_external:
                my_logger.error("ERROR: The Internal Active Directory DNS and external AWS Route 53 DNS servers think different environments are live!  Internal DNS says %s and external says %s. EXITING!" %(_fp_dns_internal, _fp_dns_external))
                sys.exit(-1)
            else:
                if _fp_live != "a" and _fp_live != "b":
                    my_logger.error("ERROR: Could not resolve which prod environment is live, A or B. Script got %s from %s. EXITING!" %(_fp_live,_fp_live_dns))
                    sys.exit(-1)
                else:
                    if _fp_live == "a":
                        _copy_source_prefix="prod-a"
                    elif _fp_live == "b": 
                        _copy_source_prefix="prod-b"
        
            my_logger.info("_copy_source_prefix: %s" % _copy_source_prefix)
            return _copy_source_prefix
        except:
            e = sys.exc_info()[0]
            my_logger.info("Error Encountered %s. Exiting...." % e )
            sys.exit(-1)
    
    def findSourceSuffix():
        activeEnv=findActiveEnv()
        my_logger.info("Active Environment is %s" % activeEnv)
        activeEnvSplit=activeEnv.split("-")
        if activeEnvSplit[0]!="prod":
            my_logger.info("Error encountered in active env determination. Exiting!")
            sys.exit(-1)
        else:
            source_suffix=activeEnvSplit[1] + "." + activeEnvSplit[0] + ".aws.3top.com"
            my_logger.info("Source Host: %s" %source_suffix)
        return source_suffix    
    
