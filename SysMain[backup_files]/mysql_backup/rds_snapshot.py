def takesnapshot():
    host="b.prod.aws.3top.com"
    suffix="before-dump"
    import sys
    import time
    import boto.rds2
    Region = 'us-east-1'
    dict_Dbinstance={}
    timestamp= time.strftime("%Y%m%d-%H%M%S")
    print"started"
    try:
        conn = boto.rds2.connect_to_region(Region)
        _dbinstances = conn.describe_db_instances()
        #print "dbInstances: ", _dbinstances
        """
        for dbi in _dbinstances['DescribeDBInstancesResponse']['DescribeDBInstancesResult']['DBInstances']:
            DbinstanceId=dbi['DBInstanceIdentifier']
            #print "DbinstanceId: %s"%(DbinstanceId)
            _tags = conn.list_tags_for_resource("arn:aws:rds:us-east-1:671788406991:db:" + dbi['DBInstanceIdentifier'])
            #print "tags: ",_tags
            DbinstanceName= _tags['ListTagsForResourceResponse']['ListTagsForResourceResult']['TagList'][0]["Value"]
            #print "DbinstanceName: ",DbinstanceName
            InstSplit=DbinstanceName.split("-")
            env_name=InstSplit[2]+"."+InstSplit[1]
            #print "env_name: ",env_name
            if env_name in host:
                print "%s in %s" %(env_name,host)
                snapshotId="fp-"+timestamp+"-target-"+InstSplit[1]+"-"+InstSplit[2]+"-"+suffix
                instId= DbinstanceId
        print "SnapshotId= ",snapshotId
        print "DBInstanceId= ",instId
        #print "Creating snapshot for : ",host
        #print "DBInstance information: ", dict_Dbinstance
        
        """
        
        """
        try:
            for instId, snapId in dict_Dbinstance.items():
                conn.create_db_snapshot(snapId, instId)
        except:
            print("Error Encountered %s. Exiting...."%str(sys.exc_info()))
            print("Error encountered takesnapshot for %s. Exiting! Logfile location:" %(host))
            sys.exit(-1)
        """
        
          
        _snapshots = conn.describe_db_snapshots()
        #print _snapshots
        len_snap=len(_snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'])
        #print len_snap
        
        for i in range(len_snap):
            if _snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'][i]['DBSnapshotIdentifier']== 'fp-20140912-113138sourceprod-b-before-copy':
                print i, _snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots'][i]['DBSnapshotIdentifier']
            #i=i+1
            # == 'fp-20140912-113138sourceprod-b-before-copy':
            #   print"snap_loc", i
            #break
            
        """
        snap_status=[]
        for i in _snapshots['DescribeDBSnapshotsResponse']['DescribeDBSnapshotsResult']['DBSnapshots']:
            if i['DBSnapshotIdentifier'] == snapshotId:
                print "Status: ",i['Status']
                while i['Status'] != 'available':
                    print "waiting..."
                    time.sleep(15)
                print "Status: ",i['Status']
        ret_str="Snapshot created successfully!"
        """
            
    
    except:
        e = str(sys.exc_info())
        print("Error Encountered %s. Exiting...."%e)
        sys.exit(-1)
        
    #return ret_str

takesnapshot()