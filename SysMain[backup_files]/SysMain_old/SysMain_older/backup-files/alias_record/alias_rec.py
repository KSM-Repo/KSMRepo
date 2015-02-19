
#
# route53 add_alias ZPO9LGHZ43QB9 alias.example.com A Z3DZXE0Q79N41H lb-1218761514.us-east-1.elb.amazonaws.com.
# route53 change_alias ZPO9LGHZ43QB9 alias.example.com. A Z3DZXE0Q79N41H lb2-1218761514.us-east-1.elb.amazonaws.com.
# route53 delete_alias ZPO9LGHZ43QB9 alias.example.com. A Z3DZXE0Q79N41H lb2-1218761514.us-east-1.elb.amazonaws.com.

def get(conn, hosted_zone_id, type=None, name=None, maxitems=None):
    """Get all the records for a single zone"""
    response = conn.get_all_rrsets(hosted_zone_id, type, name, maxitems=maxitems)
    # If a maximum number of items was set, we limit to that number
    # by turning the response into an actual list (copying it)
    # instead of allowing it to page
    if maxitems:
        response = response[:]
    print '%-40s %-5s %-20s %s' % ("Name", "Type", "TTL", "Value(s)")
    for record in response:
        print '%-40s %-5s %-20s %s' % (record.name, record.type, record.ttl, record.to_print())
        
def _add_del(conn, hosted_zone_id, change, name, type, identifier, weight, values, ttl, comment):
    from boto.route53.record import ResourceRecordSets
    changes = ResourceRecordSets(conn, hosted_zone_id, comment)
    change = changes.add_change(change, name, type, ttl,
                                identifier=identifier, weight=weight)
    for value in values.split(','):
        change.add_value(value)
    print changes.commit()
    
def _add_del_alias(conn, hosted_zone_id, change, name, type, identifier, weight, alias_hosted_zone_id, alias_dns_name, comment):
    from boto.route53.record import ResourceRecordSets
    changes = ResourceRecordSets(conn, hosted_zone_id, comment)
    change = changes.add_change(change, name, type,
                                identifier=identifier, weight=weight)
    change.set_alias(alias_hosted_zone_id, alias_dns_name)
    print changes.commit()
    
def add_record(conn, hosted_zone_id, name, type, values, ttl=600,
               identifier=None, weight=None, comment=""):
    """Add a new record to a zone.  identifier and weight are optional."""
    _add_del(conn, hosted_zone_id, "CREATE", name, type, identifier,
             weight, values, ttl, comment)
    
def del_record(conn, hosted_zone_id, name, type, values, ttl=600,
               identifier=None, weight=None, comment=""):
    """Delete a record from a zone: name, type, ttl, identifier, and weight must match."""
    _add_del(conn, hosted_zone_id, "DELETE", name, type, identifier,
             weight, values, ttl, comment)
    
def add_alias(conn, hosted_zone_id, name, type, alias_hosted_zone_id,
              alias_dns_name, identifier=None, weight=None, comment=""):
    """Add a new alias to a zone.  identifier and weight are optional."""
    _add_del_alias(conn, hosted_zone_id, "CREATE", name, type, identifier,
                   weight, alias_hosted_zone_id, alias_dns_name, comment)
    
def del_alias(conn, hosted_zone_id, name, type, alias_hosted_zone_id,
              alias_dns_name, identifier=None, weight=None, comment=""):
    """Delete an alias from a zone: name, type, alias_hosted_zone_id, alias_dns_name, weight and identifier must match."""
    _add_del_alias(conn, hosted_zone_id, "DELETE", name, type, identifier,
                   weight, alias_hosted_zone_id, alias_dns_name, comment)
    
def change_record(conn, hosted_zone_id, name, type, newvalues, ttl=600,
                   identifier=None, weight=None, comment=""):
    """Delete and then add a record to a zone.  identifier and weight are optional."""
    from boto.route53.record import ResourceRecordSets
    changes = ResourceRecordSets(conn, hosted_zone_id, comment)
    # Assume there are not more than 10 WRRs for a given (name, type)
    responses = conn.get_all_rrsets(hosted_zone_id, type, name, maxitems=10)
    for response in responses:
        if response.name != name or response.type != type:
            continue
        if response.identifier != identifier or response.weight != weight:
            continue
        change1 = changes.add_change("DELETE", name, type, response.ttl,
                                     identifier=response.identifier,
                                     weight=response.weight)
        for old_value in response.resource_records:
            change1.add_value(old_value)
    change2 = changes.add_change("CREATE", name, type, ttl,
            identifier=identifier, weight=weight)
    for new_value in newvalues.split(','):
        change2.add_value(new_value)
    print changes.commit()
    
def change_alias(conn, hosted_zone_id, name, type, new_alias_hosted_zone_id, new_alias_dns_name, identifier=None, weight=None, comment=""):
    """Delete and then add an alias to a zone.  identifier and weight are optional."""
    from boto.route53.record import ResourceRecordSets
    changes = ResourceRecordSets(conn, hosted_zone_id, comment)
    # Assume there are not more than 10 WRRs for a given (name, type)
    responses = conn.get_all_rrsets(hosted_zone_id, type, name, maxitems=10)
    for response in responses:
        if response.name != name or response.type != type:
            continue
        if response.identifier != identifier or response.weight != weight:
            continue
        change1 = changes.add_change("DELETE", name, type, 
                                     identifier=response.identifier,
                                     weight=response.weight)
        change1.set_alias(response.alias_hosted_zone_id, response.alias_dns_name)
    change2 = changes.add_change("CREATE", name, type, identifier=identifier, weight=weight)
    change2.set_alias(new_alias_hosted_zone_id, new_alias_dns_name)
    print changes.commit()

    sys.exit(1)
    
if __name__ == "__main__":
    import boto
    import sys
    conn = boto.connect_route53()
    self = sys.modules['__main__']
    if len(sys.argv) >= 2:
        try:
            cmd = getattr(self, sys.argv[1])
        except:
            cmd = None
        args = sys.argv[2:]
    else:
        cmd = help
        args = []
    if not cmd:
        cmd = help
    try:
        cmd(conn, *args)
    except TypeError, e:
        print e
        help(conn, cmd.__name__)
