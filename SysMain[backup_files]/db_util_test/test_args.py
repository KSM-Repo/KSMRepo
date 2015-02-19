import sys
import re
import os
import logging
import ttLib.logger as logger
#my_logger = logging.getLogger(os.environ['logger_name'])

def main():
	#print sys.argv
	if len(sys.argv) < 7:
		sys.exit("Too few arguments found. Exiting! \n Use: dbUtil.py -s [source_env] -t [target_env] -o [db_operation] -d [db_service]")
	
	s_count = 0
	t_count = 0
	o_count = 0
	d_count = 0
	vmt_count = 0
	vms_count = 0
	
	for i in range(0, len(sys.argv)-1):
		#print("comparing %s and %s" %(sys.argv[i], "-s"))
		if (sys.argv[i] == "-s"):
			s_count = s_count + 1
			source_loc = i + 1
		elif (sys.argv[i] == "-t"):
			t_count = t_count + 1
			target_loc = i + 1
		elif (sys.argv[i] == "-o"):
			o_count = o_count + 1
			operation_loc = i + 1
		elif (sys.argv[i] == "-d"):
			d_count = d_count + 1
			service_loc = i + 1
		elif (sys.argv[i] == "-vmt"):
			vmt_count = vmt_count + 1
			vm_target_loc = i + 1
		elif (sys.argv[i] == "-vms"):
			vms_count = vms_count + 1
			vm_source_loc = i + 1
			
		
	if s_count == 0:
		if vms_count == 0:
			print "Source argument not found! Finding current active environment."
		elif (vms_count == 1):
			source_env = sys.argv[vm_source_loc]
			print("Received source_env: %s. Format validity cannot be established (VM)"%source_env)
		elif (vms_count > 1):
			sys.exit("Too many VM source environments found. Exiting!")
			
	elif s_count == 1:
		if vms_count == 0:
			source_env = sys.argv[source_loc]
			if "aws.3top.com" in source_env:
				source_env_list = source_env.split(".")
				if (source_env_list[0] in ['a', 'b', 'c']) and (source_env_list[1] in ['dev', 'prod']):
					print("Received source_env: %s."%source_env)
			elif "nyc.3top.com" in source_env:
				source_env_list = source_env.split(".")
				if (source_env_list[0] == 'dev'):
					print("Received source_env: %s."%source_env)
			else:
				sys.exit("Received source_env: %s. Status: Invalid [Use Format: [a/b/c] . [dev/prod] . [aws/nyc] . [.3top.com] Exiting!"%source_env)
		elif (vms_count == 1) or (vms_count > 1):
			sys.exit("Unable to identify source. Found both source and VM source. Exiting!")
	elif s_count > 1:
		sys.exit("Multiple source environments found in arguments. Exiting!")
	
	
	if t_count == 0:
		if vmt_count == 0:
			sys.exit("Target environment not found! Exiting.")
		elif vmt_count == 1:
			target_env = sys.argv[vm_target_loc]
			print("Received target_env: %s. Format validity cannot be established (VM)"%target_env)
		elif vmt_count > 1:
			sys.exit("Too many VM target environments found. Exiting!")
			
	elif t_count == 1:
		if vmt_count == 0:
			target_env = sys.argv[target_loc]
			if "aws.3top.com" in target_env:
				target_env_list = target_env.split(".")
				if (target_env_list[0] in ['a', 'b', 'c']) and (target_env_list[1] in ['dev', 'prod']):
					print("Received target_env: %s."%target_env)
			elif "nyc.3top.com" in target_env:
				target_env_list = target_env.split(".")
				if (target_env_list[0] == 'dev'):
					print("Received target_env: %s."%target_env)
			else:
				sys.exit("Received target_env: %s. Status: Invalid. Exiting!"%target_env)
		elif (vmt_count == 1) or (vmt_count > 1):
			sys.exit("Unable to identify target. Both target and VM Target found. Exiting!")
	elif t_count > 1:
		sys.exit("Multiple target environments found in arguments. Exiting!")

	
	if o_count == 0:
		sys.exit("DB Operation not found! Exiting.")
	elif o_count == 1:
		db_operation = sys.argv[operation_loc].upper()
		if db_operation in ['SWAP', 'COPY']:
			print "DB_Operation received: %s." %db_operation
		else:
			sys.exit("DB_Operation received: %s. Status: Invalid \n [Choose: swap/copy]. Exiting!"%db_operation)
	elif o_count > 1:
		sys.exit("Mulitple db_operation found in arguments. Exiting!")
		
	
	if d_count == 0:
		sys.exit("DB Operation not found! Exiting.")
	elif d_count == 1:
		db_service = sys.argv[service_loc].upper()
		if db_service.upper() in ['ALL', 'MYSQL', 'MONGODB', 'NEO4J', 'VIRTUOSO']:
			print "DB_Service received: %s." %db_service
		else:
			sys.exit("DB_Service received: %s. Status: Invalid \n [Choose: all/mysql/mongodb/neo4j/virtuoso]. Exiting!"%db_operation)
	elif d_count > 1:
		sys.exit("Mulitple db_operation found in arguments. Exiting!")
	
	
	if db_operation == 'SWAP':
		if (source_env == 'a.prod.aws.3top.com') and (target_env == 'b.prod.aws.3top.com'):
			print "Arguments validated. Starting %s for SERVICE: %s between from %s to %s"%(db_operation, db_service, source_env, target_env)
		elif (source_env == 'b.prod.aws.3top.com') and (target_env == 'a.prod.aws.3top.com'):
			print "Arguments validated. Starting %s for SERVICE: %s between from %s to %s"%(db_operation, db_service, source_env, target_env)
		else:
			sys.exit("SWAP operation can be possible only between a.prod and b.prod environments only. Exiting!")
	elif db_operation == 'COPY':
		print "Arguments validated. Starting %s for SERVICE: %s from %s to %s"%(db_operation, db_service, source_env, target_env)
		




main()
