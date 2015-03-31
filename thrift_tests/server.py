#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys, glob
import logging
import random

sys.path.append('/home/yadunand/swift-k/cogkit/modules/provider-localscheduler/libexec/radical-pilot-provider/thrift_tests/gen-py')
#sys.path.insert(0, glob.glob('../../lib/py/build/lib.*')[0])

from radical_interface import RadicalPilotInterface
import radical.pilot as rp


from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

def extract_configs(task_file):
	configs = {}
	configs['db_url'] = 'mongodb://127.0.0.1:50055'
	configs['userpass'] = 'userpass'
	print "[extract_configs] task_file is ignored now : ", task_file
	return configs

def pilot_state_cb (pilot, state) :
	print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)
	if  state == rp.FAILED :
		sys.exit (1)

def unit_state_cb (unit, state) :
	print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

def rp_radical_init (configs):
	print "[rp_radical_init]"
	try:
		session = rp.Session(database_url=configs['db_url'])
		c = rp.Context(configs['userpass'])
		session.add_context(c)
		print "Initializing Pilot Manager ..."
		pmgr = rp.PilotManager(session=session)
		pmgr.register_callback(pilot_state_cb)

		# Combine the ComputePilot, the ComputeUnits and a scheduler via
		# a UnitManager object.
		print "Initializing Unit Manager ..."
		umgr = rp.UnitManager (session=session,
							   scheduler=rp.SCHED_DIRECT_SUBMISSION)

		# Register our callback with the UnitManager. This callback will get
		# called every time any of the units managed by the UnitManager
		# change their state.
		umgr.register_callback(unit_state_cb)

		pdesc = rp.ComputePilotDescription ()
		pdesc.resource = "local.localhost"  # NOTE: This is a "label", not a hostname
		pdesc.runtime  = 10 # minutes
		pdesc.cores    = 1
		pdesc.cleanup  = True

		# submit the pilot.
		print "Submitting Compute Pilot to Pilot Manager ..."
		pilot = pmgr.submit_pilots(pdesc)

		# Add the created ComputePilot to the UnitManager.
		print "Registering Compute Pilot with Unit Manager ..."
		umgr.add_pilots(pilot)
		#session = "session_name"
		#pmgr    = "pmgr_foo"
		#umgr    = "umpr_blah"
		return [session, pmgr, umgr]

	except Exception as e:
		print "An error occurred: %s" % ((str(e)))
		sys.exit (-1)


def rp_compose_compute_unit(task_filename):

	task_desc = open(task_filename, 'r').readlines()

	for line, index in 

	# Strip out the definition for the executable
	if task_desc[0].startswith('CMD_STRING="') :
		task_string     = task_desc[0][12:][:-2]
		task_executable = task_string.split(' ')[0]
		task_args       = task_string[len(task_executable)+1:]

	input_sd = {
		'source': '/home/yadunand/swift-k/dist/swift-svn/libexec/_swiftwrap.staging',
		'target': '_swiftwrap.staging'
    }

	'''
	for lines in task_desc :
		if lines
	'''
	output_sd = {
		'source': 'wrapper.log',
		'target': '/home/yadunand/swift-k/tests/aimes_testing/wrapper.log'
    }

	cudesc = rp.ComputeUnitDescription()
	cudesc.environment = {'CU_NO': 1}
	cudesc.executable  = task_executable
	cudesc.arguments   = task_args.split(' ')
	cudesc.cores       = 1
	cudesc.input_staging =  input_sd
	cudesc.output_staging = output_sd

	logging.debug("EXEC : " + cudesc.executable)
	logging.debug("ARGS : " + task_args)
	#logging.debug("input_staging : " + cudesc.input_staging)
	#logging.debug("output_staging : " + cudesc.output_staging)

	return [cudesc]

def rp_submit_task(unit_manager, task_filename):
	cu_desc = rp_compose_compute_unit(task_filename)
	c_unit  = unit_manager.submit_units(cu_desc)
	return c_unit


class RadicalPilotHandler:
	def __init__(self):
		self.session = 'NULL'
		self.pmgr    = 'NULL'
		self.umgr    = 'NULL'
		self.log     = {}
		self.configs = {}
		#self.rp_lock = threading.Lock()
		self.task_lookup = {}
		self.session = 'NULL'
		logging.debug("Init done")

	def submit_task(self, task_filename):
		print "[SUBMIT_TASK] :", task_filename

		# If self.configs is empty, this is the first task, which requires
		# radical pilots to be setup
		if not self.configs :
			logging.debug("[SUBMIT_TASK] : Starting radical.pilots")
			self.configs = extract_configs(task_filename)
			[self.session, self.pmgr, self.umgr] = rp_radical_init(self.configs)
			print [self.session, self.pmgr, self.umgr]
			logging.debug("done with radical_init")

		cu_list = rp_submit_task(self.umgr, task_filename)
		print cu_list[0]
		hash_id = str(len(self.task_lookup))
		self.task_lookup[hash_id] = cu_list[0]

		return hash_id

	def cancel_task(self, task_name):
		logging.debug("Cancelling task :" + task_name)
		return "Cancelled task"

	def status_task(self, task_name):

		radical_states = { 'PendingExecution' : 'Q',
						   'Scheduling'       : 'Q',
						   'Executing'        : 'R',
						   'Done'             : 'C',
						   'Failed'           : 'F' }

		if task_name not in self.task_lookup:
			return str(task_name) + " F -1 Task id not in the Radical Pilot lookup registry"

		state = self.task_lookup[task_name].state
		if state not in radical_states :
			logging.debug( "[DEBUG] task_name:" + task_name + " state: " +  state)
			return str(task_name) + " Q"

		logging.debug("[DEBUG] task_name:{0} state:{1}".format(task_name, state))
		return str(task_name) + " " + radical_states[state]

	def server_die(self, die_string):
		logging.debug("Server terminating. Received message: " + die_string)
		exit(0)

	def getStruct(self, key):
		print 'getStruct(%d)' % (key)
		return self.log[key]

	def zip(self):
		print 'zip()'


# Start logging
if ( len(sys.argv) < 2 ):
	print "[ERROR] Missing log_file argument"

logging.basicConfig(filename=sys.argv[1], level=logging.DEBUG)
logging.debug('Starting the server...')

handler   = RadicalPilotHandler()
processor = RadicalPilotInterface.Processor(handler)
transport = TSocket.TServerSocket(port=9090)
tfactory  = TTransport.TBufferedTransportFactory()
pfactory  = TBinaryProtocol.TBinaryProtocolFactory()

server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

# You could do one of these for a multithreaded server
#server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
#server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)

server.serve()
logging.debug('done.')

