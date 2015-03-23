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
logging.basicConfig()
sys.path.append('gen-py')
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


def rp_compose_compute_unit():
	cudesc = rp.ComputeUnitDescription()
	cudesc.environment = {'CU_NO': 1}
	cudesc.executable  = "/bin/sleep"
	cudesc.arguments   = ['$((30 + $(($RANDOM % 30))))']
	cudesc.cores       = 1
	return [cudesc]

def rp_submit_task(unit_manager):
	cu_desc = rp_compose_compute_unit()
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
		print "Init done"

	def submit_task(self, task_filename):
		print "[SUBMIT_TASK] :", task_filename

		# If self.configs is empty, this is the first task, which requires
		# radical pilots to be setup
		if not self.configs :
			print "[SUBMIT_TASK] : Starting radical.pilots"
			self.configs = extract_configs(task_filename)
			[self.session, self.pmgr, self.umgr] = rp_radical_init(self.configs)
			print [self.session, self.pmgr, self.umgr]
			print "done with radical_init"

		cu_list = rp_submit_task(self.umgr)
		print cu_list[0]
		self.task_lookup[task_filename] = cu_list[0]

		return "Task_filename : " + task_filename

	def cancel_task(self, task_name):
		print "Cancelling task :", task_name
		return "Cancelled task"

	def status_task(self, task_name):
		print "Status task :", task_name
		'''

		print "State : ", self.task_lookup[task_name].state
		'''
		print self.task_lookup[task_name]
		print self.task_lookup[task_name].state
		return "Status task: [ACTIVE/DONE]"

	def server_die(self, die_string):
		print "Server terminating. Received message: ", die_string
		exit(0)

	def getStruct(self, key):
		print 'getStruct(%d)' % (key)
		return self.log[key]

	def zip(self):
		print 'zip()'


handler   = RadicalPilotHandler()
processor = RadicalPilotInterface.Processor(handler)
transport = TSocket.TServerSocket(port=9090)
tfactory  = TTransport.TBufferedTransportFactory()
pfactory  = TBinaryProtocol.TBinaryProtocolFactory()

server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

# You could do one of these for a multithreaded server
#server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
#server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)

print 'Starting the server...'
server.serve()
print 'done.'
