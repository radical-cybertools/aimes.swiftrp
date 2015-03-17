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
import threading

sys.path.append('gen-py')
#sys.path.insert(0, glob.glob('../../lib/py/build/lib.*')[0])


from radical_interface import RadicalPilotInterface
import radical_server as rs

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

def extract_configs(task_file):
  configs = {}
  configs['db_url'] = 'mongodb://127.0.0.1:50055'
  configs['userpass'] = 'userpass'
  print "task_file is ignored now : ", task_file
  return configs

class RadicalPilotHandler:
  def __init__(self):
    self.session = 'NULL'
    self.pmgr    = 'NULL'
    self.log     = {}
    self.configs = {}
    self.rp_lock = threading.Lock()
    self.task_lookup = {}
    
  def submit_task(self, task_filename):
    '''
    if self.configs == {}:
      with self.rp_lock and self.configs == {}:
        self.configs = extract_configs(task_filename)
        [self.session, self.pmgr, self.umgr] = rs.radical_init(configs)

    cu_list = rs.submit_task(self.umgr)    
    self.task_lookup[task_filename] = cu_list[0]
    '''
    print "Task_filename : ", task_filename

  def cancel_task(self, task_name):
    print "Cancelling task :", task_name
    return "Cancelled task"

  def status_task(self, task_name):
    '''
    print "Status task :", task_name
    print self.task_lookup[task_name]
    print "State : ", self.task_lookup[task_name].state
    '''
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

