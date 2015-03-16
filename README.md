# aimes.swiftrp
Swift connector for radical.pilot


Swift interfaces with the execution provider via three calls :
      - (jobid)  stsubmit (taskinfo via stdin) 
      - (exitcode) stcancel (jobid)
      - (R|Q|C) ststat (jobid)
       
Note: Each submission is a separate task, no batching is possible here.


TODOs:
1. Daemonize the radical pilot server script with thrift
   - [
   - Test with stubs

2. Calls to daemon from stsubmit/stcancel/ststat

3. Handle transfer of the _swiftwrap
   - This should just be hardcoded

4. Handle transfer of data.