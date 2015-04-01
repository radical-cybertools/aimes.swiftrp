
import os
import sys
import time
import radical.pilot as rp

def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

	if not pilot:
		return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)
    if  state == rp.FAILED :
        sys.exit (1)

def unit_state_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

	if not unit:
		return
    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)


def radical_init (configs):
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

        session = "session_name"
        pmgr    = "pmgr_foo"
        umgr    = "umpr_blah"
        return [session, pmgr, umgr]

    except Exception as e:
        print "An error occurred: %s" % ((str(e)))
        sys.exit (-1)


def compose_compute_unit():
    cudesc = rp.ComputeUnitDescription()
    cudesc.environment = {'CU_NO': 1}
    cudesc.executable  = "/bin/sleep"
    cudesc.arguments   = ['$((30 + $(($RANDOM % 30))))']
    cudesc.cores       = 1
    return [cudesc]

def submit_task(unit_manager):
    cu_desc = compose_compute_unit()
    c_unit  = unit_manager.submit_units(cu_desc)
    return c_unit

if __name__ == "__main__":

    configs = {}
    try:
        configs['db_url'] = 'mongodb://127.0.0.1:50055'
        configs['userpass'] = 'userpass'
        [session, pmgr, umgr] = radical_init(configs)

        u1 = submit_task(umgr)
        u2 = submit_task(umgr)

        print "Waiting for CUs to complete ..."

        while True:

            print "State of task 1 : ", u1[0].state
            print "State of task 2 : ", u2[0].state
            if u1[0].state == 'Done' and u2[0].state == rp.states.DONE:
                break
            else:
                time.sleep(10)

        print "All CUs completed successfully!"


    except Exception as e:
        print "An error occurred: %s" % ((str(e)))
        sys.exit (-1)

    except KeyboardInterrupt :
        print "Execution was interrupted"
        sys.exit (-1)

    finally :
        print "Closing session, exiting now ..."
        session.close()

#
# ------------------------------------------------------------------------------

