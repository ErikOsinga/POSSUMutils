"""
Functions to run CANFAR sessions with polling and retries.
This is needed because sometimes Prefect does not know that CANFAR session has been terminated or
failed and the job would need to be manually rerun.
In this module, we try to rerun the job upon failure, up to set amount of retries.
If we've reached maximum retries and the job still fails, raise an exception to trigger a Prefect failure.
"""
import os
import time
from prefect import task
from prefect.variables import Variable
from canfar.sessions import Session
from possum_pipeline_control import util

#Default polling interval is 60s
POLLING_INTERVAL = Variable.get('canfar-polling-interval-seconds', 60)
#Default retries is 2x before giving up
MAX_RETRY = Variable.get('canfar-num-retries', 2)
session = Session()

@task(log_prints=True)
def run_canfar_task_with_polling(canfar_task, *args: None):
    """
    Run CANFAR task with session polling to check for session status.
    If the job has failed, it will retry up to the MAX_RETRY set in the config.env.
    If MAX_RETRY has been exceeded, it will not retry.
    """
    retry_count = 0
    while retry_count <= MAX_RETRY:
        if retry_count > 0:
            print(f"Retrying CANFAR task...\nRetry attempt: {retry_count}")
        
        session_id = ''
        try:
            session_id = canfar_task(*args)  # run the task
        except Exception as e:
            print(f"Failed to launch CANFAR task. Exception: {e}")

        if session_id:
            status = poll_canfar(session_id)     # keep polling until it stopped running        
            # print CANFAR logs before we lose them
            logs_dict = session.logs(session_id).get(session_id)
            print("CANFAR logs from session ", session_id, "\n", logs_dict)

            # check status and handle accordingly
            if status in ("Completed", "Succeeded"):
                return
        # loop to retry upon failing
        retry_count += 1
    # We have reached maximum retries here. This should be propagated to Prefect as failure
    raise RuntimeError(f"CANFAR task failed after {MAX_RETRY} retries.")    

def get_session_status(session_id):
    """
    Get session status from CANFAR. If session is not found (e.g. if it has expired), return None.
    """
    session_info = Session.info(session, session_id)
    if len(session_info) == 0:
        # session is not found
        return None
    return session_info[0].get('status')

@task(log_prints=True)
def poll_canfar(session_id: str):
    """
    Poll CANFAR session until it finishes or crashes.
    """
    while True:
        status = get_session_status(session_id)
        status_str = status if status is not None else 'Not Found'
        print(f"Polling CANFAR session {session_id}. Status is {status_str}")
        # Possible values:
        # "Pending", "Running", "Terminating", "Succeeded", "Completed", "Error", "Failed"
        # None if the session has been deleted
        if status in ("Completed", "Succeeded", "Error", "Failed", None):
            return status
        # keep polling if "Pending", "Running" or "Terminating" 
        # (unclear what Terminating means so we wait until it's finished)
        time.sleep(POLLING_INTERVAL)
