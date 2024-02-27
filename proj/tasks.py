from .celery import app
from celery.signals import worker_process_init, worker_process_shutdown
from celery import Celery, states
from celery.exceptions import Ignore, Reject

from .config import setup

import psycopg
import os
import json



client = None
collection_name = None


@worker_process_init.connect
def init_worker(**kwargs):
    global client, db
    print("Initializing databse connection for worker")
    client = setup()


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global client
    if client:
        print("Closing Postgresql connection")
        client.close()


@app.task(bind=True)
def fetch_ticks(self):
    record_id, error = process_and_save_transaction()
    if error:
        # manually update the task state
        msg = f"\tFAILURE:: Task # {index} from batch {batch_name} failed due to: {error}"

        self.update_state(
            state = states.FAILURE,
            meta = msg 
        )

        # ignore the task so no other state is recorded
        raise Reject(msg)
        return None
    return record_id


@app.task(bind=True)
def run_inference(self):
    pass


@app.task(bind=True)
def trade_execution(self):
    pass


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        crontab(hour="*", minute="2,32"),
        fetch_ticks.s(),
    )