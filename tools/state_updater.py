# Listens to messages that control the update of the stat of the system.
import pika
import json
import sys
import os
from func_adl_request_broker.db_access import FuncADLDBAccess, ADLRequestInfo

def process_add_file(db, ch, method, properties, body):
    info = json.loads(body)
    hash = info['hash']
    file_ref = info['file']
    treename = info['treename']

    # Update state. Just silently ignore if this thing isn't there.
    state = db.lookup_results(hash)
    if state is not None:
        new_files = list(state.files)
        new_files.append((file_ref, treename))
        new_files = list(set(new_files))
        new_done = len(new_files) >= state.jobs
        new_state = ADLRequestInfo(done=new_done, files=new_files, jobs=state.jobs, phase=state.phase, hash=state.hash if not new_done else 'done')
        db.save_results(hash, new_state)
    else:
        print(f'Unable to find an entry for hash {hash}. Ignoring adding file {file_ref}.')

    ch.basic_ack(delivery_tag=method.delivery_tag)

def process_update_state(db, ch, method, properties, body):
    info = json.loads(body)
    hash = info['hash']
    new_phase = info['phase']

    # Update state. Just silently ignore if this thing isn't there.
    state = db.lookup_results(hash)
    if state is not None:
        new_state = ADLRequestInfo(done=state.done, files=state.files, jobs=state.jobs, phase=new_phase, hash=state.hash)
        db.save_results(hash, new_state)
    else:
        print(f'Unable to find an entry for hash {hash} to update it to state {new_phase}.')

    ch.basic_ack(delivery_tag=method.delivery_tag)

def process_number_jobs(db, ch, method, properties, body):
    info = json.loads(body)
    hash = info['hash']
    new_n_jobs = info['njobs']

    # Update state. Just silently ignore if this thing isn't there.
    state = db.lookup_results(hash)
    if state is not None:
        new_state = ADLRequestInfo(done=state.done, files=state.files, jobs=new_n_jobs, phase=state.phase, hash=state.hash)
        db.save_results(hash, new_state)
    else:
        print(f'Unable to find an entry for hash {hash} to update it to state {new_phase}.')

    ch.basic_ack(delivery_tag=method.delivery_tag)

def listen_to_queue(rabbit_node, mongo_db_server, rabbit_user, rabbit_pass):
    if rabbit_pass in os.environ:
        rabbit_pass = os.environ[rabbit_pass]
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_node, credentials=credentials))
    channel = connection.channel()

    # Open up the mongo db which we will be doing lots of updates to.
    db = FuncADLDBAccess(mongo_db_server)

    # status_add_file - sent when a file is done and ready for someone downstream to use
    channel.queue_declare(queue='status_add_file')
    channel.basic_consume(queue='status_add_file', on_message_callback=lambda ch, method, properties, body: process_add_file(db, ch, method, properties, body), auto_ack=False)

    # status_change_state - sent when the state needs to change for a particular job
    channel.queue_declare(queue='status_change_state')
    channel.basic_consume(queue='status_change_state', on_message_callback=lambda ch, method, properties, body: process_update_state(db, ch, method, properties, body), auto_ack=False)

    # status_n_jobs - The total number of jobs that are running to deal with this request needs to be updated.
    channel.queue_declare(queue='status_number_jobs')
    channel.basic_consume(queue='status_number_jobs', on_message_callback=lambda ch, method, properties, body: process_number_jobs(db, ch, method, properties, body), auto_ack=False)

    # We are setup. Off we go. We'll never come back.
    channel.start_consuming()

if __name__ == '__main__':
    bad_args = len(sys.argv) != 5
    if bad_args:
        print ("Usage: python state_updater.py <rabbit-mq-node-address> <mongo-db-server> <rabbit-user> <rabbit-pass>")
    else:
        listen_to_queue (sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])