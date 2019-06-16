# First step in the analysis systems work flow
import sys
import pickle
import ast
import base64
import json
import pika
from func_adl_request_broker.db_access import FuncADLDBAccess, ADLRequestInfo

def process_message(db, ch, method, properties, body):
    'Process the incoming message'

    # Get the AST out of the body of the message.
    a = pickle.loads(body)
    print ("hi")
    if a is None or not isinstance(a, ast.AST):
        print (f"Body of message wasn't of type AST: {a}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Great. Next - see if we know about this already.
    status = db.lookup_results(a)
    
    # If we know nothing about this, then fire off a new task
    if status is None:
        # TODO: this needs to be atomic. Otherwise you have to clean up.
        status = db.save_results(a, ADLRequestInfo(done=False, files=[], jobs=0, phase='waiting_for_data', hash=''))
        finder_message = {
            'hash': status.hash,
            'ast': base64.b64encode(pickle.dumps(a)).decode(),
        }
        ch.basic_publish(exchange='', routing_key='find_did', body=json.dumps(finder_message))

    # Next, we have to let everyone know the thing is off and going (or done, or whatever).
    ch.basic_publish(exchange='',
        routing_key=properties.reply_to,
        properties=pika.BasicProperties(correlation_id = properties.correlation_id),
        body=json.dumps({'files': status.files, 'phase': status.phase, 'done': status.done, 'jobs': status.jobs}))
    
    # Done!
    ch.basic_ack(delivery_tag=method.delivery_tag)

def listen_to_queue(rabbit_node:str, mongo_db_server:str):
    'Download and pass on datasets as we see them'

    print ("hi from listen")

    # Save the connection to the mongo db.
    # TODO: What happens if the mongo db dies and comes back?
    db = FuncADLDBAccess(mongo_db_server)

    # Connect and setup the queues we will listen to and push once we've done.
    # TODO: What happens if the rabbitmq guy dies and comes back?
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_node))
    channel = connection.channel()

    # as_reqeusts - the queue where the initial requests come in on.
    channel.queue_declare(queue='as_request')

    # find_did - where we send out on the first step when some work needs to be done.
    channel.queue_declare(queue='find_did')

    # And setup our listener
    channel.basic_consume(queue='as_request', on_message_callback=lambda ch, method, properties, body: process_message(db, ch, method, properties, body), auto_ack=False)

    # We are setup. Off we go. We'll never come back.
    print ("ready to process!")
    channel.start_consuming()
    print ("done processing")


if __name__ == '__main__':
    bad_args = len(sys.argv) != 3
    if bad_args:
        print ("Usage: python download_did_rabbit.py <rabbit-mq-node-address> <mongo-db-server>")
    else:
        listen_to_queue (sys.argv[1], sys.argv[2])