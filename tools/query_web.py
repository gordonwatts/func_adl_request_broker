# Small hug server that manages the basics for running the back-ends. It is quite dumb in that it
# passes everything off to the internal server.
#
# To test, run with hug -f query_web.py. By default this starts on port 8000.
#
import hug
import pickle
import ast
import pika
import os
import json
import uuid

def on_response (status, corr_id, ch, method, props, body):
    'We get the info back - print it out'
    if corr_id == props.correlation_id:
        status.append(body)

@hug.post('/query')
def query(body):
    r'''
    Given a query (a pickled ast file), return the files or status.
    WARNING: Python AST's are a known security issue and should not be used.

    Arguments:
        body                The Pickle of the python AST representing the request

    Returns:
        Results of the run
    '''
    # If they are sending something too big, then we are just going to bail out of this now.
    if body.stream_len > 1024*1000*100:
        raise BaseException("Too big an AST to process!")

    # Read the AST in from the incoming data.
    raw_data = body.stream.read(body.stream_len)
    a = pickle.loads(raw_data)
    if a is None or not isinstance(a, ast.AST):
        raise BaseException(f'Incoming AST is not the proper type: {type(a)}.')

    # Now, send it into the system, and wait for a response that tells us what to do with this. This is a little messy since
    # we have to correlate a return items.

    # Open connection to Rabbit, and declare the main queue we will be sending to.
    rabbit_user = os.environ['RABBIT_USER']
    rabbit_pass = os.environ['RABBIT_PASS']
    rabbit_address = os.environ['RABBIT_NODE']
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_address, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='as_request')

    # Declare the call-back queue (an anonymous queue)
    result = channel.queue_declare(queue='', exclusive=True)
    callback_queue = result.method.queue

    status = []
    corr_id = str(uuid.uuid4())
    channel.basic_consume(queue=callback_queue, on_message_callback=lambda ch, method, props, body: on_response(status, corr_id, ch, method, props, body), auto_ack=True)

    # Now, send the message
    channel.basic_publish(exchange='',
        routing_key='as_request',
        properties=pika.BasicProperties(
            reply_to=callback_queue,
            correlation_id=corr_id
        ),
        body=pickle.dumps(a)
    )

    # Wait for a response
    while len(status) == 0:
        channel.connection.process_data_events()

    channel.close()

    # Rewrite the files.
    result = json.loads(status[0])
    if 'FILE_URL' in os.environ:
        prefix = os.environ['FILE_URL']
        result['files'] = [[f'{prefix}{u}', tn] for u,tn in result['files']]

    return result