from time import sleep, clock
from datetime import datetime
import boto3, argparse, random, threading

# prepare message queue URLs
# e.g. 'https://sqs.us-east-1.amazonaws.com/622058021374/leader.fifo'
queueURL = 'https://sqs.us-east-1.amazonaws.com/622058021374/'
queue_suffixes = ['0', '1', '2', '3', '4', 'leader', 'client-blue', 'client-red']
message_queue_URLs = {}
for suffix in  queue_suffixes:
    message_queue_URLs[suffix] = queueURL + suffix + '.fifo'

class Node:

    def __init__(self, id: str):

        self.sqs = boto3.client('sqs')
        self.id = id
        self.count = 0
        self.message_queue = message_queue_URLs[self.id]

        # purge the associated queue to make sure it is empty when node is initialized
        response = self.sqs.purge_queue(
            QueueUrl=self.message_queue
        )

    '''
    send() and receive() methods below have been adapted from the AWS python SDK
    documentation found here: 
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
    '''
    def send(self, destination: str):
        '''
        send a message to the given destination queue. 
        destination string should exist in the queue_suffixes list.
        '''
        # used to uniquely identify messages:
        self.count += 1 
        # included to ensure all messages have a different non-duplication hash:
        timestamp = str(datetime.now()) 

        message_body = 'Hello from {}. Message # {}. {}'.format(self.id, self.count, timestamp)

        # response stores confirmation data from SQS
        response = self.sqs.send_message(
            QueueUrl=message_queue_URLs[destination],
            MessageAttributes={
                'From': {
                    'DataType': 'String',
                    'StringValue': self.id
                }     
            },
            MessageGroupId='queue',
            MessageBody=(message_body)
        )

        print('Message sent from ', self.id, ' to ', destination, ': ', message_body)
    
    def receive(self):
        # response stores results of receive call from SQS
        response = self.sqs.receive_message(
            QueueUrl=self.message_queue,
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            WaitTimeSeconds=0
        )
        # For debugging, print the whole response:
        #print('\n Response: ',response, type(response))

        # check if a message was received, intended to be threaded
        if 'Messages' not in response:
            print("Receive aborted: Message Queue Empty", end='\r')
            return

        messageID = response['Messages'][0]['MessageId']
        message = response['Messages'][0]['Body']

        # this receipt handle is required to delete the  message from queue
        receipt_handle = response['Messages'][0]['ReceiptHandle']

        # delete the message after receiving
        self.sqs.delete_message(
            QueueUrl=self.message_queue,
            ReceiptHandle=receipt_handle
        )
    
        print('Received and deleted message {}: \n\"{}\"'.format(messageID, message))

#############################
#   Methods for testing:    #
#############################

    def test_method_send(self, destination: str):
        node_send = (
            threading.Thread(
                target=self.send_thread,
                args=(destination,)
            )
        )
        node_send.start()

    def send_thread(self, destination: str):
        while True:
            self.send(destination)
            sleep(.5)

    def test_method_receive(self):
        node_receive = (
            threading.Thread(
                target=self.receive_thread,
                daemon=True
            )
        )
        node_receive.start()

    def receive_thread(self):
        while True:
            self.receive()


        
if __name__ == '__main__':

    node0 = Node('0')
    node1 = Node('1')

    # send a bunch of sequential messages with a sendng thread
    node0.test_method_send('1')
    # open a receiving thread
    node0.test_method_receive()

    # Same here
    node1.test_method_send('0')
    node1.test_method_receive()