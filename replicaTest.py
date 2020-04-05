from ElectionTimer import Election_Timer
from time import sleep, clock
from datetime import datetime
from threading import Thread, Timer
import boto3, argparse, random

message_queue_URLs = {
    '0': 'https://sqs.us-east-1.amazonaws.com/622058021374/0.fifo', 
    '1': 'https://sqs.us-east-1.amazonaws.com/622058021374/1.fifo', 
    '2': 'https://sqs.us-east-1.amazonaws.com/622058021374/2.fifo', 
    '3': 'https://sqs.us-east-1.amazonaws.com/622058021374/3.fifo', 
    '4': 'https://sqs.us-east-1.amazonaws.com/622058021374/4.fifo', 
    'leader': 'https://sqs.us-east-1.amazonaws.com/622058021374/leader.fifo', 
    'client-blue': 'https://sqs.us-east-1.amazonaws.com/622058021374/client-blue.fifo', 
    'client-red': 'https://sqs.us-east-1.amazonaws.com/622058021374/client-red.fifo'
    }

class Replica:

    def __init__(self, id: str):
        self.id = id
        self.sqs = boto3.client('sqs')
        self.incoming_queue_URL = message_queue_URLs[self.id]

        self.election_state = 'follower'
        self.timer_length = 1.2
        self.current_term = 0
        self.voted_for = None
        self.vote_count = 0

        self.incoming_message_thread = self.start_incoming_message_thread()
        self.election_timer = self.start_election_timer()

    def start_incoming_message_thread(self):
        t = Thread(
            target=self.listen_for_messages,
            name='Incoming Message Thread',
            daemon=True
            )
        t.start()
        return t

    def start_election_timer(self):
        et = Election_Timer(self.timer_length, self)
        t = Thread( 
            target=et.run, 
            name='Election Timer Thread'
            )
        t.start()
        return et     

    def listen_for_messages(self):
        while True:
            # response stores results of receive call from SQS
            response = self.sqs.receive_message(
                QueueUrl=self.incoming_queue_URL,
                MaxNumberOfMessages=1,
                MessageAttributeNames=[
                    'All'
                ],
                WaitTimeSeconds=0
            )
            # check if a message was received, intended to be threaded
            if 'Messages' not in response:
                continue
            else:
                message = response['Messages'][0]['MessageAttributes']

            # this receipt handle is required to delete the  message from queue
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            # delete the message after receiving
            self.sqs.delete_message(
                QueueUrl=self.message_queue,
                ReceiptHandle=receipt_handle
            )
            print('Received and deleted message : \n\"{}\"'.format(message))  

    def receive(self, message: dict):
        pass

    def appendEntriesRPC(self) -> dict:
        stuff = {}
        return stuff
    
    def requestVoteRPC(self) -> dict:
        stuff = {} 
        return stuff

    def make_message(self, message_type: str) -> dict:
        if message_type == 'heartbeat':
            message = {
                'messageType' : 'AppendEntriesRPC',
                'leaderId': self.id,
                'term'  : str(self.current_term),
                #'prevLogIndex' : 'self.prevLogIndex',
                #'prevLogTerm' : 'self.prevLogTerm',
                #'leaderCommit' : 'self.commitIndex'
            }
            return message
        elif message_type == 'election':
            return {}
        else: 
            return {}


    def send_heartbeat(self):
        pass
    
    def startElection(self):
        self.election_state = 'candidate'
        self.current_term += 1
        self.vote_count = 1
        print(self.id, " started an election")


if __name__ == '__main__':

    r = Replica('1')


    '''
    Append Entries message attributes:
    {
        'messageType' : 'AppendEntriesRPC',
        'leaderId': self.id
        'term'  : str(self.current_term),
        'prevLogIndex' : 'self.prevLogIndex',
        'prevLogTerm' : 'self.prevLogTerm',
        'leaderCommit' : 'self.commitIndex'
    }

    Append Response message Attributes:
    {
        'term' : str(self.current_term),
        'success' : 'True'
    }

    Request Vote message attributes:
    {
        'term': str(self.current_term),
        'candidateID': self.id,
        'lastLogIndex': self.lastLogIndex,
        'lastLogTerm': self.lastLogTerm
    }

    Request Vote Reply:
    {
        'term': str(self.current_term),
        'voteGranted': 'True'
    }

    '''