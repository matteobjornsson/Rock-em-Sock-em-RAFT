from time import sleep, clock
from datetime import datetime
from threading import Thread
import boto3, random

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

class Messenger:

    def __init__(self, id: str, target):
        self.id = id
        self.sqs = boto3.client('sqs')
        self.incoming_queue_URL = message_queue_URLs[self.id]
        self.target = target
        self.incoming_message_thread = self.start_incoming_message_thread()
        self.msg_count = 0

    def start_incoming_message_thread(self):
        t = Thread(
            target=self.listen_for_messages,
            name='Incoming Message Thread',
            daemon=True
            )
        t.start()
        return t    

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

            message_type = message['messageType']
            if message_type == 'AppendEntriesRPC':
                self.target.receive_append_entries_request(message)
            elif message_type == 'AppendReply':
                self.target.receive_append_entries_reply(message)
            elif message_type == 'RequestVotesRPC':
                self.target.receive_vote_request(message)
            elif message_type == 'VoteReply':
                self.target.receive_vote_reply(message)

    def send(self, message: dict, destination: str):
        '''
        send a message to the given destination queue. 
        destination string should exist in the queue_suffixes list.
        '''
        # used to uniquely identify messages:
        self.msg_count += 1 
        # included to ensure all messages have a different non-duplication hash:
        timestamp = str(datetime.now()) 

        message_body = 'Message # {} from {}. {}'.format(self.count, self.id, timestamp)

        # response stores confirmation data from SQS
        response = self.sqs.send_message(
            QueueUrl=message_queue_URLs[destination],
            MessageAttributes=message,
            MessageGroupId='queue',
            MessageBody=message_body
        )
        print('Message sent from ', self.id, ' to ', destination, ': ', message)

    def send_heartbeat(self):
        pass


#if __name__ == '__main__':



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
        'messageType' : 'AppendReply',
        'term' : str(self.current_term),
        'success' : 'True'
    }

    Request Vote message attributes:
    {
        'messageType' : 'RequestVotesRPC',
        'term': str(self.current_term),
        'candidateID': self.id,
        'lastLogIndex': self.lastLogIndex,
        'lastLogTerm': self.lastLogTerm
    }

    Request Vote Reply:
    {
        'messageType' : 'VoteReply',
        'term': str(self.current_term),
        'voteGranted': 'True'
    }

    '''