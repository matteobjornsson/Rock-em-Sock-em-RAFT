from ElectionTimer import Election_Timer
from Heartbeat import Heartbeat
from Messenger import Messenger
from time import sleep, clock
from datetime import datetime
from threading import Thread
import boto3, argparse, random, math

class Replica:


#   Initialize:       
#################################################################
    def __init__(self, id: str):
        self.replica_count = 5
        self.id = id
        self.peers = [
            str(x) for x in range(0,self.replica_count) if x != int(self.id)
            ]
        self.election_state = 'follower'
        self.timer_length = 1.2
        self.current_term = 0
        self.voted_for = None
        self.vote_count = 0


        self.messenger = Messenger(self.id)
        self.election_timer = self.start_election_timer()
        self.heartbeat = self.start_heartbeat()

    def start_election_timer(self):
        et = Election_Timer(self.timer_length, self)
        t = Thread( 
            target=et.run, 
            name='Election Timer Thread'
            )
        t.start()
        return et    

    def start_heartbeat(self):
        hb = Heartbeat(self.timer_length, self)
        t = Thread( 
            target=hb.run, 
            name='Heartbeat Thread'
            )
        t.start()
        return hb 

# Change Replica State: 
#################################################################

    def set_state_to_follower(self, term:int):
        print('Set state to follower')
        self.election_state = 'follower'
        self.current_term = term
        self.election_timer.restart_timer()
        self.vote_count = 0
        self.voted_for = None
        self.heartbeat.stop()

    def set_state_to_leader(self):
        print('Set state to leader')
        self.election_state = 'leader'
        self.election_timer.stop()
        self.vote_count = 0
        self.voted_for = None
        self.heartbeat.restart_timer()

    def set_state_to_candidate(self):
        print('set state to candidate')
        self.election_state = 'candidate'
        self.current_term += 1
        self.vote_count = 1
        self.heartbeat.stop()

# Process incoming messages:
#################################################################

    def receive_append_entries_request(self, message: dict):
        print('received append entry request: \n', message, '\nfrom ', message['leaderID'])
        incoming_term = int(message['term'])
        if incoming_term >= self.current_term:
            print('greater term detected, reverting to follower')
            self.set_state_to_follower(incoming_term)
            reply = self.make_message('reply to append request')
            self.messenger.send(reply, message['leaderID'])
            print('replied to append request')
        else:
            print('incoming term less than current, no reply')


    def receive_vote_request(self, message: dict):
        candidate = message['candidateID']
        print('received vote request from ', candidate, ': \n', message)
        incoming_term = int(message['term'])
        if incoming_term >= self.current_term:
            self.set_state_to_follower(incoming_term)
            print('greater term detected, reverting to follower')

        candidate = message['candidateID']
        if self.voted_for == None or self.voted_for == candidate:
            self.voted_for == candidate
            print('voted for ', candidate)
        reply = self.make_message('reply to vote request', candidate)
        self.messenger.send(reply, candidate)
        print('replied to ', candidate)

    def receive_append_entries_reply(self, message: dict):
        print('received append entries reply')

    def receive_vote_reply(self, message: dict):
        print('received vote reply')
        if message['voteGranted'] == 'True':
            self.vote_count += 1
            print('vote count = ', self.vote_count)
        if self.vote_count > math.floor(self.replica_count/2):
            self.set_state_to_leader()
            print('majority votes acquired')

# Helper Functions
#################################################################

    def make_message(self, message_type: str, destination: str = '') -> dict:
        '''
        options: 'heartbeat', 'reply to append request', 'request votes', 
        'reply to vote request'. Include destination with reply to vote request. 
        '''
        if message_type == 'heartbeat':
            message = {
                'messageType' : 'AppendEntriesRPC',
                'leaderId': self.id,
                'term'  : str(self.current_term),
                'entries': []
                #'prevLogIndex' : 'self.prevLogIndex',
                #'prevLogTerm' : 'self.prevLogTerm',
                #'leaderCommit' : 'self.commitIndex'
            }
        elif message_type == 'reply to append request':
            message = {
                'messageType' : 'AppendReply',
                'term' : str(self.current_term),
                #'success' : 'True'
            }
        elif message_type == 'request votes':
            message = {
                'messageType' : 'RequestVotesRPC',
                'term': str(self.current_term),
                'candidateID': self.id,
                #'lastLogIndex': self.lastLogIndex,
                #'lastLogTerm': self.lastLogTerm
            }
        elif message_type == 'reply to vote request':
            voteGranted = 'False'
            if  self.voted_for == destination:
                voteGranted = 'True'
            message ={
                'messageType' : 'VoteReply',
                'term': str(self.current_term),
                'voteGranted': voteGranted
                }

        else:
            return {'messageType': 'blank'}


    def send_heartbeat(self):
        heartbeat = self.make_message('heartbeat')
        for peer in self.peers:
            self.messenger.send(heartbeat, peer)

    def request_votes(self):
        request = self.make_message('request votes')
        for peer in self.peers:
            self.messenger.send(request, peer)
        

    def startElection(self):
        self.set_state_to_candidate()
        self.request_votes()
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