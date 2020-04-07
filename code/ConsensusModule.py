from ElectionTimer import Election_Timer
from Heartbeat import Heartbeat
from Messenger import Messenger

from time import sleep, clock
from threading import Thread
import boto3, argparse, random, math

class ConsensusModule:

	def __init__(self, id: str, peer_count: int):
		''' Constructor. Takes id of node (str) and number of peers (int) as input.

		:param id: string. id from list '0', '1', '2', '3', '4'
		:param peer_count: int. number of peers in cluster. 

		:var .id:              string. Takes id from list '0', '1', '2', '3', '4'
		:var .peers:           list of peer IDs in cluster.
		:var .election_state:  string. 'leader' 'candidate' or 'follower'
		:var .timer_length:    float. duration of election timer in seconds (default: .15)
		:var .term: int.       monotonic counter for cluster term. init at 0
		:var .voted_for:       string. records who this node voted for in election.
		:var .vote_count:      int. Store number of votes received. Majority -> leader
		:var .messenger:       Messenger. takes care of all messaging between nodes
		:var .election_timer:  ElectionTimer. Timer thread to start election
		:var .heartbeat:       Heartbeat. Timer thread to send hearbeats when leader. 
		'''
		self.id = id
		self.peers = [str(x) for x in range(0,peer_count) if x != int(self.id)]
		self.election_state = 'follower'
		self.timer_length = 4
		self.term = 0
		self.voted_for = 'null'
		self.vote_count = 0

		self.messenger = Messenger(self.id, self)
		self.election_timer = Election_Timer(self.timer_length, self)
		self.heartbeat = Heartbeat(self.timer_length, self)

	def set_follower(self, term: int):
		''' Set the consensus module election state to 'follower', reset variables. 
		:param term: int, included for when an incoming term is discovered greater than own. 
		'''
		self.election_state = 'follower'
		self.term = term # update term to newly discovered term
		self.vote_count = 0 # followers do not have votes, reset to 0
		self.voted_for = 'null' # a new follwer has yet to vote for another peer
		self.election_timer.restart_timer() # reset election countdown
		self.heartbeat.stop_timer() # stop the heartbeat, only leaders send them
	
	def set_leader(self):
		'''Set the consensus module election state to 'leader', change timers '''
		self.election_state = 'leader' 
		self.election_timer.stop_timer() # pause the election timer, leader will remain leader
		self.send_heartbeat() # immediately send heartbeat to peers
		self.heartbeat.restart_timer() # continue sending heartbeat on interval

	def send_heartbeat(self):
		'''Make a heartbeat message and send it to all peers. Used by leader'''
		heartbeat = self.make_message('heartbeat')
		for peer in self.peers: # send to peers
			self.messenger.send(heartbeat, peer)
		
	def start_election(self):
		'''Set election state to 'candidate', vote for self, and request votes 
		for leadership
		'''
		self.election_state = 'candidate'
		self.term+= 1 # starting an election increments the term
		self.voted_for = self.id # vote for self
		self.vote_count = 1 # count of self vote
		self.heartbeat.restart_timer() # use heartbeat to re-send vote request at interval
		self.request_votes() # request votes of all other 

	def request_votes(self):
		'''Make a vote request message and send to all peers. Used by candidate '''
		request = self.make_message('request votes')
		for peer in self.peers: # send to peers
			self.messenger.send(request, peer)


	def handle_incoming_message(self, message: dict):
		message_type = message['messageType']
		if message_type == 'AppendEntriesRPC':
			self.receive_append_entries_request(message)
		elif message_type == 'AppendReply':
			self.receive_append_entries_reply(message)
		elif message_type == 'RequestVotesRPC':
			self.receive_vote_request(message)
		elif message_type == 'VoteReply':
			self.receive_vote_reply(message)
			

	def receive_append_entry_request(self, message: dict):
		pass

	def receive_append_entry_reply(self, message: dict):
		pass

	def receive_vote_request(self, message: dict):
		pass

	def receive_vote_reply(self, message: dict):
		pass


	def make_message(self, message_type: str, destination: str = '') -> dict:
		'''
		options: 'heartbeat', 'reply to append request', 'request votes', 
		'reply to vote request'. returns a dictionary
		Include destination with reply to vote request. 
		'''
		if message_type == 'heartbeat':
			message = {
				'messageType': 	'AppendEntriesRPC',
				'leaderID': 	self.id,
				'term': 		str(self.term)
				#'prevLogIndex' : 'self.prevLogIndex',
				#'prevLogTerm' : 'self.prevLogTerm',
				#'leaderCommit' : 'self.commitIndex'
			}
		elif message_type == 'reply to append request':
			message = {
				'messageType':	'AppendReply',
				'term':			str(self.term)
				#'success' : {'value': 'True'}
			}
		elif message_type == 'request votes':
			message = {
				'messageType':	'RequestVotesRPC',
				'term':			str(self.term),
				'candidateID':	self.id
				#'lastLogIndex': {'value': self.lastLogIndex},
				#'lastLogTerm': {'value': self.lastLogTerm}
			}
		elif message_type == 'reply to vote request':
			voteGranted = 'False'
			if self.voted_for == destination:
				voteGranted = 'True'

			message = {
				'messageType': 	'voteReply',
				'term':			str(self.term),
				'voteGranted':	voteGranted
			}