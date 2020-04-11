from ElectionTimer import Election_Timer
from Heartbeat import Heartbeat
from Messenger import Messenger

from time import sleep, clock
from threading import Thread
import boto3, argparse, random, math

class log_entry:
	def __init__(self, term: int, command: str):
		self.term = term
		self.command = command
	
	''' log is append only, index = position in list . need a way to reference log'''

class ConsensusModule:
	''' Constructor. Takes id of node (str) and number of peers (int) as input.
	:param id: string. id from list '0', '1', '2', '3', '4'
	:param peer_count: int. number of peers in cluster. 

	PERSISTENT STORAGE:

	:var .term: int.       monotonic counter for cluster term. init at 0
	:var .voted_for:       string. records who this node voted for in election.
	:var .log:			   list. contains the replicated log. 

	VOLITILE STATE ON ALL SERVERS

	:var .id:              string. Takes id from list '0', '1', '2', '3', '4'
	:var .peers:           list of peer IDs in cluster.
	:var .election_state:  string. 'leader' 'candidate' or 'follower'
	:var .timer_length:    float. duration of election timer in seconds (default: .15)
	:var .vote_count:      int. Store number of votes received. Majority -> leader
	:var .reply_status:	   track which peers have replied to vote or append requests.
	
	:var .commitIndex: 	   int. index of highest log entry known to be committed.
	:var .lastApplied:     int. index of highest log entry applied to state machine. 

	VOLITILE STATE ON LEADER (reinit after election):

	:var .nextIndex: 	   int. As leader: index of next log entry to send to each server
	:var .matchIndex:      int. As leader: index of highest log entry known to be relicated on each server
	
	HELPER CLASSES:

	:var .messenger:       Messenger. takes care of all messaging between nodes
	:var .election_timer:  ElectionTimer. Timer thread to start election
	:var .heartbeat:       Heartbeat. Timer thread to send hearbeats when leader. 
	'''

	def __init__(self, id: str, peer_count: int):
		# The following three variables need to survive on persistent storage. 
		self.voted_for = 'null'
		self.log = []
		self.term = 0

		# volitile variables:
		self.id = id
		self.peers = [str(x) for x in range(0,peer_count) if x != int(self.id)]
		self.election_state = 'follower'
		self.timer_length = 4
		self.vote_count = 0
		self.reply_status = {}

		self.commitIndex = 0
		self.lastApplied = 0

		self.nextIndex = {}
		self.matchIndex = {}

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
		print('\n', self.id, ' Set state to follower, setting term to: ', self.term)
	
	def set_leader(self):
		'''Set the consensus module election state to 'leader', change timers '''
		print('\n', self.id, ' Set state to leader')
		self.election_state = 'leader' 
		self.election_timer.stop_timer() # pause the election timer, leader will remain leader
		self.send_heartbeat() # immediately send heartbeat to peers
		self.heartbeat.restart_timer() # continue sending heartbeat on interval
		self.reply_status = {}
		# TODO: more CM vars need to be reset here

	def send_heartbeat(self):
		'''Make a heartbeat message and send it to all peers. Used by leader'''
		heartbeat = self.make_message('heartbeat')
		print(heartbeat)
		for peer in self.peers: # send to peers
			self.messenger.send(heartbeat, peer)
		
	def start_election(self): # this is equivalent to "set_candidate()"
		'''Set election state to 'candidate', vote for self, and request votes 
		for leadership
		'''
		if self.election_state != 'candidate':
			print('\n', self.id, ' set state to candidate')
		self.heartbeat.stop_timer()
		self.election_state = 'candidate'
		self.term+= 1 # starting an election increments the term
		self.voted_for = self.id # vote for self
		self.vote_count = 1 # count of self vote
		#self.heartbeat.restart_timer() # use heartbeat to re-send vote request at interval
		for peer in self.peers: #set reply status to track who responds to votes
			self.reply_status[peer] = False
		self.request_votes() # request votes of all other 
		

	def request_votes(self):
		'''Make a vote request message and send to all peers. Used by candidate '''
		request = self.make_message('request votes')
		for peer, replied in self.reply_status.items(): # only send requests to those not replied already
			if not replied:
				
				self.messenger.send(request, peer)
				print(self.id, ' Requesting Vote from: ', peer, ' Term: ', self.term)
		
	def handle_incoming_message(self, message: dict):
		message_type = message['messageType']
		print('\n********* You Have Passed A message Back to CM: {} *****\n'.format(message))
		if message_type == 'AppendEntriesRPC':
			self.receive_append_entry_request(message)
		elif message_type == 'AppendReply':
			self.receive_append_entry_reply(message)
		elif message_type == 'RequestVotesRPC':
			self.receive_vote_request(message)
		elif message_type == 'VoteReply':
			self.receive_vote_reply(message)
			

	def receive_append_entry_request(self, message: dict):
		'''
		This method processes append entry requests. Outcomes: For every appendRPC
		reset the election timer. If a higher term is discovered, revert to
		follower. Append any new entries to the log. If incoming log entry
		conflicts with existing entry at same index, delete existing entry and 
		all that follow. Set commit index to be min(leaderCommit, index of last 
		new entry). 
		
		more details to follow later'''
		leader = message['leaderID']
		incoming_term = int(message['term'])

		print('\n', self.id, ' received append entry request from ', leader, ': \n',  message)
		if (incoming_term == self.term and self.election_state == 'follower'):
			self.election_timer.restart_timer()
		if (incoming_term > self.term or 
			(incoming_term == self.term and self.election_state == 'candidate')): 
			self.set_follower(incoming_term) # set state to follower
			print(self.id, ' greater term/leader detected, setting state to follower.')

			# TODO: more logic required here to properly apply entries and respond
			reply = self.make_message('reply to append request')
			self.messenger.send(reply, leader)
			
			print('\n', self.id, ' replied to append request')



	def receive_append_entry_reply(self, message: dict):
		'''
		This method processes replies from followers. If successful, update nextIndex
		and matchIndex for follower. If failed, decrement nextIndex for follower
		and try again. 
		'''
		follower = message['senderID']
		incoming_term = int(message['term'])
		if (incoming_term > self.term or 
			(incoming_term == self.term and self.election_state == 'candidate')): 
			self.set_follower(incoming_term) # set state to follower
			print(self.id, ' greater term/leader detected, setting state to follower.')
		#success = message['success']
		# TODO: process receive logic. 

		print('\n', self.id, ' received append entries reply :', message)


	def receive_vote_request(self, message: dict):
		'''
		this method processes RequestVoteRPCs. If the incomingTerm < self.term, 
		reply false. If self.votedFor is 'null' or equal to requesting candidate, 
		*and* the candidate's log is at least as up to date as self, grant vote. 
		'''
		candidate = message['candidateID']
		incoming_term = int(message['term'])
		print('\n', self.id, ' received vote request from ', candidate, ': \n')

		if incoming_term > self.term: # as always, check for greater term, set to follower if true
			self.set_follower(incoming_term) # set state to follower
			print(self.id, ' greater term detected, setting state to follower.')
		
		# TODO: if candidate log shorter than self, reply false

		if self.voted_for == 'null': #&& candidate log >= self:
			self.voted_for = candidate
			print('\n', self.id, ' voted for ', self.voted_for)
		
		reply = self.make_message('reply to vote request', candidate)
		self.messenger.send(reply, candidate)
		print('\n', self.id, ' replied ', reply['voteGranted'], ' to ', candidate, ' request for votes')

	def receive_vote_reply(self, message: dict):
		
		vote_granted = message['voteGranted'] #store value of vote received
		sender = message['senderID']
		print(self.id, ' received vote reply: ', vote_granted, ' from ', sender)

		if self.election_state == 'candidate':
			self.reply_status[sender] = True # mark sender as having replied
			if vote_granted == 'True':
				self.vote_count += 1
				print('\n', self.id, ' vote count = ', self.vote_count)
			print('votes needed: ',math.floor(len(self.peers)/2)+1)
			if self.vote_count > math.floor(len(self.peers)/2):
				self.set_leader()
				print('\n', self.id, ' majority votes acquired')

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
				'senderID':		self.id,
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
				'messageType': 	'VoteReply',
				'senderID':		self.id,
				'term':			str(self.term),
				'voteGranted':	voteGranted
			}
		else:
			print('you fucked up')
		return message
	

if __name__ == '__main__':
	parser =  argparse.ArgumentParser(description='Messenger Utility')
	parser.add_argument('id', help='id', type=str)
	#parser.add_argument('peerCount', help = 'peerCount', type=int)
	args = parser.parse_args()

	r = ConsensusModule(args.id, 5)