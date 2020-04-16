from ElectionTimer import Election_Timer
from Heartbeat import Heartbeat
from Messenger import Messenger
from server_logic import *

from time import sleep, clock
from threading import Thread
import boto3, argparse, random, math, csv, ast, os, re
import numpy as np

class LogEntry:
	def __init__(self, term: int, command: str):
		self.term = term
		self.command = command
		self.iterable = [self.term, self.command]

	def __str__(self):
		return str(self.term) + '\t' + self.command
		
	def from_string(self, _str: str):
		values = re.split(r'\t+',_str)
		return LogEntry(int(values[0]), values[1])

	''' log is append only, index = position in list . need a way to reference log'''


class Log:
	''' 
	*** public methods: *** 
	
	get_entry(index): get entry in log at index
	rollback(index): delete entries after index
	append_to_end(LogEntry): append log entry to log
	print_log(): print log

	 '''

	def __init__(self, nodeID: str):
		"""
		Log constructor
		Attributes:
		:var log: list of LogEntries
		:var file_path: filepath to read and write from
		"""

		self.log = []
		self.header = ["TERM", "COMMAND"]
		self.logfile = ''
		if not os.path.isdir('../files'):
			os.mkdir('../files')
		self.file_path = f'../files/logOutput{nodeID}.tsv'
		
		#try:
		#	os.remove(self.file_path)
		#except:
		#	pass

		try:
			with open(self.file_path, 'r') as read_file:
				csv_reader = csv.reader(read_file, delimiter='\t')
				next(csv_reader)
				for line in csv_reader:
					#print(line)
					self.log.append(self.read_log_line(line))

		except FileNotFoundError:
			with open(self.file_path, 'w+', newline='') as out_file:
				log_writer = csv.writer(out_file, delimiter='\t')
				log_writer.writerow(self.header)
			# this might break things later, but something needs to exist at log[0]
			self.append_to_end(LogEntry(0,'null'))

	def read_log_line(self, line):
		"""
		Reading a single line in a log
		"""
		return LogEntry(int(line[0]), line[1])

	def __len__(self):
		return len(self.log)

	def get_entry(self, idx) -> LogEntry:
		try:
			#print(self.log[idx])
			return self.log[idx]
		except IndexError:
			print(f"There is no entry at index {idx:d} in the log.")

	def idx_exist(self, idx):
		try:
			id = self.log[idx]
			#print(f"index {idx:d} exists")
			return True
		except IndexError:
			#print(f"index {idx:d} does not")
			return False

	def append_to_end(self, logentry: LogEntry):
		"""
		Insert new log entry and add to file.
		:type logentry: object LogEntry
		"""
		self.log.append(logentry)

		with open(self.file_path, 'a', newline='') as out_file:
			log_writer = csv.writer(out_file, delimiter='\t')
			log_writer.writerow(logentry.iterable)

	def print_log(self):
		for x in self.log:
			print(str(x))

	def rollback(self, idx):
		to_delete = []
		for i in range(idx, len(self.log)):
			to_delete.append(i)
		self.log = np.delete(self.log, to_delete).tolist()

	def get_entries_string(self, idx: int) -> str:
		entries = ''
		for i in range(idx, len(self.log)):
			entries += str(self.get_entry(i)) + ';'
		entries = entries.strip(';')
		return entries

	def parse_entries_to_list(self, entries: str) -> list:
		if entries == 'heartbeat':
			return []
		else:
			split_strings = entries.split(';')
			entry_list= []
			for entry in split_strings:
				entry_list.append(LogEntry.from_string(LogEntry, entry))
			return entry_list



class ConsensusModule:
	''' Constructor. Takes id of node (str) and number of peers (int) as

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

	def __init__(self, id: str, peer_count: int, server: object):
		# The following three variables need to survive on persistent storage.
		self.voted_for = 'null'
		self.log = Log(id)
		self.term = self.log.get_entry(len(self.log)-1).term

		# volitile variables:
		self.id = id
		self.server = server
		self.peers = [str(x) for x in range(0, peer_count) if x != int(self.id)]
		self.election_state = 'follower'
		self.timer_length = 2
		self.vote_count = 0
		self.reply_status = {}

		self.commitIndex = 0
		self.lastApplied = 0

		self.nextIndex = None
		self.matchIndex = None
		self.reset_next_and_match()

		self.messenger = Messenger(self.id, self, run=True)
		self.election_timer = Election_Timer(self.timer_length, self)
		self.heartbeat = Heartbeat(self.timer_length, self)


	def set_follower(self, term: int):
		''' Set the consensus module election state to 'follower', reset variables.
		:param term: int, included for when an incoming term is discovered greater than own. 
		'''
		self.election_state = 'follower'
		self.term = term  # update term to newly discovered term
		self.vote_count = 0  # followers do not have votes, reset to 0
		self.voted_for = 'null'  # a new follwer has yet to vote for another peer
		self.election_timer.restart_timer()  # reset election countdown
		self.heartbeat.stop_timer()  # stop the heartbeat, only leaders send them
		self.server.turn_off_leader_queue()
		#print('\n', self.id, ' Set state to follower, setting term to: ', self.term)

	def set_leader(self):
		'''Set the consensus module election state to 'leader', change timers '''
		#print('\n', self.id, ' Set state to leader')
		self.election_state = 'leader'
		self.election_timer.stop_timer()  # pause the election timer, leader will remain leader
		self.reset_next_and_match()
		self.send_heartbeat()  # immediately send heartbeat to peers
		self.heartbeat.restart_timer()  # continue sending heartbeat on interval
		self.server.turn_on_leader_queue()

	def reset_next_and_match(self):
			#print("lenght of log: ", len(self.log))
			self.nextIndex = dict.fromkeys(self.peers, len(self.log))
			print('nextIndex initialized to length of current log :', self.nextIndex)
			self.matchIndex = dict.fromkeys(self.peers, 0)

	def start_election(self):  # this is equivalent to "set_candidate()"
		'''Set election state to 'candidate', vote for self, and request votes
		for leadership
		'''
		#if self.election_state != 'candidate':
			# print('\n', self.id, ' set state to candidate')
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
				#print(self.id, ' Requesting Vote from: ', peer, ' Term: ', self.term)
		
	def send_heartbeat(self):
		'''Make a heartbeat message and send it to all peers. Used by leader'''	
		if self.election_state == 'leader':
			for peer in self.peers:  # send to peers
				entries_string = self.log.get_entries_string(self.nextIndex[peer])
				if not entries_string:
					entries_string = 'heartbeat'
				heartbeat = self.make_message('heartbeat', entries=entries_string, destination=peer)
				#print('Append entries: ', heartbeat)
				self.messenger.send(heartbeat, peer)

	def handle_incoming_message(self, message: dict):
		message_type = message['messageType']
		incoming_term = int(message['term'])
		if (incoming_term > self.term):
			self.set_follower(incoming_term)
			#print(self.id, ' greater term detected, setting state to follower.')
		print(
			'\n**** Message Received: {}\n'.format(message), " self term: ", 
			self.term, " self state: ", self.election_state
			)
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
		#print("****HEREHEREHRER ***", message['entries'])
		entries = self.log.parse_entries_to_list(message['entries'])

		leaderCommit = int(message['leaderCommit'])
		prevLogIndex = int(message['prevLogIndex'])
		prevLogTerm = int(message['prevLogTerm'])
		prevLogCommand = message['prevLogCommand']
		# special scenario for converting to follower: 
		if incoming_term == self.term and self.election_state == 'candidate':
			self.set_follower(incoming_term)
			#print(self.id, ' leader of greater or equal term detected as candidate, setting state to follower.')
		
		# reset election timer
		#print('\n', self.id, ' received append entry request from ', leader, ': \n',  message)
		if (self.election_state == 'follower'):
			self.election_timer.restart_timer()

		success, match = self.process_AppendRPC(
			entries=entries, 
			leaderCommit=leaderCommit, 
			prevLogIndex=prevLogIndex, 
			prevLogTerm=prevLogTerm,
			prevLogCommand=prevLogCommand
			)
		#print(success, match, '=================================')
		reply = self.make_message('reply to append request', success=success)
		self.messenger.send(reply, leader)
			
		#print('\n', self.id, ' replied to append request')

	def process_AppendRPC(self, entries: list, leaderCommit: int, prevLogIndex: int,
								prevLogTerm: int, prevLogCommand: str)-> (bool, int):
		# if logs are inconsistent or out of term, reply false
		if (not self.log.idx_exist(prevLogIndex) ):
			print("no log entry at prev log index")
		elif (self.log.get_entry(prevLogIndex).term != prevLogTerm):
			print("previous log entry not same term as incoming")
		elif (self.log.get_entry(prevLogIndex).command != prevLogCommand):
			print("command at prev log index does not match incoming")

		#reply false if log doesn’t contain an entry at prevLogIndex whose 
		# term matches prevLogTerm (§5.3)
		if ((not self.log.idx_exist(prevLogIndex))
			or (self.log.get_entry(prevLogIndex).term != prevLogTerm)):
			#or (self.log.get_entry(prevLogIndex).command != prevLogCommand)): 
			return False, 0
		# otherwise, check if outdated entry exists in initial append spot. 
		# if so, delete that entry and all following
		else:
			if self.log.idx_exist(prevLogIndex+1):
				self.log.rollback(prevLogIndex+1)
			# Append the new entries to the log
			for entry in entries:
				self.log.append_to_end(entry)
			#update the commit index of this server, equal to leader or last applied
			# whichever is smaller
			if leaderCommit > self.commitIndex:
				self.commitIndex = min(leaderCommit, len(self.log)-1)
			# return True and tell leader index of last applied to update match
			return True, len(self.log)-1



	def receive_append_entry_reply(self, message: dict):
		'''
		This method processes replies from followers. If successful, update nextIndex
		and matchIndex for follower. If failed, decrement nextIndex for follower
		and try again. 
		'''
	
		incoming_term = int(message['term'])
		if incoming_term > self.term:
			self.set_follower(incoming_term)  # set state to follower
			#print(self.id, ' greater term/leader detected, setting state to follower.')

		follower = message['senderID']
		success = bool(message['success'])
		incoming_match = int(message['match'])
		# only do the following if we are currently leader. 
		if self.election_state == 'leader':
			if success: # follower is up to date
				self.matchIndex[follower] = incoming_match
				self.nextIndex[follower] = incoming_match + 1
			else: # follower is not up to date, decrement index for next time
				if self.nextIndex[follower] > 1: # next index should never be less than 1
					self.nextIndex[follower] -= 1

			#############################
			# Commit available entries: #
			#############################
			N = self.commitIndex + 1
			if self.log.idx_exist(N): # if an entry exists to commit
				print('log entry to be committed exists')
				# collect the number of peers that have replicated entry N
				match_count = 0
				for peer in self.peers: 
					if self.matchIndex[peer] >= N:
						match_count += 1 
				print("match count: ", match_count, " log term: ", self.log.get_entry(N).term)
				# If there exists an N such that N > commitIndex, a majority of 
				# matchIndex[i] ≥ N, and log[N].term == currentTerm:
				# set commitIndex = N (§5.3, §5.4).
				if (self.log.get_entry(N).term == self.term 
						and match_count + 1 > math.floor(len(self.peers) / 2)):
					# if that entry is the correct term, and if a majority of 
					# servers have replicated that entry, commit that entry. 
					print("increment commit index")
					self.commitIndex = N

		#print('\n', self.id, ' received append entries reply :', message)

	def receive_vote_request(self, message: dict):
		'''
		this method processes RequestVoteRPCs. If the incomingTerm < self.term, 
		reply false. If self.votedFor is 'null' or equal to requesting candidate, 
		*and* the candidate's log is at least as up to date as self, grant vote. 
		'''
		candidate = message['candidateID']
		incoming_term = int(message['term'])
		#print('\n', self.id, ' received vote request from ', candidate, ': \n')

		if incoming_term > self.term:  # as always, check for greater term, set to follower if true
			self.set_follower(incoming_term)  # set state to follower
			#print(self.id, ' greater term detected, setting state to follower.')

		# TODO: if candidate log shorter than self, reply false

		if self.voted_for == 'null':  # && candidate log is at least as up to date as self:
			# at least as up to date is defined as: 
			# term of incoming last log entry is equal or greater than self. 
			# if equal term, index of incoming last log entry is equal or greater than self. 
			self.voted_for = candidate
			#print('\n', self.id, ' voted for ', self.voted_for)

		voteGranted = False
		if self.voted_for == candidate:
			voteGranted = True

		reply = self.make_message('reply to vote request', voteGranted= voteGranted)
		self.messenger.send(reply, candidate)
		
		#print('\n', self.id, ' replied ', reply['voteGranted'], ' to ', candidate, ' request for votes')

	def receive_vote_reply(self, message: dict):

		vote_granted = message['voteGranted']  # store value of vote received
		sender = message['senderID']
		incoming_term = int(message['term'])
		#print(self.id, ' received vote reply: ', vote_granted, ' from ', sender)

		if self.election_state == 'candidate' and incoming_term == self.term:
			if not self.reply_status[sender]:
				self.reply_status[sender] = True  # mark sender as having replied
				if vote_granted == 'True':
					self.vote_count += 1
					#print('\n', self.id, ' vote count = ', self.vote_count)
				#print('votes needed: ', math.floor(len(self.peers) / 2) + 1)
				if self.vote_count > math.floor(len(self.peers) / 2):
					self.set_leader()
					#print('\n', self.id, ' majority votes acquired')

	def make_message(self, message_type: str, voteGranted:bool = False, 
	success: bool = False, entries: str = '[]', destination = '') -> dict:
		'''
		options: 'heartbeat', 'reply to append request', 'request votes', 
		'reply to vote request'. returns a dictionary
		Include destination with reply to vote request. 
		'''
		if message_type == 'heartbeat':
			prevLogIndex = self.nextIndex[destination]-1
			print("make heartbeat. NextIndex: ", self.nextIndex[destination])
			prevLog = self.log.get_entry(prevLogIndex)
			message = {
				'messageType': 	'AppendEntriesRPC',
				'leaderID': 	self.id,
				'term': 		str(self.term),
				'entries'		:	entries,
				'prevLogIndex' : str(prevLogIndex), 
				'prevLogTerm' : str(prevLog.term),
				'prevLogCommand': str(prevLog.command),
				'leaderCommit' : str(self.commitIndex),
				'nextIndex' : str(self.nextIndex[destination])
			}
		elif message_type == 'reply to append request':
			message = {
				'messageType':	'AppendReply',
				'senderID':		self.id,
				'term':			str(self.term),
				'match':		str(len(self.log)-1), #return index of last appended entry if true
				'success' : 	str(success)
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
			message = {
				'messageType': 	'VoteReply',
				'senderID':		self.id,
				'term':			str(self.term),
				'voteGranted':	str(voteGranted)
			}
		else:
			print('you fucked up')
		return message

	def add_client_command_to_log(self, command: str):
		self.log.append_to_end(LogEntry(self.term, command))

	def get_command(self, idx)-> dict:
		command_str = self.log.get_entry(idx).command
		command = ast.literal_eval(command_str)
		return command
	
	def simulation_print(self):
		node = f"Node:\t\t{self.id}\n"
		term = f"Term:\t\t{str(self.term)}\n"
		commitIndex = f"Commit Index:\t{str(self.commitIndex)}\n"
		electionState = f"Election State:\t{self.election_state}\n"
		votedFor = f"Voted For:\t{self.voted_for}\n"
		voteCount = f"Vote Count:\t{str(self.vote_count)}\n"

		
		loglen = len(self.log)
		display_width = 14
		log_height = 10
		log_contents = ''
		if self.election_state != 'leader':
			log_contents += f'\nLog Contents: (Most Recent {log_height:d} Logs)\nIndex\tTerm\tCommand\n'
			if loglen <= log_height:
				for x in range(0, loglen):
					log_contents += str(x) +'\t' + str(self.log.get_entry(x)) + '\n'
			else:
				for x in range(loglen-log_height, loglen):
					log_contents += str(x) +'\t' + str(self.log.get_entry(x)) + '\n'

		header1 = ''
		header2 = ''
		peerStatusHeader = ''
		peerStatus = ''
		peerStatus2 = ''
		if self.election_state == 'leader':
			level1 = False
			level2 = False
			for peer in self.peers:
				if self.matchIndex[peer]<= display_width-1:
					level1 = True
				else: 
					level2 = True
			if level1:
				header1 = '----------' + '--------'*display_width +'\n'+"Index: "
				for x in range(0, display_width + 1): 
					header1 += '\t ' + str(x)
				header1 += '\n' + '----------' + '--------'*display_width+'\n'
			if level2:
				#if loglen > display_width:
				header2 = '----------' + '--------'*display_width +'\n'+"Index: "
				for x in range(display_width, 2*display_width+1): 
					header2 += '\t ' + str(x)
				header2 += '\n' + '----------' + '--------'*display_width+'\n'
			
			peerStatusHeader = '\nFollower Match * and Next ^ Indices:\n'

			for peer in self.peers:
				match = self.matchIndex[peer]
				next = self.nextIndex[peer]
				if match <= display_width-1:
					peerStatus += 'Node ' + peer + ':'
					mtab = '\t'*(match+1)
					ntab=''
					ntab2=''
					rtrn=''
					if next <= display_width:
						ntab = '\t'*(next - match) + ' ^\n'
					else:
						rtrn += '\n'
						peerStatus2+= 'Node ' + peer + ':'
						ntab2 = '\t'*(next+1 - display_width) + ' ^\n'
						peerStatus2 += ntab2
					peerStatus += mtab + ' *' + rtrn  + ntab 
				else:
					peerStatus2 += 'Node ' + peer + ':'
					mtab = '\t'*(match+1 - display_width)
					ntab = '\t'*(next - match) + ' ^\n'
					peerStatus2 += mtab + ' *' + ntab 

		status = (node + term + commitIndex + electionState+  
				 log_contents + peerStatusHeader + header1 + peerStatus + header2 + peerStatus2)
		
		file = open(f"../files/status{self.id}.txt", 'w')
		file.write(status)
		file.close()
		
if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Messenger Utility')
	parser.add_argument('id', help='id', type=str)
	# parser.add_argument('peerCount', help = 'peerCount', type=int)
	args = parser.parse_args()

	r = ConsensusModule(args.id, 5)
