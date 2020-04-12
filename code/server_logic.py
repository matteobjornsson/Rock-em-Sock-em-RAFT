import sys
sys.path.append('..')
from Messenger import Messenger
from ConsensusModule import *
from threading import Thread
import random


class Server:
    """
    Actual server, will call server logic
    and tell it whether it is the leader or not.
    This is running on every node and keeps track of overall game status.
    """

    def __init__(self, nodeID):
        self.id = nodeID
        self.messenger = Messenger('leader', self)
        self.game_state = ''
        self.log = ''
        self.cm = ConsensusModule(self.id, 5)  # ConsensusModule() set to "leader" for testing purposes
        self.server_logic = ServerLogic(self.cm)
        self.lastApplied = 0
        self.log_checker = Thread(
            target=self.check_for_committed_commands,
            daemon=True
        ).start()

    def check_for_committed_commands(self):
        while True:
            if self.lastApplied < self.cm.commitIndex:
                self.lastApplied += 1
                command = self.cm.get_command(self.lastApplied)
                self.update_status(command)
                self.check_game_status()


    def handle_incoming_message(self, msg):
        received_msg = str(msg)
        self.cm.add_client_command_to_log(received_msg)
        #print(received_msg)
        #self.update_status(received_msg)
        #self.check_game_status()

    def update_status(self, received_msg):
        """
        Updates game status.
        Checks whether the received message came from red or blue client,
        server logic is updated and performed accordingly.
        Finally, message is sent to appropriate client.
        :return:
        """
        if received_msg['_id'] == 'client-red':
            self.server_logic.set_red_status(received_msg['state'])
            if not received_msg['state'] == 'exit':
                self.game_state, msg_to_send = self.server_logic.logic_after_commit(received_msg['_id'])
                if self.cm.election_state == 'leader':
                    self.messenger.send(msg_to_send, 'client-red')
            else:
                if self.cm.election_state == 'leader':
                    msg_to_send = {'msg': 'exit'}
                    self.messenger.send(msg_to_send, 'client-blue')
        elif received_msg['_id'] == 'client-blue':
            self.server_logic.set_blue_status(received_msg['state'])
            if not received_msg['state'] == 'exit':
                self.game_state, msg_to_send = self.server_logic.logic_after_commit(received_msg['_id'])
                if self.cm.election_state == 'leader':
                    self.messenger.send(msg_to_send, 'client-blue')
            else:
                if self.cm.election_state == 'leader':
                    msg_to_send = {'msg': 'exit'}
                    self.messenger.send(msg_to_send, 'client-red')

    def check_game_status(self):
        if self.cm.election_state == 'leader':
            msg_to_send = {'msg': 'lost'}
            if self.game_state == 'blue_won':
                self.messenger.send(msg_to_send, 'client-red')
            elif self.game_state == 'red_won':
                self.messenger.send(msg_to_send, 'client-blue')


class ServerLogic:
    """
    Logic for each server, i.e., what to do upon receiving a message.
    Differs between leader and follower nodes.
    Keeps track of blue and red's respective status.
    """

    def __init__(self, consensus_module):
        """
        ServerLogic constructor.
        Keeps track of blue and red states.
        """
        self.cm = consensus_module
        self.blue_status = ''
        self.red_status = ''

    def set_blue_status(self, blue_status):
        self.blue_status = blue_status

    def set_red_status(self, red_status):
        self.red_status = red_status

    def logic_after_commit(self, msg_id):
        """

        :param msg_id:
        :return game_state, return, message:
        """
        return_message = ''
        game_state = ''
        print('blue is ', self.blue_status)
        print('red is ', self.red_status)
        if msg_id == 'client-red':
            if (self.red_status == 'punch_right' and not self.blue_status == 'block_left') or (
                    self.red_status == 'punch_left' and not self.blue_status == 'block_right'):
                if self.cm.election_state == 'leader' and random.random() <= 0.1:
                    return_message = 'won'
                    game_state = 'red_won'
                else:
                    return_message = 'failed'
                    game_state = 'ongoing'
            elif (self.red_status == 'punch_right' and self.blue_status == 'block_left') or (
                    self.red_status == 'punch_left' and self.blue_status == 'block_right'):
                return_message = 'blocked'
                game_state = 'ongoing'
            else:
                game_state = 'ongoing'
                return_message = 'Nothing happened.'

        elif msg_id == 'client-blue':
            if (self.blue_status == 'punch_right' and not self.red_status == 'block_left') or (
                    self.blue_status == 'punch_left' and not self.red_status == 'block_right'):
                if self.cm.election_state == 'leader' and  random.random() <= 0.1:
                    return_message = 'won'
                    game_state = 'blue_won'
                else:
                    return_message = 'failed'
                    game_state = 'ongoing'
            elif (self.blue_status == 'punch_right' and self.red_status == 'block_left') or (
                    self.blue_status == 'punch_left' and self.red_status == 'block_right'):
                return_message = 'blocked'
                game_state = 'ongoing'
            else:
                game_state = 'ongoing'
                return_message = 'Nothing happened.'

        return_msg_dict = {'msg': return_message}
        return game_state, return_msg_dict
