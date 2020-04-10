from code.Messenger import Messenger
import random


class Server:
    """
    Actual server, will call server logic
    and tell it whether it is the leader or not.
    This is running on every node and keeps track of overall game status.
    """

    def __init__(self, nodeID):
        self.id = nodeID
        self.messenger = Messenger(self.id, self)
        self.game_state = ''
        self.log = ''
        self.consensus_module = 'leader'  # ConsensusModule() set to "leader" for testing purposes
        self.server_logic = ServerLogic()

    def handle_incoming_message(self, msg):
        received_msg = msg
        print(received_msg)
        self.update_status(received_msg)
        self.check_game_status()

    def update_status(self, received_msg):
        """
        Updates game status.
        Checks whether the received message came from red or blue client,
        server logic is updated and performed accordingly.
        Finally, message is sent to appropriate client.
        :return:
        """
        print("update status")
        if received_msg['_id'] == 'client-red':
            print("entered client red")
            self.server_logic.set_red_status(received_msg['state'])
            if received_msg['state'] != 'exit':
                self.game_state, msg_to_send = self.server_logic.logic_after_commit(received_msg['_id'])
                if self.consensus_module == 'leader':
                    self.messenger.send(msg_to_send, 'client-red')
            elif received_msg['state'] == 'exit':
                msg_to_send = {'msg': 'exit'}
                self.messenger.send(msg_to_send, 'client-blue')
        elif received_msg['_id'] == 'client-blue':
            self.server_logic.set_blue_status(received_msg['state'])
            if received_msg['state'] != 'exit':
                self.game_state, msg_to_send = self.server_logic.logic_after_commit(received_msg['_id'])
                if self.consensus_module == 'leader':
                    self.messenger.send(msg_to_send, 'client-blue')
            elif received_msg['state'] == 'exit':
                msg_to_send = {'msg': 'exit'}
                self.messenger.send(msg_to_send, 'client-red')

    def check_game_status(self):
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

    def __init__(self):
        """
        ServerLogic constructor.
        Keeps track of blue and red states.
        """
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
                if random.random() <= 0.1:
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
                if random.random() <= 0.1:
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
