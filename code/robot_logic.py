import threading
from Messenger import Messenger
from server_logic import Server


class RobotBlockedError(ValueError):
    """
    Raise error when robot is blocked from punching
    """

    def __init__(self, message):
        self.message = message


class Robot:

    """
    Robot class to perform the different punch and block actions, as well as any required logic.
    :var color: What robot the player is using
    :var action_state: Robot action state (punch_left, punch_right, block_left, block_right)
    :var game_state: Overall robot state in game (won, lost, blocked, failed, exit)
    :var blocked: Boolean to check whether robot is blocked from punching (True) or not (False)
    :var timer: Timer to set when robot is blocked from punching
    :var _id: messenger queue id
    :var messenger: Handles incoming messages and sends messages.
    """
    def __init__(self, color, ui):
        """
        Robot constructor.
        Starts a listener for its own thread.
        :param color: identifies which robot is being used.
        """
        self.color = color
        self.action_state = ''
        self.game_state = ''
        self.blocked = False
        self.timer = None
        self._id = 'client-' + self.color
        self.messenger = Messenger(self._id, self)
        self.ui = ui

    def handle_incoming_message(self, msg:dict):
        """
        Method that needs to be implemented for Messenger class.
        Performs the following actions when a message is pulled from the queue.
        1. checks game state based on message
        2. Sets punch block timer for 3 seconds if punch was blocked
        :param msg:
        :return:
        """
        self.game_state = msg['msg']
        if self.game_state == 'blocked':
            self.punch_blocked()
        self.set_running_game()

    def set_running_game(self):
        if self.game_state == 'won':
            print("Congratulations! You knocked your opponents' head off!")
            self.ui.running_game = False
        elif self.game_state == 'lost':
            print('Oops, looks like your head got knocked off. ')
            self.ui.running_game = False
        elif self.game_state == 'exit':
            print("Your opponent forfeited, you win!")
            self.ui.running_game = False
        elif self.game_state == 'blocked':
            print("Your opponent blocked your punch. ")
            self.ui.running_game = True
        elif self.game_state == 'failed':
            print("You missed...")
            self.ui.running_game = True
        elif self.game_state == 'blocked_punch':
            print("You blocked a punch!")
            self.ui.running_game = True
        else:
            self.ui.running_game = True

    def timer_action(self):
        """
        Action to perform when timer finishes.
        Sets blocked to False, so robot can punch again.
        Resets timer variable.
        :return:
        """
        self.blocked = False
        self.timer = None

    def start_timer_secs(self, seconds):
        """
        Set timer for n seconds.
        Starts a thread that sleeps until time is over.
        Once it wakes up, it calls the function to perform.
        :param seconds: Number of seconds timer should run.
        :return:
        """
        new_timer = threading.Timer(seconds, self.timer_action)
        new_timer.start()
        self.timer = new_timer

    def stop_game(self):
        """
        User chose to stop game.
        Send message to server to notify other player.
        :return:
        """
        self.action_state = 'exit'
        self.send_to_leader()

    def punch_with_left(self):
        """
        Punch with left arm if not blocked from punching.
        Block robot from punching for 1 second.
        :return:
        """
        if not self.blocked:
            self.action_state = 'punch_left'
            self.blocked = True
            self.send_to_leader()
            self.start_timer_secs(1.0)
        else:
            raise RobotBlockedError("Robot is blocked from punching.")

    def punch_with_right(self):
        """
        Punch with right arm if not blocked from punching.
        Block robot from punching for 1 second.
        :return:
        """
        if not self.blocked:
            self.action_state = 'punch_right'
            self.blocked = True
            self.send_to_leader()
            self.start_timer_secs(1.0)
        else:
            raise RobotBlockedError("Robot is blocked from punching.")

    def punch_blocked(self):
        """
        Call if robot receives message that punch failed.
        Blocks robot from punching for 3 seconds.
        :return:
        """
        if self.blocked:
            self.timer.cancel()
            self.start_timer_secs(3.0)
        else:
            self.blocked = True
            self.start_timer_secs(3.0)

    def block_with_left(self):
        """
        Block with left arm.
        :return:
        """
        self.action_state = 'block_left'
        self.send_to_leader()

    def block_with_right(self):
        """
        Block with right arm.
        :return:
        """
        self.action_state = 'block_right'
        self.send_to_leader()

    def send_to_leader(self):
        """
        Send message to server, i.e., leader queue.
        :return:
        """
        msg_dictionary = {'_id': self._id, 'state': self.action_state}
        self.messenger.send(msg_dictionary, "leader")


if __name__ == '__main__':
    robot_blue = Robot('blue')
    robot_red = Robot('red')
    robot_blue.punch_with_right()
