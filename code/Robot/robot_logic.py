import threading, time
from code.Messenger import Messenger


class RobotBlockedError(ValueError):
    """
    Raise error when robot is blocked from punching
    """

    def __init__(self, message):
        self.message = message

class Robot:
    """

    """

    def __init__(self, color):
        """
        Robot constructor.
        Starts a listener for its own thread.
        :param color: identifies which robot is being used.
        """
        self.color = color
        self.state = ''
        self.robot_game_state = ''
        self.blocked = False
        self.timer = None
        self._id = 'client-' + self.color
        self.messenger = Messenger(self._id, self)

    def handle_incoming_message(self, msg:dict):
        self.robot_game_state = msg['msg']
        if self.robot_game_state == 'blocked':
            self.punch_blocked()

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
        self.state = 'exit'
        self.send_to_leader()

    def punch_with_left(self):
        """
        Punch with left arm if not blocked from punching.
        Block robot from punching for 1 second.
        :return:
        """
        if not self.blocked:
            self.state = 'punch_left'
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
            self.state = 'punch_right'
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
        self.state = 'block_left'
        self.send_to_leader()

    def block_with_right(self):
        """
        Block with right arm.
        :return:
        """
        self.state = 'block_right'
        self.send_to_leader()

    def send_to_leader(self):
        """
        Send message to server, i.e., leader queue.
        :return:
        """
        msg_dictionary = {'id': self._id, 'state': self.state}
        self.messenger.send(msg_dictionary, "leader")
