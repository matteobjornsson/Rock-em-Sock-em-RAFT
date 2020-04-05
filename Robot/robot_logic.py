import threading, time

"""

"""

class Robot:
    def __init__(self, color):
        self.color = color
        self.state = ''
        self.blocked = False
        self.timer = None

    def set_timer(self):
        self.blocked = False
        self.timer = None

    def start_timer_secs(self, seconds):
        new_timer = threading.Timer(seconds, self.set_timer)
        new_timer.start()
        self.timer = new_timer

    def punch_with_left(self):
        if not self.blocked:
            self.state = 'punch_left'
            self.blocked = True
            self.start_timer_secs(1.0)
        else:
            print("blocked from punching")

    def punch_with_right(self):
        if not self.blocked:
            self.state = 'punch_right'
            self.blocked = True
            self.start_timer_secs(1.0)
        else:
            print("blocked from punching")

    def punch_failed(self):
        if self.blocked:
            self.timer.cancel()
            self.start_timer_secs(3.0)

    def block_with_left(self):
        self.state = 'block_left'

    def block_with_right(self):
        self.state = 'block_right'

