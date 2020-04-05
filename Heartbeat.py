from time import sleep, clock
from threading import Thread, Timer
import random

class Heartbeat:

    def __init__(self, duration: float, target):
        self.target = target
        self.duration = duration/2
        self.running = True
        self.restart = False
        self.stop = True

    def kill_thread(self):
        self.running = False

    def stop(self):
        self.stop = True

    def restart_timer(self):
        self.restart = True
        self.stop = False

    def new_timeout(self) -> float:
        return (self.duration + self.duration * random.random())

    def run(self):
        # randomize timeouts to avoid conflicting elections
        timeout = self.new_timeout()
        #start the timer
        start = clock()
        while self.running:
            while not self.stop:
                if self.restart:
                    timeout = self.new_timeout()
                    start = clock()
                    self.restart = False

                elapsed_time = clock() - start
                if elapsed_time > timeout:
                    print('Sending Heartbeat ........       ')
                    self.target.send_heartbeat()
                    self.restart = True
                    break
                else:
                    print('Heartbeat Timer: ', timeout-elapsed_time, end = '\r')  
