from time import sleep, clock
from threading import Thread, Timer
import random

class Election_Timer:

    def __init__(self, duration: float, target):
        self.target = target
        self.duration = duration
        self.running = True
        self.restart = False
        self.stop = False

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
                    print('Countodwn elapsed, Starting Election       ')
                    self.target.startElection()
                    self.restart_timer()
                    break
                else:
                    print('Election Timer: ', timeout-elapsed_time, end = '\r')  
