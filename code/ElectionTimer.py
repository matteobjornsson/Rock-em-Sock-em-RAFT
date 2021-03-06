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

        t = Thread( 
			target=self.run, 
			name='Election Timer Thread'
			)
        t.start()

    def kill_thread(self):
        self.running = False

    def stop_timer(self):
        self.stop = True

    def restart_timer(self):
        self.restart = True
        self.stop = False

    def new_timeout(self) -> float:
        return (self.duration + 2*self.duration * random.random())

    def run(self):
        # randomize timeouts to avoid conflicting elections
        timeout = self.new_timeout()
        #start the timer
        start = clock()
        count = 0
        while self.running:
            while not self.stop:
                count += 1
                if self.restart:
                    timeout = self.new_timeout()
                    start = clock()
                    self.restart = False

                elapsed_time = clock() - start
                if elapsed_time > timeout:
                    print('\nCountodwn elapsed ', timeout, ',', self.target.id, ' Starting Election       \n')
                    self.target.start_election()
                    self.restart_timer()
                    break
                else:
                    if count > 20000: 
                        print('Election Timer: ', timeout-elapsed_time)  
                        count =0
