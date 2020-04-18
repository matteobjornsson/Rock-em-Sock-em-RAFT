from robot_logic import *
import sys, threading, os
from server_logic import Server

class UI:
    """
    UI that runs game on client side and passes through appropriate messages.
    """
    def __init__(self, color):
        self.robot = Robot(color, self)
        self.running_game = True
        self.color = color
        #self.server = Server('leader')

    def start(self):
        t = threading.Thread(target=self.run_game)
        t.start()

    def run_game(self):
        os.system('cls' if os.name=='nt' else 'clear')
        if self.color == 'blue':
            print(blue)
        else:
            print(red)
        print("Welcome to ROCK 'EM SOCK 'EM!\nYou can use the following commands to move your "
              "robot\n---------------------------------------")
        print("Q = punch with left \nW = punch with right \nA = block with left \nS = block with right \n0 = Exit")
        while self.running_game:
            user_choice = input()
            if user_choice.upper() == 'Q':
                try:
                    self.robot.punch_with_left()
                    print('You punched left')
                except RobotBlockedError:
                    print("robot cannot punch right now")
            elif user_choice.upper() == 'W':
                try:
                    self.robot.punch_with_right()
                    print('You punched right')
                except RobotBlockedError:
                    print("robot cannot punch right now")
            elif user_choice.upper() == 'A':
                self.robot.block_with_left()
                print("Blocked with left")
            elif user_choice.upper() == 'S':
                self.robot.block_with_right()
                print("Blocked with right")
            elif user_choice == '0':
                self.robot.stop_game()
                self.running_game = False
                print("We hope you had fun! Goodbye")
            else:
                print('Not a valid input, try again.')

blue = '''
   ___  __   __  ______
  / _ )/ /  / / / / __/
 / _  / /__/ /_/ / _/  
/____/____/\____/___/                        
'''
red = '''
   ___  _______ 
  / _ \/ __/ _ \\
 / , _/ _// // /
/_/|_/___/____/ 
'''

if __name__ == '__main__':
    color = sys.argv[1]
    ui = UI(color)
    ui.start()
