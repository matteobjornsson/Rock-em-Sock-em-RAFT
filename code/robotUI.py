from robot_logic import *
import sys, threading


class UI:
    """
    UI that runs game on client side and passes through appropriate messages.
    """
    def __init__(self, color):
        self.robot = Robot(color, self)
        self.running_game = True

    def start(self):
        t = threading.Thread(target=self.run_game)
        t.start()

    def run_game(self):
        print("Welcome to ROCK 'EM SOCK 'EM!\nYou can use the following commands to move your "
              "robot\n---------------------------------------")
        print("Q = punch with left \nW = punch with right \nA = block with left \nS = block with right \n0 = Exit")
        while self.running_game:
            user_choice = input()
            if user_choice.upper() == 'Q':
                try:
                    self.robot.punch_with_left()
                except RobotBlockedError:
                    print("robot cannot punch right now")
            elif user_choice.upper() == 'W':
                try:
                    self.robot.punch_with_right()
                except RobotBlockedError:
                    print("robot cannot punch right now")
            elif user_choice.upper() == 'A':
                self.robot.block_with_left()
            elif user_choice.upper() == 'S':
                self.robot.block_with_right()
            elif user_choice == '0':
                self.robot.stop_game()
                self.running_game = False
                print("We hope you had fun! Goodbye")
            else:
                print('Not a valid input, try again.')


if __name__ == '__main__':
    color = sys.argv[1]
    print(color)
    ui = UI(color)
    server = Server('leader')
    ui.start()
