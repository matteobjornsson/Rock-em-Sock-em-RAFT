from Robot.robot_logic import *


class UI:
    """

    """
    def __init__(self):
        self.robot = Robot(color='blue')
        self.running_game = True

    def run_game(self):
        print("Welcome to ROCK 'EM SOCK 'EM!\nYou can use the following commands to move your "
              "robot\n---------------------------------------")
        print("Q = punch with left \nW = punch with right \nA = block with left \nS = block with right \n0 = Exit")
        while self.running_game:
            user_choice = input()
            if user_choice == 'Q':
                try:
                    self.robot.punch_with_left()
                except RobotBlockedError:
                    print("robot cannot punch right now")
            elif user_choice == 'W':
                try:
                    self.robot.punch_with_right()
                except RobotBlockedError:
                    print("robot cannot punch right now")
            elif user_choice == 'A':
                self.robot.block_with_left()
            elif user_choice == 'S':
                self.robot.block_with_right()
            elif user_choice == '0':
                self.running_game = False
                print("We hope you had fun! Goodbye")
            else:
                print("not a valid input")


if __name__ == '__main__':

    ui = UI()
    ui.run_game()
