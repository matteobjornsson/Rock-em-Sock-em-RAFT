from code.Server.server_logic import *
from code.Robot.robot_logic import *
import unittest
from moto import mock_sqs


class TestServerMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.robot_blue = Robot('blue')
        self.robot_red = Robot('red')
        self.server = Server('leader')

    def test_update_status_blocked(self):
        self.robot_blue.block_with_left()
        # self.robot_red.punch_with_right()

    """
    def test_update_status_punch(self):
        msg_dict_red = {'id': 'client-red', 'state': 'punch-left'}
        msg_dict_blue = {'id': 'client-blue', 'state': 'punch-right'}
        self.server.handle_incoming_message(msg_dict_red)
        self.server.handle_incoming_message(msg_dict_blue)

    def test_update_status_exit(self):
        msg_dict_red = {'id': 'client-red', 'state': 'punch-left'}
        msg_dict_blue = {'id': 'client-blue', 'state': 'exit'}
        self.server.handle_incoming_message(msg_dict_red)
        self.server.handle_incoming_message(msg_dict_blue)
    """

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestServerMethods('test_update_status_blocked'))
    #suite.addTest(TestServerMethods('test_update_status_punch'))
    #suite.addTest(TestServerMethods('test_update_status_exit'))


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    runner.run(suite())