import unittest
from Robot.robot_logic import Robot

class TestRobotMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.robot = Robot("blue")

    def test_punch_right(self):
        self.robot.punch_with_right()
        self.assertEqual(self.robot.state, "punch_right")
        self.assertTrue(self.robot.blocked)