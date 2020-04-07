import unittest, time


class TestRobotMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.robot = Robot("blue")

    def test_punch_right(self):
        self.robot.punch_with_right()
        self.assertEqual(self.robot.state, "punch_right", "robot state was not adjusted")
        self.assertTrue(self.robot.blocked, "robot did not get blocked")
        self.assertNotEqual(self.robot.timer, None, "timer did not get instantiated")
        time.sleep(1.001)
        self.assertEqual(self.robot.timer, None, "timer was not reset to none")
        self.assertFalse(self.robot.blocked, "robot is still blocked")

    def test_punch_left(self):
        self.robot.punch_with_left()
        self.assertEqual(self.robot.state, "punch_left", "robot state was not adjusted")
        self.assertTrue(self.robot.blocked, "robot did not get blocked")
        self.assertNotEqual(self.robot.timer, None, "timer did not get instantiated")
        time.sleep(1.001)
        self.assertEqual(self.robot.timer, None, "timer was not reset to none")
        self.assertFalse(self.robot.blocked, "robot is still blocked")

    def test_block_right(self):
        self.robot.block_with_right()
        self.assertEqual(self.robot.state, "block_right", "robot state was not adjusted")

    def test_block_left(self):
        self.robot.block_with_left()
        self.assertEqual(self.robot.state, "block_left", "robot state was not adjusted")

    def test_punch_block(self):
        self.robot.punch_with_left()
        with self.assertRaises(RobotBlockedError):
            self.robot.punch_with_right()
        self.assertEqual(self.robot.state, "punch_left", "robot state was incorrectly adjusted")


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestRobotMethods('test_punch_right'))
    suite.addTest(TestRobotMethods('test_punch_left'))
    suite.addTest(TestRobotMethods('test_block_right'))
    suite.addTest(TestRobotMethods('test_block_left'))
    suite.addTest(TestRobotMethods('test_punch_block'))
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    runner.run(suite())
