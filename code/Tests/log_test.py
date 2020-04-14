import unittest, os
from ConsensusModule import Log, LogEntry


class TestLog(unittest.TestCase):
    def setUp(self) -> None:
        if os.path.exists("../files/logOutput.tsv"):
            os.remove("../files/logOutput.tsv")
        else:
            print('file not found')

        self.log = Log()
        self.log.append_to_end(LogEntry(1, 'blue_block_left'))
        self.log.append_to_end(LogEntry(1, 'red_punch_right'))
        self.log.append_to_end(LogEntry(1, 'red_block_left'))
        self.log.append_to_end(LogEntry(2, 'blue_punch_left'))
        self.log.append_to_end(LogEntry(3, 'blue_punch_right'))

    def test_add_log_entries(self):
        self.assertEqual(len(self.log), 5)
        self.assertEqual(self.log.get_entry(0).command, 'blue_block_left')

    def test_rollback_log(self):
        self.log.rollback(3)
        self.assertEqual(self.log.get_entry(-1).command, 'red_block_left')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestLog.test_add_log_entries())
    suite.addTest(TestLog.test_rollback_log())
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    runner.run(suite())