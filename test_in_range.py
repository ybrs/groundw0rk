import unittest
import os
os.environ['DATA_DIR'] = 'db-test'
os.environ['DB_DIR'] = 'db-test'
from utils import b
from cli import in_range

class TestMetricDatabase(unittest.TestCase):

    def test_in_range(self):
        ranges = [
            (1,5, '1'),
            (6,10, '2'),
            (11,15, '3'),
            (16,20, '4')
        ]
        self.assertEqual(in_range(ranges, 1, 2), [(1, 5, '1')])
        self.assertEqual(in_range(ranges, 7, 8), [(6, 10, '2')])
        self.assertEqual(in_range(ranges, 1, 6), [(1, 5, '1'), (6, 10, '2')])
        self.assertEqual(in_range(ranges, 7, 12), [(6, 10, '2'), (11, 15, '3')])
        self.assertEqual(in_range(ranges, 7, 17), [(6, 10, '2'), (11, 15, '3'), (16, 20, '4')])
        self.assertEqual(in_range(ranges, 12, 25), [(11, 15, '3'), (16, 20, '4')])

if __name__ == '__main__':
    unittest.main()