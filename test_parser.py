import unittest

from parser import parse, Commands, parse_and_return_fns

class TestParser(unittest.TestCase):

    def _test_parser(self):
        self.assertEqual(parse('aws.regions.amsterdam.zone1.server1.load_avg_5 ', 0, 0),
                               [('load_files_m', ['aws.regions.amsterdam.zone1.server1.load_avg_5', '0', '0'])])

        self.assertEqual(parse('aws.regions.amsterdam.zone1.server1.load_avg_5 | multiply 3.14', 0, -1),
                            [('load_files_m', ['aws.regions.amsterdam.zone1.server1.load_avg_5', '0', '-1']),
                             ('multiply', ['3.14'])
                             ])

        self.assertEqual(parse('aws.regions.amsterdam.zone1.server1.load_avg_5 | multiply 3.14 | moving_avg', 0, 0),
                         [('load_files_m', ['aws.regions.amsterdam.zone1.server1.load_avg_5', '0', '0']),
                          ('multiply', ['3.14']),
                          ('moving_avg', [None])

                          ])

    def test_commander(self):
        commands = Commands()

        def load_files_m(*metric_names, ts_start=0, ts_end=None):
            return

        def moving_avg(df):
            return df

        commands.add_command(load_files_m, '<metric_names> <ts_start:float> <ts_end:float>')
        commands.add_command(moving_avg, '<df>')
        parse_and_return_fns('aws.regions.amsterdam.zone1.server1.load_avg_5 | moving_avg', 0, 0, commands)



if __name__ == '__main__':
    unittest.main()