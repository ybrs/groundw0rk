import unittest

from parser import parse, Commands, parse_and_return_fns, run_commands


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

        import pandas as pd
        import numpy as np

        def load_files_m(*metric_names, ts_start=0, ts_end=None):
            df = pd.DataFrame(np.random.randn(250), columns = ['C1'],
                               index = pd.date_range('20130101', periods=250, freq='S'))
            return [df]

        def moving_avg(df):
            return [df]

        def pct_change(df):
            return [df.pct_change()]

        def cond_set(df, cond, val):
            assert cond and isinstance(cond, str)
            df[df.C1 < 100] = val
            return [df]


        commands.add_command(load_files_m, '<*metric_names> <ts_start:float> <ts_end:float>')
        commands.add_command(moving_avg, '<*df>')
        commands.add_command(pct_change, "<*df>")
        commands.add_command(cond_set, "<*df> <cond:str> <val:numeric>")

        fn_and_args = parse_and_return_fns('aws.regions.amsterdam.zone1.server1.load_avg_5', 0, 0, commands)

        assert len(fn_and_args) == 1
        assert fn_and_args[0]['fn'] == load_files_m
        assert fn_and_args[0]['expected_args'][0]['name'] == '*metric_names'
        assert fn_and_args[0]['expected_args'][1]['name'] == 'ts_start'
        assert fn_and_args[0]['expected_args'][1]['fixer'] == float
        assert fn_and_args[0]['expected_args'][2]['name'] == 'ts_end'
        assert fn_and_args[0]['expected_args'][2]['fixer'] == float

        fn_and_args = parse_and_return_fns('aws.regions.amsterdam.zone1.server1.load_avg_5 | moving_avg', 0, 0, commands)
        assert len(fn_and_args) == 2
        assert fn_and_args[0]['fn'] == load_files_m
        assert fn_and_args[0]['expected_args'][0]['name'] == '*metric_names'
        assert fn_and_args[0]['expected_args'][1]['name'] == 'ts_start'
        assert fn_and_args[0]['expected_args'][1]['fixer'] == float
        assert fn_and_args[0]['expected_args'][2]['name'] == 'ts_end'
        assert fn_and_args[0]['expected_args'][2]['fixer'] == float

        parsed = parse_and_return_fns('aws.regions.amsterdam.zone1.server1.load_avg_5 aws.regions.amsterdam.zone1.server1.load_avg_10 | pct_change | cond_set >100 100', 0, 0, commands)
        run_commands(parsed)



if __name__ == '__main__':
    unittest.main()