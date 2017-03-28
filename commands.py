from parser import commands
import cli

"""
most of these functions are direct wrappers in pandas.dataframe
"""

@commands.command('<*names> <ts_start:int_or_none> <ts_end:int_or_none> <step:int_or_none>')
def load_files_m(*names, ts_start, ts_end, step):
    """
    actually we only accept the first name

    - multiple metric names are reserved for future use.

    :param names:
    :param ts_start:
    :param ts_end:
    :step: step: in seconds
    :return: dataframe
    """
    print("-> names ->", names, ts_start, ts_end, step)
    return cli.load_files(names[0], ts_start, ts_end, step)

@commands.command()
def asis(df):
    """

    :return: pandas.DataFrame
    """
    return df

@commands.command()
def moving_avg(df):
    return df