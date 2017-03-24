def parse(cmd):
    """

    returns a list of function names and arguments


    :return:
    """
    cmds = cmd.split('|')
    cmds = [c.strip() for c in cmds]

    return cmds


if __name__ == '__main__':
    parse('aws.regions.amsterdam.zone1.server1.load_avg_5')