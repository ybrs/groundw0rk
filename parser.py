import re

class NoArg(object):
    pass

def parse(cmd, ts_start, ts_end):
    """

    returns a list of filters names and arguments

    foo.bar.baz | multiply 2 | moving_avg

    :return: a list of function names and arguments pair


    """
    cmds = cmd.split('|')
    cmds = [c.strip() for c in cmds]
    # every command should start with one or more metric names
    metric_names = cmds.pop(0)
    cmds = ['load_files_m {} {} {}'.format(metric_names, ts_start, ts_end)] + cmds

    ret = []
    for c in cmds:
        try:
            i = c.index(' ')
        except:
            i = len(c)
        fn_name = c[:i]
        args = [i.strip() or NoArg for i in c[i:].strip().split(' ')]
        n = (fn_name, args)
        ret.append(n)
    return ret

class Commands(object):
    def __init__(self):
        self.commands = {}

    def add_command(self, fn, signature):
        self.commands[fn.__name__] = (fn, signature)

    def get(self, fn_name):
        return self.commands.get(fn_name)

def asis(s):
    return s

def fix_len(l, wanted_len):
    if len(l) == wanted_len:
        return l
    if len(l) < wanted_len:
        l += [None] * (wanted_len - len(l))
        return l
    return l[0:wanted_len]

def intorfloat(v):
    try:
        return int(v)
    except ValueError:
        return float(v)

type_fixers = {
    'int': int,
    'float': float,
    'numeric': intorfloat
}

def parse_fn_signature(s):
    g = re.findall(r'\<(.*?)\>', s)
    if not g:
        raise Exception('function signature is wrong')
    args = []
    for gi in g:
        argname, type_fixer = fix_len(gi.split(':'), 2)
        type_fixer_fn = type_fixers.get(type_fixer, asis)
        args.append({'name': argname, 'fixer': type_fixer_fn})
    return args

def parse_and_return_fns(cmd, ts_start, ts_end, commands):
    """
    parses the command and checks for signatures and
    returns a list of callables and arguments

    :param cmd:
    :param ts_start:
    :param ts_end:
    :param commands: instance of Commands
    :return:
    """
    parsed = parse(cmd, ts_start, ts_end)
    ret = []
    for ln in parsed:
        fn_name = ln[0]
        fn, signature = commands.get(fn_name)
        if not fn:
            raise Exception('registered function not found {}'.format(fn_name))
        args = ln[1]
        expected_args = parse_fn_signature(signature)
        ret.append({
            'fn': fn,
            'expected_args': expected_args,
            'args': args
        })
    return ret

def run_commands(fn_list):
    returned = []
    for f in fn_list:
        call_with = f['args']
        fn = f['fn']
        calling = {
            'args': [],
            'kwargs': {}
        }

        for arg in reversed(f['expected_args']):
            if arg['name'].startswith('*') or arg['name'].endswith('...'):
                # put the rest in args argument
                # this is always the first argument - it has to be
                calling['args'] = call_with
            else:
                calling['kwargs'][arg['name']] = call_with.pop()
        # now calling the function with args
        fnargs = returned + [a for a in calling['args'] if a is not NoArg]
        returned = fn(*fnargs, **calling['kwargs'])
        if isinstance(returned, tuple):
            returned = list(returned)
        if not isinstance(returned, list):
            returned = [list]


# this is our default commander
commands = Commands()

if __name__ == '__main__':
    parse('aws.regions.amsterdam.zone1.server1.load_avg_5')