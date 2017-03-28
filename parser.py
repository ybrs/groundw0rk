import re
import inspect
from functools import wraps

class NoArg(object):
    pass

def parse(cmd, ts_start, ts_end, step):
    """

    returns a list of filters names and arguments

    foo.bar.baz | multiply 2 | moving_avg

    :return: a list of function names and arguments pair


    """
    cmds = cmd.split('|')
    cmds = [c.strip() for c in cmds]
    # every command should start with one or more metric names
    metric_names = cmds.pop(0)
    cmds = ['load_files_m {} {} {} {}'.format(metric_names, ts_start, ts_end, step)] + cmds

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

def guess_signature_if_needed(fn, signature):
    if signature:
        return signature
    sig = inspect.signature(fn)
    s = []
    for v in sig.parameters.values():
        s.append((v.name, v.kind, v.default))
    args = s.pop(0)
    ret = ['<*{}>'.format(args[0])]
    for sa in s:
        ret.append('<{}>'.format(sa[0]))
    return ' '.join(ret)

class Commands(object):
    def __init__(self):
        self.commands = {}

    def add_command(self, fn, signature):
        self.commands[fn.__name__] = (fn, signature)

    def get(self, fn_name):
        return self.commands.get(fn_name)

    def command(self, signature=None):
        # we try to guess signature
        def wrapper(fn):
            self.add_command(fn, guess_signature_if_needed(fn, signature))
            @wraps(fn)
            def wrap_(*args, **kwargs):
                fn(*args, **kwargs)
            return wrap_
        return wrapper

    def run(self, query, start_ts, end_ts, step):
        parsed = parse_and_return_fns(query, start_ts, end_ts, step, self)
        r = run_commands(parsed)
        return r


def asis(s):
    return s

def fix_len(l, wanted_len):
    if len(l) == wanted_len:
        return l
    if len(l) < wanted_len:
        l += [None] * (wanted_len - len(l))
        return l
    return l[0:wanted_len]

def int_or_float(v):
    try:
        return int(v)
    except ValueError:
        return float(v)

def int_or_none(v):
    if v is None or v == 'None':
        return None
    return int(v)

type_fixers = {
    'int': int,
    'int_or_none': int_or_none,
    'float': float,
    'numeric': int_or_float
}

def parse_fn_signature(s):
    assert s
    g = re.findall(r'\<(.*?)\>', s)
    if not g:
        raise Exception('function signature is wrong')
    args = []
    for gi in g:
        argname, type_fixer = fix_len(gi.split(':'), 2)
        type_fixer_fn = type_fixers.get(type_fixer, asis)
        args.append({'name': argname, 'fixer': type_fixer_fn})
    return args

def parse_and_return_fns(cmd, ts_start, ts_end, step, commands):
    """
    parses the command and checks for signatures and
    returns a list of callables and arguments

    :param cmd:
    :param ts_start:
    :param ts_end:
    :param commands: instance of Commands
    :return:
    """
    parsed = parse(cmd, ts_start, ts_end, step)
    ret = []
    for ln in parsed:
        fn_name = ln[0]

        fn_signature = commands.get(fn_name)
        if not fn_signature:
            raise Exception('function [%s] is not registered' % fn_name)
        fn, signature = fn_signature
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
                fixer_fn = arg['fixer']
                calling['kwargs'][arg['name']] = fixer_fn(call_with.pop())

        # now calling the function with args
        fnargs = returned + [a for a in calling['args'] if a is not NoArg]
        # this is our main dispatcher
        returned = fn(*fnargs, **calling['kwargs'])
        if isinstance(returned, tuple):
            returned = list(returned)
        if not isinstance(returned, list):
            returned = [returned]
    return returned


# this is our default commander
commands = Commands()

if __name__ == '__main__':
    parse('aws.regions.amsterdam.zone1.server1.load_avg_5')