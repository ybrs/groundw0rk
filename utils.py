def b(v):
    """
    returns bytes, if v is something it converts to string and then returns bytes
    :param v:
    :return:
    """
    if isinstance(v, str):
        return v.encode()
    if isinstance(v, bytes):
        return v

    if isinstance(v, list):
        return [b(vt) for vt in v]

    return str(v).encode()

def u(*v):
    """
    always returns a string.

    if v is a list of somethings, returns list of strings

    :param v:
    :return:
    """
    if len(v) == 1:
        return _u(v[0])
    return [_u(i) for i in v]

def _u(v):
    if isinstance(v, str):
        return v

    if isinstance(v, bytes):
        return v.decode()

    if isinstance(v, list):
        return [u(vt) for vt in v]

    return str(v)

def strip_all(l):
    return [i.strip() for i in l]

if __name__ == '__main__':
    # print(u('1', 2))
    print(u('1'))
    # print(u(1234))