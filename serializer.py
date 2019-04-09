from __future__ import annotations

from typing import get_type_hints, Callable

import msgpack

immutable = (str, int, float, bool, type(None))
_HEAPTYPE = 1 << 9


def _reduce(obj):
    tp = type(obj)
    if tp in immutable:
        return obj
    if tp in (list, tuple, frozenset):
        return tuple(_reduce(i) for i in obj)
    if tp is dict:
        return {_reduce(k): _reduce(v) for k, v in obj.items()}

    _, (_, _, arg), *opt = obj.__reduce__()
    arg = _reduce(arg)
    if not opt:
        return (arg,)
    state, *rest = opt
    # No need to transfer field names
    state = tuple(_reduce(v) for v in state.values())
    # TODO: reduce rest
    return (arg, state, *rest)


def dumps(obj):
    return msgpack.dumps(_reduce(obj), use_bin_type=True)


def _construct(cls, arg):
    for base in cls.__mro__:
        if hasattr(base, '__flags__') and not base.__flags__ & _HEAPTYPE:
            break
    else:
        base = object  # not really reachable
    if base is object:
        return object.__new__(cls)
    obj = base.__new__(cls, arg)
    if base.__init__ != object.__init__:
        base.__init__(obj, arg)
    return obj


def restore(cls: type, value):
    if cls in immutable:
        return value

    name, types = getattr(cls, '_name', None), getattr(cls, '__args__', None)
    if name == 'List':
        return [restore(types[0], item) for item in value]
    elif name == 'Tuple':
        if len(types) == 2 and types[1] is ...:
            return tuple(restore(types[0], item) for item in value)
        return tuple(restore(tp, item) for tp, item in zip(types, value))
    elif name == 'Dict':
        kt, vt = types
        return {restore(kt, k): restore(vt, v) for k, v in value.items()}
    elif name == 'FrozenSet':
        return frozenset(restore(types[0], item) for item in value)

    arg, *rest = value
    for tp in getattr(cls, '__orig_bases__', ()):
        if hasattr(tp, '_name') and hasattr(tp, '__args__'):
            arg = restore(tp, arg)
            break

    obj = _construct(cls, arg)
    if not rest:
        return obj

    state, *rest = rest
    hints = get_type_hints(cls)
    # add field names back
    state = {name: restore(type, value) for (name, type), value in
             zip(hints.items(), state)}

    try:
        setstate = obj.__setstate__
    except AttributeError:
        try:
            obj.__dict__.update(state)
        except AttributeError:
            if hasattr(obj, '__slots__'):
                raise TypeError('a class that defines __slots__ without '
                                'defining __getstate__ cannot be pickled')
    else:
        setstate(state)
    return obj


def loads(type: type, data: bytes, ):
    return restore(type, msgpack.loads(data, raw=False, use_list=False))
