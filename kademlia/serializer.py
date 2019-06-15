from __future__ import annotations

from typing import (Any, get_type_hints, Type, TypeVar, Union, Callable,
                    Optional, _GenericAlias)

import msgpack

_IMMUTABLE = {str, bytes, int, float, bool, type(None)}
_HEAPTYPE = 1 << 9
T = TypeVar('T')
_EMPTY = object()


def _reduce(obj):
    if isinstance(obj, BaseException):
        print('****', obj)
        raise obj
    tp = type(obj)
    if tp in _IMMUTABLE:
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
    # No need to transfer field names and '__orig_class__'
    state = tuple(_reduce(v) for k, v in state.items()
                  if k != '__orig_class__')
    # TODO: reduce rest
    return (arg, state, *rest)


def dumps(obj: Any) -> bytes:
    return msgpack.dumps(_reduce(obj), use_bin_type=True)


def _is_subscripted_generic(tp):
    # subscripted generics are _GenericAlias instances (but not classes)
    return isinstance(tp, _GenericAlias) and tp.__origin__ != Union


def _is_union(tp):
    return isinstance(tp, _GenericAlias) and tp.__origin__ is Union


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


class Decoder():
    def __init__(self, infer_generic, infer_union):
        self._infer_generic = infer_generic
        if infer_generic is not None:
            self._infer_generic_arg = tuple(get_type_hints(infer_generic))[0]

        self._infer_union = infer_union
        if infer_union is not None:
            self._infer_union_arg = tuple(get_type_hints(infer_union))[0]

        self._generic_values = {}
        self._union_values = {}

    def _get_type_var(self, type_var):
        try:
            return self._generic_values[type_var]
        except KeyError:
            if self._infer_generic is None:
                raise ValueError(f'Please specify type parameter {type_var} '
                                 'or infer_generic() callback.') \
                    from None
            else:
                raise ValueError('Argument for infer_generic() '
                                 'is not yet ready') from None

    def _choose_union(self, name, union, value):
        try:
            return self._union_values[name]
        except KeyError:
            tp = type(value)
            if tp in _IMMUTABLE:
                return tp
            candidates = set(union.__args__) - _IMMUTABLE
            if len(candidates) == 1:
                return candidates.pop()

            if self._infer_union is None:
                raise ValueError('infer_union() callback is required '
                                 'for unions.') from None
            else:
                raise ValueError('Argument for infer_union() '
                                 'is not yet ready') from None

    def decode(self, cls, value):
        if cls in _IMMUTABLE:
            return value

        name = getattr(cls, '_name', None)
        types = getattr(cls, '__args__', None)
        if name == 'List':
            return [self.decode(types[0], item) for item in value]
        elif name == 'Tuple':
            if len(types) == 2 and types[1] is ...:
                return tuple(self.decode(types[0], item) for item in value)
            return tuple(self.decode(tp, item)
                         for tp, item in zip(types, value))
        elif name == 'Dict':
            kt, vt = types
            return {self.decode(kt, k): self.decode(vt, v)
                    for k, v in value.items()}
        elif name == 'FrozenSet':
            return frozenset(self.decode(types[0], item) for item in value)

        if _is_subscripted_generic(cls):
            origin = cls.__origin__
            orig_class = cls  # uses later
            self._generic_values.update(
                zip(origin.__parameters__, cls.__args__))
            cls = origin
        elif isinstance(cls, TypeVar):
            cls = self._get_type_var(cls)
            return self.decode(cls, value)
        elif _is_union(cls):
            cls = self._choose_union(None, cls, value)
            return self.decode(cls, value)

        arg, *rest = value
        for tp in getattr(cls, '__orig_bases__', ()):
            if tp._name is not None:
                arg = self.decode(tp, arg)
                break

        hints = get_type_hints(cls)

        obj = _construct(cls, arg)
        if not rest:
            return obj
        state, *rest = rest

        # add field names back
        state = {name: value for name, value in zip(hints, state)}

        try:
            arg = state[self._infer_generic_arg]
        except (AttributeError, KeyError):
            pass
        else:
            self._generic_values.update(self._infer_generic(arg))
            del self._infer_generic_arg

        try:
            arg = state[self._infer_union_arg]
        except (AttributeError, KeyError):
            pass
        else:
            self._union_values.update(self._infer_union(arg))
            del self._infer_union_arg

        for name, tp in hints.items():
            if name in state:
                if _is_union(tp):
                    tp = self._choose_union(name, tp, state[name])
                state[name] = self.decode(tp, state[name])

        try:
            state['__orig_class__'] = orig_class
        except NameError:
            pass

        try:
            setstate = obj.__setstate__
        except AttributeError:
            try:
                obj.__dict__.update(state)
            except AttributeError:
                if hasattr(obj, '__slots__'):
                    raise TypeError(
                        'a class that defines __slots__ without '
                        'defining __getstate__ cannot be decoded') from None
        else:
            setstate(state)
        return obj


def loads(cls: Type[T], data: bytes,
          infer_generic: Optional[Callable] = None,
          infer_union: Optional[Callable] = None) -> T:
    value = msgpack.loads(data, raw=False, use_list=False)
    return Decoder(infer_generic, infer_union).decode(cls, value)
