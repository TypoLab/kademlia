from __future__ import annotations

from typing import (get_type_hints, Type, TypeVar, Any, Dict, Generic, Callable,
                    Optional,
                    _GenericAlias)

import msgpack

_IMMUTABLE = (str, int, float, bool, type(None))
_HEAPTYPE = 1 << 9
T = TypeVar('T')
_EMPTY = object()
InferTypeVar = Optional[Callable[[TypeVar, Any], Any]]


def _reduce(obj):
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
    state = tuple(_reduce(v) for k, v in state.items() if k != '__orig_class__')
    # TODO: reduce rest
    return (arg, state, *rest)


def dumps(obj):
    return msgpack.dumps(_reduce(obj), use_bin_type=True)


def _is_subscripted_generic(tp):
    # subscripted generics are _GenericAlias instances (but not classes)
    return isinstance(tp, _GenericAlias)


def _is_unsubscripted_generic(tp):
    return isinstance(tp, type) and issubclass(tp, Generic) \
           and tp.__parameters__


class Decoder():
    def __init__(self, infer_type_var: InferTypeVar = None):
        super().__init__()
        self._infer_type_var = infer_type_var
        if infer_type_var is not None:
            self._field_name = next(k for k, v in
                                    get_type_hints(infer_type_var).items()
                                    if v != TypeVar)
            self._field_value = _EMPTY
        self._generic_values: Dict[TypeVar, Any] = {}

    @staticmethod
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

    def _get_generic_value(self, type_var):
        try:
            return self._generic_values[type_var]
        except KeyError:
            if self._infer_type_var is None:
                raise TypeError(f'Please specify type parameter {type_var} '
                                'or infer_type() callback.') from None
            if self._field_value is _EMPTY:
                raise TypeError('Argument for infer_type() '
                                'is not yet ready') from None
            value = self._infer_type_var(type_var, self._field_value)
            self._generic_values[type_var]  = value
            return value

    def decode(self, cls: Type[T], value: Any) -> T:
        if cls in _IMMUTABLE:
            return value

        name, types = getattr(cls, '_name', None), getattr(cls, '__args__',
                                                           None)
        if name == 'List':
            return [self.decode(types[0], item) for item in value]
        elif name == 'Tuple':
            if len(types) == 2 and types[1] is ...:
                return tuple(self.decode(types[0], item) for item in value)
            return tuple(
                self.decode(tp, item) for tp, item in zip(types, value))
        elif name == 'Dict':
            kt, vt = types
            return {self.decode(kt, k): self.decode(vt, v) for k, v in
                    value.items()}
        elif name == 'FrozenSet':
            return frozenset(self.decode(types[0], item) for item in value)

        arg, *rest = value
        for tp in getattr(cls, '__orig_bases__', ()):
            if tp._name is not None:
                arg = self.decode(tp, arg)
                break

        if _is_subscripted_generic(cls):
            orig_class = cls
            origin = cls.__origin__
            self._generic_values.update(
                zip(origin.__parameters__, cls.__args__))
            cls = origin
        hints = get_type_hints(cls)

        obj = self._construct(cls, arg)
        if not rest:
            return obj
        state, *rest = rest

        # add field names back
        state = {name: value for name, value in zip(hints, state)}
        try:
            self._field_value = state[self._field_name]
        except (AttributeError, KeyError):
            pass
        for (name, tp), value in zip(hints.items(), state):
            if isinstance(tp, TypeVar):
                tp = self._get_generic_value(tp)
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
                    raise TypeError('a class that defines __slots__ without '
                                    'defining __getstate__ cannot be pickled')
        else:
            setstate(state)
        return obj


def loads(cls: Type[T], data: bytes, infer_type_var: InferTypeVar = None) -> T:
    return Decoder(infer_type_var).decode(cls, msgpack.loads(data, raw=False,
                                                             use_list=False))
