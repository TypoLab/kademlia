from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, FrozenSet, Tuple, TypeVar, Generic, Union, Any

from kademlia.serializer import dumps, loads


@dataclass
class Address:
    host: str
    port: int


def test_dataclass():
    @dataclass
    class Person:
        name: str
        age: Dict[str, int]
        skills: Tuple[str, ...]
        friends: FrozenSet[str]
        addr: Address

    a = Person(name='CSM', age={'real': 21}, skills=('programming', 'cooking'),
               friends=frozenset(('A', 'B')), addr=Address('localhost', 22))
    b = loads(Person, dumps(a))

    assert type(b) is Person
    assert type(b.name) is str
    assert type(b.age) is dict
    assert type(b.skills) is tuple
    assert type(b.friends) is frozenset
    assert type(b.addr) is Address
    assert a == b


def test_subclasses():
    class MyInt(int):
        pass

    i = MyInt(123)
    j = loads(MyInt, dumps(i))

    assert type(j) is MyInt
    assert j == i

    class MyList(List[MyInt]):
        pass

    a = MyList([i, i])
    b = loads(MyList, dumps(a))
    assert type(b) is MyList
    assert type(b[0]) is MyInt
    assert b == a


A = TypeVar('A')
R = TypeVar('R')


def test_subscripted_generic():
    @dataclass
    class Func(Generic[A, R]):
        args: A
        ret: R

    cls = Func[Tuple[int, str], List[int]]
    a = cls(args=(123, '456'), ret=[123, 456])
    b = loads(cls, dumps(a))
    assert type(b) is Func
    assert b == a
    assert b.__orig_class__ == cls


def test_unsubscripted_generic():
    @dataclass
    class Msg(Generic[A]):
        is_int: bool
        value: A

    a = Msg(is_int=True, value=123)
    b = Msg(is_int=False, value='hi')

    def infer_generic(is_int: bool):
        return {A: int if is_int else str}

    a1 = loads(Msg, dumps(a), infer_generic)
    b1 = loads(Msg, dumps(b), infer_generic)

    assert type(a1) is Msg
    assert type(a1.value) is int
    assert type(b1) is Msg
    assert type(b1.value) is str
    assert a1 == a
    assert b1 == b


def test_union():
    @dataclass
    class Server:
        is_str: bool
        addr: Union[Address, str]

    a = Server(False, Address('127.0.0.1', 22))
    b = Server(True, '127.0.0.1:22')

    def infer_union(is_str: bool):
        return {'addr': str if is_str else Address}

    a1 = loads(Server, dumps(a), infer_union=infer_union)
    b1 = loads(Server, dumps(b), infer_union=infer_union)

    assert type(a1) is Server
    assert type(b1) is Server
    assert type(a1.addr) is Address
    assert type(b1.addr) is str
    assert a1 == a
    assert b1 == b


def test_union_guessing():
    tp = Union[bytes, List[int]]
    a = b'a'
    b = [1, 2]

    assert loads(tp, dumps(a)) == a
    assert loads(tp, dumps(b)) == b

    @dataclass
    class Ip:
        val: Union[Tuple[int, int, int, int], str]

    local = Ip((127, 0, 0, 1))
    dns = Ip('8.8.8.8')

    assert loads(Ip, dumps(local)) == local
    assert loads(Ip, dumps(dns)) == dns
