from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, FrozenSet, Tuple

from .serializer import dumps, loads


def test_dataclass():
    @dataclass
    class Person:
        name: str
        age: Dict[str, int]
        skills: Tuple[str, ...]
        friends: FrozenSet[str]

    a = Person(name='CSM', age={'real': 21}, skills=('programming', 'cooking'),
               friends=frozenset(('A', 'B')))
    b = loads(Person, dumps(a))

    assert type(b) is Person
    assert type(b.name) is str
    assert type(b.age) is dict
    assert type(b.skills) is tuple
    assert type(b.friends) is frozenset
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
