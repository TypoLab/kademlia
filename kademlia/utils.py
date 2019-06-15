import asyncio
from heapq import _heapify_max, _heapreplace_max
from typing import Callable, Awaitable, TypeVar, Iterator, List
from .node import Node, ID
from .config import ksize


T = TypeVar('T')
AsyncFunc = Callable[..., Awaitable[T]]


def limited(sem: asyncio.Semaphore, func: AsyncFunc) -> AsyncFunc:
    '''Limit the number of coroutines running at the same time.'''
    async def wrapper(*args, **kwargs) -> T:
        with (await sem):
            return await func(*args, **kwargs)

    return wrapper


def xor_key(id: ID) -> Callable[[Node], int]:
    return lambda n: n.id ^ id


class TopK(List[Node]):
    """
    >>> addr = ('127.0.0.1', 1234)
    >>> list = list(range(64, 32))
    >>> id = ID(0)
    >>> nodes = TopK([Node(ID(i), addr) for i in list], id))
    >>> list(nodes) == sorted(list, key=xor_key)[:ksize]
    True
    >>> nodes.extend([Node(ID(128), addr)])
    False
    >>> nodes.extend([Node(ID(1), addr), Node(ID(31))])
    True
    >>> nodes == sorted(list + [1, 31])[:ksize]
    True
    """
    def __init__(self, nodes: Iterator[Node], id: ID) -> None:
        self._id = id
        super().__init__([(xor_key(node, id), node) for node in nodes[:ksize]])
        _heapify_max(self)
        self.extend(nodes[:ksize])

    def extend(self, nodes: Iterator[Node]) -> bool:
        changed = False
        for node in nodes:
            distance = xor_key(node, self._id)
            if distance < xor_key(self[0], self._id):
                _heapreplace_max(self, (distance, node))

    def __getitem__(self, item):
        return super().__getitem__(item)[1]
