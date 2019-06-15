from __future__ import annotations

import asyncio
import logging
import random
from heapq import nsmallest
from itertools import chain
from typing import List, Union, Optional, Tuple, Iterator, Callable, Dict

from . import rpc
from .config import asize, ksize
from .node import ID, Node, Addr

log = logging.getLogger(__name__)


class KBucket(List[Node]):
    def __init__(self, range: Tuple[int, int]) -> None:
        self.range = range
        super().__init__()

    def __repr__(self) -> str:
        return f'<KBucket: {len(self)} nodes in {self.range}>'

    def covers(self, node: Node) -> bool:
        return self.range[0] <= node.id < self.range[1]

    def full(self) -> bool:
        return len(self) >= ksize

    def divide(self, mask: int) -> Tuple[KBucket, KBucket]:
        mid = (self.range[0] + self.range[1]) // 2
        left = KBucket((self.range[0], mid))
        right = KBucket((mid, self.range[1]))
        for node in self:
            if node.id & mask:
                right.append(node)
            else:
                left.append(node)
        return left, right


class LookupQueue(asyncio.Queue):
    def __init__(self, xor: Callable[[Node], int], nodes: Iterator[Node]):
        self._xor = xor
        self._queue = nsmallest(ksize, nodes, key=xor)
        # reversed to get better pop() performance
        self._queue.reverse()
        super().__init__()

    def _init(self, maxsize):
        pass

    def _put(self, node: Node):
        lo, hi = 0, len(self._queue)
        distance = self._xor(node)
        while lo < hi:
            mid = (lo + hi) // 2
            if distance > self._xor(self._queue[mid]):
                hi = mid
            else:
                lo = mid + 1
        self._queue.insert(lo, node)
        self._queue = self._queue[-ksize:]

    def _get(self):
        return self._queue.pop()


class ValueFound(Exception):
    pass


class NodeFound(Exception):
    pass


def xor_key(id: ID) -> Callable[[Node], int]:
    return lambda n: n.id ^ id


class Server:
    def __init__(self, addr: Addr, id: Optional[ID] = None) -> None:
        if id is None:
            id = ID(random.getrandbits(160))
        self.node = Node(id, addr)
        self.node_level = 0
        self.routing_table: List[KBucket] = [KBucket((0, 2 ** 160))]
        self.storage: Dict[ID, bytes] = {}

    async def start(self, bootstrap: Optional[List[Node]] = None):

        self.rpc = await rpc.start(self.node, on_rpc=self.update_routing_table, timeout=300)
        register = self.rpc.register

        @register
        def ping(caller: Node) -> str:
            return 'pong'

        @register
        def store(caller: Node, key: ID, value: bytes) -> None:
            self.storage[key] = value

        @register
        async def find_node(caller: Node, id: ID) -> List[Node]:
            return await self._lookup_node(caller, id, 'find_node')

        @register
        async def find_value(caller: Node, id: ID) -> Union[List[Node], bytes]:
            try:
                return self.storage[id]
            except KeyError:
                return await self._lookup_node(caller, id, 'find_value')

        # join the network
        if bootstrap is None:
            return

        tasks = (self.rpc.find_node(node.addr, self.node.id)
                 for node in bootstrap)
        res = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, new_nodes in enumerate(res):
            if isinstance(new_nodes, Exception):
                log.error(f'failed to connect.')
                continue
            await self.update_routing_table(bootstrap[idx])
            for node in new_nodes:
                await self.update_routing_table(node)

    def __repr__(self):
        return f'<Kademlia ID={self.node.id}>'

    async def update_routing_table(self, new: Node):
        if new == self.node:
            log.debug('Ignoring this node.')
            return
        bucket = next(bucket for bucket in self.routing_table
                      if bucket.covers(new))

        if new in bucket:
            bucket.remove(new)
            bucket.append(new)
            return

        if not bucket.full():
            bucket.append(new)
            return

        if bucket.covers(self.node):
            mask = (1 << self.node_level) & self.node.id
            self.node_level += 1
            self.routing_table.remove(bucket)
            self.routing_table += bucket.divide(mask)
            await self.update_routing_table(new)
            return

        oldest = bucket[0]
        try:
            await self.rpc.ping(oldest.addr)
        except asyncio.TimeoutError:
            bucket.remove(oldest)
            bucket.append(new)

        # the new node is dropped

    async def _lookup_node(self, caller: Node, id: ID, rpc_func: str) -> List[Node]:
        """Locate the k closest nodes to the given node ID.
        """
        xor = xor_key(id)
        nodes = filter(lambda n: n != self.node and n.id != caller.id,
                       chain(*self.routing_table))
        queue = LookupQueue(xor, nodes)
        queried = set()

        async def query():
            while not queue.empty():
                node = await queue.get()
                queried.add(node)
                new_nodes = await self.rpc.call(node.addr, rpc_func, id)
                if isinstance(new_nodes, bytes):
                    raise ValueFound(new_nodes)
                for node in new_nodes:
                    if node.id == id:
                        raise NodeFound(node)
                    if node not in queried:
                        queue.put_nowait(node)

        try:
            await asyncio.gather(*(query() for _ in range(asize)))
        except NodeFound as exc:
            return [exc.args[0]]
        return nsmallest(ksize, queried, key=xor)

    async def set(self, key: ID, value: bytes) -> None:
        self.storage[key] = value
        nodes = await self._lookup_node(self.node, key, 'find_node')
        await asyncio.gather(
            *(self.rpc.store(node.addr, key, value) for node in nodes))

    async def get(self, key: ID) -> bytes:
        try:
            return self.storage[key]
        except KeyError:
            try:
                nodes = await self._lookup_node(self.node, key, 'find_value')
            except ValueFound as exc:
                return exc.args[0]

    async def close(self):
        self.rpc.close()
