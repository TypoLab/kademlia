from __future__ import annotations

import asyncio
import logging
import random
from typing import List, Union, Optional, Tuple

from . import rpc
from .config import asize, ksize
from .node import ID, Node, Addr

log = logging.getLogger(__name__)


class ValueFound(Exception):
    pass


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


class Server:
    def __init__(self, addr: Addr, id: Optional[ID] = None) -> None:
        if id is None:
            id = ID(random.getrandbits(160))
        self.node = Node(id, addr)
        self.node_level = 0
        self.routing_table: List[KBucket] = [KBucket((0, 2 ** 160))]
        self.storage: dict = {}

    def __repr__(self):
        return f'<Kademlia ID={self.id}>'

    async def start(self, known_nodes: List[Node] = None):
        # setup the RPC
        self.rpc_server = s = await rpc.start_server(self.node.addr,
                                                     self.update_routing_table)

        @s.register
        def ping() -> str:
            return 'pong'

        @s.register
        def store(key: ID, value: bytes) -> None:
            self.storage[key] = value

        @s.register
        def find_node(id: ID) -> List[Node]:
            return self.routing_table.get_nodes_nearby(id)

        @s.register
        def find_value(id: ID) -> Union[List[Node], bytes]:
            try:
                return self.storage[id]
            except KeyError:
                return find_node(id)

        # join the network
        if known_nodes is None:
            return
        tasks = (node.rpc.find_node(self.this_node.id) for node in known_nodes)
        res = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, new_nodes in enumerate(res):
            if isinstance(new_nodes, rpc.NetworkError):
                log.error(f'{known_nodes[idx]} failed to connect.')
            else:
                await self.routing_table.update(known_nodes[idx])
                for node in new_nodes:
                    await self.routing_table.update(node)

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
            mask = 1 << self.node_level & self.node.id
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

    async def _query(self, nodes: List[Node], id: ID,
                     rpc_func: str, sem: asyncio.Semaphore) -> List[Node]:
        """Query the given k nodes, then merge their results and
        return the k nodes that are closest to id.
        """

        async def query_one_node(node):
            nonlocal nodes
            try:
                with (await sem):
                    res = await getattr(node.rpc, rpc_func)(id)
            except rpc.NetworkError:
                nodes.remove(node)
            else:
                if isinstance(res, bytes):
                    raise ValueFound(res)
                nodes += res

        fs = (asyncio.ensure_future(query_one_node(node)) for node in nodes)
        try:
            await asyncio.gather(*fs)
        except ValueFound:
            for f in fs:
                f.cancel()
            raise
        else:
            nodes = list(set(nodes))
            try:
                nodes.remove(self.this_node)
            except ValueError:
                pass
            nodes.sort(key=lambda n: n.id ^ id)
            return nodes[:ksize]

    async def set(self, key: ID, value: bytes) -> None:
        #  nodes = self.routing_table.get_nodes_nearby(key)
        nodes = await self.lookup_node(key)
        await asyncio.gather(*(node.rpc.store(key, value) for node in nodes))

    async def get(self, key: ID) -> bytes:
        nodes = self.routing_table.get_nodes_nearby(key)
        sem = asyncio.Semaphore(asize)
        while True:
            try:
                res = await self._query(nodes.copy(), key, 'find_value', sem)
            except ValueFound as exc:
                return exc.args[0]
            else:
                if res == nodes:
                    break
                nodes = res
        return b'not found'

    async def lookup_node(self, id: ID) -> List[Node]:
        nodes = self.routing_table.get_nodes_nearby(id)
        sem = asyncio.Semaphore(asize)
        while True:
            res = await self._query(nodes.copy(), id, 'find_node', sem)
            if res == nodes:
                break
            nodes = res
        return nodes

    async def close(self):
        self.rpc_server.close()

