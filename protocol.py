import asyncio
from typing import List, Union

from . import rpc
from .config import asize, ksize
from .node import ID, Node
from .routing import RoutingTable


class ValueFound(Exception):
    pass


class Server:
    def __init__(self):
        self.routing_table = RoutingTable(self)
        self.storage = {}
        self.rpc = rpc.Server()
        self.id = ID()
        self.sem = asyncio.Semaphore(asize)

        @self.rpc.register
        def ping() -> str:
            return 'pong'

        @self.rpc.register
        def store(key, value) -> None:
            self.storge[key] = value

        @self.rpc.register
        def find_node(id: ID) -> List[Node]:
            return self.routing_table.get_nodes_nearby(id)

        @self.rpc.register
        def find_value(id: ID) -> Union[List[Node], bytes]:
            try:
                return self.storage[id]
            except KeyError:
                return find_node(id)

    async def _query(self, nodes: List[Node], id: ID,
                     rpc_func: str) -> List[Node]:
        """Query the given k nodes, then merge their results and
        return the k nodes that are closest to id.
        """
        async def query_one_node(node):
            nonlocal nodes
            try:
                with (await self.sem):
                    res = await getattr(node, rpc_func)(id)
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
            nodes.sort(key=lambda n: n.id ^ id)
            return nodes[:ksize]

    async def lookup_node(self, id: ID) -> Node:
        nodes = self.routing_table.get_nodes_nearby(id)
        while True:
            res = await self._query(nodes.copy(), id, 'find_node')
            if res == nodes:
                break
            nodes = res
        return nodes

    async def set(self, key: bytes, value: bytes) -> None:
        nodes = self.routing_table.get_nodes_nearby(id)
        await asyncio.gather(*(node.rpc.store(key, value) for node in nodes))

    async def get(self, key: ID) -> bytes:
        nodes = self.routing_table.get_nodes_nearby(key)
        while True:
            try:
                res = await self._query(nodes.copy(), key, 'find_value')
            except ValueFound as exc:
                return exc.args[0]
            else:
                if res == nodes:
                    break
                nodes = res
        return b'not found'
