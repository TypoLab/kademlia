from typing import List, Tuple

import rpc

from .node import Node, NodeID
from .routing import KBucket, RoutingTable
from .config import ksize
from .config import asize


class Server:
    def __init__(self):
        self.routing_table = RoutingTable(self)
        self.storage = {}
        self.rpc = rpc.Server()
        self.id = NodeID()

        @self.rpc.register
        def ping():
            return 'pong'

        @self.rpc.register
        def store(key, value):
            self.storge[key] = value

        @self.rpc.register
        def find_node(id: NodeID) -> List[Tuple]:
            nodes = self.routing_table.get_nodes_nearby(id, ksize)
            return [(n.host, n.port, n.id) for n in nodes]

        @self.rpc.register
        def find_value(id: NodeID):
            try:
                return self.storage[id]
            except KeyError:
                return find_node(id)

    async def lookup_node(id: NodeID) -> Node:
        nodes = self.routing_table.get_nodes_nearby(id, asize)
        while 
        tasks = [n.rpc.find_node(id) for n in nodes]
        res = await asyncio.gather(*tasks, return_exceptions=True)
        nodes = [n for ns in res for n in ns if isinstance(n, tuple)]
        nodes.sort(key=lambda n: n[2] ^ id)
        return nodes[:num]
