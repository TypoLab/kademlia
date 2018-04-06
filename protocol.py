import asyncio
import random
from typing import List, Union

from . import rpc
from .config import asize, ksize
from .node import ID, Node
from .routing import RoutingTable


class ValueFound(Exception):
    pass


class Server:
    instance = None

    def __new__(cls, host: str, port: int, id: ID = None):
        if cls.instance is None:
            cls.instance = super().__new__(cls)
            return cls.instance
        else:
            raise RuntimeError(
                f'There is alreadly a Kademlia server object: {cls.instance}')

    def __init__(self, host: str, port: int, id: ID = None) -> None:
        if id is None:
            id = ID(random.getrandbits(160))
        self.this_node = Node(id, host, port)
        self.routing_table = RoutingTable(self.this_node)
        rpc.this_node = self.this_node
        self.storage: dict = {}

    async def start(self, known_nodes: List[Node] = None):
        # setup the RPC
        s = rpc.Server(self.this_node.host, self.this_node.port)

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

        async def update(node: Node):
            await self.routing_table.update(node)
        s.on_rpc = update

        await s.start()

        # join the network
        if known_nodes is None:
            return
        tasks = (node.rpc.find_node(self.this_node.id) for node in known_nodes)
        res = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, new_nodes in enumerate(res):
            if isinstance(new_nodes, rpc.NetworkError):
                print(f'{known_nodes[idx]} failed to connect.')
            else:
                await self.routing_table.update(known_nodes[idx])
                for node in new_nodes:
                    await self.routing_table.update(node)

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
        print('here')
        import ipdb; ipdb.set_trace()
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
            print(f'the res={res}')
            input()
            if res == nodes:
                break
            nodes = res
        return nodes
