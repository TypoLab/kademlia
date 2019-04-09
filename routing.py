import logging
from typing import List, Tuple

from . import rpc
from .config import ksize
from .node import ID, Node

log = logging.getLogger(__name__)




class RoutingTable:
    def __init__(self, this_node: Node) -> None:
        self.this_node = this_node

    def __repr__(self):
        return f'<RoutingTable: {len(self.buckets)} KBucket>'

    def __iter__(self):
        return iter(self.buckets)

    def get_nodes(self):
        def gen():
            for bucket in self:
                for node in bucket:
                    yield node

        return list(gen())

    def get_nodes_nearby(self, id: ID) -> List[Node]:
        nodes: List[Node] = self.get_nodes()
        nodes.sort(key=lambda n: n.id ^ id)
        return nodes[:ksize]
