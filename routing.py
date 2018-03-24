from typing import List

from . import rpc
from .config import ksize
from .node import ID, Node


class KBucket:
    def __init__(self, range: range, size: int = 20) -> None:
        self.range = range
        self.size = size
        self.nodes: List[Node] = []

    def __repr__(self):
        return f'<KBucket: {len(self.nodes)} node in {self.range}>'

    def __iter__(self):
        return iter(self.nodes)

    def __contains__(self, node: Node):
        return node in self.nodes

    def covers(self, id: ID) -> bool:
        return id in self.range

    def full(self) -> bool:
        return len(self.nodes) == self.size

    async def update(self, new):
        def move_to_tail(node):
            self.nodes.remove(node)
            self.nodes.append(node)

        if new in self.nodes:
            move_to_tail(new)
        else:
            if len(self.nodes) < self.size:
                self.nodes.append(new)
            else:
                oldest = self.nodes[0]
                try:
                    await oldest.rpc.ping()
                except rpc.NetworkError:
                    self.nodes.remove(oldest)
                    self.nodes.append(new)
                else:
                    move_to_tail(oldest)

    def split(self):
        r = self.range
        mid = (r.start + r.stop) // 2
        left = KBucket(range(r.start, mid), self.size)
        right = KBucket(range(mid, r.stop), self.size)
        for node in self:
            if node.id < mid:
                left.nodes.append(node)
            else:
                right.nodes.append(node)
        return [left, right]


class RoutingTable:
    def __init__(self, server_id: ID) -> None:
        self.buckets: List[KBucket] = [KBucket(range(0, 2**160))]
        self.server_id = server_id

    def __repr__(self):
        return f'<RoutingTable: {len(self.buckets)} KBucket>'

    def __iter__(self):
        return iter(self.buckets)

    def update(self, new: Node):
        def bucket_covers():
            for bucket in self.buckets:
                if bucket.covers(new.id):
                    return bucket
            else:
                raise RuntimeError('bug')
        bucket = bucket_covers()
        if not bucket.full():
            bucket.update(new)
        else:
            if bucket.covers(self.server_id):
                self.buckets.remove(bucket)
                self.buckets += bucket.split()
                self.update(new)

    def get_nodes_nearby(self, node: Node):
        def gen():
            for bucket in self:
                for node in bucket:
                    yield node
        nodes: List[Node] = list(gen())
        nodes.sort(key=lambda n: n.id ^ node.id)
        return nodes[:ksize]
