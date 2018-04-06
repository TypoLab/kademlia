from typing import List, Tuple

from . import rpc
from .config import ksize
from .node import ID, Node


class KBucket:
    def __init__(self, range: Tuple[int, int]) -> None:
        self.range = range
        self.nodes: List[Node] = []

    def __repr__(self):
        return f'<KBucket: {len(self.nodes)} node in {self.range}>'

    def __iter__(self):
        return iter(self.nodes)

    def __contains__(self, node: Node):
        return node in self.nodes

    def covers(self, node: Node) -> bool:
        return self.range[0] <= node.id < self.range[1]

    def full(self) -> bool:
        return len(self.nodes) == ksize

    async def update(self, new):
        def move_to_tail(node):
            self.nodes.remove(node)
            self.nodes.append(node)

        if new in self.nodes:
            move_to_tail(new)
        else:
            if len(self.nodes) < ksize:
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
        mid = (r[0] + r[1]) // 2
        left = KBucket((r[0], mid))
        right = KBucket((mid, r[1]))
        for node in self:
            if node.id < mid:
                left.nodes.append(node)
            else:
                right.nodes.append(node)
        return [left, right]


class RoutingTable:
    def __init__(self, this_node: Node) -> None:
        self.this_node = this_node
        self.buckets: List[KBucket] = [KBucket((0, 2**160))]

    def __repr__(self):
        return f'<RoutingTable: {len(self.buckets)} KBucket>'

    def __iter__(self):
        return iter(self.buckets)

    async def update(self, new: Node):
        if new == self.this_node:
            print('ignoring self')
            return

        def bucket_covers():
            for bucket in self.buckets:
                if bucket.covers(new):
                    return bucket
            else:
                print(f'** {self.buckets}')
                raise RuntimeError(f'{new} not in any of bucket!')
        bucket = bucket_covers()
        if not bucket.full():
            await bucket.update(new)
        else:
            if bucket.covers(self.this_node):
                self.buckets.remove(bucket)
                self.buckets += bucket.split()
                await self.update(new)

    def get_nodes_nearby(self, id: ID) -> List[Node]:
        def gen():
            for bucket in self:
                for node in bucket:
                    yield node
        nodes: List[Node] = list(gen())
        nodes.sort(key=lambda n: n.id ^ id)
        return nodes[:ksize]
