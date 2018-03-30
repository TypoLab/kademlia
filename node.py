import base64
import random

from . import rpc


class ID:
    '''Node ID with readable representation.'''
    def __init__(self, id: int = None) -> None:
        if id is None:
            id = random.getrandbits(160)
        self.id = id

    def __repr__(self):
        return base64.b32encode(self.id.to_bytes(20, 'big')).decode()

    def __eq__(self, other):
        return self.id == other.id

    def __xor__(self, other):
        return self.__class__(self.id ^ other.id)

    def __lt__(self, other):
        return self.id < other.id


class Node:
    def __init__(self, id: ID, host: str, port: int) -> None:
        self.id = id
        self.host = host
        self.port = port

    def __repr__(self):
        return f'<Node {self.id} {self.host}:{self.port}>'

    @property
    def rpc(self):
        self.rpc = rpc.Server()
        return self.rpc
