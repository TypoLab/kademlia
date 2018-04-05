import base64
import hashlib

from . import rpc

HASH = hashlib._hashlib.HASH


class ID:
    '''Node ID with readable representation.'''
    # TODO: doctest

    def __init__(self, num: int = None, *,
                 hash: HASH = None, base32: str = None) -> None:
        if sum(1 for arg in (num, hash, base32) if arg) != 1:
            raise ValueError('Expected excactly one arguement.')
        if num:
            self.num = num
        elif hash:
            self.num = int.from_bytes(hash.digest(), 'big')
        elif base32:
            self.num = int.from_bytes(base64.b32decode(base32), 'big')

    def __repr__(self):
        return base64.b32encode(self.num.to_bytes(20, 'big')).decode()

    def _get_num(self, obj):
        if isinstance(obj, ID):
            return obj.num
        elif isinstance(obj, int):
            return obj
        else:
            raise NotImplementedError

    def __eq__(self, other):
        try:
            return self.num == self._get_num(other)
        except NotImplementedError:
            return NotImplemented

    def __xor__(self, other):
        try:
            return self.__class__(self.num ^ self._get_num(other))
        except NotImplementedError:
            return NotImplemented

    def __lt__(self, other):
        try:
            return self.num < self._get_num(other)
        except NotImplementedError:
            return NotImplemented


class Node:
    def __init__(self, id: ID, host: str, port: int) -> None:
        self.id = id
        self.host = host
        self.port = port
        self.rpc = rpc.Client(host, port)

    def __repr__(self):
        return f'<Node {self.id} {self.host}:{self.port}>'
