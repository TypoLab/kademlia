from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import Union, Tuple


class ID(int):
    """Node ID with readable representation."""

    def __new__(cls, num_or_base32: Union[int, str]) -> ID:
        if isinstance(num_or_base32, int):
            return super().__new__(cls, num_or_base32)
        elif isinstance(num_or_base32, str):
            return super().__new__(cls, int.from_bytes(base64.b32decode(num_or_base32), 'little'))
        else:
            raise ValueError(f'unsupported type: {type(num_or_base32)}')

    def __repr__(self):
        return base64.b32encode(self.to_bytes(20, 'little')).decode()


Addr = Tuple[str, int]


@dataclass(eq=True, frozen=True)
class Node:
    id: ID
    addr: Addr = field(compare=False)
