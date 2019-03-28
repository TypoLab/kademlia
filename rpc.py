from __future__ import annotations

import asyncio
import logging
import pickle
from asyncio import Future, Handle
from asyncio.transports import BaseTransport, DatagramTransport
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Dict, Union, Text, Tuple
from typing import cast


@dataclass
class Call:
    name: str
    args: tuple
    kwargs: dict
    id: int


@dataclass
class Result:
    ok: bool
    value: Any
    id: int


log = logging.getLogger(__name__)


class RpcServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.funcs: Dict[str, Callable] = {}

    def register(self, func: Callable) -> Callable:
        self.funcs[func.__name__] = func
        return func

    def do_call(self, call: Call) -> Result:
        try:
            func = self.funcs[call.name]
        except KeyError:
            return Result(
                False, ValueError(f'no such RPC: {call.name}'), call.id)
        try:
            res = func(*call.args, **call.kwargs)
        except Exception as exc:
            return Result(False, exc, call.id)
        else:
            return Result(True, res, call.id)

    def connection_made(self, transport: BaseTransport) -> None:
        self.transport = cast(DatagramTransport, transport)

    def datagram_received(self, data: Union[bytes, Text],
                          addr: Tuple[str, int]) -> None:
        assert isinstance(data, bytes)
        try:
            call: Call = pickle.loads(data)
        except pickle.UnpicklingError:
            log.warning(f'received invalid RPC request: {data}')
        else:
            res = self.do_call(call)
            self.transport.sendto(pickle.dumps(res), addr)


class RpcClientProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop: asyncio.AbstractEventLoop,
                 timeout: int = 30) -> None:
        self.loop = loop
        self.timeout = timeout
        self.requests: Dict[int, Tuple[Future, Handle]] = {}
        self.req_id = 0

    def call(self, name: str, *args, **kwargs) -> Future:
        call = Call(name, args, kwargs, self.req_id)
        self.req_id += 1
        on_finished = self.loop.create_future()
        on_timeout = self.loop.call_later(
            self.timeout, self.timed_out, call.id)
        self.requests[call.id] = on_finished, on_timeout
        self.transport.sendto(pickle.dumps(call))
        return on_finished

    def timed_out(self, call_id: int) -> None:
        on_finished, _ = self.requests.pop(call_id)
        on_finished.set_exception(asyncio.TimeoutError)

    def __getattr__(self, name):
        # pickle.dumps(self) calls self.__getstate__
        if name.startswith('_'):
            raise AttributeError
        return partial(self.call, name)

    def connection_made(self, transport: BaseTransport) -> None:
        self.transport = cast(DatagramTransport, transport)

    def datagram_received(self, data: Union[bytes, Text],
                          addr: Tuple[str, int]) -> None:
        assert isinstance(data, bytes)
        try:
            res: Result = pickle.loads(data)
        except pickle.UnpicklingError:
            log.warning(f'received invalid RPC response: {data}')
        else:
            on_call_finished, on_timeout = self.requests.pop(res.id)
            on_timeout.cancel()
            if res.ok:
                on_call_finished.set_result(res.value)
            else:
                on_call_finished.set_exception(res.value)
