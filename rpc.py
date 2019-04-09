from __future__ import annotations

import asyncio
import logging
from asyncio import Future, Handle, AbstractEventLoop
from asyncio.transports import BaseTransport, DatagramTransport
from dataclasses import dataclass, astuple
from typing import Any, Callable, Dict, Union, Text, Tuple, Optional, Generic, \
    TypeVar
from typing import cast

import msgpack

from .node import Node, Addr

A = TypeVar('A')
R = TypeVar('R')


@dataclass
class Call(Generic[A]):
    caller: Node
    func: str
    args: A


@dataclass
class Result(Generic[R]):
    ok: bool
    value: R


@dataclass
class Message(Generic[A, R]):
    id: int
    is_call: bool
    data: Union[Call[A], Result[R]]


log = logging.getLogger(__name__)
RpcCallback = Optional[Callable[[Call], None]]


class RpcProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop: AbstractEventLoop, caller: Node,
                 on_rpc: RpcCallback, timeout: float) -> None:
        self.loop = loop
        self.caller = caller
        self.on_rpc = on_rpc
        self.timeout = timeout

        self.funcs: Dict[str, Callable] = {}
        self.requests: Dict[int, Tuple[Future, Handle]] = {}
        self.req_id = 0

    def register(self, func: Callable) -> Callable:
        self.funcs[func.__name__] = func
        return func

    def call(self, addr: Addr, func: str, *args, **kwargs) -> Future:
        msg = Message(id=self.req_id, is_call=True,
                      data=Call(self.caller, func, args, kwargs))
        self.req_id += 1

        on_finished = self.loop.create_future()
        on_timeout = self.loop.call_later(
            self.timeout, self.timed_out, msg.id)
        self.requests[msg.id] = on_finished, on_timeout
        log.debug(f'Sending RPC request {msg} to {addr}')
        self.transport.sendto(msg.to_bytes(), addr)
        return on_finished

    def timed_out(self, msg_id: int) -> None:
        log.warning(f'RPC {msg_id} timed out')
        on_finished, _ = self.requests.pop(msg_id)
        on_finished.set_exception(asyncio.TimeoutError)

    def __getattr__(self, func: str):
        if func.startswith('_'):
            raise AttributeError

        def f(addr: Addr, *args, **kwargs):
            return self.call(addr, func, *args, **kwargs)

        return f

    def do_call(self, call: Call) -> Result:
        try:
            func = self.funcs[call.func]
        except KeyError:
            return Result(False, ValueError(f'no such RPC: {call.func}'))

        if self.on_rpc is not None:
            self.on_rpc(call)

        try:
            res = func(*call.args, **call.kwargs)
        except Exception as exc:
            return Result(False, exc)
        else:
            return Result(True, res)

    def connection_made(self, transport: BaseTransport) -> None:
        self.transport = cast(DatagramTransport, transport)

    def close(self) -> None:
        self.transport.close()

    def datagram_received(self, data: Union[bytes, Text], addr: Addr) -> None:
        assert isinstance(data, bytes)
        try:
            msg = Message.from_bytes(data)
        except msgpack.UnpackException:
            log.warning(f'Received invalid RPC request/response: {data}')
            return
        log.debug(f'Received RPC: {msg} from {addr}')

        if msg.is_call:
            res = Message(id=msg.id, is_call=False, data=self.do_call(msg.data))
            log.debug(f'Sending RPC response {res} to {addr}')
            self.transport.sendto(res.to_bytes(), addr)
        else:
            on_call_finished, on_timeout = self.requests.pop(msg.id)
            on_timeout.cancel()
            if msg.data.ok:
                on_call_finished.set_result(msg.data.value)
            else:
                on_call_finished.set_exception(msg.data.value)


async def start(addr: Addr, caller: Node, on_rpc: RpcCallback = None,
                timeout: float = 30) -> RpcProtocol:
    loop = asyncio.get_running_loop()
    _, protocol = await loop.create_datagram_endpoint(
        lambda: RpcProtocol(loop, caller, on_rpc, timeout),
        local_addr=addr
    )
    return cast(RpcProtocol, protocol)
