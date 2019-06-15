from __future__ import annotations

import asyncio
import logging
from asyncio import Future, Handle, AbstractEventLoop
from asyncio.transports import BaseTransport, DatagramTransport
from dataclasses import dataclass, field
from functools import partial
from typing import Callable, Dict, Union, Text, Tuple, Optional, Generic, \
    TypeVar, ClassVar, cast, get_type_hints, Awaitable

import msgpack

from .node import Node, Addr
from .serializer import dumps, loads

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
    # FIXME
    func: str
    data: Union[Call[A], Result[R]]

    id_gen: ClassVar[int] = 0

    @classmethod
    def new_call(cls, caller: Node, func: str, args: A) -> Message:
        msg = Message(Message.id_gen, True, func, Call(caller, func, args))
        Message.id_gen += 1
        return msg

    @classmethod
    def new_result(cls, id: int, func: str, result: Result) -> Message:
        return Message(id, False, func, result)

    @classmethod
    def _infer_union(cls, is_call: bool):
        return {'data': Call if is_call else Result}

    @classmethod
    def from_bytes(cls, data: bytes,
                   infer_generic: Optional[Callable] = None) -> Message:
        return loads(cls, data, infer_generic=infer_generic,
                     infer_union=cls._infer_union)

    def to_bytes(self):
        return dumps(self)

    # FIXME
    def __class_getitem__(cls, name):
        item = super().__class_getitem__(name)
        orig_getattr = item.__getattr__

        def with_type(self, name):
            attr = orig_getattr(name)
            if name == 'from_bytes':
                attr = partial(attr.__func__, cls=cls)
            return attr

        item.__getattr__ = with_type
        return item


@dataclass
class Function:
    func: Callable
    args_type: type = field(init=False)
    return_type: type = field(init=False)

    def __post_init__(self):
        hints = get_type_hints(self.func)
        self.return_type = hints.pop('return', type(None))
        del hints['caller']
        self.args_type = Tuple[tuple(hints.values())]


log = logging.getLogger(__name__)
RpcCallback = Optional[Callable[[Node], Awaitable]]


class RpcProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop: AbstractEventLoop, caller: Node,
                 on_rpc: RpcCallback, timeout: float) -> None:
        self.loop = loop
        self.caller = caller
        self.on_rpc = on_rpc
        self.timeout = timeout

        self.funcs: Dict[str, Function] = {}
        self.requests: Dict[int, Tuple[Future, Handle]] = {}

    def register(self, func: Callable) -> Callable:
        self.funcs[func.__name__] = Function(func)
        return func

    def call(self, addr: Addr, func_name: str, *args) -> Future:
        msg = Message.new_call(self.caller, func_name, args)

        on_finished = self.loop.create_future()
        on_timeout = self.loop.call_later(
            self.timeout, self.timed_out, msg.id)
        self.requests[msg.id] = (on_finished, on_timeout)

        log.debug(f'Sending RPC request #{msg.id} {func_name}() to {addr}')
        self.transport.sendto(msg.to_bytes(), addr)
        return on_finished

    def timed_out(self, msg_id: int) -> None:
        log.warning(f'RPC #{msg_id} timed out')
        on_finished = self.requests.pop(msg_id)[0]
        on_finished.set_exception(asyncio.TimeoutError)

    def __getattr__(self, func: str):
        if func.startswith('__'):
            raise AttributeError

        def f(addr: Addr, *args):
            return self.call(addr, func, *args)

        return f

    async def do_call(self, call: Call) -> Result:
        try:
            func = self.funcs[call.func].func
        except KeyError:
            return Result(False, ValueError(f'no such RPC: {call.func}'))

        if self.on_rpc is not None:
            await self.on_rpc(call.caller)

        try:
            res = func(call.caller, *call.args)
            if asyncio.iscoroutinefunction(func):
                res = await res
        except Exception as exc:
            return Result(False, exc)
        else:
            return Result(True, res)

    def connection_made(self, transport: BaseTransport) -> None:
        self.transport = cast(DatagramTransport, transport)

    def close(self) -> None:
        self.transport.close()

    def _infer_generic(self, func: str):
        func = self.funcs[func]
        return {A: func.args_type, R: func.return_type}

    async def handle_request(self, msg: Message, addr: Addr):
        log.debug(f'Received RPC request #{msg.id}')
        res = Message.new_result(msg.id, msg.data.func, await self.do_call(msg.data))
        log.debug(f'Sending RPC response #{msg.id} back')
        self.transport.sendto(res.to_bytes(), addr)

    def handle_response(self, msg: Message):
        log.debug(f'Received RPC response #{msg.id} '
                  f"{'OK' if msg.data.ok else 'FAIL'}")
        try:
            on_call_finished, on_timeout = self.requests.pop(msg.id)
        except KeyError:
            log.warning(f'RPC #{msg.id} not found')
            return
        on_timeout.cancel()
        if msg.data.ok:
            on_call_finished.set_result(msg.data.value)
        else:
            on_call_finished.set_exception(msg.data.value)

    def datagram_received(self, data: Union[bytes, Text], addr: Addr) -> None:
        assert isinstance(data, bytes)
        try:
            msg = Message.from_bytes(data, self._infer_generic)
        except msgpack.UnpackException:
            log.warning(f'Received invalid RPC request/response: {data[:8]}...')
            return
        if msg.is_call:
            asyncio.create_task(self.handle_request(msg, addr))
        else:
            self.handle_response(msg)


async def start(caller: Node, on_rpc: RpcCallback = None,
                timeout: float = 30) -> RpcProtocol:
    loop = asyncio.get_running_loop()
    _, protocol = await loop.create_datagram_endpoint(
        lambda: RpcProtocol(loop, caller, on_rpc, timeout),
        local_addr=caller.addr
    )
    return cast(RpcProtocol, protocol)
