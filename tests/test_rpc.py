import asyncio
import random

import pytest

from kademlia import ID, Node
from kademlia.rpc import start, Call

addr = ('127.0.0.1', 7890)
node = Node(ID(123), addr)


@pytest.fixture
async def rpc():
    rpc = await start(node, timeout=1)
    try:
        yield rpc
    finally:
        rpc.close()


@pytest.mark.asyncio
async def test_simple_call(rpc):
    @rpc.register
    def echo(a: int) -> int:
        return a

    for i in range(10):
        assert await rpc.echo(addr, i) == i


@pytest.mark.asyncio
async def test_async_rpc_func(rpc):
    @rpc.register
    async def async_echo(a: int) -> int:
        return a

    for i in range(10):
        assert await rpc.async_echo(addr, i) == i


# FIXME: Exceptions cannot be dumped by the serializer now.
'''
@pytest.mark.asyncio
async def test_exceptions(rpc):
    @rpc.register
    def throw():
        raise FileNotFoundError

    with pytest.raises(FileNotFoundError):
        await rpc.throw(addr)

    with pytest.raises(ValueError):
        await rpc.not_exists(addr)

    @rpc.register
    def no_args():
        pass

    with pytest.raises(TypeError):
        await rpc.no_args(addr, 'arg')
'''


@pytest.mark.asyncio
async def test_timeout(rpc):
    @rpc.register
    def f():
        pass

    with pytest.raises(asyncio.TimeoutError):
        await rpc.f(('127.0.0.1', 1111))


@pytest.mark.asyncio
async def test_concurrent_calls(rpc):
    @rpc.register
    def echo(a: int) -> int:
        return a

    inputs = list(range(32))
    random.shuffle(inputs)
    results = await asyncio.gather(*(rpc.echo(addr, i) for i in inputs))
    assert results == inputs


@pytest.mark.asyncio
async def test_on_rpc_callback():
    async def on_rpc(caller: Node) -> None:
        assert caller == node

    rpc = await start(node, on_rpc, .5)

    @rpc.register
    def f():
        pass

    try:
        await rpc.f(addr)
    finally:
        rpc.close()
