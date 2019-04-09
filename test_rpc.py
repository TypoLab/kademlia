import asyncio
import random

import pytest

from .rpc import start
from .node import ID, Node

addr = ('127.0.0.1', 7890)
node = Node(ID(123), addr)


@pytest.fixture
async def rpc():
    rpc = await start(addr, node, timeout=1)
    try:
        yield rpc
    finally:
        rpc.close()


@pytest.mark.asyncio
async def test_simple_call(rpc):
    @rpc.register
    def echo(a):
        return a

    for i in ['str', 42, [addr, addr]]:
        assert await rpc.echo(addr, i) == i


@pytest.mark.asyncio
async def test_exception(server, client):
    @server.register
    def throw():
        raise FileNotFoundError

    with pytest.raises(FileNotFoundError):
        await client.throw(addr)

    with pytest.raises(ValueError):
        await client.not_exists(addr)

    @server.register
    def no_args():
        pass

    with pytest.raises(TypeError):
        await client.no_args(addr, 'arg')


@pytest.mark.asyncio
async def test_timeout():
    client = await rpc.make_client(node, timeout=.5)
    with pytest.raises(asyncio.TimeoutError):
        await client.no_server(addr)
    client.close()


@pytest.mark.asyncio
async def test_concurrent_calls(server, client):
    @server.register
    def echo(a):
        return a

    input = list(range(32))
    random.shuffle(input)
    results = await asyncio.gather(*(client.echo(addr, i) for i in input))
    assert results == input


@pytest.mark.asyncio
async def test_on_rpc_callback():
    caller: Node

    def on_rpc(call: rpc.Call) -> None:
        nonlocal caller
        caller = call.caller

    server = await rpc.start_server(addr, on_rpc)
    client = await rpc.make_client(node)

    @server.register
    def f():
        pass

    await client.f(addr)
    assert caller == node

    server.close()
    client.close()
