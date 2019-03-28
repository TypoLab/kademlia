import asyncio
import random
from asyncio import AbstractEventLoop

import pytest

from . import rpc

addr = ('127.0.0.1', 7890)


@pytest.fixture
async def server(event_loop: AbstractEventLoop):
    transport, protocol = await event_loop.create_datagram_endpoint(
        lambda: rpc.RpcServerProtocol(event_loop),
        local_addr=addr
    )
    try:
        yield protocol
    finally:
        transport.close()


@pytest.fixture
async def client(event_loop: AbstractEventLoop):
    transport, protocol = await event_loop.create_datagram_endpoint(
        lambda: rpc.RpcClientProtocol(event_loop, timeout=1),
        remote_addr=addr
    )
    try:
        yield protocol
    finally:
        transport.close()


@pytest.mark.asyncio
async def test_simple_call(server, client):
    @server.register
    def echo(a):
        return a

    for i in ['str', 42, [addr, addr]]:
        assert await client.echo(i) == i


@pytest.mark.asyncio
async def test_exception(server, client):
    @server.register
    def throw():
        raise FileNotFoundError

    with pytest.raises(FileNotFoundError):
        await client.throw()

    with pytest.raises(ValueError):
        await client.not_exists(12)

    @server.register
    def no_args():
        pass

    with pytest.raises(TypeError):
        await client.no_args('arg')


@pytest.mark.asyncio
async def test_timeout(client):
    with pytest.raises(asyncio.TimeoutError):
        await client.no_server()


@pytest.mark.asyncio
async def test_concurrent_calls(server, client):
    @server.register
    def echo(a):
        return a

    input = list(range(32))
    random.shuffle(input)
    results = await asyncio.gather(*(client.echo(i) for i in input))
    assert results == input
