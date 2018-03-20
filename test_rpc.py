import pytest

import rpc

port = 7890


@pytest.fixture
async def server():
    s = rpc.Server('127.0.0.1', port)
    await s.start()
    yield s
    await s.close()


@pytest.fixture
async def client():
    c = rpc.Client('127.0.0.1', port)
    yield c
    await c.close()


@pytest.mark.asyncio
async def test_simple_call(server, client):
    @server.register
    def echo(a):
        return a

    assert await client.echo('str') == 'str'
    assert await client.echo(42) == 42


class SomeError(Exception):
    pass


@pytest.mark.asyncio
async def test_exception(server, client):

    @server.register
    def throw():
        raise SomeError

    with pytest.raises(SomeError):
        await client.throw()

    with pytest.raises(rpc.NoSuchRpcError):
        await client.dummy(12)

    @server.register
    def no_args():
        pass

    with pytest.raises(TypeError):
        await client.no_args('arg')
