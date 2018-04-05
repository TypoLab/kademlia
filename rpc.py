import pickle
from collections import namedtuple
from functools import partial
from typing import Callable, Dict, Any

import aiohttp
from aiohttp import web

Call = namedtuple('Call', 'name, args, kwargs, node')
Result = namedtuple('Result', 'ok, value')
# set in .protocol.Server.__init__
this_node: Any = None


class NoSuchRpcError(Exception):
    pass


class NetworkError(Exception):
    pass


class Server:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.funcs: Dict[str, Callable] = {}
        self.on_rpc: Callable = None
        app = web.Application()
        self.runner = web.AppRunner(app)
        app.router.add_post('/rpc', self.handler)

    def register(self, func: Callable) -> Callable:
        self.funcs[func.__name__] = func
        return func

    async def handler(self, request: web.Request) -> web.Response:
        call = pickle.loads(await request.read())
        if self.on_rpc:
            self.on_rpc(call.node)
        res = self.do_call(call)
        return web.Response(body=pickle.dumps(res))

    def do_call(self, call: Call) -> Result:
        try:
            func = self.funcs[call.name]
        except KeyError:
            return Result(False, NoSuchRpcError)
        try:
            res = func(*call.args, **call.kwargs)
        except Exception as exc:
            return Result(False, exc)
        else:
            return Result(True, res)

    async def start(self) -> None:
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()

    async def close(self) -> None:
        await self.runner.cleanup()


class Client:
    def __init__(self, host: str, port: int) -> None:
        self.session: aiohttp.ClientSession = None
        self.url = f'http://{host}:{port}/rpc'

    async def call(self, name: str, *args, **kwargs):
        if self.session is None:
            self.session = aiohttp.ClientSession()
        data = pickle.dumps(Call(name, args, kwargs, this_node))
        async with self.session.post(self.url, data=data) as resp:
            if resp.status >= 400:
                raise NetworkError
            res = pickle.loads(await resp.read())
            if res.ok:
                return res.value
            else:
                raise res.value

    def __getattr__(self, name):
        # pickle.dumps(self) calls self.__getstate__
        if name.startswith('_'):
            raise AttributeError
        return partial(self.call, name)

    async def close(self) -> None:
        if self.session is not None:
            await self.session.close()
