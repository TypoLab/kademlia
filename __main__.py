import argparse
import sys
import asyncio
import kademlia
from kademlia import ID, Node


ap = argparse.ArgumentParser(description='A demo for using kademlia lib.')
ap.add_argument('--port', '-p', default=7890, type=int, help='Port to listen.')
ap.add_argument('--id',  default=0, type=int, help='Node ID.')
ap.add_argument('--bootstrap', '-b', nargs='*', help='Bootstrap peers.')
args = ap.parse_args()


class AioInput:
    def __init__(self):
        self.q = asyncio.Queue()
        loop.add_reader(sys.stdin.fileno(), self._got)

    def close(self):
        loop.remove_reader(sys.stdin.fileno())

    def _got(self):
        try:
            data = input()
        except EOFError as exc:
            data = exc
        self.q.put_nowait(data)

    async def __call__(self, prompt=None):
        if prompt:
            print(prompt, end='', flush=True)
        data = await self.q.get()
        if isinstance(data, EOFError):
            raise data
        else:
            return data


async def repl():
    while True:
        try:
            cmd = await ainput('> ')
        except EOFError:
            return
        cmds = cmd.split()
        if not cmds:
            continue
        if cmds[0] == 'info':
            print(f'  Server: {dht}\n  Nodes: {dht.routing_table.get_nodes()}\n  Storage: {dht.storage}')
        else:
            id = ID(int(cmds[1]))
            if cmds[0] == 'set':
                await dht.set(id, cmds[2].encode())
            elif cmds[0] == 'get':
                print((await dht.get(id)).decode())
            else:
                print('Unknown cmd.')


dht = kademlia.Server('0.0.0.0', args.port, ID(args.id))
loop = asyncio.get_event_loop()
ainput = AioInput()

if args.bootstrap is None:
    bootstrap_nodes = None
else:
    bootstrap_nodes = []
    for info in args.bootstrap:
        id, host, port = info.split(',')
        id = ID(int(id))
        bootstrap_nodes.append(Node(id, host, port))

loop.run_until_complete(dht.start(bootstrap_nodes))

try:
    loop.run_until_complete(repl())
except KeyboardInterrupt:
    ainput.close()
    loop.run_until_complete(dht.close())
    loop.close()
