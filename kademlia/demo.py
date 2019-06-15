import argparse
import asyncio
import logging
import sys

from kademlia import ID, Node, Server


class AioInput:
    def __init__(self):
        self.q = asyncio.Queue()

    def __enter__(self):
        asyncio.get_running_loop().add_reader(sys.stdin.fileno(), self._got)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.get_running_loop().remove_reader(sys.stdin.fileno())

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


def make_args():
    ap = argparse.ArgumentParser(description='A demo for using kademlia lib.')
    ap.add_argument('--port', '-p', default=7890, type=int,
                    help='UDP port to listen. (default: 7890)')
    ap.add_argument('--id', help='Node ID. (default: random)')
    ap.add_argument('--bootstrap', '-b', nargs='*',
                    help='Bootstrap peers. (id,host,port)')
    ap.add_argument('--log-level', '-l', choices=('CRITICAL', 'FATAL', 'ERROR',
                                                  'WARNING', 'WARN', 'INFO',
                                                  'DEBUG', 'NOTSET'),
                    default='WARNING',
                    help='Set logging level. (default: DEBUG)')
    return ap.parse_args()


async def start_repl():
    args = make_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    if args.bootstrap is None:
        bootstrap_nodes = None
    else:
        bootstrap_nodes = []
        for info in args.bootstrap:
            id, host, port = info.split(',')
            id = ID(int(id))
            port = int(port)
            bootstrap_nodes.append(Node(id, (host, port)))

    id = ID(int(args.id)) if args.id else None
    dht = Server(('127.0.0.1', args.port), id)
    await dht.start(bootstrap_nodes)

    while True:
        with AioInput() as ainput:
            try:
                cmd = await ainput('> ')
            except EOFError:
                return
            cmds = cmd.split()
            if not cmds:
                continue
            if cmds[0] == 'help':
                print('Cmds:\n'
                      '   info\n'
                      '   set <id:int> <data>\n'
                      '   get <id>')
            elif cmds[0] == 'info':
                print(f'  Server: {dht}\n'
                      f'  Nodes: {dht.routing_table}\n'
                      f'  Storage: {dht.storage}')
            elif cmds[0] == 'set' or cmds[0] == 'get':
                id = ID(int(cmds[1]))
                if cmds[0] == 'set':
                    await dht.set(id, cmds[2].encode())
                elif cmds[0] == 'get':
                    print((await dht.get(id)).decode())
            else:
                print('Unknown cmd.')


def main():
    asyncio.run(start_repl())
