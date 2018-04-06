import argparse
import asyncio
import kademlia
from kademlia import ID, Node


ap = argparse.ArgumentParser(description='A demo for using kademlia lib.')
ap.add_argument('--listen', '-l', default='0.0.0.0', help='Address to bind.')
ap.add_argument('--port', '-p', default=7890, type=int, help='Port to listen.')
ap.add_argument('--id',  default=0, type=int, help='Node ID.')
ap.add_argument('--bootstrap', '-b', nargs='*', help='Bootstrap peers.')
args = ap.parse_args()

dht = kademlia.Server(args.listen, args.port)
loop = asyncio.get_event_loop()


async def repl():
    while True:
        try:
            cmd = input('> ').split()
        except EOFError:
            return
        if cmd[0] == 'serve':
            await loop.create_future()

        id = ID(int(cmd[1]))
        if cmd[0] == 'set':
            await dht.set(id, cmd[2].encode())
        elif cmd[0] == 'get':
            print((await dht.get(id)).decode())
        else:
            print('Unknown cmd.')


async def main():
    if args.bootstrap is None:
        bootstrap_nodes = None
    else:
        bootstrap_nodes = []
        for info in args.bootstrap:
            base32, host, port = info.split(',')
            id = ID(base32=base32)
            bootstrap_nodes.append(Node(id, host, port))
            print(bootstrap_nodes)
    await dht.start(bootstrap_nodes)
    print(dht)

try:
    loop.run_until_complete(main())
    loop.run_until_complete(repl())
except KeyboardInterrupt:
    loop.run_until_complete(dht.close())
