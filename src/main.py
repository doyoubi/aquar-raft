import gevent.monkey
gevent.monkey.patch_all()

import sys
import logging

from gevent.server import StreamServer

from .config import NODE_TABLE
from .redis_server import RedisProtocolServer
from .raft_server import RaftServer


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))


def simple_handler(cmd, proto_handler):
    if cmd[0].lower() == 'ping':
        proto_handler.send('+PONG\r\n')
    elif cmd[0].lower() == 'get':
        proto_handler.send_data('Foo')
    elif cmd[0].lower() == 'set':
        proto_handler.send('+OK\r\n')
    else:
        proto_handler.send_err('Unknown command')


def serve():
    # Usage: python main.py node_id
    node_id = sys.argv[1]
    addr = NODE_TABLE[node_id]
    raft_server = RaftServer(node_id)
    server = StreamServer(
        ('0.0.0.0', addr['port']),
        RedisProtocolServer(raft_server.handle_cmd).handle_conn)
    logger.info('Starting echo server on port {}'.format(addr['port']))
    server.start()
    logger.info('getting into loop')
    raft_server.loop()
