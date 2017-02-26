import socket
import logging
import StringIO

import redis_protocol
from gevent.server import StreamServer


logger = logging.getLogger(__name__)


class InvalidRedisPacket(Exception):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return 'InvalidRedisPacket<{}>'.format(self.data)


def parse_redis_cmd(data):
    AGAIN = (None, None)
    buf = StringIO.StringIO(data)
    arr_len = parse_redis_len(buf, '*')
    if arr_len is None:
        return AGAIN
    cmd = []
    for _ in range(arr_len):
        l = parse_redis_len(buf, '$')
        if l is None:
            return AGAIN
        s = ''
        while True:
            line = buf.readline()
            if not line:
                return AGAIN
            s += line
            if len(s) > l + 2:
                raise InvalidRedisPacket(data)
            if len(s) == l + 2:
                s = s[:-2]
                cmd.append(s)
                break
    return cmd, buf.pos


def parse_redis_len(buf, prefix):
    if '\n' not in buf.buf[buf.pos:]:
        return
    line = buf.readline()
    assert line[0] == prefix
    try:
        return int(line.strip('{}\r\n'.format(prefix)))
    except ValueError:
        raise InvalidRedisPacket(buf.buf)


class RedisProtocolServer(object):
    def __init__(self, cmd_handler):
        self.cmd_handler = cmd_handler

    def handle_conn(self, socket, address):
        logger.info('accepted new connection')
        addr = '{}:{}'.format(*address)
        proto_handler = RedisProtocolHandler(socket, self.cmd_handler)
        while True:
            data = socket.recv(10)
            if not data:
                break
            proto_handler.handle_read(addr, data)
        socket.close()


class RedisProtocolHandler(object):
    def __init__(self, socket, callback):
        self.data = ''
        self.socket = socket
        self.callback = callback

    def handle_read(self, address, data):
        self.data += data
        while self.data:
            cmd, pos = parse_redis_cmd(self.data)
            if cmd is None:
                return
            self.callback(address, cmd, self)
            self.data = self.data[pos:]

    def send_data(self, data):
        if isinstance(data, list):
            self.send(redis_protocol.encode(*data))
        else:
            resp = '${}\r\n{}\r\n'.format(len(data), data)
            self.send(resp)

    def send_err(self, err):
        err = '-{}\r\n'.format(err)
        self.send(err)

    def send(self, data):
        self.socket.sendall(data)

# Example:
# def pong(address, cmd, proto_handler):
#    proto_handler.send('+PONG\r\n')

# server = StreamServer(('0.0.0.0', 7777), RedisProtocolServer(pong).handle_conn)
# server.serve_forever()

