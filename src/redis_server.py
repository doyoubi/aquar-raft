import time
import socket
import logging
import StringIO
from collections import namedtuple

import redis_protocol
from gevent.server import StreamServer


logger = logging.getLogger(__name__)


class InvalidRedisPacket(Exception):
    def __init__(self, data, index):
        self.data = data
        self.index = index

    def __str__(self):
        return 'InvalidRedisPacket<{}, {}>'.format(self.index, self.data)


AGAIN = namedtuple('Again', 'foo')
OK_RESP = '+OK\r\n'


def parse_redis_cmd(data):
    buf = StringIO.StringIO(data)
    cmd = parse_redis_array(buf)
    if cmd == AGAIN:
        return (None, None)
    return cmd, buf.pos


def parse_redis_array(buf):
    arr_len = parse_redis_len(buf, '*')
    if arr_len is None:
        return AGAIN
    arr = []
    for _ in range(arr_len):
        if len(buf.buf) == buf.pos:
            return AGAIN
        prefix = buf.buf[buf.pos]
        if prefix == '*':
            res = parse_redis_array(buf)
        elif prefix == '$':
            res = parse_redis_bulk_str(buf)
        else:
            raise InvalidRedisPacket(buf.buf, buf.pos)
        if res == AGAIN:
            return AGAIN
        arr.append(res)
    return arr


def parse_redis_bulk_str(buf):
    l = parse_redis_len(buf, '$')
    if l == AGAIN:
        return AGAIN
    s = ''
    while True:
        line = buf.readline()
        if not line:
            return AGAIN
        s += line
        if len(s) > l + 2:
            raise InvalidRedisPacket(buf.buf, buf.pos)
        if len(s) == l + 2:
            s = s[:-2]
            break
    return s


def parse_redis_len(buf, prefix):
    if '\n' not in buf.buf[buf.pos:]:
        return AGAIN
    line = buf.readline()
    assert line[0] == prefix
    try:
        return int(line.strip('{}\r\n'.format(prefix)))
    except ValueError:
        raise InvalidRedisPacket(buf.buf, buf.pos)


class RedisProtocolServer(object):
    def __init__(self, cmd_handler):
        self.cmd_handler = cmd_handler

    def handle_conn(self, socket, address):
        addr = '{}:{}'.format(*address)
        proto_handler = RedisProtocolHandler(socket, self.cmd_handler)
        timeout = 5
        start = time.time()
        while True:
            # TODO: handler connection error
            data = socket.recv(1000)
            if time.time() - start > timeout:
                break
            if not data and proto_handler.response_sent:
                break
            if not data:
                continue
            start = time.time()
            proto_handler.handle_read(addr, data)
        socket.close()


class RedisProtocolHandler(object):
    def __init__(self, socket, callback):
        self.data = ''
        self.socket = socket
        self.callback = callback
        self.response_sent = True  # will be modified by outer object

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

    def send_ok(self):
        self.send(OK_RESP)

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

