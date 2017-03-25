import time
import socket
import logging
import StringIO
from collections import namedtuple

import redis_protocol
import gevent
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
    def __init__(self, cmd_server):
        self.cmd_server = cmd_server

    def handle_conn(self, conn, address):
        addr = '{}:{}'.format(*address)
        proto_handler = RedisProtocolHandler(conn,
                                             self.cmd_server.handle_cmd)
        while not proto_handler.broken:
            data = self.read_request(conn, address, proto_handler)
            if not data and proto_handler.response_sent:
                break
            if data:
                proto_handler.handle_read(addr, data)
            gevent.sleep(0)  # switch
        logger.info('connection closed')
        conn.close()

    def read_request(self, conn, address, proto_handler):
        try:
            return conn.recv(1000)
        except socket.error as e:
            logger.error(e)
            if 'Bad file descriptor' not in str(e):
                raise
            self.cmd_server.remove_client(address)
            proto_handler.broken = True
            proto_handler.response_sent = True


class RedisProtocolHandler(object):
    def __init__(self, conn, callback):
        self.data = ''
        self.conn = conn
        self.callback = callback
        self.response_sent = True  # will be modified by outer object
        self.broken = False

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

    def send_null_bulk_str(self):
        self.send('$-1\r\n')

    def send(self, data):
        try:
            self.conn.sendall(data)
        except socket.error as e:
            logger.error(e)
            self.broken = True
            if 'Bad file descriptor' not in str(e):
                raise
