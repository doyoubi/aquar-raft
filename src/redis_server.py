import socket
import StringIO
import redis_protocol
from gevent.server import StreamServer


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
        proto_handler = RedisProtocolHandler(socket, self.cmd_handler)
        while True:
            data = socket.recv(10)
            if not data:
                print("client disconnected")
                break
            proto_handler.handle_read(data)
        socket.close()


class RedisProtocolHandler(object):
    def __init__(self, socket, callback):
        self.data = ''
        self.socket = socket
        self.callback = callback

    def handle_read(self, data):
        self.data += data
        while self.data:
            cmd, pos = parse_redis_cmd(self.data)
            if cmd is None:
                return
            self.callback(cmd, self)
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


def simple_handler(cmd, proto_handler):
    if cmd[0].lower() == 'ping':
        proto_handler.send('+PONG\r\n')
    elif cmd[0].lower() == 'get':
        proto_handler.send_data('Foo')
    elif cmd[0].lower() == 'set':
        proto_handler.send('+OK\r\n')
    else:
        proto_handler.send_err('Unknown command')


if __name__ == '__main__':
    server = StreamServer(('0.0.0.0', 7777),
        RedisProtocolServer(simple_handler).handle_conn)
    # to start the server asynchronously, use its start() method;
    # we use blocking serve_forever() here because we have no other jobs
    print('Starting echo server on port 7777')
    server.serve_forever()


# Example:
# def pong(cmd, proto_handler):
#    proto_handler.send('+PONG\r\n')

# server = StreamServer(('0.0.0.0', 7777), RedisProtocolServer(pong).handle_conn)
# server.serve_forever()

