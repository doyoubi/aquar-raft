import logging
import json
import time

import crc16
import gevent
import redis

import config
from state import (FollowerState, RequestVote, AppendEntries,
    RequestVoteResponse, AppendEntriesResponse, ProposeRequest,
    ProposeResponse, LogEntry, QueryRequest, QueryResponse)


logger = logging.getLogger(__name__)


CMD_PREFIX = 'AQUAR RAFT'
REQUESTVOTE = 'REQUESTVOTE'
APPENDENTRY = 'APPENDENTRY'
RESPONSEVOTE = 'RESPONSEVOTE'
RESPONSEAPPEND = 'RESPONSEAPPEND'
INFO = 'INFO'

''' Command
Inner commands:
REQUESTVOTE term candidate_id last_log_index last_log_term
APPENDENTRY term leader_id prev_log_index prev_log_term leader_commit read_index [entries...]
RESPONSEVOTE src_node_id term vote_granted
RESPONSEAPPEND src_node_id term success last_recv_index read_index

Outer commands:
AQUAR RAFT INFO
DUMP
SET <key> <value>
GET <key>
'''


class InvalidCmd(Exception):
    def __init__(self, cmd):
        self.cmd = cmd

    def __str__(self):
        return 'InvalidCmd<{}>'.format(' '.join(self.cmd))


class ServerError(Exception):
    def __init__(self, msg):
        self.msg = msg


def encode_request_vote(request_vote):
    return '{} {} {} {} {} {}'.format(
        CMD_PREFIX,
        REQUESTVOTE,
        request_vote.term,
        request_vote.candidate_id,
        request_vote.last_log_index,
        request_vote.last_log_term,
    )


def encode_append_entries(append_entries):
    return '{} {} {} {} {} {} {} {} {}'.format(
        CMD_PREFIX,
        APPENDENTRY,
        append_entries.term,
        append_entries.leader_id,
        append_entries.prev_log_index,
        append_entries.prev_log_term,
        append_entries.leader_commit,
        append_entries.read_index,
        json.dumps(append_entries.entries).replace(' ', ''),
    )


def encode_request_vote_response(request_vote_response):
    return '{} {} {} {} {}'.format(
        CMD_PREFIX,
        RESPONSEVOTE,
        request_vote_response.node_id,
        request_vote_response.term,
        1 if request_vote_response.vote_granted else 0
    )


def encode_append_entries_response(append_entries_response):
    return '{} {} {} {} {} {} {}'.format(
        CMD_PREFIX,
        RESPONSEAPPEND,
        append_entries_response.node_id,
        append_entries_response.term,
        1 if append_entries_response.success else 0,
        append_entries_response.last_recv_index,
        append_entries_response.read_index,
    )


def decode_request_vote(redis_response):
    term = int(redis_response[0])
    candidate_id = redis_response[1]
    last_log_index = int(redis_response[2])
    last_log_term = int(redis_response[3])
    return RequestVote(term, candidate_id, last_log_index, last_log_term)


def decode_append_entries(redis_response):
    term = int(redis_response[0])
    leader_id = redis_response[1]
    prev_log_index = int(redis_response[2])
    prev_log_term = int(redis_response[3])
    leader_commit = int(redis_response[4])
    read_index = int(redis_response[5])
    entries = [LogEntry(**e) for e in json.loads(redis_response[6])]
    return AppendEntries(term, leader_id,
        prev_log_index, prev_log_term, leader_commit, read_index, entries)


def decode_request_vote_response(redis_response):
    node_id = redis_response[0]
    term = int(redis_response[1])
    vote_granted = int(redis_response[2]) != 0
    return RequestVoteResponse(node_id, term, vote_granted)


def decode_append_entries_response(redis_response):
    node_id = redis_response[0]
    term = int(redis_response[1])
    success = int(redis_response[2]) != 0
    if redis_response[3] != 'None':
        last_recv_index = int(redis_response[3])
    else:
        last_recv_index = None
    read_index = int(redis_response[4])
    return AppendEntriesResponse(node_id, term, success, last_recv_index,
                                 read_index)


class RaftServer(object):
    def load_shard(self):
        return {
            'term': 0,
            'nodes': config.NODE_TABLE,
        }

    def __init__(self, node_id):
        shard = self.load_shard()
        assert node_id in shard['nodes']
        self.node_id = node_id
        self.node_table = shard['nodes']
        self.state = FollowerState(self, shard['term'])
        self.client_map = {}
        self.client_time_map = {}

    def handle_cmd(self, address, cmd, proto_handler):
        logger.info('recv from {}: {}'.format(address, repr(' '.join(cmd))))
        try:
            self.dispatch_cmd(address, cmd, proto_handler)
        except InvalidCmd as e:
            logger.error(e)
            proto_handler.send_err('Invalid command')

    def dispatch_cmd(self, address, cmd, proto_handler):
        if cmd[0].lower() == 'aquar' and cmd[1].lower() == 'raft':
            self.handle_aquar_raft(cmd[2:], proto_handler)
        elif len(cmd) == 3 and cmd[0].lower() == 'set':
            self.add_unfinished_client(address, proto_handler)
            self.handle_set_request(cmd, address, proto_handler)
        elif len(cmd) == 2 and cmd[0].lower() == 'get':
            self.add_unfinished_client(address, proto_handler)
            self.handle_get_request(cmd, address, proto_handler)
        elif cmd[0].lower() == 'dump':
            self.handle_dump_request(proto_handler)
        elif len(cmd) == 2 and \
                cmd[0].lower() == 'cluster' and cmd[1].lower() == 'nodes':
            self.handle_cluster_nodes(proto_handler)
        else:
            raise InvalidCmd(cmd)

    def add_unfinished_client(self, client_id, proto_handler):
        proto_handler.response_sent = False
        self.client_map[client_id] = proto_handler
        self.client_time_map[client_id] = time.time()

    def remove_unfinished_client(self, client_id):
        proto_handler = self.client_map.pop(client_id)
        proto_handler.response_sent = True
        start = self.client_time_map.pop(client_id)
        logger.info('finish request using {}'.format(time.time() - start))

    def handle_aquar_raft(self, cmd, proto_handler):
        subcmd = cmd[0].upper()
        if subcmd == REQUESTVOTE:
            self.state.append_recv_queue(
                decode_request_vote(cmd[1:])
                )
        elif subcmd == APPENDENTRY:
            self.state.append_recv_queue(
                decode_append_entries(cmd[1:])
                )
        elif subcmd == RESPONSEVOTE:
            self.state.append_recv_queue(
                decode_request_vote_response(cmd[1:])
                )
        elif subcmd == RESPONSEAPPEND:
            self.state.append_recv_queue(
                decode_append_entries_response(cmd[1:])
                )
        elif subcmd == INFO:
            proto_handler.send_data(self.info_cmd())
            return
        else:
            raise InvalidCmd(cmd)
        proto_handler.send_ok()

    def handle_dump_request(self, proto_handler):
        data = self.state.kvstorage
        lines = ''
        for k, v in data.iteritems():
            lines += '{}:{}\n'.format(k, v)
        proto_handler.send_data(lines)

    def gen_slot(self, key):
        return crc16.crc16xmodem(key) % 16384

    def handle_set_request(self, cmd, client_id, proto_handler):
        if len(cmd) != 3:
            raise InvalidCmd(cmd)
        rpc = ProposeRequest(client_id, cmd)
        self.state.append_recv_queue(rpc)

    def handle_set_response(self, proto_handler, propose_response):
        if propose_response.error:
            proto_handler.send_err(propose_response.error)
        elif propose_response.redirect_node_id:
            node = self.node_table[propose_response.redirect_node_id]
            addr = '{host}:{port}'.format(**node)
            slot = self.gen_slot(propose_response.item[1])
            proto_handler.send_err('MOVED {} {}'.format(slot, addr))
        else:
            proto_handler.send_ok()

    def handle_get_request(self, cmd, client_id, proto_handler):
        if len(cmd) != 2:
            raise InvalidCmd(cmd)
        rpc = QueryRequest(client_id, cmd)
        self.state.append_recv_queue(rpc)

    def handle_get_response(self, proto_handler, query_response):
        if query_response.error:
            proto_handler.send_err(query_response.error)
        elif query_response.redirect_node_id:
            node = self.node_table[query_response.redirect_node_id]
            addr = '{host}:{port}'.format(**node)
            slot = self.gen_slot(query_response.item[1])
            proto_handler.send_err('MOVED {} {}'.format(slot, addr))
        elif query_response.not_available:
            proto_handler.send_err('LEADER NOT READY')
        elif query_response.result is None:
            proto_handler.send_null_bulk_str()
        else:
            proto_handler.send_data(query_response.result)

    def loop(self):
        while True:
            self.cron()
            gevent.sleep(float(config.RAFT_TICK_INTERVAL) / 1000)

    def cron(self):
        while True:
            client_id, rpc = self.state.pop_client_queue()
            if rpc is None:
                break
            proto_handler = self.client_map.get(client_id)
            if proto_handler is None:
                break  # connection closed
            if isinstance(rpc, ProposeResponse):
                self.handle_set_response(proto_handler, rpc)
            elif isinstance(rpc, QueryResponse):
                self.handle_get_response(proto_handler, rpc)
            else:
                raise ServerError('unexpected response: {}'.format(rpc))
            self.remove_unfinished_client(client_id)

        self.state.tick()
        while True:
            node_id, rpc = self.state.pop_send_queue()
            if rpc is None:
                break
            self.send_rpc(node_id, rpc)

    def info_cmd(self):
        info = {
            'node_id': self.state.node_id,
            'leader_id': self.state.leader_id,
            'term': self.state.current_term,
        }
        packet = '\r\n'.join(
            '{}:{}'.format(f, v) for f, v in info.iteritems()
            )
        return packet

    def handle_cluster_nodes(self, proto_handler):
        line_fmt = '{name} {addr} {myself}{role} {slaveof} {ping_sent} ' \
                   '{pong_sent} {config_epoch} {connected}{slots}'
        basic_dict = {
            'ping_sent': 0,
            'pong_sent': 0,
            'connected': 'connected',
        }
        lines = []
        for node_id, addr in self.node_table.items():
            name = '{:0>40}'.format(node_id)
            addr = '{}:{}'.format(addr["host"], addr["port"])
            myself = 'myself,' if node_id == self.node_id else ''
            if node_id == self.state.leader_id:
                slaveof = '-'
                role = 'master'
                slots = ' 0-16383'
            else:
                leader_id = self.state.leader_id
                leader_id = leader_id if leader_id else 'no_leader'
                slaveof = '{:0>40}'.format(leader_id)
                role = 'slave'
                slots = ''
            config_epoch = self.state.current_term
            lines.append(line_fmt.format(
                name=name,
                addr=addr,
                myself=myself,
                role=role,
                slaveof=slaveof,
                config_epoch=config_epoch,
                slots=slots,
                **basic_dict
                ))
        output = '\n'.join(lines) + '\n'
        proto_handler.send_data(output)

    def send_rpc(self, node_id, rpc):
        if isinstance(rpc, RequestVote):
            resp = self.send_redis(node_id, encode_request_vote(rpc))
        elif isinstance(rpc, AppendEntries):
            resp = self.send_redis(node_id, encode_append_entries(rpc))
        elif isinstance(rpc, RequestVoteResponse):
            resp = self.send_redis(node_id, encode_request_vote_response(rpc))
        elif isinstance(rpc, AppendEntriesResponse):
            resp = self.send_redis(node_id, encode_append_entries_response(rpc))
        else:
            raise Exception('Invalid rpc {}'.format(rpc))
        if resp is None or 'OK' not in resp:
            logger.error(resp)

    def send_redis(self, node_id, command):
        peer = self.node_table[node_id]
        r = redis.StrictRedis(host=peer['host'],
                              port=peer['port'])
        try:
            return r.execute_command(command)
        except redis.RedisError as e:
            logger.error(e)

    def gen_node_id(self):
        return self.node_id
