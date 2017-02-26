import logging

import gevent
import redis

import config
from state import (FollowerState, RequestVote, AppendEntries,
    RequestVoteResponse, AppendEntriesResponse)


logger = logging.getLogger(__name__)


CMD_PREFIX = 'AQUAR RAFT'
REQUESTVOTE = 'REQUESTVOTE'
APPENDENTRY = 'APPENDENTRY'
RESPONSEVOTE = 'RESPONSEVOTE'
RESPONSEAPPEND = 'RESPONSEAPPEND'
INFO = 'INFO'

''' Command
Inner commands:
REQUESTVOTE term candidate_id
APPENDENTRY term leader_id
RESPONSEVOTE src_node_id term vote_granted
RESPONSEAPPEND src_node_id term success

Outer commands:
INFO
'''


class InvalidCmd(Exception):
    def __init__(self, cmd):
        self.cmd = cmd

    def __str__(self):
        return 'InvalidCmd<{}>'.format(' '.join(self.cmd))


def encode_request_vote(request_vote):
    return '{} {} {} {}'.format(
        CMD_PREFIX,
        REQUESTVOTE,
        request_vote.term,
        request_vote.candidate_id
    )


def encode_append_entries(append_entries):
    return '{} {} {} {}'.format(
        CMD_PREFIX,
        APPENDENTRY,
        append_entries.term,
        append_entries.leader_id
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
    return '{} {} {} {} {}'.format(
        CMD_PREFIX,
        RESPONSEAPPEND,
        append_entries_response.node_id,
        append_entries_response.term,
        1 if append_entries_response.success else 0
    )


def decode_request_vote(redis_response):
    term = int(redis_response[0])
    candidate_id = redis_response[1]
    return RequestVote(term, candidate_id)


def decode_append_entries(redis_response):
    term = int(redis_response[0])
    leader_id = redis_response[1]
    return AppendEntries(term, leader_id)


def decode_request_vote_response(redis_response):
    node_id = redis_response[0]
    term = int(redis_response[1])
    vote_granted = int(redis_response[2]) != 0
    return RequestVoteResponse(node_id, term, vote_granted)


def decode_append_entries_response(redis_response):
    node_id = redis_response[0]
    term = int(redis_response[1])
    success = int(redis_response[2]) != 0
    return AppendEntriesResponse(node_id, term, success)


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

    def handle_cmd(self, address, cmd, proto_handler):
        logger.info('recv from {}: {}'.format(address, ' '.join(cmd)))
        try:
            self.dispatch_cmd(cmd, proto_handler)
        except InvalidCmd as e:
            logger.error(e)
            proto_handler.send_err('Invalid command')

    def dispatch_cmd(self, cmd, proto_handler):
        if cmd[0].lower() == 'aquar' and cmd[1].lower() == 'raft':
            self.handle_aquar_raft(cmd[2:], proto_handler)
        else:
            raise InvalidCmd(cmd)

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
        proto_handler.send('+OK\r\n')

    def loop(self):
        while True:
            self.cron()
            gevent.sleep(config.RAFT_TICK_INTERVAL / 1000)

    def cron(self):
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