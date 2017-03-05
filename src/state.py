from __future__ import absolute_import
import time
import random
import logging

from .config import (
    ELECTION_TIMEOUT_RANGE,
    IDLE_HEART_BEAT_INTERVAL,
    PROPOSE_HEART_BEAT_INTERVAL,
    BROADCAST_REQUEST_VOTE_INTERVAL,
)


logger = logging.getLogger(__name__)


class Rpc(object):
    pass


class InnerRpc(Rpc):
    def __init__(self, term):
        self.node_id = None
        self.term = term


class RequestVote(InnerRpc):
    def __init__(self, term, candidate_id):
        super(RequestVote, self).__init__(term)
        self.candidate_id = candidate_id
        self.node_id = candidate_id


class AppendEntries(InnerRpc):
    def __init__(self, term, leader_id,
            prev_log_index, prev_log_term, leader_commit, entries):
        super(AppendEntries, self).__init__(term)
        self.leader_id = leader_id
        self.node_id = leader_id
        # proposol
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.leader_commit = leader_commit
        self.entries = entries


class RequestVoteResponse(InnerRpc):
    def __init__(self, node_id, term, vote_granted):
        super(RequestVoteResponse, self).__init__(term)
        self.vote_granted = vote_granted
        self.node_id = node_id

    def to_dict(self):
        return {
            'term': self.term,
            'vote_granted': self.vote_granted,
        }


class AppendEntriesResponse(InnerRpc):
    def __init__(self, node_id, term, success, last_recv_index):
        super(AppendEntriesResponse, self).__init__(term)
        self.success = success
        self.node_id = node_id
        # last_recv_index will be None if not success
        self.last_recv_index = last_recv_index

    def to_dict(self):
        return {
            'term': self.term,
            'success': self.success,
        }


class ProposeRequest(Rpc):
    def __init__(self, client_id, item):
        self.client_id = client_id
        self.item = item


class ProposeResponse(Rpc):
    def __init__(self, client_id, redirect_node_id, error):
        self.client_id = client_id
        self.redirect_node_id = redirect_node_id
        self.error = error

    @classmethod
    def gen_success_resp(cls, client_id):
        # leader
        return ProposeResponse(client_id, None, None)

    @classmethod
    def gen_redirect_resp(cls, client_id, redirect_node_id):
        # follower
        return ProposeResponse(client_id, redirect_node_id, None)

    @classmethod
    def gen_error_resp(cls, client_id, error):
        # candidate
        return ProposeResponse(client_id, None, error)


NO_OP_ITERM = 'no-op-item'


class LogEntry(object):
    def __init__(self, term, log_index, item):
        self.term = term
        self.log_index = log_index
        self.item = item

    def to_dict(self):
        return {
            'term': self.term,
            'log_index': self.log_index,
            'item': self.item,
        }


class Timer(object):
    def __init__(self, callback, timeout, *args):
        self.timeout = timeout
        self.callback = callback
        self.args = args
        self.reset()

    def check_timeout(self):
        d = (time.time() - self.start_time) * 1000
        if d > self.timeout:
            self.start_time += (float(self.timeout) / 1000)
            self.callback(*self.args)

    def reset(self):
        self.start_time = time.time()


class ElectionTimer(Timer):
    def __init__(self, callback):
        super(ElectionTimer, self).__init__(callback, None)

    def reset(self):
        self.timeout = random.randint(*ELECTION_TIMEOUT_RANGE)
        super(ElectionTimer, self).reset()


class VariableTimer(Timer):
    def change_timeout(self, timeout):
        self.timeout = timeout
        self.check_timeout()


class State(object):
    def __init__(self, handler, term, logs=None, kvstorage=None):
        # self.handler.state will be changed by this object
        self.handler = handler

        self.node_id = handler.gen_node_id()
        self.node_table = handler.node_table
        self.leader_id = None
        self.current_term = term
        self.voted_for = None

        self.timers = []
        self.send_queue = []  # (node, Rpc)
        self.recv_queue = []  # Rpc
        self.client_resp_queue = []  # (client_id, Rpc)

        # log replication
        self.logs = logs or [LogEntry(-1, -1, None), LogEntry(0, 0, None)]
        self.commit_index = 0
        self.last_applied = 0
        # storage
        self.kvstorage = kvstorage or {}

        # avoid stale state caused by race condition
        self.stale = False

    def to_dict(self):
        return {
            'node_id': self.node_id,
            'leader_id': self.leader_id,
            'term': self.current_term,
            'voted_for': self.voted_for,
        }

    def get_info(self):
        state_name = type(self).__name__
        return '{}({})'.format(state_name, self.to_dict())

    # call by outer object
    def append_recv_queue(self, rpc):
        self.recv_queue.append(rpc)

    # call by outer object
    def pop_send_queue(self):
        if len(self.send_queue) == 0:
            return None, None
        return self.send_queue.pop(0)

    # call by outer object
    def pop_client_queue(self):
        if len(self.client_resp_queue) == 0:
            return None, None
        return self.client_resp_queue.pop(0)

    # call by outer object
    def tick(self):
        while len(self.recv_queue) and not self.stale:
            rpc = self.recv_queue.pop(0)
            response = self.handle_rpc(rpc)
            if response is not None:
                self.send_queue.append((rpc.node_id, response))
        if self.stale:
            return
        self.state_tick()
        self.cron()

    def state_tick(self):
        pass

    def cron(self):
        for t in self.timers:
            t.check_timeout()

    def handle_rpc(self, rpc):
        if isinstance(rpc, RequestVote):
            return self.request_vote_handler(rpc)
        elif isinstance(rpc, AppendEntries):
            return self.append_entries_handler(rpc)
        elif isinstance(rpc, RequestVoteResponse):
            return self.request_vote_response_handler(rpc)
        elif isinstance(rpc, AppendEntriesResponse):
            return self.append_entries_response_handler(rpc)
        elif isinstance(rpc, ProposeRequest):
            return self.propose_request_handler(rpc)
        else:
            raise Exception('Invalid rpc')

    def update_term_if_needed(self, rpc):
        if rpc.term > self.current_term:
            self.info('term updated from {} to {}'.format(
                self.current_term, rpc.term))
            self.current_term = rpc.term

    def change_state(self, new_state_class):
        current_state = type(self).__name__
        new_state = new_state_class.__name__
        self.handler.state = new_state_class(self.handler, self.current_term,
                                             self.logs, self.kvstorage)
        self.info('changed {}({}) to {}({})'.format(
            current_state, self.current_term,
            new_state, self.handler.state.current_term))
        self.stale = True
        return self.handler.state

    def request_vote_handler(self, rpc):
        raise NotImplementedError

    def append_entries_handler(self, rpc):
        raise NotImplementedError

    def request_vote_response_handler(self, rpc):
        self.error('rpc not handled: {}'.format(rpc.to_dict()))

    def append_entries_response_handler(self, rpc):
        self.error('rpc not handled: {}'.format(rpc.to_dict()))

    def propose_request_handler(self, propose):
        raise NotImplementedError

    def change_to_candidate(self):
        return self.change_state(CandidateState)

    def change_to_follower(self):
        return self.change_state(FollowerState)

    def info(self, msg):
        self.log('info', msg)

    def debug(self, msg):
        self.log('debug', msg)

    def error(self, msg):
        self.log('error', msg)

    def log(self, level, msg):
        state = type(self).__name__
        node_name = self.node_id
        log_func = getattr(logger, level)
        log_func('[{}:{}({})] {}'.format(
            node_name, state, self.current_term, msg))

    def recv_entries(self, append_entries):
        prev_log_index = append_entries.prev_log_index
        prev_log_term = append_entries.prev_log_term
        leader_commit = append_entries.leader_commit
        entries = append_entries.entries

        i = self.get_index_by_log_index(prev_log_index)
        if i >= len(self.logs) or self.logs[i].term != prev_log_term:
            return

        if len(entries) == 0:
            self.apply_commit(leader_commit)
            return prev_log_index

        i += 1
        j = 0
        for entry in entries:
            if i >= len(self.logs) or \
                    self.logs[i].term != entry.term:
                break
            assert self.logs[i].log_index == entry.log_index
            prev_log_index += 1
            j += 1

        entries = entries[j:]

        while self.logs[-1].log_index > prev_log_index:
            assert self.logs[-1].log_index > self.commit_index
            assert self.logs[-1].log_index > 0
            self.logs.pop()
        for entry in entries:
            self.logs.append(entry)
        self.apply_commit(leader_commit)
        last_log_index = self.logs[-1].log_index
        return last_log_index

    def apply_commit(self, leader_commit):
        last_log_index = self.logs[-1].log_index
        if leader_commit > self.commit_index:
            last_commit = self.commit_index
            self.commit_index = min(leader_commit, last_log_index)
            for i in range(last_commit + 1, self.commit_index + 1):
                self.apply_state_machine(
                    self.logs[self.get_index_by_log_index(i)])

    def apply_state_machine(self, log):
        cmd = log.item
        assert cmd[0].upper() == 'SET'
        key, value = cmd[1:]
        self.debug('SET {} {}'.format(key, value))
        self.kvstorage[key] = value

    def get_index_by_log_index(self, log_index):
        return log_index - self.logs[0].log_index


class LeaderState(State):
    def __init__(self, handler, term, logs=None, kvstorage=None):
        super(LeaderState, self).__init__(handler, term, logs, kvstorage)
        self.leader_id = self.node_id

        # Log Replication
        self.next_index = {nid: self.logs[-1].log_index + 1 \
            for nid in self.node_table.keys()}
        self.match_index = {nid: 0 for nid in self.node_table.keys()}

        self.client_map = {}  # log_index => client_id

        self.slave_timers = {
            nid: VariableTimer(self.send_heartbeat,
                               IDLE_HEART_BEAT_INTERVAL,
                               nid) \
                for nid in self.node_table.keys() if nid != self.node_id
        }
        self.broadcast_heartbeat()

    def broadcast_heartbeat(self):
        for node_id, node in self.node_table.iteritems():
            if node_id == self.node_id:
                continue
            self.send_heartbeat(node_id)

    def send_heartbeat(self, slave_id):
        start_index = self.next_index[slave_id]
        self.debug('start_index->{}: {}'.format(slave_id, start_index))
        start_index = self.get_index_by_log_index(start_index)
        prev_log_index = self.logs[start_index - 1].log_index
        prev_log_term = self.logs[start_index - 1].term
        entries = [l.to_dict() for l in self.logs[start_index:]]
        heartbeat = AppendEntries(self.current_term, self.node_id,
            prev_log_index, prev_log_term, self.commit_index, entries)
        self.debug('sending heartbeat to {}'.format(slave_id))
        self.send_queue.append((slave_id, heartbeat))

    def append_entries_handler(self, rpc):
        if rpc.term > self.current_term:
            self.info(
                'heartbeat from another leader with higer term {} < {}' \
                    .format(self.current_term, rpc.term))
            self.update_term_if_needed(rpc)
            new_state = self.change_to_follower()
            new_state.leader_id = rpc.leader_id

            last_log_index = self.recv_entries(rpc)
            success = last_log_index is not None
            return AppendEntriesResponse(self.node_id, self.current_term,
                                         success, last_log_index)
        elif rpc.term < self.current_term:
            self.info(
                'heartbeat from another leader with lower term {} > {}' \
                    .format(self.current_term, rpc.term))
            return AppendEntriesResponse(self.node_id, self.current_term, False, None)
        else:
            assert rpc.term == self.current_term
            raise Exception(
                'Invalid state, leader receive heartbeat from same term')

    def request_vote_handler(self, rpc):
        vote_granted = rpc.term > self.current_term
        if vote_granted:
            self.info('vote granted to {}'.format(rpc.candidate_id))
            self.update_term_if_needed(rpc)
            self.change_to_follower()
        else:
            self.info('rejected vote request from {}'.format(
                rpc.candidate_id))
        return RequestVoteResponse(self.node_id, self.current_term, vote_granted)

    def append_entries_response_handler(self, response):
        if response.term > self.current_term:
            self.info('heartbeat: find higer term {} < {}'.format(
                self.current_term, response.term))
            self.update_term_if_needed(response)
            self.change_to_follower()

        node_id = response.node_id
        if response.success:
            self.next_index[node_id] = response.last_recv_index + 1
            self.match_index[node_id] = response.last_recv_index
            last_log_index = self.logs[-1].log_index
            if last_log_index + 1 == self.next_index[node_id]:
                self.slave_timers[node_id].change_timeout(IDLE_HEART_BEAT_INTERVAL)
            self.debug('heartbeat succeeded')
        else:
            self.debug("heartbeat failed")
            self.next_index[node_id] -= 1
            self.send_heartbeat(node_id)
            self.slave_timers[node_id].reset()

    def propose_request_handler(self, propose):
        self.logs.append(LogEntry(self.current_term,
                                  self.logs[-1].log_index + 1,
                                  propose.item))
        last_log_index = self.logs[-1].log_index
        assert last_log_index not in self.client_map
        self.client_map[last_log_index] = propose.client_id

    def state_tick(self):
        last_log_index = self.logs[-1].log_index
        for nid in self.slave_timers.keys():
            assert last_log_index + 1 >= self.next_index[nid]
            if last_log_index + 1 == self.next_index[nid]:
                continue
            assert last_log_index + 1 > self.next_index[nid]
            assert last_log_index >= self.next_index[nid]

            if self.slave_timers[nid].timeout == PROPOSE_HEART_BEAT_INTERVAL:
                continue  # TODO: remove this

            self.debug('sending proposol with heartbeat')
            self.send_heartbeat(nid)
            self.slave_timers[nid].reset()
            self.slave_timers[nid].change_timeout(PROPOSE_HEART_BEAT_INTERVAL)
        self.check_success_proposol()

    def cron(self):
        for timer in self.slave_timers.values():
            timer.check_timeout()

    def check_success_proposol(self):
        last_log_index = self.logs[-1].log_index
        for i in range(self.commit_index + 1, last_log_index + 1):
            count = len(map(lambda mi: mi >= i, self.match_index.values()))
            if count <= len(self.node_table) / 2:
                break
            if i in self.client_map:
                self.respond_proposol(i)
            log = self.logs[self.get_index_by_log_index(i)]
            assert log is not None
            self.apply_state_machine(log)
            self.commit_index = log.log_index

    def respond_proposol(self, log_index):
        client_id = self.client_map.pop(log_index)
        rpc = ProposeResponse.gen_success_resp(client_id)
        self.client_resp_queue.append((client_id, rpc))


class FollowerState(State):
    def __init__(self, handler, term, logs=None, kvstorage=None):
        super(FollowerState, self).__init__(handler, term, logs, kvstorage)
        self.timers = [ElectionTimer(self.change_to_candidate)]

    def append_entries_handler(self, rpc):
        success = rpc.term >= self.current_term
        if success:
            if rpc.term > self.current_term:
                self.info('leader changed from {} to {}'.format(
                    self.leader_id, rpc.leader_id))
            self.leader_id = rpc.leader_id
        else:
            self.info('receive heartbeat from old leader {}'.format(
                rpc.leader_id))
        self.update_term_if_needed(rpc)
        self.timers[0].reset()

        last_log_index = None
        if success:
            last_log_index = self.recv_entries(rpc)
            success = last_log_index is not None
        return AppendEntriesResponse(self.node_id, self.current_term,
                                     success, last_log_index)

    def request_vote_handler(self, rpc):
        self.info('receive request vote from {}'.format(rpc.candidate_id))
        vote_granted = rpc.term > self.current_term or \
            (rpc.term == self.current_term and self.voted_for is None)
        if self.voted_for:
            self.info('already voted for {}'.format(self.voted_for))
        if vote_granted:
            self.info('vote granted to {}'.format(rpc.candidate_id))
            self.update_term_if_needed(rpc)
            self.voted_for = rpc.candidate_id
        else:
            self.info('rejected vote request from {}'.format(
                rpc.candidate_id))
        return RequestVoteResponse(self.node_id, self.current_term, vote_granted)

    def propose_request_handler(self, propose):
        client_id = propose.client_id
        rpc = ProposeResponse.gen_redirect_resp(client_id, self.leader_id)
        self.client_resp_queue.append((client_id, rpc))


class CandidateState(State):
    def __init__(self, handler, term, logs=None, kvstorage=None):
        super(CandidateState, self).__init__(handler, term, logs, kvstorage)
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes = { self.node_id }
        self.timers = [
            ElectionTimer(self.change_to_candidate),
            Timer(self.broadcast_request_vote,
                BROADCAST_REQUEST_VOTE_INTERVAL)
        ]
        self.broadcast_request_vote()

    def broadcast_request_vote(self):
        request_vote = RequestVote(self.current_term, self.node_id)
        for node_id, node in self.node_table.iteritems():
            if node_id == self.node_id:
                continue
            self.send_queue.append((node_id, request_vote))
        self.timers[1].reset()

    def append_entries_handler(self, rpc):
        if rpc.term >= self.current_term:
            self.info('receive heartbeat from {}'.format(rpc.leader_id))
            self.update_term_if_needed(rpc)
            new_state = self.change_to_follower()
            new_state.leader_id = rpc.leader_id
            last_log_index = self.recv_entries(rpc)
            success = last_log_index is not None
            return AppendEntriesResponse(self.node_id, self.current_term,
                                         success, last_log_index)
        self.info('reject heartbeat from {}'.format(rpc.leader_id))
        return AppendEntriesResponse(self.node_id, self.current_term, False, None)

    def request_vote_handler(self, rpc):
        self.info('receive request vote from {}'.format(rpc.candidate_id))
        vote_granted = rpc.term > self.current_term
        if vote_granted:
            self.info('vote granted to {}'.format(rpc.candidate_id))
            self.update_term_if_needed(rpc)
            new_state = self.change_to_follower()
            new_state.voted_for = rpc.candidate_id
        else:
            self.info('rejected vote request from {}'.format(
                rpc.candidate_id))
        return RequestVoteResponse(self.node_id, self.current_term, vote_granted)

    def check_quorum(self):
        if len(self.votes) > len(self.node_table) / 2:
            self.change_state(LeaderState)

    def request_vote_response_handler(self, response):
        if response.vote_granted:
            self.info('get a vote')
            self.votes.add(response.node_id)
            self.check_quorum()
        if response.term > self.current_term:
            self.info('find higher term')
            self.update_term_if_needed(response)
            self.change_to_follower()
            # then wait for leader's heartbeat

    def propose_request_handler(self, propose):
        client_id = propose.client_id
        rpc = ProposeResponse.gen_error_resp(client_id, 'No Leader')
        self.client_resp_queue.append((client_id, rpc))
