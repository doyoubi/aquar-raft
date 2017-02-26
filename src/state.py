from __future__ import absolute_import
import time
import random
import logging

from .config import (
    ELECTION_TIMEOUT_RANGE,
    HEART_BEAT_INTERVAL,
    BROADCAST_REQUEST_VOTE_INTERVAL,
)


logger = logging.getLogger(__name__)


class Rpc(object):
    def __init__(self, term):
        self.node_id = None
        self.term = term


class RequestVote(Rpc):
    def __init__(self, term, candidate_id):
        super(RequestVote, self).__init__(term)
        self.candidate_id = candidate_id
        self.node_id = candidate_id


class AppendEntries(Rpc):
    def __init__(self, term, leader_id):
        super(AppendEntries, self).__init__(term)
        self.leader_id = leader_id
        self.node_id = leader_id


class RequestVoteResponse(Rpc):
    def __init__(self, node_id, term, vote_granted):
        super(RequestVoteResponse, self).__init__(term)
        self.vote_granted = vote_granted
        self.node_id = node_id

    def to_dict(self):
        return {
            'term': self.term,
            'vote_granted': self.vote_granted,
        }


class AppendEntriesResponse(Rpc):
    def __init__(self, node_id, term, success):
        super(AppendEntriesResponse, self).__init__(term)
        self.success = success
        self.node_id = node_id

    def to_dict(self):
        return {
            'term': self.term,
            'success': self.success,
        }


class Timer(object):
    def __init__(self, callback, timeout):
        self.timeout = timeout
        self.callback = callback
        self.reset()

    def check_timeout(self):
        d = (time.time() - self.start_time) * 1000
        if d > self.timeout:
            self.callback()

    def reset(self):
        self.start_time = time.time()


class ElectionTimer(Timer):
    def __init__(self, callback):
        super(ElectionTimer, self).__init__(callback, None)

    def reset(self):
        self.timeout = random.randint(*ELECTION_TIMEOUT_RANGE)
        super(ElectionTimer, self).reset()


class State(object):
    def __init__(self, handler, term):
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
    def tick(self):
        while len(self.recv_queue):
            rpc = self.recv_queue.pop(0)
            response = self.handle_rpc(rpc)
            self.send_queue.append((rpc.node_id, response))
        self.cron()

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
        self.handler.state = new_state_class(self.handler, self.current_term)
        self.info('changed {}({}) to {}({})'.format(
            current_state, self.current_term,
            new_state, self.handler.state.current_term))
        return self.handler.state

    def request_vote_handler(self, rpc):
        raise NotImplementedError

    def append_entries_handler(self, rpc):
        raise NotImplementedError

    def request_vote_response_handler(self, rpc):
        self.error('rpc not handled: {}'.format(rpc.to_dict()))

    def append_entries_response_handler(self, rpc):
        self.error('rpc not handled: {}'.format(rpc.to_dict()))

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


class LeaderState(State):
    def __init__(self, server, term):
        super(LeaderState, self).__init__(server, term)
        self.leader_id = self.node_id
        self.timers = [
            Timer(self.broadcast_heartbeat, HEART_BEAT_INTERVAL)]
        self.broadcast_heartbeat()

    def broadcast_heartbeat(self):
        heartbeat = AppendEntries(self.current_term, self.node_id)
        for node_id, node in self.node_table.iteritems():
            if node_id == self.node_id:
                continue
            self.debug('sending heartbeat to {}'.format(node_id))
            # self.server.send_rpc(node, heartbeat)
            self.send_queue.append((node_id, heartbeat))
        self.timers[0].reset()

    def append_entries_handler(self, rpc):
        if rpc.term > self.current_term:
            self.info(
                'heartbeat from another leader with higer term {} < {}' \
                    .format(self.current_term, rpc.term))
            self.update_term_if_needed(rpc)
            new_state = self.change_to_follower()
            new_state.leader_id = rpc.leader_id
            return AppendEntriesResponse(self.node_id, self.current_term, True)
        elif rpc.term < self.current_term:
            self.info(
                'heartbeat from another leader with lower term {} > {}' \
                    .format(self.current_term, rpc.term))
            return AppendEntriesResponse(self.node_id, self.current_term, False)
        else:
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
        else:
            self.debug('heartbeat success')


class FollowerState(State):
    def __init__(self, server, term):
        super(FollowerState, self).__init__(server, term)
        self.timers = [ElectionTimer(self.change_to_candidate)]

    def append_entries_handler(self, rpc):
        success = rpc.term >= self.current_term
        if success:
            if rpc.term > self.current_term:
                self.info('leader chagned from {} to {}'.format(
                    self.leader_id, rpc.leader_id))
            self.leader_id = rpc.leader_id
        else:
            self.info('receive heartbeat from old leader {}'.format(
                rpc.leader_id))
        self.update_term_if_needed(rpc)
        self.timers[0].reset()
        return AppendEntriesResponse(self.node_id, self.current_term, success)

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


class CandidateState(State):
    def __init__(self, server, term):
        super(CandidateState, self).__init__(server, term)
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
            # self.server.send_rpc(node, request_vote)
            self.send_queue.append((node_id, request_vote))
        self.timers[1].reset()

    def append_entries_handler(self, rpc):
        if rpc.term >= self.current_term:
            self.info('receive heartbeat from {}'.format(rpc.leader_id))
            self.update_term_if_needed(rpc)
            new_state = self.change_to_follower()
            new_state.leader_id = rpc.leader_id
            return AppendEntriesResponse(self.node_id, self.current_term, True)
        self.info('reject heartbeat from {}'.format(rpc.leader_id))
        return AppendEntriesResponse(self.node_id, self.current_term, False)

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
