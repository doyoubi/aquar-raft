NODE_TABLE = {
    'n0': {'host': 'localhost', 'port': 9000 },
    'n1': {'host': 'localhost', 'port': 9001 },
    'n2': {'host': 'localhost', 'port': 9002 },
}
ELECTION_TIMEOUT_RANGE = [1500, 3000]
BROADCAST_REQUEST_VOTE_INTERVAL = 200
HEART_BEAT_INTERVAL = 300
RAFT_TICK_INTERVAL = 100  # in milliseconds
