from __future__ import absolute_import

from .statue import State


class InvalidCmd(Exception):
    def __init__(self, cmd):
        self.cmd = cmd

    def __str__(self):
        return 'InvalidCmd<{}>'.format(' '.join(cmd))


def command_handler(cmd, proto_handler):
    if cmd[0].lower() == 'aquar' and cmd[0].lower() == 'raft':
        aquar_raft_handler(cmd, proto_handler)
    else:
        raise InvalidCmd(cmd)


def aquar_raft_handler(cmd, proto_handler):
    pass
