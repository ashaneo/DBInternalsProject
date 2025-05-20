# <!-- filename: db/txn/transaction.py -->
import itertools
from .wal import log_begin, log_action, log_commit, fsync

_counter = itertools.count(1)

class Transaction:
    def __init__(self, fast=False):
        self.id = next(_counter)
        self.fast = fast
        log_begin(self.id)

    # def action(self, act:str, data:dict):
    #     log_action(self.id, act, data)
    # in txn/transaction.py
    def action(self, op:str, payload:dict):
        from .wal import log_action
        log_action(self.id, "ACTION", {"op": op, **payload})


    def commit(self):
        log_commit(self.id)
        if not self.fast: fsync()  # safe commit
