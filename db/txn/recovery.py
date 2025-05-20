# <!-- filename: db/txn/recovery.py -->
"""
Redo-only recovery:  scan WAL twice.
  pass-1: collect committed txn ids
  pass-2: replay INSERT / DELETE for committed txns
"""
import json, pathlib, os
from .wal        import WAL
from db.storage  import append_row, delete_pk
from db.catalog  import load

def _actions():
    with open(WAL) as f:
        for line in f:
            yield json.loads(line)

def recover():
    if not pathlib.Path(WAL).exists():
        return

    committed = set()
    # -------- pass 1: find committed txn ids -----------------
    for rec in _actions():
        if rec["type"] == "COMMIT":
            committed.add(rec["id"])

    if not committed:
        return

    schema = load()
    # -------- pass 2: redo actions ---------------------------
    for rec in _actions():
        if rec["type"] != "ACTION":          # skip BEGIN/COMMIT
            continue
        if rec["id"] not in committed:       # uncommitted → ignore
            continue
        op  = rec["data"]["op"]
        tbl = rec["data"]["table"]
        if op == "INSERT":
            append_row(tbl, rec["data"]["row"])
        elif op == "DELETE":
            pk = schema["tables"][tbl]["primary_key"]
            delete_pk(tbl, pk, rec["data"]["value"])
    print("[green]Recovery complete[/green]")
