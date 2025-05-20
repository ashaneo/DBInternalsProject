"""
Redo-only crash recovery with *eventual durability* semantics.

Algorithm
---------
1. Read entire WAL into memory (small demo scale).
2. Locate the *last* FLUSH record – its index is the durability barrier.
3. Collect `txn_id` for every COMMIT **at or before** the barrier.
4. Replay every ACTION whose `id` ∈ committed_txns (idempotent).
   • INSERT  -> storage.append_row()
   • DELETE  -> storage.delete_pk()

Fast transactions committed *after* the last FLUSH are ignored
(i.e., treated as lost) exactly as the ED paper requires.
"""
from __future__ import annotations
import json, pathlib
from typing import List, Dict
from .wal import WAL
from db.storage import append_row, delete_pk
from db.catalog import load

def _read_wal() -> List[Dict]:
    with open(WAL, encoding="utf-8") as f:
        return [json.loads(line) for line in f]

def recover() -> None:
    if not pathlib.Path(WAL).exists():
        return                       # clean boot – nothing to do

    records = _read_wal()
    # 1. find durability barrier
    barrier_idx = max(
        (i for i, rec in enumerate(records) if rec["type"] == "FLUSH"),
        default=-1
    )

    # 2. collect committed txn ids up to barrier
    committed: set[int] = set()
    for rec in records[: barrier_idx + 1 if barrier_idx >= 0 else len(records)]:
        if rec["type"] == "COMMIT":
            committed.add(rec["id"])

    if not committed:
        print("[yellow]Recovery: nothing to redo[/yellow]")
        return

    schema = load()
    # 3. redo actions for committed txns
    for rec in records:
        if rec["type"] != "ACTION" or rec["id"] not in committed:
            continue

        data = rec["data"]
        tbl  = data["table"]

        if data["op"] == "INSERT":
            append_row(tbl, data["row"])

        elif data["op"] == "DELETE":
            pk = schema["tables"][tbl]["primary_key"]
            delete_pk(tbl, pk, data["value"])

    print("[green]Recovery complete – applied committed transactions[/green]")
