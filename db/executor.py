# <!-- filename: db/executor.py -->
from typing import Any, Dict, List
from db.plan.optimizer import choose_best
from db.plan.physical  import SeqScan, PKScan, Filter
from db.storage import append_row, delete_pk, scan, create_table, drop_table
from db.catalog import load
from db.txn.transaction import Transaction

def _run(plan) -> List[Dict[str,Any]]:
    if isinstance(plan, SeqScan):
        return scan(plan.table)
    if isinstance(plan, PKScan):
        pk = load()["tables"][plan.table]["primary_key"]
        return [r for r in scan(plan.table) if r[pk]==plan.value]
    if isinstance(plan, Filter):
        col,op,val=plan.predicate
        rows=_run(plan.child)
        return [r for r in rows if r[col]==val]
    raise NotImplementedError

_current_txn:Transaction|None = None

def execute(ast:Dict)->List[Dict]|None:
    global _current_txn
    t=ast["type"]

    if t=="BEGIN":
        _current_txn=Transaction(fast=False); return

    if t=="COMMIT":
        if _current_txn: _current_txn.commit(); _current_txn=None
        return

    if t=="CREATE":
        create_table(ast["table"], ast["columns"], ast["pk"]); return

    if t=="DROP":
        drop_table(ast["table"]); return

    if t == "INSERT":
        schema = load()
        cols   = schema["tables"][ast["table"]]["columns"]

        if len(cols) != len(ast["values"]):
            raise ValueError("column / value count mismatch")

        row = dict(zip(cols, ast["values"]))

        if _current_txn:
            _current_txn.action("INSERT",
                {"op": "INSERT", "table": ast["table"], "row": row})

        append_row(ast["table"], row)
        return

    if t=="DELETE":
        pk=load()["tables"][ast["table"]]["primary_key"]
        if _current_txn: _current_txn.action("DELETE", {pk:ast["value"]})
        delete_pk(ast["table"], pk, ast["value"]); return
        # if _current_txn:
        #     _current_txn.action("DELETE",
        #         {"table": ast["table"], "value": ast["value"]})

    if t=="SELECT":
        plan=choose_best(ast)
        return _run(plan)

    raise NotImplementedError(f"unknown op {t}")
