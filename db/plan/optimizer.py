# <!-- filename: db/plan/optimizer.py -->
from .logical  import LogicalScan, LogicalFilter
from .physical import SeqScan, PKScan, Filter
from db.catalog import load

def _logical(ast):
    root = LogicalScan(ast["table"])
    return LogicalFilter(ast["where"], root) if ast["where"] else root

def _to_physical(lg):
    if isinstance(lg, LogicalFilter):
        col,op,val = lg.predicate
        tbl = lg.child.table
        pk  = load()["tables"][tbl]["primary_key"]
        if col==pk and op=="=":   # two alt plans
            return [Filter(lg.predicate, SeqScan(tbl)),
                    PKScan(tbl,val)]
        return [Filter(lg.predicate, SeqScan(tbl))]
    if isinstance(lg, LogicalScan):
        return [SeqScan(lg.table)]
    raise NotImplementedError

def _cost(pl):
    from .physical import SeqScan, PKScan, Filter
    if isinstance(pl, PKScan):  return 1
    if isinstance(pl, SeqScan): return 1000
    if isinstance(pl, Filter):  return _cost(pl.child)+0.1
    raise ValueError

def choose_best(ast):
    best=None; bestcost=float("inf")
    for p in _to_physical(_logical(ast)):
        c=_cost(p);  best,bestcost=(p,c) if c<bestcost else (best,bestcost)
    return best
