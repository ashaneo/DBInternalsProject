# <!-- filename: db/plan/logical.py -->
from dataclasses import dataclass

@dataclass
class LogicalScan:  table:str

@dataclass
class LogicalFilter:
    predicate:tuple   # (col, '=', value)
    child:object
