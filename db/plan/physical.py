# <!-- filename: db/plan/physical.py -->
from dataclasses import dataclass
@dataclass
class SeqScan:  table:str
@dataclass
class PKScan:   table:str; value:object
@dataclass
class Filter:   predicate:tuple; child:object
