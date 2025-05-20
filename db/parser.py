# <!-- filename: db/parser.py -->
import re, ast

CREATE = re.compile(r"CREATE\s+TABLE\s+(\w+)\s*\(([^)]+)\)\s+PRIMARY\s+KEY\s+(\w+);?", re.I)
INSERT = re.compile(r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+)\);?", re.I | re.S)
DELETE = re.compile(r"DELETE\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*=\s*(.+);?", re.I)
DROP   = re.compile(r"DROP\s+TABLE\s+(\w+);?", re.I)
SELECT = re.compile(r"SELECT\s+\*\s+FROM\s+(\w+)(?:\s+WHERE\s+(\w+)\s*=\s*(.+))?;?", re.I)
BEGIN  = re.compile(r"BEGIN;?", re.I)
COMMIT = re.compile(r"COMMIT;?", re.I)

def _split_vals(blob:str):
    parts, buf, in_s, in_d = [], [], False, False
    for ch in blob:
        if ch=="'"and not in_d: in_s=not in_s
        elif ch=='"'and not in_s: in_d=not in_d
        if ch==","and not(in_s or in_d):
            parts.append("".join(buf)); buf=[]
        else: buf.append(ch)
    parts.append("".join(buf))
    return [ast.literal_eval(p.strip()) for p in parts]

def parse(sql:str)->dict|None:
    sql=sql.strip()
    if m:=BEGIN.fullmatch(sql):  return {"type":"BEGIN"}
    if m:=COMMIT.fullmatch(sql): return {"type":"COMMIT"}
    if m:=CREATE.fullmatch(sql):
        t,c,pk=m.groups(); return {"type":"CREATE","table":t,
                                   "columns":[x.strip()for x in c.split(",")],"pk":pk}
    if m:=INSERT.fullmatch(sql):
        t,vals=m.groups(); return {"type":"INSERT","table":t,"values":_split_vals(vals)}
    if m:=DELETE.fullmatch(sql):
        t,f,v=m.groups(); return {"type":"DELETE","table":t,"field":f,"value":ast.literal_eval(v)}
    if m:=DROP.fullmatch(sql):   return {"type":"DROP","table":m.group(1)}
    if m:=SELECT.fullmatch(sql):
        t,f,v=m.groups()
        return {"type":"SELECT","table":t,"where":(f,ast.literal_eval(v)) if f else None}
    return None
