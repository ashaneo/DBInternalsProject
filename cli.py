# <!-- filename: cli.py -->
from rich.table import Table
from rich import print
from db.parser   import parse
from db.executor import execute
from db.txn.recovery import recover
from db.txn.flush_worker import start as start_flush

start_flush(interval=0.5)
recover()

def show(rows):
    if not rows: print("(no rows)"); return
    t=Table(show_header=True)
    for h in rows[0]: t.add_column(h)
    for r in rows: t.add_row(*[str(v) for v in r.values()])
    print(t)

def repl():
    print("[green]smallSQL w/ tiny optimizer & WAL  (\\quit to exit)[/green]")
    buf=[]
    while True:
        line=input("sql> ")
        if line.strip().lower() in ("\\quit","quit","exit"): break
        buf.append(line)
        if ";" in line:
            sql="\n".join(buf); buf=[]
            ast=parse(sql)
            if not ast: print("[red]syntax error[/red]"); continue
            try:
                    res = execute(ast)
                    if isinstance(res, list):
                        show(res)
            except ValueError as exc:
                    print(f"[yellow]Error:[/yellow] {exc}")

if __name__=="__main__":
    repl()
