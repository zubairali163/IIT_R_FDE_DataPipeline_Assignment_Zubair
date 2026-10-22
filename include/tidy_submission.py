"""Reorders SUBMISSION.md into assignment order. Reads SUBMISSION.md, writes SUBMISSION.tidy.md. Never edits the original."""
import re, sys
from pathlib import Path

src = Path(sys.argv[1] if len(sys.argv) > 1 else "SUBMISSION.md")
out = src.with_name("SUBMISSION.tidy.md")
lines = src.read_text(encoding="utf-8").splitlines()


def split(lines, prefix):
    """split into (preamble, [blocks]) at lines starting with prefix"""
    pre, blocks, cur = [], [], None
    for l in lines:
        if l.startswith(prefix):
            cur = [l]
            blocks.append(cur)
        elif cur is None:
            pre.append(l)
        else:
            cur.append(l)
    return pre, blocks


title_lines, h2 = split(lines, "## ")
by = {b[0][3:].strip(): b for b in h2}


def find(start):
    hits = [k for k in by if k.startswith(start)]
    if len(hits) != 1:
        sys.exit(f"expected exactly one section starting with {start!r}, found {hits}")
    return by[hits[0]]


part0, bq, partb, partc = find("Part 0"), find("Business questions"), find("Part B"), find("Part C")
partd, run_audit, parte, partf, partg = find("Part D"), find("Part F – Run audit"), find("Part E"), find("Part F – Observability"), find("Part G")
dict_blocks = [find(t) for t in ["etl_fact_orders", "etl_dim_customer", "etl_dim_product", "etl_dim_store"]]

# Part E answers: H3 blocks from Part E itself and from the tail of the last dictionary block
e_pre, e_h3 = split(parte[1:], "### ")
last = dict_blocks[-1]
i = next(k for k, l in enumerate(last) if l.startswith("### "))
tail_pre, tail_h3 = split(last[i:], "### ")
dict_blocks[-1] = last[:i]
answers = {}
for b in e_h3 + tail_h3:
    n = re.match(r"### (\d+)\.", b[0])
    if n:
        answers[int(n.group(1))] = b
missing = [n for n in range(1, 10) if n not in answers and n != 2]
if missing:
    sys.exit(f"Part E answers missing: {missing}")

dictionary = ["### 2. Data dictionary", ""]
for b in dict_blocks:
    dictionary += ["#" + b[0], *b[1:]]
dictionary = [l.replace("### etl_", "#### etl_") if l.startswith("### etl_") else l for l in dictionary]

E = ["## Part E – Data governance", *e_pre, ""]
for n in range(1, 10):
    E += dictionary if n == 2 else answers[n]
    E.append("")

# Part F: run audit goes right after the heading and intro of the experiments
f_head = partf[0]
f_idx = next(k for k, l in enumerate(partf) if l.startswith("### "))
F = [f_head, *partf[1:f_idx], "### Run audit (`pipeline_runs`)", *run_audit[1:], *partf[f_idx:]]

PART_A = """## Part A – ETL pipeline

`dags/nammamart_etl_Zubair.py`: Extract → Validate → Transform → Load, then reconcile, report and audit.

- **Extract:** five tasks read the five source files in parallel. Each copies its file into the run's own folder and logs a SHA-256 hash, so a run keeps a frozen copy of what it processed.
- **Validate:** 33 checks (all six quality dimensions). Masters (customers, products, stores) are validated first; orders are then checked against the masters that passed. Bad rows are quarantined with a failure reason.
- **Transform:** standardise values, mask email and phone, and build the star schema.
- **Load:** `etl_fact_orders` with `etl_dim_customer`, `etl_dim_product` and `etl_dim_store`, in one transaction. The load is idempotent: re-running does not change the row counts.
- **After the load:** the October increment is applied when a file exists for the run date, payments are reconciled against orders, reports are built, and a Slack alert plus a `pipeline_runs` audit row are written."""

PART_A = PART_A.splitlines()
partb_new = ["## Part B – ELT pipeline", *partb[1:]]
parts = [part0, PART_A, partb_new, partc, partd, bq, E, F, partg]
text = "\n".join(title_lines) + "\n\n" + "\n\n".join("\n".join(p).strip("\n") for p in parts) + "\n"
text = re.sub(r"\n{3,}", "\n\n", text)
out.write_text(text, encoding="utf-8")
print("written", out)
for l in text.splitlines():
    if re.match(r"#{1,4} ", l):
        print(l)