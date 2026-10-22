import pandas as pd
from pathlib import Path

D = Path("include/data/nammamart")
df = pd.read_csv("issues_log_draft.csv", dtype=str, keep_default_na=False)

# the one rule the draft did not cover: non-COD payments need a gateway reference
pay = pd.read_csv(D / "payments.csv", dtype=str, keep_default_na=False)
print("payments columns:", pay.columns.tolist())
gcol = next((c for c in pay.columns if "gateway" in c.lower()), None)
if gcol:
    bad = pay[(pay["payment_mode"].str.strip().str.lower() != "cod") & (pay[gcol].str.strip() == "")]
    bad = bad[~bad["payment_mode"].str.strip().str.lower().isin(["cash on delivery"])]
    print("non-COD payments with no gateway reference:", len(bad))
    extra = pd.DataFrame({"file": "payments.csv", "row_key": bad["payment_id"], "column": gcol,
                          "problem": "non-COD payment has no gateway reference",
                          "quality_dimension": "Completeness",
                          "proposed_rule": "gateway_ref required unless payment_mode is Cash on Delivery",
                          "severity": "Warning"})
    df = pd.concat([df, extra], ignore_index=True)

df.to_csv("issues_log.csv", index=False)

s = (df.groupby(["file", "column", "problem", "quality_dimension", "proposed_rule", "severity"])
       .agg(rows=("row_key", "size"), example=("row_key", "first")).reset_index())
lines = ["| File | Column | Problem | Dimension | Proposed rule | Severity | Rows | Example key |",
         "|---|---|---|---|---|---|---|---|"]
for r in s.itertuples(index=False):
    lines.append(f"| {r.file} | {r.column} | {r.problem} | {r.quality_dimension} | {r.proposed_rule} | {r.severity} | {r.rows} | {r.example} |")
Path("issues_log_summary.md").write_text("\n".join(lines), encoding="utf-8")
print(len(df), "issue rows;", len(s), "problem types")
print(s.groupby("quality_dimension").size())