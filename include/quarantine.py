import re
import pandas as pd
from pathlib import Path

FILES = ["orders", "payments", "customers", "products", "stores"]


def quarantine(run_id, base, who):
    safe = re.sub(r"[^\w\-]", "_", run_id)
    work = Path(base) / "include" / "work" / safe
    quar = Path(base) / "include" / "quarantine" / who
    quar.mkdir(parents=True, exist_ok=True)

    summary = {}
    for n in FILES:
        df = pd.read_csv(work / f"{n}_failed.csv", dtype=str, keep_default_na=False)
        df.to_csv(quar / f"{n}_failed_{safe}.csv", index=False)
        reasons = df["failure_reason"].str.split(";").explode().str.strip()
        reasons = reasons[reasons != ""]
        summary[n] = {"rows": int(len(df)), "reasons": reasons.value_counts().to_dict()}
        print(f"{n}: {len(df)} rows quarantined  {summary[n]['reasons']}")
    return summary