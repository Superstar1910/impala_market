import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
required = {"country", "currency", "status", "cutoff_date", "signal", "summary", "metrics", "drivers", "sources"}
errors = []
for path in sorted((ROOT / "data" / "countries").glob("*.json")):
    data = json.loads(path.read_text(encoding="utf-8"))
    missing = required - set(data)
    if missing:
        errors.append(f"{path.name}: missing {sorted(missing)}")
    if data.get("status") == "active" and not data.get("cutoff_date"):
        errors.append(f"{path.name}: active coverage requires cutoff_date")
if errors:
    raise SystemExit("\n".join(errors))
print("Country data validation passed")
