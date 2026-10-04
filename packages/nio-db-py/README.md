# nio-db-py

Ultra-lightweight Python client SDK for **NioDB** — the Agentic Database.

- **Zero dependencies** (uses Python standard library `urllib`)
- **Fast and typed**
- **Compatible with Python >= 3.8**

---

## Installation

```bash
pip install nio-db-py
```

---

## Quickstart

```python
from niodb import NioDB

db = NioDB(url="http://127.0.0.1:7432", token="client-token")

# 1. Insert & Query Documents
db.collection("tasks").insert({
    "title": "Fix auth race condition",
    "status": "pending",
    "ttl": 3600  # 1 hour auto-expiry
})

records = db.collection("tasks").find(limit=10)

# 2. Pure-Rust Vector Similarity Search
results = db.collection("knowledge").search_vector(
    vector=[0.012, -0.045, 0.089, ...],
    top_k=5,
    min_score=0.75
)

# 3. NioBridge Multi-Agent Sessions
session = db.session("session_104")
session.append_turn(
    agent="claude",
    model="claude-3-7-sonnet",
    summary="Refactored database pool",
    cost_usd=0.012
)

manifest = session.get_manifest()
print(manifest["manifest_text"])

# 4. Safe SQL Queries
sql_data = db.query("SELECT * FROM tasks WHERE status = 'pending'")
```
