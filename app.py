# app.py (FastAPI) — Data-driven insights (no hard-coded business values)
import os, io, csv, json, uuid, time, asyncio, math, statistics
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Generator, Optional, List

from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import sqlite3

# --------------------------
# Config & Tunables (env-driven, no hard-coded business thresholds)
# --------------------------
DB_PATH = os.getenv("BI_DB_PATH", "bi_agent.db")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# Revenue spike: latest %Δ must be positive and statistically larger than prior deltas
REVENUE_SPIKE_Z = float(os.getenv("REVENUE_SPIKE_Z", "1.0"))           # z-score threshold
REVENUE_SPIKE_MIN_PCT = float(os.getenv("REVENUE_SPIKE_MIN_PCT", "0")) # absolute min pct change (e.g. 0.10 => 10%)

# Trend detection (tickets as acquisition proxy)
TREND_MIN_POINTS = int(os.getenv("TREND_MIN_POINTS", "3"))  # minimum points to even attempt a trend
TREND_WINDOW = int(os.getenv("TREND_WINDOW", "3"))          # last W periods to model

# Severity mapping thresholds (based on absolute percent change)
SEVERITY_MED_PCT  = float(os.getenv("SEVERITY_MED_PCT",  "0.15"))  # 15%
SEVERITY_HIGH_PCT = float(os.getenv("SEVERITY_HIGH_PCT", "0.30"))  # 30%

HITL_DEFAULT = os.getenv("HITL_DEFAULT", "false").lower() == "true"

# --------------------------
# App & CORS
# --------------------------
app = FastAPI(title="Autonomous BI Agent API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

executor = ThreadPoolExecutor(max_workers=4)

# We'll capture the main loop on startup to safely publish from worker threads
main_loop: Optional[asyncio.AbstractEventLoop] = None

@app.on_event("startup")
async def on_startup():
    global main_loop
    main_loop = asyncio.get_running_loop()
    init_db()

# --------------------------
# Persistence helpers
# --------------------------
def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db(); c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT)""")
    c.execute("""INSERT OR IGNORE INTO settings (k, v) VALUES ('hitl', ?)""", (json.dumps(HITL_DEFAULT),))
    c.execute("""CREATE TABLE IF NOT EXISTS datasets (id TEXT PRIMARY KEY, filename TEXT, bytes INTEGER, created_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, dataset_id TEXT, status TEXT, created_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS insights (id TEXT PRIMARY KEY, dataset_id TEXT, type TEXT, title TEXT, description TEXT, confidence REAL, severity TEXT, ts TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, email TEXT, role TEXT, status TEXT, last_active TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS chat (id TEXT PRIMARY KEY, session_id TEXT, dataset_id TEXT, role TEXT, message TEXT, ts TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS reviews (id TEXT PRIMARY KEY, title TEXT, description TEXT, confidence REAL, priority TEXT, ts TEXT)""")
    conn.commit(); conn.close()

# --------------------------
# SSE broker
# --------------------------
class Broker:
    def __init__(self):
        self._subs: Dict[str, List[asyncio.Queue]] = {}  # key: dataset_id -> list of queues

    async def publish(self, dataset_id: str, data: Dict[str, Any]):
        for q in self._subs.get(dataset_id, []):
            await q.put(data)

    async def subscribe(self, dataset_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._subs.setdefault(dataset_id, []).append(q)
        return q

    def unsubscribe(self, dataset_id: str, q: asyncio.Queue):
        arr = self._subs.get(dataset_id, [])
        if q in arr:
            arr.remove(q)

broker = Broker()

# --------------------------
# Models
# --------------------------
class UploadResult(BaseModel):
    dataset_id: str
    job_id: str
    bytes: int

class QueryRequest(BaseModel):
    query: str
    session_id: Optional[str] = None
    dataset_id: Optional[str] = None

class ToggleHITL(BaseModel):
    enabled: bool

# --------------------------
# Utility helpers (data parsing & analytics)
# --------------------------
DATE_FORMATS = ["%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]

def _try_parse_date(s: str) -> Optional[datetime]:
    if s is None:
        return None
    s = s.strip().replace("\\-", "-")  # handle escaped hyphens if present
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

def _normal_cdf(x: float) -> float:
    # Approx N(0,1) CDF
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

def _severity_from_pct(pct: float) -> str:
    # pct is expressed as 0.XX (e.g., 0.3 for 30%)
    abs_pct = abs(pct)
    if abs_pct >= SEVERITY_HIGH_PCT:
        return "high"
    if abs_pct >= SEVERITY_MED_PCT:
        return "medium"
    return "low"

def _parse_csv_rows(text: str) -> List[Dict[str, Any]]:
    """
    Expected columns (case-insensitive):
      - region (optional)
      - week or date (required)
      - revenue (float, optional)
      - tickets (float/int, optional)
    Extra columns are ignored.
    Rows without a valid date or metrics are skipped.
    """
    buf = io.StringIO(text)
    reader = csv.DictReader(buf)
    rows: List[Dict[str, Any]] = []
    for raw in reader:
        # normalize keys to lowercase and strip strings
        row = { (k.lower().strip() if k else k): (v.strip() if isinstance(v, str) else v) for k, v in raw.items() }
        date_str = row.get("week") or row.get("date")
        dt = _try_parse_date(date_str)
        if not dt:
            continue

        def _num(val):
            try:
                return float(str(val).replace(",", "").strip())
            except Exception:
                return None

        revenue = _num(row.get("revenue"))
        tickets = _num(row.get("tickets"))
        if revenue is None and tickets is None:
            continue

        rows.append({
            "region": row.get("region"),
            "date": dt,
            "revenue": revenue,
            "tickets": tickets,
        })
    return rows

def _aggregate_timeseries(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Aggregate by date:
      totals["revenue"] = [{"date": dt, "value": sum_revenue_on_date}, ...]
      totals["tickets"] = [{"date": dt, "value": sum_tickets_on_date}, ...]
    """
    by_date: Dict[datetime, Dict[str, float]] = {}
    for r in rows:
        dt = r["date"]
        acc = by_date.setdefault(dt, {"revenue": 0.0, "tickets": 0.0})
        if r["revenue"] is not None:
            acc["revenue"] += float(r["revenue"])
        if r["tickets"] is not None:
            acc["tickets"] += float(r["tickets"])

    dates_sorted = sorted(by_date.keys())
    revenue_series = [{"date": d, "value": by_date[d]["revenue"]} for d in dates_sorted]
    tickets_series = [{"date": d, "value": by_date[d]["tickets"]} for d in dates_sorted]
    return {"revenue": revenue_series, "tickets": tickets_series}

def _percent_change(new: float, old: float) -> Optional[float]:
    if old is None or old == 0:
        return None
    return (new - old) / old

def _compute_revenue_spike_insight(dataset_id: str, series: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Detect an unusually large positive period-over-period change using z-scores of % changes.
    """
    if len(series) < 3:
        return None

    # Build consecutive percent deltas
    deltas: List[float] = []
    for i in range(1, len(series)):
        pct = _percent_change(series[i]["value"], series[i-1]["value"])
        if pct is not None:
            deltas.append(pct)
    if len(deltas) < 2:
        return None

    last_pct = deltas[-1]
    prior = deltas[:-1]
    mean = statistics.mean(prior) if len(prior) > 0 else 0.0
    sd = statistics.pstdev(prior) if len(prior) > 0 else 0.0
    z = (last_pct - mean) / sd if sd > 1e-9 else 0.0

    is_spike = (last_pct > 0) and (z >= REVENUE_SPIKE_Z) and (abs(last_pct) >= REVENUE_SPIKE_MIN_PCT)
    if not is_spike:
