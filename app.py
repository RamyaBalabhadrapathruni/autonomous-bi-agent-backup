# app.py (FastAPI) — Schema-agnostic insights + doc-grounded Conversational AI + RAA Mailtrap

import os, io, csv, json, uuid, time, asyncio, math, statistics, sqlite3, re, unicodedata
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Generator, Optional, List, Tuple

from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import smtplib
from email.mime.text import MIMEText
import urllib.request

# --------------------------
# Config & Tunables (env-driven)
# --------------------------
DB_PATH = os.getenv("BI_DB_PATH", "bi_agent.db")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# Revenue spike detection thresholds
REVENUE_SPIKE_Z = float(os.getenv("REVENUE_SPIKE_Z", "1.0"))            # z-score threshold
REVENUE_SPIKE_MIN_PCT = float(os.getenv("REVENUE_SPIKE_MIN_PCT", "0"))  # absolute min % (e.g. 0.10 = 10%)

# Trend detection (tickets as acquisition proxy)
TREND_MIN_POINTS = int(os.getenv("TREND_MIN_POINTS", "3"))  # minimum points to attempt trend
TREND_WINDOW = int(os.getenv("TREND_WINDOW", "3"))          # last W periods to model

# Severity thresholds (based on absolute percent change)
SEVERITY_MED_PCT  = float(os.getenv("SEVERITY_MED_PCT",  "0.15"))  # 15%
SEVERITY_HIGH_PCT = float(os.getenv("SEVERITY_HIGH_PCT", "0.30"))  # 30%

# HITL default
HITL_DEFAULT = os.getenv("HITL_DEFAULT", "false").lower() == "true"

# Conversational LLM fallback (optional)
USE_LLM = os.getenv("USE_LLM", "false").lower() == "true"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # or your Azure/OpenAI deployment name

# Mailtrap / Alerts
MAILTRAP_HOST = os.getenv("MAILTRAP_HOST", "smtp.mailtrap.io")
MAILTRAP_PORT = int(os.getenv("MAILTRAP_PORT", "587"))
MAILTRAP_USERNAME = os.getenv("MAILTRAP_USERNAME")
MAILTRAP_PASSWORD = os.getenv("MAILTRAP_PASSWORD")
MAILTRAP_FROM = os.getenv("MAILTRAP_FROM", "alerts@example.com")
ALERT_RECIPIENTS = [e.strip() for e in os.getenv("ALERT_RECIPIENTS","").split(",") if e.strip()]
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")  # optional

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
main_loop: Optional[asyncio.AbstractEventLoop] = None  # for cross-thread publishing

@app.on_event("startup")
async def on_startup():
    global main_loop
    main_loop = asyncio.get_running_loop()
    init_db()

# --------------------------
# Persistence helpers & DB schema
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
    # NEW: schema registry & value index (for generic planning)
    c.execute("""CREATE TABLE IF NOT EXISTS dataset_schema (
      dataset_id TEXT PRIMARY KEY,
      schema_json TEXT NOT NULL
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS ds_value_index (
      dataset_id TEXT NOT NULL,
      col TEXT NOT NULL,
      value_lower TEXT NOT NULL
    )""")
    conn.commit(); conn.close()
# --------------------------
# SSE broker
# --------------------------
class Broker:
    def __init__(self):
        self._subs: Dict[str, List[asyncio.Queue]] = {}

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
# Base analytics & parsing helpers
# --------------------------
DATE_FORMATS = [
    "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y",
    "%d %b %Y", "%d %B %Y", "%b %d %Y", "%B %d %Y",
    "%d %b", "%d %B", "%b %d", "%B %d",
    "%b %Y", "%B %Y"
]
MONTHS = {m.lower(): i for i, m in enumerate(
    ["January","February","March","April","May","June","July","August","September","October","November","December"], start=1)}
MONTHS.update({k[:3]: v for k, v in list(MONTHS.items()) if len(k) > 3})

def _try_parse_date(s: str, ref_year: Optional[int] = None) -> Optional[datetime]:
    if s is None:
        return None
    st = s.strip().replace("\\-", "-").replace(",", " ")
    parts = st.split()
    if len(parts) == 1 and parts[0].lower() in MONTHS:
        y = ref_year if ref_year else datetime.utcnow().year
        return datetime(y, MONTHS[parts[0].lower()], 1)
    if len(parts) == 2 and parts[0].lower() in MONTHS and parts[1].isdigit():
        y = int(parts[1]);  return datetime(y, MONTHS[parts[0].lower()], 1)
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(st, fmt)
            if "%Y" not in fmt and ref_year:
                dt = dt.replace(year=ref_year)
            return dt
        except Exception:
            continue
    return None

def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

def _severity_from_pct(pct: float) -> str:
    abs_pct = abs(pct)
    if abs_pct >= SEVERITY_HIGH_PCT: return "high"
    if abs_pct >= SEVERITY_MED_PCT:  return "medium"
    return "low"

def _parse_csv_rows(text: str) -> List[Dict[str, Any]]:
    buf = io.StringIO(text)
    reader = csv.DictReader(buf)
    rows: List[Dict[str, Any]] = []
    for raw in reader:
        row = { (k.lower().strip() if k else k): (v.strip() if isinstance(v, str) else v) for k, v in raw.items() }
        date_str = row.get("week") or row.get("date")
        dt = _try_parse_date(date_str)
        if not dt: continue

        def _num(val):
            try: return float(str(val).replace(",", "").strip())
            except Exception: return None

        revenue = _num(row.get("revenue"))
        tickets = _num(row.get("tickets"))
        if revenue is None and tickets is None:
            # keep row anyway; schema is generic later
            pass

        rows.append({
            "region": row.get("region"),
            "date": dt,
            "revenue": revenue,
            "tickets": tickets,
            **{k:v for k,v in row.items() if k not in ("region","date","week","revenue","tickets")}
        })
    return rows

def _aggregate_timeseries(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by_date: Dict[datetime, Dict[str, float]] = {}
    for r in rows:
        dt = r.get("date")
        if not dt: continue
        acc = by_date.setdefault(dt, {"revenue": 0.0, "tickets": 0.0})
        if r.get("revenue") is not None: acc["revenue"] += float(r["revenue"])
        if r.get("tickets") is not None: acc["tickets"] += float(r["tickets"])

    dates_sorted = sorted(by_date.keys())
    revenue_series = [{"date": d, "value": by_date[d]["revenue"]} for d in dates_sorted]
    tickets_series = [{"date": d, "value": by_date[d]["tickets"]} for d in dates_sorted]
    return {"revenue": revenue_series, "tickets": tickets_series}

def _percent_change(new: float, old: float) -> Optional[float]:
    if old is None or old == 0: return None
    return (new - old) / old

def _linear_regression(x: List[float], y: List[float]) -> Dict[str, float]:
    n = len(x)
    if n == 0 or n != len(y): return {"slope":0.0,"intercept":0.0,"r":0.0,"r2":0.0}
    mx, my = statistics.fmean(x), statistics.fmean(y)
    cov = sum((xi-mx)*(yi-my) for xi,yi in zip(x,y))
    vx  = sum((xi-mx)**2 for xi in x)
    vy  = sum((yi-my)**2 for yi in y)
    slope = cov/vx if vx>1e-12 else 0.0
    intercept = my - slope*mx
    r = cov/math.sqrt(vx*vy) if vx>1e-12 and vy>1e-12 else 0.0
# --------------------------
# SSE broker
# --------------------------
class Broker:
    def __init__(self):
        self._subs: Dict[str, List[asyncio.Queue]] = {}

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
# Base analytics & parsing helpers
# --------------------------
DATE_FORMATS = [
    "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y",
    "%d %b %Y", "%d %B %Y", "%b %d %Y", "%B %d %Y",
    "%d %b", "%d %B", "%b %d", "%B %d",
    "%b %Y", "%B %Y"
]
MONTHS = {m.lower(): i for i, m in enumerate(
    ["January","February","March","April","May","June","July","August","September","October","November","December"], start=1)}
MONTHS.update({k[:3]: v for k, v in list(MONTHS.items()) if len(k) > 3})

def _try_parse_date(s: str, ref_year: Optional[int] = None) -> Optional[datetime]:
    if s is None:
        return None
    st = s.strip().replace("\\-", "-").replace(",", " ")
    parts = st.split()
    if len(parts) == 1 and parts[0].lower() in MONTHS:
        y = ref_year if ref_year else datetime.utcnow().year
        return datetime(y, MONTHS[parts[0].lower()], 1)
    if len(parts) == 2 and parts[0].lower() in MONTHS and parts[1].isdigit():
        y = int(parts[1]);  return datetime(y, MONTHS[parts[0].lower()], 1)
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(st, fmt)
            if "%Y" not in fmt and ref_year:
                dt = dt.replace(year=ref_year)
            return dt
        except Exception:
            continue
    return None

def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

def _severity_from_pct(pct: float) -> str:
    abs_pct = abs(pct)
    if abs_pct >= SEVERITY_HIGH_PCT: return "high"
    if abs_pct >= SEVERITY_MED_PCT:  return "medium"
    return "low"

def _parse_csv_rows(text: str) -> List[Dict[str, Any]]:
    buf = io.StringIO(text)
    reader = csv.DictReader(buf)
    rows: List[Dict[str, Any]] = []
    for raw in reader:
        row = { (k.lower().strip() if k else k): (v.strip() if isinstance(v, str) else v) for k, v in raw.items() }
        date_str = row.get("week") or row.get("date")
        dt = _try_parse_date(date_str)
        if not dt: continue

        def _num(val):
            try: return float(str(val).replace(",", "").strip())
            except Exception: return None

        revenue = _num(row.get("revenue"))
        tickets = _num(row.get("tickets"))
        if revenue is None and tickets is None:
            # keep row anyway; schema is generic later
            pass

        rows.append({
            "region": row.get("region"),
            "date": dt,
            "revenue": revenue,
            "tickets": tickets,
            **{k:v for k,v in row.items() if k not in ("region","date","week","revenue","tickets")}
        })
    return rows

def _aggregate_timeseries(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by_date: Dict[datetime, Dict[str, float]] = {}
    for r in rows:
        dt = r.get("date")
        if not dt: continue
        acc = by_date.setdefault(dt, {"revenue": 0.0, "tickets": 0.0})
        if r.get("revenue") is not None: acc["revenue"] += float(r["revenue"])
        if r.get("tickets") is not None: acc["tickets"] += float(r["tickets"])

    dates_sorted = sorted(by_date.keys())
    revenue_series = [{"date": d, "value": by_date[d]["revenue"]} for d in dates_sorted]
    tickets_series = [{"date": d, "value": by_date[d]["tickets"]} for d in dates_sorted]
    return {"revenue": revenue_series, "tickets": tickets_series}

def _percent_change(new: float, old: float) -> Optional[float]:
    if old is None or old == 0: return None
    return (new - old) / old

def _linear_regression(x: List[float], y: List[float]) -> Dict[str, float]:
    n = len(x)
    if n == 0 or n != len(y): return {"slope":0.0,"intercept":0.0,"r":0.0,"r2":0.0}
    mx, my = statistics.fmean(x), statistics.fmean(y)
    cov = sum((xi-mx)*(yi-my) for xi,yi in zip(x,y))
    vx  = sum((xi-mx)**2 for xi in x)
    vy  = sum((yi-my)**2 for yi in y)
    slope = cov/vx if vx>1e-12 else 0.0
    intercept = my - slope*mx
    r = cov/math.sqrt(vx*vy) if vx>1e-12 and vy>1e-12 else 0.0
def _ensure_dataset_table_generic(conn, c, dataset_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        raise HTTPException(status_code=400, detail="No rows parsed from uploaded file.")
    profile = _profile_dataset(rows)
    col_map = profile["col_map"]; rev_map = _reverse_map(col_map)
    type_map = _infer_col_types(rows)

    # sanitize types
    sanitized_types = {col_map[orig]: ("REAL" if typ=="number" else "TEXT") for orig,typ in type_map.items()}

    tbl = _table_name_for(dataset_id)
    cols_ddl = ", ".join(f"{san} {typ}" for san,typ in sanitized_types.items())

    date_col = profile["date_col"]
    date_id_col = None
    if date_col:
        date_id_col = col_map[date_col] + "_id"
        cols_ddl += f", {date_id_col} INTEGER"

    c.execute(f"DROP TABLE IF EXISTS {tbl}")
    c.execute(f"CREATE TABLE {tbl} ({cols_ddl})")

    insert_cols = list(sanitized_types.keys())
    if date_id_col: insert_cols.append(date_id_col)
    placeholders = ",".join(["?"]*len(insert_cols))

    to_ins = []
    for r in rows:
        rec = []
        for san in sanitized_types.keys():
            orig = rev_map[san]
            val = r.get(orig)
            if val is None: rec.append(None); continue
            if sanitized_types[san] == "REAL":
                rec.append(_coerce_number(val))
            else:
                rec.append(str(val))
        if date_id_col:
            dt_raw = r.get(date_col)
            dt = dt_raw if isinstance(dt_raw, datetime) else _try_parse_date(str(dt_raw)) if dt_raw else None
            rec.append(int(dt.strftime("%Y%m%d")) if dt else None)
        to_ins.append(tuple(rec))

    if to_ins:
        c.executemany(f"INSERT INTO {tbl} ({','.join(insert_cols)}) VALUES ({placeholders})", to_ins)

    # Indexes
    if date_id_col:
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_{date_id_col} ON {tbl}({date_id_col})")
    for orig_dim in profile["dimensions"]:
        san = col_map[orig_dim]
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_{san} ON {tbl}({san})")

    # Store schema JSON
    schema_json = json.dumps({
        "table": tbl,
        "date_col": profile["date_col"],
        "date_id_col": date_id_col,
        "measures": profile["measures"],
        "dimensions": profile["dimensions"],
        "col_map": profile["col_map"]
    })
    c.execute("INSERT OR REPLACE INTO dataset_schema (dataset_id, schema_json) VALUES (?,?)",
              (dataset_id, schema_json))

    # Value index (for generic filter resolution)
    c.execute("DELETE FROM ds_value_index WHERE dataset_id=?", (dataset_id,))
    for orig_dim, values in profile["value_samples"].items():
        for v in values:
            c.execute("INSERT INTO ds_value_index (dataset_id, col, value_lower) VALUES (?,?,?)",
                      (dataset_id, orig_dim, v.lower()))
    conn.commit()
    return json.loads(schema_json)

def _get_schema(c, dataset_id: str) -> Dict[str, Any]:
    row = c.execute("SELECT schema_json FROM dataset_schema WHERE dataset_id=?", (dataset_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Dataset schema not found—upload may not be processed yet")
    return json.loads(row["schema_json"])

def _dt_from_date_id(i: int) -> datetime:
    return datetime.strptime(str(i), "%Y%m%d")

def _get_dataset_meta(c, dataset_id: str) -> Dict[str, Any]:
    sch = _get_schema(c, dataset_id)
    tbl = sch["table"]
    date_id_col = sch.get("date_id_col")
    min_id = max_id = None
    if date_id_col:
        row = c.execute(f"SELECT MIN({date_id_col}) AS min_d, MAX({date_id_col}) AS max_d FROM {tbl}").fetchone()
        min_id, max_id = (row["min_d"], row["max_d"]) if row else (None, None)
    return {
        "table": tbl,
        "schema": sch,
        "min_date_id": min_id,
        "max_date_id": max_id
    }

# ---------- Date phrase parsing ----------
def _month_end(y: int, m: int) -> date:
    if m == 12: return date(y,12,31)
    first_next = date(y, m+1, 1)
    return first_next - timedelta(days=1)

def _parse_relative_range(q: str, ref: date) -> Optional[Tuple[date, date, str]]:
    s = q.lower()
    if re.search(r"\btoday\b", s):      return (ref, ref, "today")
    if re.search(r"\byesterday\b", s):  d = ref - timedelta(days=1); return (d, d, "yesterday")
    m = re.search(r"\b(?:last|past)\s+(\d+)\s+(day|days|week|weeks|month|months)\b", s)
    if m:
        n = int(m.group(1)); unit = m.group(2)
        if "day" in unit:   start = ref - timedelta(days=n-1); return (start, ref, f"last {n} days")
        if "week" in unit:  start = ref - timedelta(days=7*n - 1); return (start, ref, f"last {n} weeks")
        if "month" in unit:
            y, mth = ref.year, ref.month
            y2, m2 = y, mth
            for _ in range(n-1):
                m2 -= 1
                if m2 == 0: m2 = 12; y2 -= 1
            start = date(y2, m2, 1)
            return (start, ref, f"last {n} months")
    if re.search(r"\blast\s+week\b", s):  start = ref - timedelta(days=6); return (start, ref, "last week")
    if re.search(r"\bthis\s+week\b", s):  start = ref - timedelta(days=6); return (start, ref, "this week")
    if re.search(r"\blast\s+month\b", s):
        y,mth = ref.year, ref.month
        m2 = mth - 1 or 12; y2 = y - 1 if mth == 1 else y
        start = date(y2, m2, 1); end = _month_end(y2, m2)
        return (start, min(end, ref), "last month")
    if re.search(r"\bthis\s+month\b", s):
        start = date(ref.year, ref.month, 1); return (start, ref, "this month")
    m = re.search(r"\bsince\s+([A-Za-z0-9\-/ ,]+)", s)
    if m:
        dt = _try_parse_date(m.group(1), ref.year)
        if dt: start = dt.date(); return (start, ref, f"since {start.isoformat()}")
    m = re.search(r"\bafter\s+([A-Za-z0-9\-/ ,]+)", s)
    if m:
        dt = _try_parse_date(m.group(1), ref.year)
        if dt: start = dt.date() + timedelta(days=1); return (start, ref, f"after {dt.date().isoformat()}")
    m = re.search(r"\bbefore\s+([A-Za-z0-9\-/ ,]+)", s)
    if m:
        dt = _try_parse_date(m.group(1), ref.year)
        if dt: end = dt.date(); return (date.min, end, f"before {end.isoformat()}")
    return None

def _parse_between_range(q: str, ref: date) -> Optional[Tuple[date, date, str]]:
    s = q.lower()
    m = re.search(r"\b(?:between|from)\s+([A-Za-z0-9\-/ ,]+?)\s+(?:and|to)\s+([A-Za-z0-9\-/ ,]+)", s)
    if not m:
        m2 = re.search(r"\bon\s+([A-Za-z0-9\-/ ,]+)", s)
        if m2:
            dt = _try_parse_date(m2.group(1), ref.year)
            if dt: d = dt.date(); return (d, d, f"on {d.isoformat()}")
        m3 = re.search(r"\bin\s+([A-Za-z]+)(?:\s+(\d{4}))?\b", s)
        if m3:
            mon = m3.group(1).lower(); yr = int(m3.group(2)) if m3.group(2) else ref.year
            if mon in MONTHS:
                start = date(yr, MONTHS[mon], 1); end = _month_end(yr, MONTHS[mon])
                return (start, end, f"in {mon.title()} {yr}")
        return None
    left, right = m.group(1), m.group(2)
    d1 = _try_parse_date(left, ref.year); d2 = _try_parse_date(right, ref.year)
    if d1 and d2:
        start = min(d1.date(), d2.date()); end = max(d1.date(), d2.date())
        return (start, end, f"between {start.isoformat()} and {end.isoformat()}")
    return None

def _clamp_date_id(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, x))

def _build_date_filter(q: str, meta_min_max: Dict[str, Any]) -> Tuple[str, List[Any], str]:
    min_id, max_id = meta_min_max.get("min_date_id"), meta_min_max.get("max_date_id")
    if not min_id or not max_id: return ("", [], "")
    ref_dt = _dt_from_date_id(max_id).date()
    br = _parse_between_range(q, ref_dt)
    if br:
        start, end, desc = br
        start_id = _clamp_date_id(int(start.strftime("%Y%m%d")), min_id, max_id)
        end_id   = _clamp_date_id(int(end.strftime("%Y%m%d")),   min_id, max_id)
        if start_id > end_id: start_id, end_id = end_id, start_id
        return ("date_id BETWEEN ? AND ?", [start_id, end_id], desc)
    rr = _parse_relative_range(q, ref_dt)
    if rr:
        start, end, desc = rr
        start_id = _clamp_date_id(int(start.strftime("%Y%m%d")), min_id, max_id)
        end_id   = _clamp_date_id(int(end.strftime("%Y%m%d")),   min_id, max_id)
        if start_id > end_id: start_id, end_id = end_id, start_id
        return ("date_id BETWEEN ? AND ?", [start_id, end_id], desc)
    return ("", [], "")
def _ensure_dataset_table_generic(conn, c, dataset_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        raise HTTPException(status_code=400, detail="No rows parsed from uploaded file.")
    profile = _profile_dataset(rows)
    col_map = profile["col_map"]; rev_map = _reverse_map(col_map)
    type_map = _infer_col_types(rows)

    # sanitize types
    sanitized_types = {col_map[orig]: ("REAL" if typ=="number" else "TEXT") for orig,typ in type_map.items()}

    tbl = _table_name_for(dataset_id)
    cols_ddl = ", ".join(f"{san} {typ}" for san,typ in sanitized_types.items())

    date_col = profile["date_col"]
    date_id_col = None
    if date_col:
        date_id_col = col_map[date_col] + "_id"
        cols_ddl += f", {date_id_col} INTEGER"

    c.execute(f"DROP TABLE IF EXISTS {tbl}")
    c.execute(f"CREATE TABLE {tbl} ({cols_ddl})")

    insert_cols = list(sanitized_types.keys())
    if date_id_col: insert_cols.append(date_id_col)
    placeholders = ",".join(["?"]*len(insert_cols))

    to_ins = []
    for r in rows:
        rec = []
        for san in sanitized_types.keys():
            orig = rev_map[san]
            val = r.get(orig)
            if val is None: rec.append(None); continue
            if sanitized_types[san] == "REAL":
                rec.append(_coerce_number(val))
            else:
                rec.append(str(val))
        if date_id_col:
            dt_raw = r.get(date_col)
            dt = dt_raw if isinstance(dt_raw, datetime) else _try_parse_date(str(dt_raw)) if dt_raw else None
            rec.append(int(dt.strftime("%Y%m%d")) if dt else None)
        to_ins.append(tuple(rec))

    if to_ins:
        c.executemany(f"INSERT INTO {tbl} ({','.join(insert_cols)}) VALUES ({placeholders})", to_ins)

    # Indexes
    if date_id_col:
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_{date_id_col} ON {tbl}({date_id_col})")
    for orig_dim in profile["dimensions"]:
        san = col_map[orig_dim]
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_{san} ON {tbl}({san})")

    # Store schema JSON
    schema_json = json.dumps({
        "table": tbl,
        "date_col": profile["date_col"],
        "date_id_col": date_id_col,
        "measures": profile["measures"],
        "dimensions": profile["dimensions"],
        "col_map": profile["col_map"]
    })
    c.execute("INSERT OR REPLACE INTO dataset_schema (dataset_id, schema_json) VALUES (?,?)",
              (dataset_id, schema_json))

    # Value index (for generic filter resolution)
    c.execute("DELETE FROM ds_value_index WHERE dataset_id=?", (dataset_id,))
    for orig_dim, values in profile["value_samples"].items():
        for v in values:
            c.execute("INSERT INTO ds_value_index (dataset_id, col, value_lower) VALUES (?,?,?)",
                      (dataset_id, orig_dim, v.lower()))
    conn.commit()
    return json.loads(schema_json)

def _get_schema(c, dataset_id: str) -> Dict[str, Any]:
    row = c.execute("SELECT schema_json FROM dataset_schema WHERE dataset_id=?", (dataset_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Dataset schema not found—upload may not be processed yet")
    return json.loads(row["schema_json"])

def _dt_from_date_id(i: int) -> datetime:
    return datetime.strptime(str(i), "%Y%m%d")

def _get_dataset_meta(c, dataset_id: str) -> Dict[str, Any]:
    sch = _get_schema(c, dataset_id)
    tbl = sch["table"]
    date_id_col = sch.get("date_id_col")
    min_id = max_id = None
    if date_id_col:
        row = c.execute(f"SELECT MIN({date_id_col}) AS min_d, MAX({date_id_col}) AS max_d FROM {tbl}").fetchone()
        min_id, max_id = (row["min_d"], row["max_d"]) if row else (None, None)
    return {
        "table": tbl,
        "schema": sch,
        "min_date_id": min_id,
        "max_date_id": max_id
    }

# ---------- Date phrase parsing ----------
def _month_end(y: int, m: int) -> date:
    if m == 12: return date(y,12,31)
    first_next = date(y, m+1, 1)
    return first_next - timedelta(days=1)

def _parse_relative_range(q: str, ref: date) -> Optional[Tuple[date, date, str]]:
    s = q.lower()
    if re.search(r"\btoday\b", s):      return (ref, ref, "today")
    if re.search(r"\byesterday\b", s):  d = ref - timedelta(days=1); return (d, d, "yesterday")
    m = re.search(r"\b(?:last|past)\s+(\d+)\s+(day|days|week|weeks|month|months)\b", s)
    if m:
        n = int(m.group(1)); unit = m.group(2)
        if "day" in unit:   start = ref - timedelta(days=n-1); return (start, ref, f"last {n} days")
        if "week" in unit:  start = ref - timedelta(days=7*n - 1); return (start, ref, f"last {n} weeks")
        if "month" in unit:
            y, mth = ref.year, ref.month
            y2, m2 = y, mth
            for _ in range(n-1):
                m2 -= 1
                if m2 == 0: m2 = 12; y2 -= 1
            start = date(y2, m2, 1)
            return (start, ref, f"last {n} months")
    if re.search(r"\blast\s+week\b", s):  start = ref - timedelta(days=6); return (start, ref, "last week")
    if re.search(r"\bthis\s+week\b", s):  start = ref - timedelta(days=6); return (start, ref, "this week")
    if re.search(r"\blast\s+month\b", s):
        y,mth = ref.year, ref.month
        m2 = mth - 1 or 12; y2 = y - 1 if mth == 1 else y
        start = date(y2, m2, 1); end = _month_end(y2, m2)
        return (start, min(end, ref), "last month")
    if re.search(r"\bthis\s+month\b", s):
        start = date(ref.year, ref.month, 1); return (start, ref, "this month")
    m = re.search(r"\bsince\s+([A-Za-z0-9\-/ ,]+)", s)
    if m:
        dt = _try_parse_date(m.group(1), ref.year)
        if dt: start = dt.date(); return (start, ref, f"since {start.isoformat()}")
    m = re.search(r"\bafter\s+([A-Za-z0-9\-/ ,]+)", s)
    if m:
        dt = _try_parse_date(m.group(1), ref.year)
        if dt: start = dt.date() + timedelta(days=1); return (start, ref, f"after {dt.date().isoformat()}")
    m = re.search(r"\bbefore\s+([A-Za-z0-9\-/ ,]+)", s)
    if m:
        dt = _try_parse_date(m.group(1), ref.year)
        if dt: end = dt.date(); return (date.min, end, f"before {end.isoformat()}")
    return None

def _parse_between_range(q: str, ref: date) -> Optional[Tuple[date, date, str]]:
    s = q.lower()
    m = re.search(r"\b(?:between|from)\s+([A-Za-z0-9\-/ ,]+?)\s+(?:and|to)\s+([A-Za-z0-9\-/ ,]+)", s)
    if not m:
        m2 = re.search(r"\bon\s+([A-Za-z0-9\-/ ,]+)", s)
        if m2:
            dt = _try_parse_date(m2.group(1), ref.year)
            if dt: d = dt.date(); return (d, d, f"on {d.isoformat()}")
        m3 = re.search(r"\bin\s+([A-Za-z]+)(?:\s+(\d{4}))?\b", s)
        if m3:
            mon = m3.group(1).lower(); yr = int(m3.group(2)) if m3.group(2) else ref.year
            if mon in MONTHS:
                start = date(yr, MONTHS[mon], 1); end = _month_end(yr, MONTHS[mon])
                return (start, end, f"in {mon.title()} {yr}")
        return None
    left, right = m.group(1), m.group(2)
    d1 = _try_parse_date(left, ref.year); d2 = _try_parse_date(right, ref.year)
    if d1 and d2:
        start = min(d1.date(), d2.date()); end = max(d1.date(), d2.date())
        return (start, end, f"between {start.isoformat()} and {end.isoformat()}")
    return None

def _clamp_date_id(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, x))

def _build_date_filter(q: str, meta_min_max: Dict[str, Any]) -> Tuple[str, List[Any], str]:
    min_id, max_id = meta_min_max.get("min_date_id"), meta_min_max.get("max_date_id")
    if not min_id or not max_id: return ("", [], "")
    ref_dt = _dt_from_date_id(max_id).date()
    br = _parse_between_range(q, ref_dt)
    if br:
        start, end, desc = br
        start_id = _clamp_date_id(int(start.strftime("%Y%m%d")), min_id, max_id)
        end_id   = _clamp_date_id(int(end.strftime("%Y%m%d")),   min_id, max_id)
        if start_id > end_id: start_id, end_id = end_id, start_id
        return ("date_id BETWEEN ? AND ?", [start_id, end_id], desc)
    rr = _parse_relative_range(q, ref_dt)
    if rr:
        start, end, desc = rr
        start_id = _clamp_date_id(int(start.strftime("%Y%m%d")), min_id, max_id)
        end_id   = _clamp_date_id(int(end.strftime("%Y%m%d")),   min_id, max_id)
        if start_id > end_id: start_id, end_id = end_id, start_id
        return ("date_id BETWEEN ? AND ?", [start_id, end_id], desc)
    return ("", [], "")
def _is_safe_select(sql: str) -> bool:
    s = sql.strip().upper()
    if not s.startswith(("WITH","SELECT","PRAGMA TABLE_INFO")): return False
    for bad in ("INSERT","UPDATE","DELETE","DROP","ALTER","ATTACH","DETACH","CREATE","REPLACE",";--"):
        if bad in s: return False
    if ";" in s.strip()[:-1]: return False
    return True

def _resolve_metric(query: str, sch: Dict[str, Any]) -> str:
    s = query.lower()
    measures = sch.get("measures", [])
    if not measures: return "*count*"
    for m in measures:
        if m and m.lower() in s: return m
    synonyms = {
        "revenue":["revenue","sales","turnover","gmv"],
        "tickets":["ticket","tickets","orders","qty","quantity","units","volume"],
        "amount":["amount","value","val"]
    }
    for m in measures:
        ml = m.lower()
        for syn, keys in synonyms.items():
            if syn in ml and any(k in s for k in keys):
                return m
    return measures[0]

def _resolve_dimensions_and_filters(query: str, c, dataset_id: str, sch: Dict[str, Any]) -> Tuple[List[str], List[Tuple[str,str]]]:
    s = query.lower()
    dims = [d for d in sch.get("dimensions", []) if d]
    if not dims: return [], []
    # choose dimension mentioned if any
    chosen = []
    for d in dims:
        if d.lower() in s: chosen.append(d); break
    if not chosen:
        chosen = [dims[0]]
    # build candidate phrases to match values
    tokens = {tok.strip(".,;:!?'\"()[]{}") for tok in s.split()}
    phrases = set()
    parts = re.split(r"[^A-Za-z0-9]+", s)
    for L in range(2, min(4, len(parts))+1):
        for i in range(0, len(parts)-L+1):
            phrases.add(" ".join(parts[i:i+L]))
    candidates = tokens | phrases
    # scan value index
    rows = c.execute("SELECT col, value_lower FROM ds_value_index WHERE dataset_id=?", (dataset_id,)).fetchall()
    filters = []
    for col, val in [(r["col"], r["value_lower"]) for r in rows]:
        if val in candidates:
            filters.append((col, val))
    seen_cols, final_filters = set(), []
    for col,val in filters:
        if col not in seen_cols:
            final_filters.append((col,val)); seen_cols.add(col)
    return chosen[:2], final_filters

def _plan_sql_from_query_generic(q: str, meta: Dict[str, Any], c, dataset_id: str) -> Dict[str, Any]:
    sch = meta["schema"]; tbl = sch["table"]; col_map = sch["col_map"]; date_id_col = sch.get("date_id_col")

    metric_orig = _resolve_metric(q, sch)
    if metric_orig == "*count*":
        metric_expr, metric_label = "COUNT(*)", "count"
    else:
        metric_expr, metric_label = f"SUM({col_map[metric_orig]})", metric_orig

    dims_orig, value_filters = _resolve_dimensions_and_filters(q, c, dataset_id, sch)

    where_clauses, params, where_desc_parts = [], [], []

    # date
    if date_id_col:
        s = q.lower()
        date_sql, date_params, date_desc = _build_date_filter(s, {"min_date_id":meta["min_date_id"], "max_date_id":meta["max_date_id"]})
        if date_sql:
            where_clauses.append(date_sql.replace("date_id", date_id_col))
            params.extend(date_params); where_desc_parts.append(date_desc)

    # categorical filters
    for col_orig, val_lower in value_filters:
        san = col_map.get(col_orig)
        if san:
            where_clauses.append(f"LOWER({san}) = ?"); params.append(val_lower)
            where_desc_parts.append(f"{col_orig}='{val_lower}'")

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    where_desc = ", ".join(where_desc_parts)

    s = q.lower()
    wants_trend  = any(k in s for k in ["trend","over time","time series","plot","trajectory"])
    wants_top    = any(k in s for k in ["top","highest","max"])
    wants_change = any(k in s for k in ["change","increase","decrease","vs last","vs previous"])

    k = 5
    for tok in s.split():
        if tok.isdigit(): k = max(1, min(50, int(tok))); break

    if wants_trend and date_id_col:
        date_col_san = col_map.get(sch.get("date_col"), None)
        x_col = "date" if date_col_san == "date" else (date_col_san or date_id_col)
        sql = f"""
        SELECT {x_col} AS x, {metric_expr} AS y
        FROM {tbl}
        {where_sql}
        GROUP BY x
        ORDER BY x ASC
        """
        return {"sql": sql.strip(), "params": tuple(params), "explain": f"{metric_label.title()} trend over time", "where_desc": where_desc, "kind":"trend", "x_col": x_col, "y_col":"y", "metric": metric_label, "dims": dims_orig}

    if wants_change and date_id_col:
        sql = f"""
        WITH series AS (
          SELECT {date_id_col} AS did, {metric_expr} AS val
          FROM {tbl}
          {where_sql}
          GROUP BY did
          ORDER BY did ASC
        )
        SELECT
          (SELECT val FROM series ORDER BY did DESC LIMIT 1) AS last_val,
          (SELECT val FROM series ORDER BY did DESC LIMIT 1 OFFSET 1) AS prev_val
        """
        return {"sql": sql.strip(), "params": tuple(params), "explain": f"Last vs previous {metric_label} percent change", "where_desc": where_desc, "kind":"delta", "metric": metric_label}

    if (wants_top or " by " in s) and dims_orig:
        g = col_map[dims_orig[0]]
        sql = f"""
        SELECT {g} AS g, {metric_expr} AS total
        FROM {tbl}
        {where_sql}
        GROUP BY g
        HAVING g IS NOT NULL
        ORDER BY total DESC
        LIMIT {k if wants_top else 1000}
        """
        return {"sql": sql.strip(), "params": tuple(params), "explain": f"{'Top '+str(k) if wants_top else metric_label.title()+' by '+dims_orig[0]}", "where_desc": where_desc, "kind": ("top" if wants_top else "by"), "group_col": dims_orig[0], "metric": metric_label}

    sql = f"SELECT {metric_expr} AS total FROM {tbl} {where_sql}"
    return {"sql": sql.strip(), "params": tuple(params), "explain": f"Total {metric_label}", "where_desc": where_desc, "kind":"total", "metric": metric_label}

def _llm_text_to_sql(question: str, meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not (USE_LLM and OPENAI_API_KEY): return None
    try:
        import openai
        openai.api_key = OPENAI_API_KEY
        sch = meta["schema"]; tbl = sch["table"]
        system = (
            "You are a data analyst that writes only safe, single-statement SELECT SQL for SQLite. "
            f"The table is named '{tbl}' with columns defined by the dataset. "
            "Never modify data, never use DROP/ALTER/INSERT/UPDATE, never attach databases. "
            "Return JSON as {\"sql\": \"...\", \"params\": []}."
        )
        user = f"Question: {question}\nReturn JSON only."
        resp = openai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.0,
        )
        txt = resp.choices[0].message.content.strip()
        obj = json.loads(txt)
        sql = obj.get("sql",""); params = obj.get("params",[])
        if not _is_safe_select(sql): return None
        return {"sql": sql, "params": tuple(params), "explain": "LLM-generated SQL", "where_desc": ""}
    except Exception:
        return None

# --------------------------
# File upload & processing
# --------------------------
@app.post("/upload-file", response_model=UploadResult)
async def upload_file(file: UploadFile = File(...)):
    content = await file.read()
    try: text = content.decode(errors='ignore')
    except Exception as e: raise HTTPException(status_code=400, detail=f"Unable to decode file: {e}")

    dataset_id = str(uuid.uuid4()); bytes_len = len(content)
    conn = db(); c = conn.cursor()
    c.execute("INSERT INTO datasets (id, filename, bytes, created_at) VALUES (?,?,?,?)",
              (dataset_id, file.filename, bytes_len, datetime.utcnow().isoformat()))
    conn.commit(); conn.close()

    job_id = str(uuid.uuid4())
    conn = db(); c = conn.cursor()
    c.execute("INSERT INTO jobs (id, dataset_id, status, created_at) VALUES (?,?,?,?)",
              (job_id, dataset_id, "queued", datetime.utcnow().isoformat()))
    conn.commit(); conn.close()

    executor.submit(process_dataset, dataset_id, job_id, text)
    return UploadResult(dataset_id=dataset_id, job_id=job_id, bytes=bytes_len)

@app.post("/upload")
async def begin_processing(payload: Dict[str, Any]):
    return {"ok": True}

def process_dataset(dataset_id: str, job_id: str, text: str):
    conn = db(); c = conn.cursor()
    c.execute("UPDATE jobs SET status=? WHERE id=?", ("processing", job_id)); conn.commit()

    _publish_from_thread(broker.publish(dataset_id, {"insight": {
        "id": str(uuid.uuid4()), "dataset_id": dataset_id, "type": "info",
        "title": "Dataset received", "description": "Processing has started",
        "confidence": 1.0, "severity": "low", "timestamp": datetime.utcnow().isoformat()
    }}))

    rows = _parse_csv_rows(text)
    _ensure_dataset_table_generic(conn, c, dataset_id, rows)

    totals = _aggregate_timeseries(rows)
    revenue_series = totals["revenue"]; tickets_series = totals["tickets"]

    insights: List[Dict[str, Any]] = []
    rev_spike = _compute_revenue_spike_insight(dataset_id, revenue_series)
    if rev_spike: insights.append(rev_spike)
    trend = _compute_acquisition_trend_insight(dataset_id, tickets_series)
    if trend: insights.append(trend)

    for ins in insights:
        _publish_from_thread(_publish_insight(dataset_id, conn, c, ins))

    c.execute("UPDATE jobs SET status=? WHERE id=?", ("done", job_id)); conn.commit(); conn.close()
def _is_safe_select(sql: str) -> bool:
    s = sql.strip().upper()
    if not s.startswith(("WITH","SELECT","PRAGMA TABLE_INFO")): return False
    for bad in ("INSERT","UPDATE","DELETE","DROP","ALTER","ATTACH","DETACH","CREATE","REPLACE",";--"):
        if bad in s: return False
    if ";" in s.strip()[:-1]: return False
    return True

def _resolve_metric(query: str, sch: Dict[str, Any]) -> str:
    s = query.lower()
    measures = sch.get("measures", [])
    if not measures: return "*count*"
    for m in measures:
        if m and m.lower() in s: return m
    synonyms = {
        "revenue":["revenue","sales","turnover","gmv"],
        "tickets":["ticket","tickets","orders","qty","quantity","units","volume"],
        "amount":["amount","value","val"]
    }
    for m in measures:
        ml = m.lower()
        for syn, keys in synonyms.items():
            if syn in ml and any(k in s for k in keys):
                return m
    return measures[0]

def _resolve_dimensions_and_filters(query: str, c, dataset_id: str, sch: Dict[str, Any]) -> Tuple[List[str], List[Tuple[str,str]]]:
    s = query.lower()
    dims = [d for d in sch.get("dimensions", []) if d]
    if not dims: return [], []
    # choose dimension mentioned if any
    chosen = []
    for d in dims:
        if d.lower() in s: chosen.append(d); break
    if not chosen:
        chosen = [dims[0]]
    # build candidate phrases to match values
    tokens = {tok.strip(".,;:!?'\"()[]{}") for tok in s.split()}
    phrases = set()
    parts = re.split(r"[^A-Za-z0-9]+", s)
    for L in range(2, min(4, len(parts))+1):
        for i in range(0, len(parts)-L+1):
            phrases.add(" ".join(parts[i:i+L]))
    candidates = tokens | phrases
    # scan value index
    rows = c.execute("SELECT col, value_lower FROM ds_value_index WHERE dataset_id=?", (dataset_id,)).fetchall()
    filters = []
    for col, val in [(r["col"], r["value_lower"]) for r in rows]:
        if val in candidates:
            filters.append((col, val))
    seen_cols, final_filters = set(), []
    for col,val in filters:
        if col not in seen_cols:
            final_filters.append((col,val)); seen_cols.add(col)
    return chosen[:2], final_filters

def _plan_sql_from_query_generic(q: str, meta: Dict[str, Any], c, dataset_id: str) -> Dict[str, Any]:
    sch = meta["schema"]; tbl = sch["table"]; col_map = sch["col_map"]; date_id_col = sch.get("date_id_col")

    metric_orig = _resolve_metric(q, sch)
    if metric_orig == "*count*":
        metric_expr, metric_label = "COUNT(*)", "count"
    else:
        metric_expr, metric_label = f"SUM({col_map[metric_orig]})", metric_orig

    dims_orig, value_filters = _resolve_dimensions_and_filters(q, c, dataset_id, sch)

    where_clauses, params, where_desc_parts = [], [], []

    # date
    if date_id_col:
        s = q.lower()
        date_sql, date_params, date_desc = _build_date_filter(s, {"min_date_id":meta["min_date_id"], "max_date_id":meta["max_date_id"]})
        if date_sql:
            where_clauses.append(date_sql.replace("date_id", date_id_col))
            params.extend(date_params); where_desc_parts.append(date_desc)

    # categorical filters
    for col_orig, val_lower in value_filters:
        san = col_map.get(col_orig)
        if san:
            where_clauses.append(f"LOWER({san}) = ?"); params.append(val_lower)
            where_desc_parts.append(f"{col_orig}='{val_lower}'")

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    where_desc = ", ".join(where_desc_parts)

    s = q.lower()
    wants_trend  = any(k in s for k in ["trend","over time","time series","plot","trajectory"])
    wants_top    = any(k in s for k in ["top","highest","max"])
    wants_change = any(k in s for k in ["change","increase","decrease","vs last","vs previous"])

    k = 5
    for tok in s.split():
        if tok.isdigit(): k = max(1, min(50, int(tok))); break

    if wants_trend and date_id_col:
        date_col_san = col_map.get(sch.get("date_col"), None)
        x_col = "date" if date_col_san == "date" else (date_col_san or date_id_col)
        sql = f"""
        SELECT {x_col} AS x, {metric_expr} AS y
        FROM {tbl}
        {where_sql}
        GROUP BY x
        ORDER BY x ASC
        """
        return {"sql": sql.strip(), "params": tuple(params), "explain": f"{metric_label.title()} trend over time", "where_desc": where_desc, "kind":"trend", "x_col": x_col, "y_col":"y", "metric": metric_label, "dims": dims_orig}

    if wants_change and date_id_col:
        sql = f"""
        WITH series AS (
          SELECT {date_id_col} AS did, {metric_expr} AS val
          FROM {tbl}
          {where_sql}
          GROUP BY did
          ORDER BY did ASC
        )
        SELECT
          (SELECT val FROM series ORDER BY did DESC LIMIT 1) AS last_val,
          (SELECT val FROM series ORDER BY did DESC LIMIT 1 OFFSET 1) AS prev_val
        """
        return {"sql": sql.strip(), "params": tuple(params), "explain": f"Last vs previous {metric_label} percent change", "where_desc": where_desc, "kind":"delta", "metric": metric_label}

    if (wants_top or " by " in s) and dims_orig:
        g = col_map[dims_orig[0]]
        sql = f"""
        SELECT {g} AS g, {metric_expr} AS total
        FROM {tbl}
        {where_sql}
        GROUP BY g
        HAVING g IS NOT NULL
        ORDER BY total DESC
        LIMIT {k if wants_top else 1000}
        """
        return {"sql": sql.strip(), "params": tuple(params), "explain": f"{'Top '+str(k) if wants_top else metric_label.title()+' by '+dims_orig[0]}", "where_desc": where_desc, "kind": ("top" if wants_top else "by"), "group_col": dims_orig[0], "metric": metric_label}

    sql = f"SELECT {metric_expr} AS total FROM {tbl} {where_sql}"
    return {"sql": sql.strip(), "params": tuple(params), "explain": f"Total {metric_label}", "where_desc": where_desc, "kind":"total", "metric": metric_label}

def _llm_text_to_sql(question: str, meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not (USE_LLM and OPENAI_API_KEY): return None
    try:
        import openai
        openai.api_key = OPENAI_API_KEY
        sch = meta["schema"]; tbl = sch["table"]
        system = (
            "You are a data analyst that writes only safe, single-statement SELECT SQL for SQLite. "
            f"The table is named '{tbl}' with columns defined by the dataset. "
            "Never modify data, never use DROP/ALTER/INSERT/UPDATE, never attach databases. "
            "Return JSON as {\"sql\": \"...\", \"params\": []}."
        )
        user = f"Question: {question}\nReturn JSON only."
        resp = openai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.0,
        )
        txt = resp.choices[0].message.content.strip()
        obj = json.loads(txt)
        sql = obj.get("sql",""); params = obj.get("params",[])
        if not _is_safe_select(sql): return None
        return {"sql": sql, "params": tuple(params), "explain": "LLM-generated SQL", "where_desc": ""}
    except Exception:
        return None

# --------------------------
# File upload & processing
# --------------------------
@app.post("/upload-file", response_model=UploadResult)
async def upload_file(file: UploadFile = File(...)):
    content = await file.read()
    try: text = content.decode(errors='ignore')
    except Exception as e: raise HTTPException(status_code=400, detail=f"Unable to decode file: {e}")

    dataset_id = str(uuid.uuid4()); bytes_len = len(content)
    conn = db(); c = conn.cursor()
    c.execute("INSERT INTO datasets (id, filename, bytes, created_at) VALUES (?,?,?,?)",
              (dataset_id, file.filename, bytes_len, datetime.utcnow().isoformat()))
    conn.commit(); conn.close()

    job_id = str(uuid.uuid4())
    conn = db(); c = conn.cursor()
    c.execute("INSERT INTO jobs (id, dataset_id, status, created_at) VALUES (?,?,?,?)",
              (job_id, dataset_id, "queued", datetime.utcnow().isoformat()))
    conn.commit(); conn.close()

    executor.submit(process_dataset, dataset_id, job_id, text)
    return UploadResult(dataset_id=dataset_id, job_id=job_id, bytes=bytes_len)

@app.post("/upload")
async def begin_processing(payload: Dict[str, Any]):
    return {"ok": True}

def process_dataset(dataset_id: str, job_id: str, text: str):
    conn = db(); c = conn.cursor()
    c.execute("UPDATE jobs SET status=? WHERE id=?", ("processing", job_id)); conn.commit()

    _publish_from_thread(broker.publish(dataset_id, {"insight": {
        "id": str(uuid.uuid4()), "dataset_id": dataset_id, "type": "info",
        "title": "Dataset received", "description": "Processing has started",
        "confidence": 1.0, "severity": "low", "timestamp": datetime.utcnow().isoformat()
    }}))

    rows = _parse_csv_rows(text)
    _ensure_dataset_table_generic(conn, c, dataset_id, rows)

    totals = _aggregate_timeseries(rows)
    revenue_series = totals["revenue"]; tickets_series = totals["tickets"]

    insights: List[Dict[str, Any]] = []
    rev_spike = _compute_revenue_spike_insight(dataset_id, revenue_series)
    if rev_spike: insights.append(rev_spike)
    trend = _compute_acquisition_trend_insight(dataset_id, tickets_series)
    if trend: insights.append(trend)

    for ins in insights:
        _publish_from_thread(_publish_insight(dataset_id, conn, c, ins))

    c.execute("UPDATE jobs SET status=? WHERE id=?", ("done", job_id)); conn.commit(); conn.close()
# --------------------------
# Insights (REST & SSE)
# --------------------------
@app.get("/live-insights")
def get_insights(dataset_id: str):
    conn = db(); c = conn.cursor()
    cur = c.execute(
        "SELECT id, dataset_id, type, title, description, confidence, severity, ts "
        "FROM insights WHERE dataset_id=? ORDER BY ts DESC", (dataset_id,)
    )
    data = []
    for row in cur.fetchall():
        d = dict(row); d["timestamp"] = d.pop("ts"); data.append(d)
    conn.close()
    return {"insights": data}

@app.get("/live-insights/stream")
async def insights_stream(request: Request, dataset_id: str):
    q = await broker.subscribe(dataset_id)
    async def event_gen() -> Generator[str, None, None]:
        try:
            while True:
                if await request.is_disconnected(): break
                data = await q.get(); yield f"data: {json.dumps(data)}\n\n"
        finally:
            broker.unsubscribe(dataset_id, q)
    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","Connection":"keep-alive","X-Accel-Buffering":"no"}
    )

# --------------------------
# Conversational QA (Grounded)
# --------------------------
def _inject_where_desc(text: str, plan: Dict[str, Any]) -> str:
    desc = plan.get("where_desc")
    return f"{text} ({desc})." if desc else text + "."

def _fmt_num(x):
    if x is None: return "0"
    try: x = float(x)
    except: return str(x)
    if abs(x) >= 1000: return f"{x:,.0f}"
    if abs(x) >= 100:  return f"{x:,.0f}"
    if abs(x) >= 10:   return f"{x:,.1f}"
    return f"{x:,.2f}"

def _narrate_answer(q: str, plan: Dict[str, Any], cols: List[str], rows: List[sqlite3.Row], meta: Dict[str, Any]) -> str:
    kind = plan.get("kind"); metric = plan.get("metric","value").title()
    if kind == "trend" and rows:
        pts = len(rows)
        try:
            first_v = float(rows[0]["y"]); last_v = float(rows[-1]["y"])
            if first_v == 0:
                txt = f"{metric} trend over time across {pts} points"
            else:
                pct = (last_v - first_v)/first_v; dir = "up" if pct>0 else "down"
                txt = f"{metric} is {dir} {round(abs(pct)*100,1)}% from {_fmt_num(first_v)} to {_fmt_num(last_v)} over {pts} periods"
        except Exception:
            txt = f"{metric} trend over {pts} periods"
        return _inject_where_desc(txt, plan)

    if kind == "delta" and rows:
        r = rows[0]; last_val, prev_val = r["last_val"], r["prev_val"]
        if last_val is None or prev_val in (None,0):
            return _inject_where_desc("I couldn't compute a valid last vs previous comparison from the data", plan)
        pct = (last_val - prev_val)/prev_val; direction = "increase" if pct>0 else "decrease"
        txt = f"{direction.title()} of {round(abs(pct)*100,1)}% (last {_fmt_num(last_val)} vs previous {_fmt_num(prev_val)})"
        return _inject_where_desc(txt, plan)

    if kind in ("top","by") and rows:
        group_col = plan.get("group_col","group")
        tops = [f"{r['g']}: {_fmt_num(r['total'])}" for r in rows[:5]]
        txt = f"{metric} by {group_col}: " + ", ".join(tops)
        return _inject_where_desc(txt, plan)

    if kind == "total" and rows:
        txt = f"{metric} total: {_fmt_num(rows[0]['total'])}"
        return _inject_where_desc(txt, plan)

    if rows and cols:
        preview = ", ".join(f"{cols[i]}={_fmt_num(rows[0][i])}" for i in range(len(cols)))
        return _inject_where_desc(f"Here’s a quick snapshot: {preview}", plan)

    return _inject_where_desc("No rows returned", plan)

@app.post("/query")
def query(req: QueryRequest):
    if not req.dataset_id:
        raise HTTPException(status_code=400, detail="dataset_id is required for grounded answers")

    conn = db(); c = conn.cursor()
    meta = _get_dataset_meta(c, req.dataset_id)
    if not meta["schema"]:
        conn.close(); raise HTTPException(status_code=404, detail="Dataset schema missing")

    plan = _plan_sql_from_query_generic(req.query, meta, c, req.dataset_id)
    if not plan:
        plan = _llm_text_to_sql(req.query, meta)

    if not plan:
        conn.close()
        msg = ("I couldn't map your question to a dataset query. "
               "Try asking about totals, trends, top groups, or last vs previous for any numeric column, "
               "optionally with a date window like 'last 2 weeks' or 'between 1 Aug and 15 Aug 2025'.")
        _log_chat(req, msg);  return {"response": msg, "grounding": {"dataset_id": req.dataset_id}}

    sql, params = plan["sql"], plan["params"]
    if not _is_safe_select(sql):
        conn.close(); raise HTTPException(status_code=400, detail="Unsafe SQL rejected")

    cur = c.execute(sql, params)
    cols = [d[0] for d in cur.description] if cur.description else []
    rows = cur.fetchall(); row_count = len(rows)
    answer = _narrate_answer(req.query, plan, cols, rows, meta)

    now = datetime.utcnow().isoformat()
    c.execute("INSERT INTO chat (id, session_id, dataset_id, role, message, ts) VALUES (?,?,?,?,?,?)",
              (str(uuid.uuid4()), req.session_id, req.dataset_id, "user", req.query, now))
    c.execute("INSERT INTO chat (id, session_id, dataset_id, role, message, ts) VALUES (?,?,?,?,?,?)",
              (str(uuid.uuid4()), req.session_id, req.dataset_id, "ai", answer, now))
    conn.commit(); conn.close()

    grounding = {
        "dataset_id": req.dataset_id,
        "sql": sql,
        "params": list(params),
        "row_count": row_count,
        "date_range": {"min": str(meta["min_date_id"]), "max": str(meta["max_date_id"])},
        "explain": plan.get("explain"),
        "where_desc": plan.get("where_desc")
    }
    return {"response": answer, "grounding": grounding}

def _log_chat(req: QueryRequest, answer: str):
    conn = db(); c = conn.cursor()
    now = datetime.utcnow().isoformat()
    c.execute("INSERT INTO chat (id, session_id, dataset_id, role, message, ts) VALUES (?,?,?,?,?,?)",
              (str(uuid.uuid4()), req.session_id, req.dataset_id, "user", req.query, now))
    c.execute("INSERT INTO chat (id, session_id, dataset_id, role, message, ts) VALUES (?,?,?,?,?,?)",
              (str(uuid.uuid4()), req.session_id, req.dataset_id, "ai", answer, now))
    conn.commit(); conn.close()

# --------------------------
# Chat logs REST
# --------------------------
@app.get("/chat-messages")
def list_chat(session_id: Optional[str] = None, dataset_id: Optional[str] = None):
    conn = db(); c = conn.cursor()
    q = "SELECT id, session_id, dataset_id, role as type, message, ts as timestamp FROM chat WHERE 1=1"
    params: List[Any] = []
    if session_id: q += " AND session_id=?"; params.append(session_id)
    if dataset_id: q += " AND dataset_id=?"; params.append(dataset_id)
    q += " ORDER BY ts ASC"
    rows = [dict(r) for r in c.execute(q, params).fetchall()]
    conn.close()
    return rows

@app.post("/chat-messages")
def create_chat(msg: Dict[str, Any]):
    conn = db(); c = conn.cursor()
    c.execute("INSERT INTO chat (id, session_id, dataset_id, role, message, ts) VALUES (?,?,?,?,?,?)",
              (str(uuid.uuid4()), msg.get("session_id"), msg.get("dataset_id"), msg.get("type"),
               msg.get("message"), datetime.utcnow().isoformat()))
    conn.commit(); conn.close()
    return {"ok": True}
