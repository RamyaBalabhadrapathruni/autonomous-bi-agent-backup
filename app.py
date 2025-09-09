# app.py (FastAPI) — Data-driven insights + doc-grounded conversational AI
import os, io, csv, json, uuid, time, asyncio, math, statistics, sqlite3
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Generator, Optional, List

from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# --------------------------
# Config & Tunables (env-driven, no hard-coded business thresholds)
# --------------------------
DB_PATH = os.getenv("BI_DB_PATH", "bi_agent.db")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# Revenue spike detection: positive last %Δ and statistically larger than prior deltas
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
# Data parsing & analytics helpers
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
        return None

    confidence = _normal_cdf(abs(z))  # higher z => higher confidence
    sev = _severity_from_pct(last_pct)
    ts = datetime.utcnow().isoformat()

    return {
        "id": str(uuid.uuid4()),
        "dataset_id": dataset_id,
        "type": "anomaly",
        "title": "Revenue Spike Detected",
        "description": f"{round(last_pct*100, 1)}% increase vs prior period",
        "confidence": round(confidence, 2),
        "severity": sev,
        "timestamp": ts,
    }

def _linear_regression(x: List[float], y: List[float]) -> Dict[str, float]:
    """
    Simple OLS for slope/intercept/r without numpy.
    """
    n = len(x)
    if n == 0 or n != len(y):
        return {"slope": 0.0, "intercept": 0.0, "r": 0.0, "r2": 0.0}
    mean_x = statistics.fmean(x)
    mean_y = statistics.fmean(y)
    cov = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    var_x = sum((xi - mean_x) ** 2 for xi in x)
    var_y = sum((yi - mean_y) ** 2 for yi in y)
    slope = cov / var_x if var_x > 1e-12 else 0.0
    intercept = mean_y - slope * mean_x
    r = cov / math.sqrt(var_x * var_y) if var_x > 1e-12 and var_y > 1e-12 else 0.0
    return {"slope": slope, "intercept": intercept, "r": r, "r2": r*r}

def _describe_window(dates: List[datetime]) -> str:
    """
    Describe the window (days/weeks/periods) based on spacing.
    """
    if len(dates) < 2:
        return "recent period"
    diffs = [(dates[i] - dates[i-1]).days for i in range(1, len(dates))]
    step = sorted(diffs)[len(diffs)//2] if diffs else 7
    if step <= 1:
        return f"past {min(len(dates), TREND_WINDOW)} days"
    if 2 <= step <= 5:
        return f"past {min(len(dates), TREND_WINDOW)} business days"
    if 6 <= step <= 9:
        return f"past {min(len(dates), TREND_WINDOW)} weeks"
    if step <= 31:
        return f"past {min(len(dates), TREND_WINDOW)} periods"
    return "recent period"

def _compute_acquisition_trend_insight(dataset_id: str, series: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Detect upward trend in tickets (customer acquisition proxy) over last TREND_WINDOW periods.
    """
    if len(series) < TREND_MIN_POINTS:
        return None

    W = min(TREND_WINDOW, len(series))
    recent = series[-W:]
    dates = [p["date"] for p in recent]
    y = [float(p["value"]) for p in recent]
    x = list(range(len(recent)))
    reg = _linear_regression(x, y)

    if reg["slope"] <= 0:
        return None

    first, last = y[0], y[-1]
    pct = _percent_change(last, first)
    if pct is None or pct <= 0:
        return None

    conf = 0.5 + 0.49 * min(1.0, abs(reg["r"]))  # map |r| ∈ [0,1] -> [0.5,0.99]
    sev = _severity_from_pct(pct)
    ts = datetime.utcnow().isoformat()

    return {
        "id": str(uuid.uuid4()),
        "dataset_id": dataset_id,
        "type": "trend",
        "title": "Customer Acquisition Trending Up",
        "description": f"{round(pct*100, 1)}% increase { _describe_window(dates) }",
        "confidence": round(conf, 2),
        "severity": sev,
        "timestamp": ts,
    }

async def _publish_insight(dataset_id: str, conn, c, insight: Dict[str, Any]):
    """
    Persist the insight and publish it to SSE subscribers.
    """
    iid = insight["id"]; ts = insight["timestamp"]
    c.execute(
        """INSERT INTO insights (id, dataset_id, type, title, description, confidence, severity, ts)
           VALUES (?,?,?,?,?,?,?,?)""",
        (iid, dataset_id, insight["type"], insight["title"], insight["description"],
         float(insight["confidence"]), insight.get("severity"), ts)
    )
    conn.commit()
    await broker.publish(dataset_id, {"insight": insight})

def _publish_from_thread(coro):
    """
    Run a coroutine from a worker thread on the main event loop.
    """
    if not main_loop:
        raise RuntimeError("Main event loop not initialized")
    fut = asyncio.run_coroutine_threadsafe(coro, main_loop)
    try:
        fut.result(timeout=5)
    except Exception:
        pass

# --------------------------
# Conversational grounding: SQL helpers
# --------------------------
SAFE_SQL_PREFIXES = ("WITH", "SELECT", "PRAGMA table_info")  # read-only
DISALLOWED_TOKENS = ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "ATTACH", "DETACH", "CREATE", "REPLACE", ";--")

def _table_name_for(dataset_id: str) -> str:
    return "ds_" + dataset_id.replace("-", "")

def _date_id(dt: datetime) -> int:
    return int(dt.strftime("%Y%m%d"))

def _ensure_dataset_table(conn, c, dataset_id: str, rows: List[Dict[str, Any]]):
    """
    Create or replace the per-dataset table and load rows.
    Schema: (date_id INT, date TEXT, region TEXT, revenue REAL, tickets REAL)
    """
    tbl = _table_name_for(dataset_id)
    c.execute(f"""CREATE TABLE IF NOT EXISTS {tbl} (
        date_id INTEGER,
        date TEXT,
        region TEXT,
        revenue REAL,
        tickets REAL
    )""")
    c.execute(f"DELETE FROM {tbl}")

    to_ins = []
    for r in rows:
        to_ins.append((_date_id(r["date"]), r["date"].strftime("%Y-%m-%d"), r.get("region"), r.get("revenue"), r.get("tickets")))
    if to_ins:
        c.executemany(f"INSERT INTO {tbl} (date_id, date, region, revenue, tickets) VALUES (?,?,?,?,?)", to_ins)

    c.execute(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_date ON {tbl}(date_id)")
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_region ON {tbl}(region)")
    conn.commit()

def _get_dataset_meta(c, dataset_id: str) -> Dict[str, Any]:
    tbl = _table_name_for(dataset_id)
    row = c.execute(f"SELECT MIN(date_id) AS min_d, MAX(date_id) AS max_d FROM {tbl}").fetchone()
    min_d, max_d = (row["min_d"], row["max_d"]) if row else (None, None)
    regions = [r["region"] for r in c.execute(f"SELECT DISTINCT region FROM {tbl} WHERE region IS NOT NULL").fetchall()]
    return {"table": tbl, "min_date_id": min_d, "max_date_id": max_d, "regions": regions}

def _is_safe_select(sql: str) -> bool:
    s = sql.strip().upper()
    if not s.startswith(SAFE_SQL_PREFIXES):
        return False
    for bad in DISALLOWED_TOKENS:
        if bad in s:
            return False
    # no multiple statements (allow trailing semicolon only)
    if ";" in s.strip()[:-1]:
        return False
    return True

# --------------------------
# NL → SQL heuristic planner
# --------------------------
def _plan_sql_from_query(q: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns {"sql": "...", "params": tuple, "explain": "..."} or {} if no plan matched.
    Covers: totals, by region, trend over time, last vs previous, top-k, filters by region.
    """
    s = q.lower()
    tbl = meta["table"]
    regions = [r.lower() for r in meta["regions"]]
    params: List[Any] = []
    explain = ""

    # Region filter if user mentions a known region
    region_filter = None
    for r in regions:
        if r and r in s:
            region_filter = r
            break

    # Metric selection
    metric = "revenue" if ("revenue" in s or "sales" in s or "turnover" in s) else ("tickets" if ("ticket" in s or "orders" in s) else "revenue")

    # Intent flags
    wants_trend = ("trend" in s or "over time" in s or "time series" in s or "plot" in s)
    wants_top = ("top" in s or "highest" in s or "max" in s)
    wants_change = ("change" in s or "increase" in s or "decrease" in s or "vs last" in s or "vs previous" in s)

    # K extraction (first number in query)
    k = 5
    for tok in s.split():
        if tok.isdigit():
            k = max(1, min(50, int(tok)))
            break

    # WHERE clause
    where = []
    if region_filter:
        where.append("LOWER(region) = ?")
        params.append(region_filter)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    if wants_trend:
        sql = f"""
        SELECT date, SUM({metric}) AS {metric}
        FROM {tbl}
        {where_sql}
        GROUP BY date
        ORDER BY date ASC
        """
        explain = f"{metric.title()} trend over time" + (f" for region '{region_filter}'" if region_filter else "")
        return {"sql": sql.strip(), "params": tuple(params), "explain": explain}

    if wants_change or "vs" in s:
        sql = f"""
        WITH series AS (
          SELECT date_id, SUM({metric}) AS val
          FROM {tbl}
          {where_sql}
          GROUP BY date_id
          ORDER BY date_id ASC
        )
        SELECT
          (SELECT val FROM series ORDER BY date_id DESC LIMIT 1) AS last_val,
          (SELECT val FROM series ORDER BY date_id DESC LIMIT 1 OFFSET 1) AS prev_val
        """
        explain = f"Last vs previous {metric} percent change" + (f" for region '{region_filter}'" if region_filter else "")
        return {"sql": sql.strip(), "params": tuple(params), "explain": explain}

    if wants_top or "by region" in s:
        sql = f"""
        SELECT region, SUM({metric}) AS total_{metric}
        FROM {tbl}
        GROUP BY region
        HAVING region IS NOT NULL
        ORDER BY total_{metric} DESC
        LIMIT {k}
        """
        explain = f"Top {k} regions by total {metric}"
        return {"sql": sql.strip(), "params": tuple(), "explain": explain}

    # Default: overall total
    sql = f"SELECT SUM({metric}) AS total_{metric} FROM {tbl} {where_sql}"
    explain = f"Total {metric}" + (f" for region '{region_filter}'" if region_filter else "")
    return {"sql": sql.strip(), "params": tuple(params), "explain": explain}

# --------------------------
# Optional LLM Text-to-SQL (safe, validated)
# --------------------------
def _llm_text_to_sql(question: str, meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not (USE_LLM and OPENAI_API_KEY):
        return None
    try:
        import openai  # pip install openai
        openai.api_key = OPENAI_API_KEY
        tbl = meta["table"]
        system = (
            "You are a data analyst that writes only safe, single-statement SELECT SQL for SQLite. "
            f"The table is named '{tbl}' with columns: date_id(INT, YYYYMMDD), date(TEXT, YYYY-MM-DD), "
            "region(TEXT), revenue(REAL), tickets(REAL). "
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
        sql = obj.get("sql", "")
        params = obj.get("params", [])
        if not _is_safe_select(sql):
            return None
        return {"sql": sql, "params": tuple(params), "explain": "LLM-generated SQL"}
    except Exception:
        return None

# --------------------------
# File upload endpoints
# --------------------------
@app.post("/upload-file", response_model=UploadResult)
async def upload_file(file: UploadFile = File(...)):
    content = await file.read()
    try:
        text = content.decode(errors='ignore')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to decode file: {e}")

    dataset_id = str(uuid.uuid4())
    bytes_len = len(content)

    conn = db(); c = conn.cursor()
    c.execute("INSERT INTO datasets (id, filename, bytes, created_at) VALUES (?,?,?,?)",
              (dataset_id, file.filename, bytes_len, datetime.utcnow().isoformat()))
    conn.commit(); conn.close()

    job_id = str(uuid.uuid4())
    conn = db(); c = conn.cursor()
    c.execute("INSERT INTO jobs (id, dataset_id, status, created_at) VALUES (?,?,?,?)",
              (job_id, dataset_id, "queued", datetime.utcnow().isoformat()))
    conn.commit(); conn.close()

    # Process asynchronously
    executor.submit(process_dataset, dataset_id, job_id, text)

    return UploadResult(dataset_id=dataset_id, job_id=job_id, bytes=bytes_len)

@app.post("/upload")
async def begin_processing(payload: Dict[str, Any]):
    # Optional explicit trigger, kept for compatibility
    return {"ok": True}

# --------------------------
# Dataset processing (DATA-DRIVEN INSIGHTS + materialization for grounding)
# --------------------------
def process_dataset(dataset_id: str, job_id: str, text: str):
    conn = db(); c = conn.cursor()
    c.execute("UPDATE jobs SET status=? WHERE id=?", ("processing", job_id))
    conn.commit()

    # Optional: immediate info event
    _publish_from_thread(broker.publish(dataset_id, {"insight": {
        "id": str(uuid.uuid4()), "dataset_id": dataset_id, "type": "info",
        "title": "Dataset received", "description": "Processing has started",
        "confidence": 1.0, "severity": "low", "timestamp": datetime.utcnow().isoformat()
    }}))

    # Parse & materialize per-dataset table for grounded queries
    rows = _parse_csv_rows(text)
    _ensure_dataset_table(conn, c, dataset_id, rows)

    # Compute dynamic insights
    totals = _aggregate_timeseries(rows)
    revenue_series = totals["revenue"]
    tickets_series = totals["tickets"]

    insights: List[Dict[str, Any]] = []

    rev_spike = _compute_revenue_spike_insight(dataset_id, revenue_series)
    if rev_spike:
        insights.append(rev_spike)

    trend = _compute_acquisition_trend_insight(dataset_id, tickets_series)
    if trend:
        insights.append(trend)

    for ins in insights:
        _publish_from_thread(_publish_insight(dataset_id, conn, c, ins))

    c.execute("UPDATE jobs SET status=? WHERE id=?", ("done", job_id))
    conn.commit(); conn.close()

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
        d = dict(row)
        d["timestamp"] = d.pop("ts")
        data.append(d)
    conn.close()
    return {"insights": data}

@app.get("/live-insights/stream")
async def insights_stream(request: Request, dataset_id: str):
    q = await broker.subscribe(dataset_id)

    async def event_gen() -> Generator[str, None, None]:
        try:
            while True:
                if await request.is_disconnected():
                    break
                data = await q.get()
                yield f"data: {json.dumps(data)}\n\n"
        finally:
            broker.unsubscribe(dataset_id, q)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

# --------------------------
# Conversational QA with doc grounding (Heuristic + optional LLM)
# --------------------------
@app.post("/query")
def query(req: QueryRequest):
    # Guard: require dataset
    if not req.dataset_id:
        raise HTTPException(status_code=400, detail="dataset_id is required for grounded answers")

    conn = db(); c = conn.cursor()
    meta = _get_dataset_meta(c, req.dataset_id)
    if not meta["min_date_id"]:
        conn.close()
        raise HTTPException(status_code=404, detail="Dataset table not materialized or empty")

    # 1) Heuristic planner
    plan = _plan_sql_from_query(req.query, meta)

    # 2) Optional LLM fallback (validated)
    if not plan:
        plan = _llm_text_to_sql(req.query, meta)

    # 3) If still no plan, return guidance
    if not plan:
        conn.close()
        msg = ("I couldn't map your question to a dataset query. "
               "Try asking about totals, trends, top regions, or last vs previous for revenue or tickets.")
        _log_chat(req, msg)
        return {"response": msg, "grounding": {"dataset_id": req.dataset_id}}

    sql, params = plan["sql"], plan["params"]
    if not _is_safe_select(sql):
        conn.close()
        raise HTTPException(status_code=400, detail="Unsafe SQL rejected")

    # Execute
    cur = c.execute(sql, params)
    cols = [d[0] for d in cur.description] if cur.description else []
    rows = cur.fetchall()
    row_count = len(rows)

    # Build a natural language answer
    answer = _narrate_answer(req.query, plan, cols, rows, meta)

    # Persist chat
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
        "explain": plan.get("explain")
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

def _fmt_num(x):
    if x is None:
        return "0"
    try:
        x = float(x)
    except:
        return str(x)
    if abs(x) >= 1000:
        return f"{x:,.0f}"
    if abs(x) >= 100:
        return f"{x:,.0f}"
    if abs(x) >= 10:
        return f"{x:,.1f}"
    return f"{x:,.2f}"

def _narrate_answer(q: str, plan: Dict[str, Any], cols: List[str], rows: List[sqlite3.Row], meta: Dict[str, Any]) -> str:
    s = q.lower()
    sql = plan["sql"].lower()

    # last vs previous
    if "last_val" in sql and "prev_val" in sql and rows:
        r = rows[0]
        last_val = r["last_val"]
        prev_val = r["prev_val"]
        if last_val is None or prev_val in (None, 0):
            return "I couldn't compute a valid last vs previous comparison from the data."
        pct = (last_val - prev_val) / prev_val
        direction = "increase" if pct > 0 else "decrease"
        return f"{direction.title()} of {round(abs(pct)*100,1)}% (last {_fmt_num(last_val)} vs previous {_fmt_num(prev_val)})."

    # trend over time
    if "group by date" in sql and rows:
        pts = len(rows)
        if pts == 0:
            return "No time series data found."
        try:
            first_v = float(rows[0][1]); last_v = float(rows[-1][1])
            pct = None if first_v == 0 else (last_v - first_v)/first_v
            if pct is not None:
                dir = "up" if pct > 0 else "down"
                return f"Trend over time: {dir} {round(abs(pct)*100,1)}% from {_fmt_num(first_v)} to {_fmt_num(last_v)} across {pts} periods."
        except Exception:
            pass
        return f"Trend over time with {pts} points."

    # top regions
    if "group by region" in sql and "total_" in sql and rows:
        top_list = []
        metric_col = [c for c in cols if c.startswith("total_")]
        metric_col = metric_col[0] if metric_col else cols[-1]
        for r in rows[:5]:
            top_list.append(f"{r['region']}: {_fmt_num(r[metric_col])}")
        return "Top regions by total: " + ", ".join(top_list)

    # total metric
    if len(rows) == 1 and (cols and cols[0].startswith("total_")):
        return f"{cols[0].replace('total_','').title()} total: {_fmt_num(rows[0][0])}"

    # fallback: preview
    preview = []
    for r in rows[:3]:
        preview.append(", ".join(f"{cols[i]}={_fmt_num(r[i])}" for i in range(len(cols))))
    return ("Here are the results I computed from your data.\n"
            + ("\n".join(preview) if preview else "No rows returned."))

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

# --------------------------
# HITL / RAA
# --------------------------
@app.post("/toggle-hitl")
def toggle_hitl(body: ToggleHITL):
    conn = db(); c = conn.cursor()
    c.execute("UPDATE settings SET v=? WHERE k='hitl'", (json.dumps(body.enabled),))
    conn.commit(); conn.close()
    return {"enabled": body.enabled}

@app.get("/pending-reviews")
def pending_reviews():
    conn = db(); c = conn.cursor()
    rows = [dict(r) for r in c.execute(
        "SELECT id, title, description, confidence, priority, ts FROM reviews ORDER BY ts DESC"
    ).fetchall()]
    reviews = [{
        "id": r["id"], "title": r["title"], "description": r["description"],
        "confidence": r["confidence"], "priority": r["priority"], "timestamp": r["ts"]
    } for r in rows]
    conn.close()
    return {"reviews": reviews}

@app.post("/review-action")
def review_action(body: Dict[str, Any]):
    rid = body.get("review_id")
    action = body.get("action")
    conn = db(); c = conn.cursor()
    c.execute("DELETE FROM reviews WHERE id=?", (rid,))
    conn.commit(); conn.close()
    return {"ok": True, "action": action}

@app.post("/raa")
def trigger_raa():
    conn = db(); c = conn.cursor()
    row = c.execute("SELECT v FROM settings WHERE k='hitl'").fetchone()
    hitl = json.loads(row["v"]) if row else True
    if hitl:
        c.execute("""INSERT INTO reviews (id, title, description, confidence, priority, ts)
                     VALUES (?,?,?,?,?,?)""",
                  (str(uuid.uuid4()), "Revenue anomaly candidate",
                   "Detected unusual behavior—awaiting approval", 0.75, "high",
                   datetime.utcnow().isoformat()))
        conn.commit(); conn.close()
        return {"status": "queued_for_review"}
    else:
        conn.close()
        return {"status": "executed"}

# --------------------------
# Metrics & Agent status
# --------------------------
@app.get("/metrics")
def metrics():
    conn = db(); c = conn.cursor()
    totalUsers = c.execute("SELECT count(*) AS c FROM users").fetchone()["c"]
    filesUploaded = c.execute("SELECT count(*) AS c FROM datasets").fetchone()["c"]
    aiQueries = c.execute("SELECT count(*) AS c FROM chat WHERE role='ai'").fetchone()["c"]
    totalActions = c.execute("SELECT count(*) AS c FROM jobs WHERE status='done'").fetchone()["c"]

    now = datetime.utcnow()
    seven_days_ago = (now - timedelta(days=7)).isoformat()
    row = c.execute("SELECT count(*) AS c FROM users WHERE last_active >= ?", (seven_days_ago,)).fetchone()
    active_users = row["c"] if row else 0
    engagement = round(100.0 * (active_users / totalUsers), 1) if totalUsers > 0 else 0.0

    conn.close()
    return {
        "totalUsers": totalUsers,
        "engagement": engagement,
        "filesUploaded": filesUploaded,
        "aiQueries": aiQueries,
        "totalActions": totalActions
    }

@app.get("/agents/status")
def agent_status():
    return {
        "status": "active",
        "uptime": "99.8%",
        "lastUpdate": datetime.utcnow().isoformat(),
        "connections": 3,
        "processing": 0,
        "alerts": 0
    }

# --------------------------
# Users CRUD (preserved)
# --------------------------
@app.get("/users")
def list_users(order: Optional[str] = None):
    conn = db(); c = conn.cursor()
    rows = [dict(r) for r in c.execute("SELECT id, name, email, role, status, last_active FROM users").fetchall()]
    conn.close()
    return rows

@app.post("/users")
def create_user(body: Dict[str, Any]):
    conn = db(); c = conn.cursor()
    uid = str(uuid.uuid4())
    c.execute("INSERT INTO users (id, name, email, role, status, last_active) VALUES (?,?,?,?,?,?)",
              (uid, body["name"], body["email"], body.get("role","analyst"),
               body.get("status","active"), body.get("last_active", datetime.utcnow().toisoformat() if hasattr(datetime.utcnow(), "toisoformat") else datetime.utcnow().isoformat())))
    conn.commit(); conn.close()
    return {"id": uid, **body}

@app.patch("/users/{uid}")
def update_user(uid: str, patch: Dict[str, Any]):
    sets = ", ".join([f"{k}=?" for k in patch.keys()])
    conn = db(); c = conn.cursor()
    c.execute(f"UPDATE users SET {sets} WHERE id=?", (*patch.values(), uid))
    conn.commit(); conn.close()
    return {"id": uid, **patch}

@app.delete("/users/{uid}")
def delete_user(uid: str):
    conn = db(); c = conn.cursor()
    c.execute("DELETE FROM users WHERE id=?", (uid,))
    conn.commit(); conn.close()
