# app.py — Autonomous BI Agent API (Gemini + FastAPI + Render-ready)

import os, io, csv, json, uuid, time, asyncio, math, statistics, sqlite3, re
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Generator, Optional, List, Tuple

from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from dotenv import load_dotenv

import smtplib
from email.mime.text import MIMEText
import google.generativeai as genai

# Load environment variables
load_dotenv()

# --------------------------
# Config & Tunables
# --------------------------
DB_PATH = os.getenv("BI_DB_PATH", "/tmp/bi_agent.db")  # Render-compatible path

ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
ALLOW_ORIGIN_REGEX = os.getenv("ALLOW_ORIGIN_REGEX")  # optional
ALLOW_CREDENTIALS = os.getenv("ALLOW_CREDENTIALS", "false").lower() == "true"

cors_common = dict(allow_methods=["*"], allow_headers=["*"], expose_headers=["*"], max_age=86400)

if ALLOW_ORIGIN_REGEX:
    app.add_middleware(CORSMiddleware, allow_origin_regex=ALLOW_ORIGIN_REGEX, allow_credentials=False, **cors_common)
else:
    app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_credentials=ALLOW_CREDENTIALS, **cors_common)


REVENUE_SPIKE_Z = float(os.getenv("REVENUE_SPIKE_Z", "1.0"))
REVENUE_SPIKE_MIN_PCT = float(os.getenv("REVENUE_SPIKE_MIN_PCT", "0"))

TREND_MIN_POINTS = int(os.getenv("TREND_MIN_POINTS", "3"))
TREND_WINDOW = int(os.getenv("TREND_WINDOW", "3"))

SEVERITY_MED_PCT = float(os.getenv("SEVERITY_MED_PCT", "0.15"))
SEVERITY_HIGH_PCT = float(os.getenv("SEVERITY_HIGH_PCT", "0.30"))

HITL_DEFAULT = os.getenv("HITL_DEFAULT", "false").lower() == "true"

USE_LLM = os.getenv("USE_LLM", "false").lower() == "true"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

MAILTRAP_HOST = os.getenv("MAILTRAP_HOST", "smtp.mailtrap.io")
MAILTRAP_PORT = int(os.getenv("MAILTRAP_PORT", "587"))
MAILTRAP_USERNAME = os.getenv("MAILTRAP_USERNAME")
MAILTRAP_PASSWORD = os.getenv("MAILTRAP_PASSWORD")
MAILTRAP_FROM = os.getenv("MAILTRAP_FROM", "alerts@example.com")
ALERT_RECIPIENTS = [e.strip() for e in os.getenv("ALERT_RECIPIENTS", "").split(",") if e.strip()]
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")  # Optional — safe to omit

# Gemini setup
if USE_LLM and GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

# --------------------------
# App & CORS
# --------------------------

app = FastAPI(
    title="Autonomous BI Agent API",
    docs_url="/docs",
    redoc_url=None,
    openapi_url="/openapi.json",
    redirect_slashes=False,  
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS or ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],


executor = ThreadPoolExecutor(max_workers=4)
main_loop: Optional[asyncio.AbstractEventLoop] = None

@app.on_event("startup")
async def on_startup():
    global main_loop
    main_loop = asyncio.get_running_loop()
    init_db()
# --------------------------
# Database Setup
# --------------------------
def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db(); c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT)""")
    c.execute("""INSERT OR IGNORE INTO settings (k, v) VALUES ('hitl', ?)""", (json.dumps(HITL_DEFAULT),))
    c.execute("""CREATE TABLE IF NOT EXISTS datasets (
        id TEXT PRIMARY KEY,
        filename TEXT,
        bytes INTEGER,
        created_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        dataset_id TEXT,
        status TEXT,
        created_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS insights (
        id TEXT PRIMARY KEY,
        dataset_id TEXT,
        type TEXT,
        title TEXT,
        description TEXT,
        confidence REAL,
        severity TEXT,
        ts TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        name TEXT,
        email TEXT,
        role TEXT,
        status TEXT,
        last_active TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS chat (
        id TEXT PRIMARY KEY,
        session_id TEXT,
        dataset_id TEXT,
        role TEXT,
        message TEXT,
        ts TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS reviews (
        id TEXT PRIMARY KEY,
        title TEXT,
        description TEXT,
        confidence REAL,
        priority TEXT,
        ts TEXT
    )""")
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
# SSE Broker (Live Stream)
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
# Pydantic Models
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
# Analytics & Parsing Helpers
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
        y = int(parts[1])
        return datetime(y, MONTHS[parts[0].lower()], 1)
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(st, fmt)
            if "%Y" not in fmt and ref_year:
                dt = dt.replace(year=ref_year)
            return dt
        except Exception:
            continue
    return None

def _parse_csv_rows(text: str) -> List[Dict[str, Any]]:
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

        rows.append({
            "region": row.get("region"),
            "date": dt,
            "revenue": revenue,
            "tickets": tickets,
            **{k: v for k, v in row.items() if k not in ("region", "date", "week", "revenue", "tickets")}
        })
    return rows

def _aggregate_timeseries(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by_date: Dict[datetime, Dict[str, float]] = {}
    for r in rows:
        dt = r.get("date")
        if not dt:
            continue
        acc = by_date.setdefault(dt, {"revenue": 0.0, "tickets": 0.0})
        if r.get("revenue") is not None:
            acc["revenue"] += float(r["revenue"])
        if r.get("tickets") is not None:
            acc["tickets"] += float(r["tickets"])

    dates_sorted = sorted(by_date.keys())
    revenue_series = [{"date": d, "value": by_date[d]["revenue"]} for d in dates_sorted]
    tickets_series = [{"date": d, "value": by_date[d]["tickets"]} for d in dates_sorted]
    return {"revenue": revenue_series, "tickets": tickets_series}

def _percent_change(new: float, old: float) -> Optional[float]:
    if old is None or old == 0:
        return None
    return (new - old) / old

def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

def _severity_from_pct(pct: float) -> str:
    abs_pct = abs(pct)
    if abs_pct >= SEVERITY_HIGH_PCT:
        return "high"
    if abs_pct >= SEVERITY_MED_PCT:
        return "medium"
    return "low"
# --------------------------
# Dataset Upload & Processing
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

    executor.submit(process_dataset, dataset_id, job_id, text)
    return UploadResult(dataset_id=dataset_id, job_id=job_id, bytes=bytes_len)

def process_dataset(dataset_id: str, job_id: str, text: str):
    conn = db(); c = conn.cursor()
    c.execute("UPDATE jobs SET status=? WHERE id=?", ("processing", job_id))
    conn.commit()

    _publish_from_thread(broker.publish(dataset_id, {"insight": {
        "id": str(uuid.uuid4()), "dataset_id": dataset_id, "type": "info",
        "title": "Dataset received", "description": "Processing has started",
        "confidence": 1.0, "severity": "low", "timestamp": datetime.utcnow().isoformat()
    }}))

    rows = _parse_csv_rows(text)
    _ensure_dataset_table_generic(conn, c, dataset_id, rows)

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

def _publish_from_thread(coro):
    asyncio.run_coroutine_threadsafe(coro, main_loop)

def _publish_insight(dataset_id: str, conn, c, ins: Dict[str, Any]):
    ins["timestamp"] = ins.pop("ts", datetime.utcnow().isoformat())
    c.execute("""INSERT INTO insights (id, dataset_id, type, title, description, confidence, severity, ts)
                 VALUES (?,?,?,?,?,?,?,?)""",
              (ins["id"], ins["dataset_id"], ins["type"], ins["title"], ins["description"],
               ins["confidence"], ins["severity"], ins["timestamp"]))
    conn.commit()
    return broker.publish(dataset_id, {"insight": ins})

# --------------------------
# LiveInsight REST & Stream
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
                if await request.is_disconnected():
                    break
                data = await q.get()
                yield f"data: {json.dumps(data)}\n\n"
        finally:
            broker.unsubscribe(dataset_id, q)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"}
    )
# --------------------------
# Conversational QA (Gemini + SQL)
# --------------------------
def _is_safe_select(sql: str) -> bool:
    s = sql.strip().upper()
    if not s.startswith(("WITH", "SELECT", "PRAGMA TABLE_INFO")):
        return False
    for bad in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "ATTACH", "DETACH", "CREATE", "REPLACE", ";--"):
        if bad in s:
            return False
    if ";" in s.strip()[:-1]:
        return False
    return True

def _llm_text_to_sql(question: str, meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not (USE_LLM and GEMINI_API_KEY):
        return None
    try:
        sch = meta["schema"]
        tbl = sch["table"]
        prompt = (
            f"You are a data analyst writing safe, single-statement SELECT SQL for SQLite.\n"
            f"The table is named '{tbl}' with columns: {', '.join(sch['col_map'].values())}.\n"
            "Never modify data. Return JSON as {\"sql\": \"...\", \"params\": []}.\n"
            f"Question: {question}"
        )
        model = genai.GenerativeModel("gemini-pro")
        response = model.generate_content(prompt)
        txt = response.text.strip()
        obj = json.loads(txt)
        sql = obj.get("sql", "")
        params = obj.get("params", [])
        if not _is_safe_select(sql):
            return None
        return {"sql": sql, "params": tuple(params), "explain": "LLM-generated SQL", "where_desc": ""}
    except Exception as e:
        print(f"Gemini error: {e}")
        return None

@app.post("/query")
def query(req: QueryRequest):
    if not req.dataset_id:
        raise HTTPException(status_code=400, detail="dataset_id is required for grounded answers")

    conn = db(); c = conn.cursor()
    meta = _get_dataset_meta(c, req.dataset_id)
    if not meta["schema"]:
        conn.close()
        raise HTTPException(status_code=404, detail="Dataset schema missing")

    plan = _llm_text_to_sql(req.query, meta)
    if not plan:
        conn.close()
        msg = "I couldn't map your question to a dataset query. Try asking about totals, trends, or comparisons."
        _log_chat(req, msg)
        return {"response": msg, "grounding": {"dataset_id": req.dataset_id}}

    sql, params = plan["sql"], plan["params"]
    if not _is_safe_select(sql):
        conn.close()
        raise HTTPException(status_code=400, detail="Unsafe SQL rejected")

    cur = c.execute(sql, params)
    cols = [d[0] for d in cur.description] if cur.description else []
    rows = cur.fetchall()
    row_count = len(rows)

    answer = f"Query executed successfully. Returned {row_count} rows."
    _log_chat(req, answer)

    grounding = {
        "dataset_id": req.dataset_id,
        "sql": sql,
        "params": list(params),
        "row_count": row_count,
        "explain": plan.get("explain"),
        "where_desc": plan.get("where_desc")
    }
    conn.close()
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
# Chat Logs REST
# --------------------------
@app.get("/chat-messages")
def list_chat(session_id: Optional[str] = None, dataset_id: Optional[str] = None):
    conn = db(); c = conn.cursor()
    q = "SELECT id, session_id, dataset_id, role as type, message, ts as timestamp FROM chat WHERE 1=1"
    params: List[Any] = []
    if session_id:
        q += " AND session_id=?"
        params.append(session_id)
    if dataset_id:
        q += " AND dataset_id=?"
        params.append(dataset_id)
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
# Revenue Spike Detection
# --------------------------
def _compute_revenue_spike_insight(dataset_id: str, series: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if len(series) < 3:
        return None
    values = [r["value"] for r in series if r["value"] is not None]
    if len(values) < 3:
        return None
    mean = statistics.fmean(values)
    stdev = statistics.stdev(values)
    last = values[-1]
    z = (last - mean) / stdev if stdev > 0 else 0
    pct = _percent_change(last, values[-2]) if len(values) >= 2 else 0
    if z >= REVENUE_SPIKE_Z and abs(pct) >= REVENUE_SPIKE_MIN_PCT:
        return {
            "id": str(uuid.uuid4()),
            "dataset_id": dataset_id,
            "type": "revenue_spike",
            "title": "Revenue spike detected",
            "description": f"Latest revenue is {last:.2f}, which is {z:.2f} standard deviations above the mean.",
            "confidence": _normal_cdf(z),
            "severity": _severity_from_pct(pct),
            "ts": datetime.utcnow().isoformat()
        }
    return None

# --------------------------
# Acquisition Trend Detection
# --------------------------
def _compute_acquisition_trend_insight(dataset_id: str, series: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if len(series) < TREND_MIN_POINTS:
        return None
    x = list(range(len(series)))
    y = [r["value"] for r in series]
    if any(v is None for v in y):
        return None
    mx, my = statistics.fmean(x), statistics.fmean(y)
    cov = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    vx = sum((xi - mx) ** 2 for xi in x)
    slope = cov / vx if vx > 0 else 0
    r2 = (cov ** 2) / (vx * sum((yi - my) ** 2 for yi in y)) if vx > 0 else 0
    if abs(slope) < 1e-3:
        return None
    direction = "upward" if slope > 0 else "downward"
    return {
        "id": str(uuid.uuid4()),
        "dataset_id": dataset_id,
        "type": "trend",
        "title": "Acquisition trend detected",
        "description": f"Tickets show a {direction} trend with slope {slope:.2f} and R²={r2:.2f}.",
        "confidence": min(1.0, abs(slope) * 10),
        "severity": "medium",
        "ts": datetime.utcnow().isoformat()
    }

# --------------------------
# Dataset Table Creation
# --------------------------
def _ensure_dataset_table_generic(conn, c, dataset_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        raise HTTPException(status_code=400, detail="No rows parsed from uploaded file.")
    sample = rows[0]
    columns = sample.keys()
    col_types = {}
    for col in columns:
        values = [r[col] for r in rows if r.get(col) is not None]
        if all(isinstance(v, (int, float)) for v in values):
            col_types[col] = "REAL"
        else:
            col_types[col] = "TEXT"

    table_name = f"ds_{dataset_id.replace('-', '')}"
    ddl = ", ".join(f"{col} {col_types[col]}" for col in columns)
    c.execute(f"DROP TABLE IF EXISTS {table_name}")
    c.execute(f"CREATE TABLE {table_name} ({ddl})")

    placeholders = ",".join(["?"] * len(columns))
    values = [[r.get(col) for col in columns] for r in rows]
    c.executemany(f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})", values)

    schema_json = json.dumps({
        "table": table_name,
        "col_map": {col: col for col in columns},
        "measures": [col for col in columns if col_types[col] == "REAL"],
        "dimensions": [col for col in columns if col_types[col] == "TEXT"]
    })
    c.execute("INSERT OR REPLACE INTO dataset_schema (dataset_id, schema_json) VALUES (?, ?)", (dataset_id, schema_json))
    conn.commit()
    return json.loads(schema_json)

def _get_dataset_meta(c, dataset_id: str) -> Dict[str, Any]:
    row = c.execute("SELECT schema_json FROM dataset_schema WHERE dataset_id=?", (dataset_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Dataset schema not found")
    schema = json.loads(row["schema_json"])
    return {"schema": schema, "table": schema["table"]}

@app.get("/ping")
def ping():
    return {"status": "ok"}
