# app.py (FastAPI)
import os, io, csv, json, uuid, time, asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Generator, Optional, List

from fastapi import FastAPI, UploadFile, File, Form, Request, Response, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import sqlite3

DB_PATH = os.getenv("BI_DB_PATH", "bi_agent.db")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
HITL_DEFAULT = os.getenv("HITL_DEFAULT", "false").lower() == "true"

app = FastAPI(title="Autonomous BI Agent API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

executor = ThreadPoolExecutor(max_workers=4)

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

init_db()

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
# File upload endpoints
# --------------------------
@app.post("/upload-file")
async def upload_file(file: UploadFile = File(...)):
  content = await file.read()
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

  # process in background
  loop = asyncio.get_running_loop()
  loop.run_in_executor(executor, process_dataset, dataset_id, job_id, content.decode(errors='ignore'))

  return UploadResult(dataset_id=dataset_id, job_id=job_id, bytes=bytes_len)

@app.post("/upload")
async def begin_processing(payload: Dict[str, Any]):
  # If you prefer explicit trigger separate from upload-file
  return {"ok": True}

def process_dataset(dataset_id: str, job_id: str, text: str):
  # naive CSV detection + compute toy insights (replace with your logic)
  conn = db(); c = conn.cursor()
  c.execute("UPDATE jobs SET status=? WHERE id=?", ("processing", job_id)); conn.commit()

  # emit an initial insight
  asyncio.run(broker.publish(dataset_id, {"insight": {
    "id": str(uuid.uuid4()), "dataset_id": dataset_id, "type": "info",
    "title": "Dataset received", "description": "Processing has started",
    "confidence": 1.0, "severity": "low", "timestamp": datetime.utcnow().isoformat()
  }}))

  # Simulate work & generate example insights
  time.sleep(1.0)
  ins = [
    ("anomaly", "Revenue Spike Detected", "15% increase vs prior week", 0.87, "medium"),
    ("trend", "Customer Acquisition Trending Up", "30% increase past 3 days", 0.92, "low"),
  ]
  for typ, title, desc, conf, sev in ins:
    iid = str(uuid.uuid4()); ts = datetime.utcnow().isoformat()
    c.execute("""INSERT INTO insights (id, dataset_id, type, title, description, confidence, severity, ts)
                 VALUES (?,?,?,?,?,?,?,?)""", (iid, dataset_id, typ, title, desc, conf, sev, ts))
    conn.commit()
    asyncio.run(broker.publish(dataset_id, {"insight": {
      "id": iid, "type": typ, "title": title, "description": desc, "confidence": conf,
      "severity": sev, "timestamp": ts
    }}))
    time.sleep(0.6)

  c.execute("UPDATE jobs SET status=? WHERE id=?", ("done", job_id)); conn.commit(); conn.close()

# --------------------------
# Insights
# --------------------------
@app.get("/live-insights")
def get_insights(dataset_id: str):
  conn = db(); c = conn.cursor()
  cur = c.execute("SELECT id, dataset_id, type, title, description, confidence, severity, ts FROM insights WHERE dataset_id=? ORDER BY ts DESC", (dataset_id,))
  data = [dict(row) | {"timestamp": row["ts"]} for row in cur.fetchall()]
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

  return StreamingResponse(event_gen(), media_type="text/event-stream")

# --------------------------
# Conversational AI
# --------------------------
@app.post("/query")
def query(req: QueryRequest):
  # TODO: plug in your LLM with dataset context
  answer = f"I received your query: '{req.query}'. I will analyze the currently active dataset and report back."
  conn = db(); c = conn.cursor()
  now = datetime.utcnow().isoformat()
  c.execute("INSERT INTO chat (id, session_id, dataset_id, role, message, ts) VALUES (?,?,?,?,?,?)",
            (str(uuid.uuid4()), req.session_id, req.dataset_id, "user", req.query, now))
  c.execute("INSERT INTO chat (id, session_id, dataset_id, role, message, ts) VALUES (?,?,?,?,?,?)",
            (str(uuid.uuid4()), req.session_id, req.dataset_id, "ai", answer, now))
  conn.commit(); conn.close()
  return {"response": answer}

@app.get("/chat-messages")
def list_chat(session_id: Optional[str] = None, dataset_id: Optional[str] = None):
  conn = db(); c = conn.cursor()
  q = "SELECT id, session_id, dataset_id, role as type, message, ts as timestamp FROM chat WHERE 1=1"
  params = []
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
            (str(uuid.uuid4()), msg.get("session_id"), msg.get("dataset_id"), msg.get("type"), msg.get("message"), datetime.utcnow().isoformat()))
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
  rows = [dict(r) for r in c.execute("SELECT id, title, description, confidence, priority, ts FROM reviews ORDER BY ts DESC").fetchall()]
  # shape for UI
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
  # Log the decision elsewhere if needed
  return {"ok": True, "action": action}

@app.post("/raa")
def trigger_raa():
  # auto-create sample review (if HITL enabled) or auto-execute
  conn = db(); c = conn.cursor()
  # read HITL
  row = c.execute("SELECT v FROM settings WHERE k='hitl'").fetchone()
  hitl = json.loads(row["v"]) if row else True
  if hitl:
    c.execute("INSERT INTO reviews (id, title, description, confidence, priority, ts) VALUES (?,?,?,?,?,?)",
              (str(uuid.uuid4()), "High Revenue Anomaly Alert", "Detected 15% spike—needs approval", 0.89, "high", datetime.utcnow().isoformat()))
    conn.commit(); conn.close()
    return {"status": "queued_for_review"}
  else:
    # perform the action now (placeholder)
    conn.close()
    return {"status": "executed"}
  
# --------------------------
# Users & Metrics & Status
# --------------------------
@app.get("/metrics")
def metrics():
  conn = db(); c = conn.cursor()
  totalUsers = c.execute("SELECT count(*) AS c FROM users").fetchone()["c"]
  filesUploaded = c.execute("SELECT count(*) AS c FROM datasets").fetchone()["c"]
  aiQueries = c.execute("SELECT count(*) AS c FROM chat WHERE role='ai'").fetchone()["c"]
  totalActions = c.execute("SELECT count(*) AS c FROM jobs WHERE status='done'").fetchone()["c"]
  # TODO: compute real engagement
  result = {"totalUsers": totalUsers, "engagement": 94.2, "filesUploaded": filesUploaded, "aiQueries": aiQueries, "totalActions": totalActions}
  conn.close()
  return result

@app.get("/agents/status")
def agent_status():
  # Replace with real signals from your workers
  return {"status": "active", "uptime": "99.8%", "lastUpdate": datetime.utcnow().isoformat(), "connections": 3, "processing": 0, "alerts": 0}

# Users CRUD for BIUser
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
            (uid, body["name"], body["email"], body.get("role","analyst"), body.get("status","active"), body.get("last_active", datetime.utcnow().isoformat())))
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
  return {"ok": True}
