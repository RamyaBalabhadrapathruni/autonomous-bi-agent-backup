import os
import uuid
import json
import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import numpy as np

from fastapi import FastAPI, UploadFile, File, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, HTTP_500_INTERNAL_SERVER_ERROR

# ---------- App & CORS ----------
app = FastAPI(title="Autonomous BI Agent API", version="1.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],    # tighten for prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.getcwd()
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

DB_PATH = os.path.join(BASE_DIR, "bi_agent.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) if os.path.dirname(DB_PATH) else None

def _ts() -> str:
    return datetime.utcnow().isoformat() + "Z"

def _json_error(status: int, msg: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": msg, "time": _ts()})

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            file_id TEXT PRIMARY KEY,
            filename TEXT,
            saved_path TEXT,
            size INTEGER,
            uploaded_at TEXT
        )
    """)
    conn.commit()
    return conn

DB = _connect()

def save_upload_record(file_id: str, filename: str, saved_path: str) -> None:
    DB.execute(
        "INSERT OR REPLACE INTO uploads (file_id, filename, saved_path, size, uploaded_at) VALUES (?, ?, ?, ?, ?)",
        (file_id, filename, saved_path, os.path.getsize(saved_path) if os.path.exists(saved_path) else 0, _ts())
    )
    DB.commit()

def get_path_by_file_id(file_id: str) -> Optional[str]:
    cur = DB.execute("SELECT saved_path FROM uploads WHERE file_id = ?", (file_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_path_by_filename(filename: str) -> Optional[str]:
    cur = DB.execute("SELECT saved_path FROM uploads WHERE filename = ?", (filename,))
    row = cur.fetchone()
    if row:
        return row[0]
    candidate = os.path.join(UPLOAD_DIR, filename)
    return candidate if os.path.exists(candidate) else None

def resolve_path_from_payload(payload: Dict[str, Any]) -> Optional[str]:
    file_id = payload.get("fileId") or payload.get("dataset_id") or payload.get("datasetId")
    if file_id:
        p = get_path_by_file_id(str(file_id))
        if p and os.path.exists(p):
            return p
    p = payload.get("path") or payload.get("saved_path") or payload.get("savedPath")
    if p and os.path.exists(p):
        return p
    fn = payload.get("filename") or payload.get("name")
    if fn:
        p = get_path_by_filename(str(fn))
        if p and os.path.exists(p):
            return p
    msg = payload.get("upload_message")
    if isinstance(msg, str) and os.path.sep in msg:
        for tok in msg.split():
            if os.path.sep in tok and os.path.exists(tok):
                return tok
    ref = payload.get("ref")
    if isinstance(ref, dict):
        nested = {
            "fileId": ref.get("fileId") or ref.get("dataset_id") or ref.get("datasetId"),
            "path": ref.get("path") or ref.get("saved_path") or ref.get("savedPath"),
            "filename": ref.get("filename") or ref.get("name"),
            "upload_message": ref.get("upload_message"),
        }
        nested = {k: v for k, v in nested.items() if v}
        if nested:
            p = resolve_path_from_payload(nested)
            if p:
                return p
    # Fallback to latest file
    try:
        files = [os.path.join(UPLOAD_DIR, f) for f in os.listdir(UPLOAD_DIR) if os.path.isfile(os.path.join(UPLOAD_DIR, f))]
        files.sort(key=lambda fp: os.path.getmtime(fp), reverse=True)
        return files[0] if files else None
    except Exception:
        return None

# ---------- File I/O ----------
def read_dataframe(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv", ".txt"]:
        try:
            df = pd.read_csv(path)
        except Exception:
            df = pd.read_csv(path, sep=";")
    elif ext in [".parquet", ".pq"]:
        df = pd.read_parquet(path)
    elif ext in [".json"]:
        df = pd.read_json(path)
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

    # Attempt to parse date-like columns
    for c in df.columns:
        if df[c].dtype == "object" and any(tok in c.lower() for tok in ["date", "time", "timestamp"]):
            try:
                df[c] = pd.to_datetime(df[c], errors="coerce")
            except Exception:
                pass
    return df

def pick_date_column(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        if np.issubdtype(df[c].dtype, np.datetime64):
            return c
    # heuristic: try to parse any column named like date/time
    for c in df.columns:
        if any(tok in c.lower() for tok in ["date", "time", "timestamp"]):
            try:
                pd.to_datetime(df[c], errors="raise")
                return c
            except Exception:
                continue
    return None

def robust_z_scores(x: pd.Series) -> pd.Series:
    """Robust Z using MAD; ignores NaNs."""
    med = np.nanmedian(x.values)
    mad = np.nanmedian(np.abs(x.values - med))
    if mad == 0 or np.isnan(mad):
        return pd.Series(np.zeros(len(x)), index=x.index)
    return 0.6745 * (x - med) / mad

def detect_univariate_anomalies(df: pd.DataFrame, top_k: int = 5) -> List[Dict[str, Any]]:
    out = []
    numeric_cols = [c for c in df.columns if np.issubdtype(df[c].dtype, np.number)]
    date_col = pick_date_column(df)

    for col in numeric_cols:
        z = robust_z_scores(df[col].astype(float))
        # threshold 3.5 ~ classical robust cutoff
        mask = np.abs(z) >= 3.5
        if not mask.any():
            continue
        idx = np.argsort(-np.abs(z.values))[:top_k]
        for i in idx:
            if not mask.iloc[i]:
                continue
            sev = "high" if abs(z.iloc[i]) >= 5 else ("medium" if abs(z.iloc[i]) >= 4.25 else "low")
            title = f"Outlier in {col}"
            when = df[date_col].iloc[i].isoformat() if date_col and pd.notna(df[date_col].iloc[i]) else "N/A"
            desc = f"Value {df[col].iloc[i]:,.4g} deviates from median by {abs(z.iloc[i]):.1f}σ (robust)."
            out.append({
                "id": str(uuid.uuid4()),
                "type": "anomaly",
                "severity": sev,
                "title": title,
                "description": desc,
                "confidence": int(min(99, 70 + min(30, 6 * (abs(z.iloc[i]) - 3.5)))),  # simple mapping
                "timestamp": _ts(),
                "reasoning": f"Computed robust Z using MAD for column '{col}'. Threshold 3.5. Observation at {when} exceeded threshold with z={z.iloc[i]:.2f}.",
                "evidence": [
                    {"metric": "column", "value": col},
                    {"metric": "value", "value": float(df[col].iloc[i]) if pd.notna(df[col].iloc[i]) else None},
                    {"metric": "robust_z", "value": float(z.iloc[i])},
                    {"metric": "when", "value": when},
                ],
            })
    return out[:top_k]

def detect_emerging_changes(df: pd.DataFrame, window: int = 7, top_k: int = 5) -> List[Dict[str, Any]]:
    out = []
    date_col = pick_date_column(df)
    if not date_col:
        return out
    numeric_cols = [c for c in df.columns if np.issubdtype(df[c].dtype, np.number)]
    if not numeric_cols:
        return out

    # aggregate by date (daily frequency)
    g = df.copy()
    g = g.dropna(subset=[date_col])
    if g.empty:
        return out
    g["_date"] = pd.to_datetime(g[date_col]).dt.normalize()
    agg = g.groupby("_date")[numeric_cols].sum().sort_index()

    if len(agg) < (2 * window):
        return out

    last = agg.tail(window).mean()
    prev = agg.tail(2 * window).head(window).mean()
    change = (last - prev) / (np.where(prev == 0, np.nan, prev))
    change = change.replace([np.inf, -np.inf], np.nan).dropna().sort_values(ascending=False)

    for col, pct in change.head(top_k).items():
        sev = "high" if abs(pct) >= 0.5 else ("medium" if abs(pct) >= 0.2 else "low")
        direction = "increase" if pct >= 0 else "decrease"
        out.append({
            "id": str(uuid.uuid4()),
            "type": "anomaly",
            "severity": sev,
            "title": f"Emerging {direction} in {col}",
            "description": f"Last {window} periods vs previous {window}: {pct*100:.1f}% {direction}.",
            "confidence": int(80 + min(19, abs(pct) * 40)),  # 80-99
            "timestamp": _ts(),
            "reasoning": f"Aggregated {col} by date; compared mean of last {window} periods to the prior {window}. "
                         f"Computed percent change = (last - prev) / prev.",
            "evidence": [
                {"metric": "column", "value": col},
                {"metric": "window", "value": window},
                {"metric": "pct_change", "value": float(pct)},
                {"metric": "last_mean", "value": float(last[col])},
                {"metric": "prev_mean", "value": float(prev[col])},
            ],
        })
    return out

def detect_correlations(df: pd.DataFrame, r_threshold: float = 0.7, top_k: int = 5) -> List[Dict[str, Any]]:
    out = []
    numeric_cols = [c for c in df.columns if np.issubdtype(df[c].dtype, np.number)]
    if len(numeric_cols) < 2:
        return out
    corr = df[numeric_cols].corr(method="pearson")
    pairs = []
    for i, c1 in enumerate(numeric_cols):
        for j, c2 in enumerate(numeric_cols):
            if j <= i:
                continue
            r = corr.loc[c1, c2]
            if pd.notna(r) and abs(r) >= r_threshold:
                pairs.append((c1, c2, r))
    pairs.sort(key=lambda t: abs(t[2]), reverse=True)
    for (c1, c2, r) in pairs[:top_k]:
        sev = "high" if abs(r) >= 0.9 else ("medium" if abs(r) >= 0.8 else "low")
        out.append({
            "id": str(uuid.uuid4()),
            "type": "pattern",
            "severity": sev,
            "title": f"Strong {'positive' if r > 0 else 'negative'} correlation: {c1} vs {c2}",
            "description": f"Pearson r = {r:.2f} between {c1} and {c2}.",
            "confidence": int(85 + min(14, (abs(r) - 0.7) * 50)),  # 85-99
            "timestamp": _ts(),
            "reasoning": f"Computed Pearson correlation across numeric columns; kept |r|≥{r_threshold}.",
            "evidence": [
                {"metric": "col_x", "value": c1},
                {"metric": "col_y", "value": c2},
                {"metric": "pearson_r", "value": float(r)},
            ],
        })
    return out

def detect_category_patterns(df: pd.DataFrame, target: Optional[str] = None, top_k: int = 5) -> List[Dict[str, Any]]:
    out = []
    numeric_cols = [c for c in df.columns if np.issubdtype(df[c].dtype, np.number)]
    cat_cols = [c for c in df.columns if not np.issubdtype(df[c].dtype, np.number)]
    if not numeric_cols or not cat_cols:
        return out
    tgt = target or numeric_cols[0]
    for cat in cat_cols:
        try:
            s = df.groupby(cat)[tgt].sum().sort_values(ascending=False).head(top_k)
            total = s.sum()
            for k, v in s.items():
                share = 0 if total == 0 else v / total
                sev = "high" if share >= 0.5 else ("medium" if share >= 0.25 else "low")
                out.append({
                    "id": str(uuid.uuid4()),
                    "type": "pattern",
                    "severity": sev,
                    "title": f"Top contributor in {cat}: {k}",
                    "description": f"{k} accounts for {share*100:.1f}% of total {tgt}.",
                    "confidence": int(80 + min(19, share * 100 * 0.2)),  # 80-99
                    "timestamp": _ts(),
                    "reasoning": f"Grouped by '{cat}', summed '{tgt}', ranked by contribution share.",
                    "evidence": [
                        {"metric": "category_col", "value": cat},
                        {"metric": "category", "value": str(k)},
                        {"metric": "target", "value": tgt},
                        {"metric": "share", "value": float(share)},
                        {"metric": "sum_value", "value": float(v)},
                    ],
                })
        except Exception:
            continue
    return out[:top_k]

def generate_insights(df: pd.DataFrame, insight_type: str = "auto", top_k: int = 5, window: int = 7) -> List[Dict[str, Any]]:
    """
    Main orchestrator for insights.
    """
    df = df.copy()
    # try to coerce numeric
    for c in df.columns:
        if df[c].dtype == "object":
            # attempt numeric coercion without destroying category cols too aggressively
            try:
                df[c] = pd.to_numeric(df[c], errors="ignore")
            except Exception:
                pass

    res: List[Dict[str, Any]] = []
    if insight_type in ("auto", "anomalies"):
        res += detect_univariate_anomalies(df, top_k=top_k)
        res += detect_emerging_changes(df, window=window, top_k=top_k)
    if insight_type in ("auto", "patterns"):
        res += detect_correlations(df, r_threshold=0.7, top_k=top_k)
        res += detect_category_patterns(df, target=None, top_k=top_k)
    # sort: anomalies first (high → low), then patterns
    order_rank = {"high": 0, "medium": 1, "low": 2}
    res.sort(key=lambda x: (0 if x["type"] == "anomaly" else 1, order_rank.get(x["severity"], 3)))
    return res[: max(top_k, 5)]

@app.get("/health")
def health():
    return {"status": "ok", "time": _ts(), "service": "autonomous-bi-agent"}

@app.post("/upload-data")
async def upload_data(file: UploadFile = File(...)):
    if not file or not file.filename:
        return _json_error(HTTP_400_BAD_REQUEST, "No file provided")
    saved_path = os.path.join(UPLOAD_DIR, file.filename)
    try:
        with open(saved_path, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
    except Exception as e:
        return _json_error(HTTP_500_INTERNAL_SERVER_ERROR, f"Failed to save file: {e}")
    file_id = str(uuid.uuid4())
    save_upload_record(file_id, file.filename, saved_path)
    return JSONResponse(
        {"fileId": file_id, "filename": file.filename, "saved_path": saved_path, "size": os.path.getsize(saved_path), "uploaded_at": _ts()},
        status_code=HTTP_200_OK,
    )

@app.post("/query")
async def query(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    path = resolve_path_from_payload(payload)
    if not path or not os.path.exists(path):
        return _json_error(HTTP_400_BAD_REQUEST, "Could not resolve uploaded file. Provide fileId/path/filename or upload first.")
    try:
        df = read_dataframe(path)
    except Exception as e:
        return _json_error(HTTP_500_INTERNAL_SERVER_ERROR, f"Failed to read file: {e}")

    insight_type = (payload.get("insightType") or "auto").lower()
    top_k = int(payload.get("topK") or 7)
    window = int(payload.get("window") or 7)

    try:
        insights = generate_insights(df, insight_type=insight_type, top_k=top_k, window=window)
        return JSONResponse(insights, status_code=HTTP_200_OK)
    except Exception as e:
        return _json_error(HTTP_500_INTERNAL_SERVER_ERROR, f"Analysis error: {e}")

@app.get("/agent-feed")
def agent_feed():
    now = _ts()
    agents = [
        {
            "id": 1, "name": "Conversation Agent", "type": "NLP / Q&A", "status": "online",
            "uptime": "3h 42m", "currentTask": "Intent parsing / response drafting",
            "current_task": "Intent parsing / response drafting",
            "tasksCompleted": 127, "tasks_completed": 127, "performanceRate": 95, "performance_rate": 95,
            "lastActive": now, "last_active": now, "confidence": 93, "precision": 90,
            "anomalyScore": 0.08, "anomaly_score": 0.08, "human_in_loop": False
        },
        {
            "id": 2, "name": "Insights Agent", "type": "Pattern Detection / Summary", "status": "processing",
            "uptime": "1h 12m", "currentTask": "Generating insights for latest upload",
            "current_task": "Generating insights for latest upload",
            "tasksCompleted": 48, "tasks_completed": 48, "performanceRate": 91, "performance_rate": 91,
            "lastActive": now, "last_active": now, "confidence": 90, "precision": 88,
            "anomalyScore": 0.21, "anomaly_score": 0.21, "human_in_loop": True
        },
        {
            "id": 3, "name": "Revenue Analysis Agent (RAA)", "type": "KPI / Trend", "status": "online",
            "uptime": "6h 05m", "currentTask": "Scanning revenue anomalies",
            "current_task": "Scanning revenue anomalies",
            "tasksCompleted": 73, "tasks_completed": 73, "performanceRate": 94, "performance_rate": 94,
            "lastActive": now, "last_active": now, "confidence": 92, "precision": 89,
            "anomalyScore": 0.15, "anomaly_score": 0.15, "human_in_loop": False
        },
        {
            "id": 4, "name": "Alert Generation Agent", "type": "Notification", "status": "error",
            "uptime": "N/A", "currentTask": "SMTP connection retry", "current_task": "SMTP connection retry",
            "tasksCompleted": 0, "tasks_completed": 0, "performanceRate": 0, "performance_rate": 0,
            "lastActive": now, "last_active": now, "confidence": 0, "precision": 0,
            "anomalyScore": 0.98, "anomaly_score": 0.98, "human_in_loop": True
        },
        {
            "id": 5, "name": "Data Pipeline Agent", "type": "Aggregation", "status": "offline",
            "uptime": "—", "currentTask": "Idle", "current_task": "Idle",
            "tasksCompleted": 256, "tasks_completed": 256, "performanceRate": 99, "performance_rate": 99,
            "lastActive": now, "last_active": now, "confidence": 99, "precision": 99,
            "anomalyScore": 0.05, "anomaly_score": 0.05, "human_in_loop": False
        },
    ]
    return JSONResponse(agents, status_code=HTTP_200_OK)

@app.get("/agent-status")
def agent_status_summary():
    data = agent_feed().body
    try:
        agents = json.loads(data)
    except Exception:
        agents = []
    total = len(agents)
    active = sum(1 for a in agents if a.get("status") in ("online", "processing"))
    tasks = sum(int(a.get("tasksCompleted") or a.get("tasks_completed") or 0) for a in agents)
    avg_perf = 0
    if total:
        avg_perf = round(sum(int(a.get("performanceRate") or a.get("performance_rate") or 0) for a in agents) / total)
    return JSONResponse({"totalAgents": total, "activeAgents": active, "totalTasks": tasks, "avgSuccessRate": avg_perf, "time": _ts()}, status_code=HTTP_200_OK)

@app.post("/review-action")
async def review_action(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    decision_id = body.get("decisionId")
    action = body.get("action")
    reason = body.get("reason", "")
    if not decision_id or action not in {"approved", "rejected", "modified"}:
        return _json_error(HTTP_400_BAD_REQUEST, "Invalid decision payload")
    return JSONResponse({"status": "ok", "decisionId": decision_id, "action": action, "reason": reason, "processed_at": _ts()}, status_code=HTTP_200_OK)

@app.get("/metrics")
def metrics():
    try:
        import platform
        sys_info = {"pythonVersion": platform.python_version(), "platform": platform.platform()}
    except Exception:
        sys_info = {}
    data = agent_feed().body
    try:
        agents = json.loads(data)
    except Exception:
        agents = []
    total = len(agents)
    active = sum(1 for a in agents if a.get("status") in ("online", "processing"))
    return JSONResponse({"service": "autonomous-bi-agent", "time": _ts(), "agents": {"total": total, "active": active}, "system": sys_info}, status_code=HTTP_200_OK)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))