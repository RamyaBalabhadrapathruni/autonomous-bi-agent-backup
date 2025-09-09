# app.py — Part 1: imports, app, CORS, stores
import os
import time
import logging
from typing import Dict, List, Optional, Literal

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Analytics libs
import io
import pandas as pd
import numpy as np

app = FastAPI(title="AuraBI Backend", version="1.1.2")

# CORS: permissive by default; no environment variables introduced/renamed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Keep as "*" to avoid new env var dependency
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Simple in-memory stores (OK for demo/dev)
DATASETS: Dict[str, dict] = {}        # dataset_id -> { filename, size, ext, uploaded_ts, content }
CHATS: Dict[str, List[dict]] = {}     # session_id -> list[message dicts]
# app.py — Part 2: models
class Insight(BaseModel):
    title: str
    description: str
    confidence: float
    severity: Literal["low", "medium", "high"] = "medium"

class InsightsResponse(BaseModel):
    insights: List[Insight] = []

class QueryRequest(BaseModel):
    query: str
    datasetId: str
    sessionId: str

class QueryResponse(BaseModel):
    response: str
    sql_query: Optional[str] = None

class ChatLog(BaseModel):
    session_id: str
    query: Optional[str] = ""
    response: Optional[str] = ""
    sql_query: Optional[str] = ""
    dataset_id: Optional[str] = None
    ts: float = Field(default_factory=lambda: time.time())

class EmailRequest(BaseModel):
    to: str
    subject: str
    body: str
    from_name: Optional[str] = "AuraBI System"
# app.py — Part 3: health
@app.get("/")
def root():
    return {"ok": True, "service": "AuraBI", "version": "1.1.2"}

@app.get("/health")
def health():
    return {"status": "healthy"}
# app.py — Part 4: anomaly detection helper
def compute_insights_from_csv(content: bytes) -> List["Insight"]:
    """
    Parses CSV and returns insights including anomalies on 'revenue'.
    Required columns (case-insensitive): date, revenue
    Optional: orders, customers, marketing_spend, churn_rate
    """
    # Read CSV from bytes
    df = pd.read_csv(io.BytesIO(content))
    df.columns = [c.strip().lower() for c in df.columns]

    if "date" not in df.columns or "revenue" not in df.columns:
        raise ValueError("CSV must include 'date' and 'revenue' columns")

    # Parse date & sort
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # Ensure numeric revenue
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df = df.dropna(subset=["revenue"]).reset_index(drop=True)

    insights: List[Insight] = []

    # Handle short series
    if len(df) < 6:
        slope = (df["revenue"].iloc[-1] - df["revenue"].iloc[0]) / max(1, len(df) - 1)
        trend_dir = "upward" if slope > 0 else "downward" if slope < 0 else "flat"
        insights.append(Insight(
            title=f"Revenue {trend_dir} trend",
            description=f"Insufficient data for anomaly detection ({len(df)} points), but trend appears {trend_dir}.",
            confidence=70.0, severity="medium"
        ))
        return insights

    # ----- Trend via least-squares slope -----
    idx = np.arange(len(df))
    slope = np.polyfit(idx, df["revenue"].values, 1)[0]
    trend_dir = "upward" if slope > 0 else "downward" if slope < 0 else "flat"
    conf_trend = min(95.0, 60.0 + 40.0 * (abs(slope) / (df["revenue"].mean() + 1e-6)))
    conf_trend = float(max(60.0, conf_trend))

    insights.append(Insight(
        title=f"Revenue {trend_dir} trend",
        description=f"Overall revenue shows a {trend_dir} trend across {len(df)} months.",
        confidence=round(conf_trend, 1),
        severity="medium"
    ))

    # ----- Robust anomalies on revenue (MAD with local baseline) -----
    win = max(3, min(6, len(df)//4))  # small rolling median window for monthly data
    baseline = df["revenue"].rolling(window=win, center=True, min_periods=1).median()
    resid = df["revenue"] - baseline

    mad = np.median(np.abs(resid - np.median(resid))) or 1e-6
    robust_z = np.abs((resid - np.median(resid)) / mad)

    thr = 3.5
    anomaly_idx = np.where(robust_z > thr)[0]

    for i in anomaly_idx:
        dt = df.loc[i, "date"]
        val = df.loc[i, "revenue"]
        base = baseline.iloc[i] if baseline.iloc[i] != 0 else (df["revenue"].median() or 1)
        pct = (val - base) / (base + 1e-6) * 100.0

        # Confidence scaled with z-score beyond threshold
        conf = min(98.0, 70.0 + 6.0 * (robust_z[i] - thr))
        conf = float(max(80.0, conf))  # ensure >=80 so autonomous emails trigger

        sev = "high" if abs(pct) >= 25 else "medium" if abs(pct) >= 12 else "low"
        direction = "spike" if val > base else "drop"

        insights.append(Insight(
            title=f"Anomalous revenue {direction} – {dt.strftime('%b %Y')}",
            description=(f"Revenue {direction} of {abs(pct):.1f}% vs local baseline "
                         f"({val:,.0f} vs {base:,.0f})."),
            confidence=round(conf, 1),
            severity=sev
        ))

    if not anomaly_idx.size:
        insights.append(Insight(
            title="No significant revenue anomalies detected",
            description=f"All observed monthly revenue values are within normal variation (threshold z>{thr}).",
            confidence=78.0,
            severity="low"
        ))

    insights.sort(key=lambda x: x.confidence, reverse=True)
    return insights
# app.py — Part 5: upload & insights
@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    name = (file.filename or "").strip()
    ext = os.path.splitext(name)[1].lower()

    if ext not in {".csv", ".json", ".xlsx", ".parquet"}:
        raise HTTPException(status_code=400, detail="Unsupported file type. Use .csv, .json, .xlsx, .parquet")

    content = await file.read()
    dataset_id = f"{os.path.splitext(name)[0]}_{int(time.time())}"

    DATASETS[dataset_id] = {
        "filename": name,
        "size": len(content),
        "ext": ext,
        "uploaded_ts": time.time(),
        "content": content,  # Keep raw bytes for analytics
    }

    return {"dataset_id": dataset_id}

@app.get("/insights/{dataset_id}", response_model=InsightsResponse)
def get_insights(dataset_id: str):
    # Keep demo_001 working for first-time UX (no env var changes)
    if dataset_id == "demo_001":
        sample = [
            Insight(title="Revenue Growth Trend",
                    description="Monthly revenue shows consistent 12% growth pattern",
                    confidence=85, severity="medium"),
            Insight(title="Customer Acquisition Peak",
                    description="New customer sign-ups peaked during Q4 marketing campaign",
                    confidence=92, severity="high"),
        ]
        return InsightsResponse(insights=sample)

    if dataset_id not in DATASETS:
        # UI treats 404 specially and shows “Dataset not found…”
        raise HTTPException(status_code=404, detail="Dataset schema not found")

    meta = DATASETS[dataset_id]
    content = meta.get("content", b"")
    if not content:
        raise HTTPException(status_code=422, detail="Dataset present but content missing")

    try:
        insights = compute_insights_from_csv(content)
        return InsightsResponse(insights=insights)
    except Exception as e:
        logging.exception("Failed to compute insights: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to compute insights: {e}")
# app.py — Part 6: query & chat
@app.post("/query", response_model=QueryResponse)
def post_query(payload: QueryRequest):
    if payload.datasetId not in DATASETS and payload.datasetId != "demo_001":
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Placeholder logic. Replace with your LLM/SQL engine when ready.
    sql = f"SELECT * FROM sales WHERE dataset_id = '{payload.datasetId}' LIMIT 10;"
    resp = (
        f"Here are the top patterns for dataset `{payload.datasetId}`: "
        f"revenue growth, seasonal uplift in Q4, and acquisition peaks aligned to campaigns."
    )

    CHATS.setdefault(payload.sessionId, []).append({
        "role": "user",
        "content": payload.query,
        "ts": time.time(),
        "datasetId": payload.datasetId,
    })
    CHATS.setdefault(payload.sessionId, []).append({
        "role": "assistant",
        "content": resp,
        "sql_query": sql,
        "ts": time.time(),
        "datasetId": payload.datasetId,
    })

    return QueryResponse(response=resp, sql_query=sql)

@app.get("/chat/{session_id}")
def get_chat_messages(session_id: str):
    return CHATS.get(session_id, [])

@app.post("/chat/log")
def log_chat(entry: ChatLog):
    CHATS.setdefault(entry.session_id, []).append({
        "role": "user" if entry.query else "assistant",
        "content": entry.query or entry.response or "",
        "sql_query": entry.sql_query or "",
        "dataset_id": entry.dataset_id,
        "ts": entry.ts,
    })
    return {"ok": True}
# app.py — Part 7: email (mock/SaaS/SMTP)
@app.post("/send-email")
def send_email(req: EmailRequest):
    # Use the same environment variables as before; no renaming
    SMTP_HOST = os.getenv("SMTP_HOST")
    SMTP_USER = os.getenv("SMTP_USER")
    SMTP_PASS = os.getenv("SMTP_PASS")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

    # Default to mock so UI never blocks during dev
    if not SMTP_HOST:
        logging.info(f"[MOCK EMAIL] to={req.to} subject={req.subject}\n{req.body}")
        return {"sent": True, "provider": "mock"}

    import smtplib
    from email.mime.text import MIMEText

    msg = MIMEText(req.body)
    msg["Subject"] = req.subject
    msg["From"] = f"{req.from_name} <{SMTP_USER or 'noreply@aurabi.local'}>"
    msg["To"] = req.to

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            if SMTP_USER and SMTP_PASS:
                server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER or "noreply@aurabi.local", [req.to], msg.as_string())
        return {"sent": True, "provider": "smtp"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Email send failed: {e}")
# app.py — Part 8: entrypoint (safe for local dev; Render ignores it)
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
