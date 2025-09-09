import os
import uuid
import json
import sqlite3
import smtplib
import pandas as pd
from email.mime.text import MIMEText
from fastapi import FastAPI, UploadFile, File, Query, Form
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any
from langchain.agents import initialize_agent, tool
from langchain_google_genai import ChatGoogleGenerativeAI

# -----------------------------
# CONFIG
# -----------------------------
app = FastAPI(title="Autonomous BI Agent")

# Load SMTP environment variables
SMTP_HOST = os.environ['SMTP_HOST']
SMTP_PORT = int(os.environ['SMTP_PORT'])
SMTP_USERNAME = os.environ['SMTP_USERNAME']
SMTP_PASSWORD = os.environ['SMTP_PASSWORD']
EMAIL_FROM = os.environ['EMAIL_FROM']

# Enable CORS (so UI can connect)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "autobi.db"
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Gemini LLM
llm = ChatGoogleGenerativeAI(model="gemini-pro", temperature=0)

# HITL toggle (default ON for safety)
HITL_ENABLED = True
ADMIN_KEY = os.getenv("ADMIN_KEY", "demo-admin")

# SQLite helper
def _conn():
    return sqlite3.connect(DB_PATH)

# Ensure tables
with _conn() as conn:
    conn.execute("""CREATE TABLE IF NOT EXISTS actions (
        id TEXT PRIMARY KEY,
        source_id TEXT,
        decision TEXT,
        edited_alert TEXT,
        decided_by TEXT,
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS uploads (
        id TEXT PRIMARY KEY,
        filename TEXT,
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS pending_reviews (
        id TEXT PRIMARY KEY,
        detector TEXT,
        metric TEXT,
        group_json TEXT,
        time_value TEXT,
        score REAL,
        criticality REAL,
        confidence REAL,
        draft_alert TEXT,
        status TEXT DEFAULT 'pending',
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()

# Global dataframe
dataframe: pd.DataFrame = None

# -----------------------------
# DETECTION FUNCTIONS
# -----------------------------
def detect_univariate_anomalies(df: pd.DataFrame, metric: str) -> Dict[str, Any]:
    mean_val = df[metric].mean()
    std_val = df[metric].std()
    anomalies = df[df[metric] > mean_val + 2 * std_val]
    return {
        "metric": metric,
        "mean": mean_val,
        "anomalies": anomalies.to_dict(orient="records"),
        "confidence": 0.95 if not anomalies.empty else 0.7
    }

def detect_trends(df: pd.DataFrame, metric: str) -> Dict[str, Any]:
    trend = df[metric].diff().mean()
    return {
        "metric": metric,
        "trend": "increasing" if trend > 0 else "decreasing",
        "confidence": 0.9
    }

# -----------------------------
# SEND INITIAL RESPONSE
# -----------------------------
def send_initial_response(agent: str, message: str, medium: str = "console"):
    if medium == "console":
        print(f"[AUTO-RESPONSE] {agent}: {message}")
    elif medium == "email":
        msg = MIMEText(message)
        msg["Subject"] = f"Auto Response from {agent}"
        msg["From"] = "noreply@autobi.com"
        msg["To"] = "recipient@demo.com"
        with smtplib.SMTP("sandbox.smtp.mailtrap.io", 2525) as server:
            server.login("username", "password")  # replace with Mailtrap creds
            server.send_message(msg)
    elif medium == "slack":
        import requests
        webhook_url = "https://hooks.slack.com/services/XXXX"
        requests.post(webhook_url, json={"text": message})
    return {"status": "sent", "medium": medium, "message": message}

# -----------------------------
# LANGCHAIN TOOLS
# -----------------------------
@tool
def anomaly_tool(data: str, metric: str) -> str:
    df = pd.read_csv(data)
    result = detect_univariate_anomalies(df, metric)
    return json.dumps(result)

@tool
def trend_tool(data: str, metric: str) -> str:
    df = pd.read_csv(data)
    result = detect_trends(df, metric)
    return json.dumps(result)

# -----------------------------
# AGENTS
# -----------------------------
tools = [anomaly_tool, trend_tool]

conversational_agent = initialize_agent(
    tools, llm, agent="zero-shot-react-description", verbose=True
)

live_insights_agent = initialize_agent(
    tools, llm, agent="zero-shot-react-description", verbose=True
)

def revenue_analysis_agent(df: pd.DataFrame, metric: str):
    result = detect_univariate_anomalies(df, metric)
    draft_alert = f"🚨 Revenue anomaly in {metric}! Mean={result['mean']:.2f}, anomalies={len(result['anomalies'])}"

    if result["confidence"] >= 0.9 and result["anomalies"]:
        if HITL_ENABLED:
            with _conn() as conn:
                conn.execute("""INSERT INTO pending_reviews
                    (id, detector, metric, group_json, time_value, score, criticality, confidence, draft_alert)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    (str(uuid.uuid4()), "revenue_detector", metric, "{}", "latest",
                     0, 1.0, result["confidence"], draft_alert))
                conn.commit()
            return {"status": "queued_for_review", "draft_alert": draft_alert, "confidence": result["confidence"]}
        else:
            send_initial_response("RAA", draft_alert, medium="console")
            with _conn() as conn:
                conn.execute("INSERT INTO actions (id, source_id, decision, edited_alert, decided_by) VALUES (?,?,?,?,?)",
                    (f"act_{uuid.uuid4().hex[:8]}", "auto", "auto_approved", draft_alert, "agent"))
                conn.commit()
            return {"status": "auto_executed", "alert": draft_alert, "confidence": result["confidence"]}
    return {"status": "no_high_confidence", "confidence": result["confidence"]}
# app.py (Part 3 of 3)

# -----------------------------
# FILE UPLOAD
# -----------------------------
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    global dataframe
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())
    dataframe = pd.read_csv(file_path)
    with _conn() as conn:
        conn.execute("INSERT INTO uploads (id, filename) VALUES (?,?)", (str(uuid.uuid4()), file.filename))
        conn.commit()
    return {"status": "success", "filename": file.filename}

# -----------------------------
# QUERY (Conversational Agent)
# -----------------------------
@app.post("/query")
async def query_agent(query: str = Form(...), metric: str = Form("revenue")):
    if dataframe is None:
        return {"error": "No data uploaded"}
    file_path = os.path.join(UPLOAD_DIR, os.listdir(UPLOAD_DIR)[-1])
    response = conversational_agent.run(f"{query} using {file_path} and metric {metric}")
    return {"query": query, "response": response}

# -----------------------------
# LIVE INSIGHTS
# -----------------------------
@app.get("/live-insights")
async def live_insights(metric: str = "revenue"):
    if dataframe is None:
        return {"error": "No data uploaded"}
    file_path = os.path.join(UPLOAD_DIR, os.listdir(UPLOAD_DIR)[-1])
    response = live_insights_agent.run(f"Find anomalies and trends in {metric} using {file_path}")
    return {"metric": metric, "insights": response}

# -----------------------------
# REVENUE ANALYSIS AGENT
# -----------------------------
@app.get("/raa")
async def raa(metric: str = "revenue"):
    if dataframe is None:
        return {"error": "No data uploaded"}
    result = revenue_analysis_agent(dataframe, metric)
    return result

# -----------------------------
# AGENT FEED
# -----------------------------
@app.get("/agent-feed")
async def agent_feed():
    agents = [
        {"id": 1, "name": "Conversational Agent", "type": "Q&A", "status": "online"},
        {"id": 2, "name": "Live Insights Agent", "type": "Pattern/Anomaly", "status": "online"},
        {"id": 3, "name": "Revenue Analysis Agent", "type": "Alerts", "status": "online"}
    ]
    return {"agents": agents}

# -----------------------------
# PENDING REVIEWS
# -----------------------------
@app.get("/pending-reviews")
async def pending_reviews():
    with _conn() as conn:
        rows = conn.execute("SELECT * FROM pending_reviews WHERE status='pending'").fetchall()
        cols = [c[1] for c in conn.execute("PRAGMA table_info(pending_reviews)")]
    return {"pending": [dict(zip(cols, row)) for row in rows]}

# -----------------------------
# REVIEW ACTION
# -----------------------------
@app.post("/review-action")
async def review_action(index: str = Form(...), decision: str = Form(...), edited_alert: str = Form("")):
    with _conn() as conn:
        row = conn.execute("SELECT * FROM pending_reviews WHERE id=?", (index,)).fetchone()
        if not row:
            return {"error": "Invalid index"}
        if decision == "approve":
            conn.execute("INSERT INTO actions (id, source_id, decision, edited_alert, decided_by) VALUES (?,?,?,?,?)",
                (f"act_{uuid.uuid4().hex[:8]}", index, decision, edited_alert or row[8], "human"))
            conn.execute("UPDATE pending_reviews SET status='approved' WHERE id=?", (index,))
        elif decision == "reject":
            conn.execute("UPDATE pending_reviews SET status='rejected' WHERE id=?", (index,))
        conn.commit()
    return {"status": "processed", "decision": decision}

# -----------------------------
# TOGGLE HITL
# -----------------------------
@app.post("/toggle-hitl")
async def toggle_hitl(enable: bool = Form(...), admin_key: str = Form(...)):
    global HITL_ENABLED
    if admin_key != ADMIN_KEY:
        return {"error": "Unauthorized"}
    HITL_ENABLED = enable
    mode = "Human-in-the-Loop" if enable else "Autonomous"
    return {"status": "ok", "HITL_ENABLED": HITL_ENABLED, "mode": mode}

# -----------------------------
# HITL STATUS
# -----------------------------
@app.get("/hitl-status")
async def hitl_status():
    mode = "Human-in-the-Loop" if HITL_ENABLED else "Autonomous"
    return {"HITL_ENABLED": HITL_ENABLED, "mode": mode}

@app.get("/")
async def read_root():
    return {"message": "Hello! App is running."}

@app.get("/send-test-email")
async def send_test_email():
    try:
        msg = EmailMessage()
        msg['Subject'] = 'Test Email from FastAPI'
        msg['From'] = EMAIL_FROM
        msg['To'] = 'recipient@example.com'  # any email for testing
        msg.set_content('Hello from Mailtrap SMTP!')

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)

        return {"status": "success", "message": "Email sent!"}

    except Exception as e:
        return {"status": "error", "message": str(e)}
