import os
import sqlite3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime

load_dotenv()
app = FastAPI()
templates = Jinja2Templates(directory="templates")

BREVO_API_KEY = os.getenv("BREVO_API_KEY")
TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")

DB = "contacts.db"
conn = sqlite3.connect(DB)
conn.execute("""
CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT,
    company TEXT,
    list_name TEXT,
    timestamp TEXT
)
""")
conn.commit()
conn.close()

FORM_LIST_MAP = {
    "newsletter_form_a": 12,
    "kontaktformular_b": 34
}

def normalize_name(name):
    import re
    return re.sub(r'\W+', '', name or '').lower().strip()

@retry(stop=stop_after_attempt(3), wait=wait_exponential())
def create_brevo_contact(email, company, list_id):
    url = "https://api.brevo.com/v3/contacts"
    headers = {
        "api-key": BREVO_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    data = {
        "email": email,
        "attributes": {"COMPANY": company or ""},
        "listIds": [list_id],
        "updateEnabled": True
    }
    response = requests.post(url, json=data, headers=headers)
    response.raise_for_status()
    return response.json()

def send_teams_notification(email, company, list_name):
    text = f"🔜 Neuer Kontakt: {email} (Firma: {company}) → Liste: {list_name}"
    requests.post(TEAMS_WEBHOOK_URL, json={"text": text})

def save_to_db(email, company, list_name):
    conn = sqlite3.connect(DB)
    conn.execute("INSERT INTO contacts (email, company, list_name, timestamp) VALUES (?, ?, ?, ?)",
                 (email, company or "", list_name, datetime.now().isoformat()))
    conn.commit()
    conn.close()

@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()
    email = data.get("email")
    company = data.get("company", "")
    form_id = data.get("form_id")

    if not email or form_id not in FORM_LIST_MAP:
        return {"status": "ignored", "reason": "missing data"}

    list_id = FORM_LIST_MAP[form_id]
    list_name = form_id
    normalized_company = normalize_name(company)

    try:
        result = create_brevo_contact(email, company, list_id)
        send_teams_notification(email, company, list_name)
        save_to_db(email, company, list_name)
        return {"status": "ok", "brevo": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/admin", response_class=HTMLResponse)
def dashboard(request: Request):
    conn = sqlite3.connect(DB)
    rows = conn.execute("SELECT email, company, list_name, timestamp FROM contacts ORDER BY timestamp DESC").fetchall()
    conn.close()
    return templates.TemplateResponse("dashboard.html", {"request": request, "contacts": rows})
