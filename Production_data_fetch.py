import requests
import json
import re
import logging
import sys
import os
from datetime import date, datetime
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
import pandas as pd
import pytz
from dotenv import load_dotenv
from pathlib import Path
import time
load_dotenv()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger()

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

MODEL = "mrp.report.custom"
REPORT_BUTTON_METHOD = "action_generate_xlsx_report"
REPORT_TYPE = "invs"

# Default date range: first-to-last of current month
today = date.today()
from_date_env = os.getenv("FROM_DATE", "").strip()
to_date_env = os.getenv("TO_DATE", "").strip()

FROM_DATE = from_date_env if from_date_env else today.replace(day=1).isoformat()
TO_DATE = to_date_env if to_date_env else today.isoformat()

log.info(f"Using FROM_DATE={FROM_DATE}, TO_DATE={TO_DATE}")

COMPANIES = {
    1: "Zipper",
    3: "Metal Trims",
}

download_dir = "./downloads"
os.makedirs(download_dir, exist_ok=True)

# ========= START SESSION ==========
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

# ----------------------
# Step 1: Login
login_url = f"{ODOO_URL}/web/session/authenticate"
login_payload = {
    "jsonrpc": "2.0",
    "params": {
        "db": DB,
        "login": USERNAME,
        "password": PASSWORD
    }
}
resp = session.post(login_url, json=login_payload)
resp.raise_for_status()
uid = resp.json().get("result", {}).get("uid")
print("✅ Logged in, UID =", uid)

# ----------------------
def refresh_csrf():
    resp = session.get(f"{ODOO_URL}/web")
    match = re.search(r'var odoo = {\s*csrf_token: "([A-Za-z0-9]+)"', resp.text)
    return match.group(1) if match else None

# ----------------------
# Google Sheets setup
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = service_account.Credentials.from_service_account_file('gcreds.json', scopes=scope)
client = gspread.authorize(creds)
local_tz = pytz.timezone('Asia/Dhaka')

# ----------------------
# Main loop for companies
for company_id, cname in COMPANIES.items():
    print(f"\n🔹 Processing company: {cname} (ID={company_id})")

    # Create wizard
    create_url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/create"
    create_payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": "create",
            "args": [{}],
            "kwargs": {"context": {"uid": uid}}
        }
    }
    resp = session.post(create_url, json=create_payload)
    resp.raise_for_status()
    wizard_id = resp.json().get("result")
    print("✅ Wizard created, ID =", wizard_id)

    # Save wizard
    save_url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/web_save"
    save_payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": "web_save",
            "args": [[], {"report_type": REPORT_TYPE, "date_from": FROM_DATE, "date_to": TO_DATE}],
            "kwargs": {
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": uid,
                    "allowed_company_ids": [company_id]
                },
                "specification": {"report_type": {}, "date_from": {}, "date_to": {}}
            }
        }
    }
    resp = session.post(save_url, json=save_payload)
    resp.raise_for_status()
    wizard_id = resp.json().get("result", [{}])[0].get("id")
    print("✅ Wizard saved, ID =", wizard_id)

    # Call report button
    button_url = f"{ODOO_URL}/web/dataset/call_button"
    button_payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": REPORT_BUTTON_METHOD,
            "args": [[wizard_id]],
            "kwargs": {
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": uid,
                    "allowed_company_ids": [company_id]
                }
            }
        }
    }
    resp = session.post(button_url, json=button_payload)
    resp.raise_for_status()
    report_info = resp.json().get("result")
    print("✅ Report info received for", cname)

    csrf_token = refresh_csrf()
    if company_id == 1:  # Zipper
        time.sleep(10)

    options = {"date_from": FROM_DATE, "date_to": TO_DATE, "company_id": company_id}
    context = {
        "lang": "en_US",
        "tz": "Asia/Dhaka",
        "uid": uid,
        "allowed_company_ids": [company_id],
        "active_model": MODEL,
        "active_id": wizard_id,
        "active_ids": [wizard_id]
    }

    REPORT_TEMPLATE = report_info.get("report_name") or "taps_manufacturing.pi_xls_template"
    report_path = f"/report/xlsx/{REPORT_TEMPLATE}?options={json.dumps(options)}&context={json.dumps(context)}"
    download_payload = {
        "data": json.dumps([report_path, "xlsx"]),
        "context": json.dumps(context),
        "token": "dummy-because-api-expects-one",
        "csrf_token": csrf_token
    }

    download_url = f"{ODOO_URL}/report/download"
    headers = {"X-CSRF-Token": csrf_token, "Referer": f"{ODOO_URL}/web"}

    try:
        resp = session.post(download_url, data=download_payload, headers=headers, timeout=60)
        if resp.status_code == 200 and "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in resp.headers.get("content-type", ""):
            filename = Path(download_dir) / f"{cname.replace(' ', '_')}_{REPORT_TYPE}_{FROM_DATE}_to_{TO_DATE}.xlsx"
            with open(filename, "wb") as f:
                f.write(resp.content)
            print(f"✅ Report downloaded for {cname}: {filename}")

            # === Load file and paste to Google Sheets ===
            df_sheet1 = pd.read_excel(filename)
            
            if company_id == 1:  # Zipper Sheets
                sheet1 = client.open_by_key("1acV7UrmC8ogC54byMrKRTaD9i1b1Cf9QZ-H1qHU5ZZc").worksheet("Production Data")
            else:  # Metal Trims Sheets
                sheet1 = client.open_by_key("1acV7UrmC8ogC54byMrKRTaD9i1b1Cf9QZ-H1qHU5ZZc").worksheet("MT_Production_QTY")

            for df, ws in zip([df_sheet1], [sheet1]):
                if df.empty:
                    print("Skip: DataFrame empty, not pasting to sheet.")
                else:
                    df = df.fillna("")
                    ws.batch_clear(["A:AB"])
                    set_with_dataframe(ws, df)
                    timestamp = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
                    ws.update("AC2", [[timestamp]])
                    print(f"Data pasted to {ws.title} with timestamp {timestamp}")

        else:
            print(f"❌ Failed to download report for {cname}, status={resp.status_code}")
    except Exception as e:
        print(f"❌ Exception during download/paste for {cname}: {e}")
