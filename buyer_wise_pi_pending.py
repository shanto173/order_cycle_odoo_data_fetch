import requests
import json
import pandas as pd
from datetime import datetime
import os
import pytz

import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# --------- Odoo Config (from env) ---------
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")

# --------- Google Sheet Config ---------
SHEET_ID = "1acV7UrmC8ogC54byMrKRTaD9i1b1Cf9QZ-H1qHU5ZZc"
creds = Credentials.from_service_account_file("gcreds.json", scopes=["https://www.googleapis.com/auth/spreadsheets"])
client = gspread.authorize(creds)

# --------- Requests Session ---------
session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

# --------- Login ---------
def odoo_login():
    url = f"{ODOO_URL}/web/session/authenticate"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "db": ODOO_DB,
            "login": ODOO_USERNAME,
            "password": ODOO_PASSWORD
        },
        "id": 1
    }
    resp = session.post(url, data=json.dumps(payload))
    resp.raise_for_status()
    uid = resp.json()["result"]["uid"]
    print(f"✅ Logged in! UID: {uid}")
    return uid

# --------- Fetch all data ---------
def fetch_all_data(uid, company_id, batch_size=1000):
    all_records = []
    offset = 0
    domain = [
        "&", ["sales_type", "=", "sale"],
        "&", "|", ["oa_count", "=", False], ["oa_count", "=", 0],
        "&", ["is_active", "=", True],
        "&", ["pi_type", "=", "regular"],
        ["state", "!=", "cancel"]
    ]
    specification = {
        "name": {},
        "buyer_name": {},
        "buying_house": {},
        "partner_id": {"fields": {"display_name": {}}},
        "company_id": {"fields": {"display_name": {}}},
        "pi_date": {},
        "date_order": {},
        "team_id": {"fields": {"display_name": {}}},
        "user_id": {"fields": {"display_name": {}}},
        "total_product_qty": {},
        "amount_total": {},
        "amount_invoiced": {},
        "state": {},
        "lc_number": {},
        "payment_term_id": {"fields": {"display_name": {}}},
    }
    while True:
        url = f"{ODOO_URL}/web/dataset/call_kw/sale.order/web_search_read"
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "sale.order",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "domain": domain,
                    "specification": specification,
                    "offset": offset,
                    "limit": batch_size,
                    "order": "",
                    "context": {
                        "lang": "en_US",
                        "tz": "Asia/Dhaka",
                        "uid": uid,
                        "allowed_company_ids": [company_id],
                        "bin_size": True,
                        "current_company_id": company_id
                    },
                    "count_limit": 10001
                }
            },
            "id": 2
        }
        resp = session.post(url, data=json.dumps(payload))
        resp.raise_for_status()
        result = resp.json()["result"]
        records = result["records"]
        all_records.extend(records)
        print(f"[Company {company_id}] Fetched {len(records)} records, total so far: {len(all_records)}")
        if len(records) < batch_size:
            break
        offset += batch_size
    print(f"✅ Company {company_id} total records fetched: {len(all_records)}")
    return all_records

# --------- Flatten record ---------
def flatten_record(rec):
    flat = {}
    flat["Order Reference"] = rec.get("name", "")
    flat["Buyer"] = rec.get("buyer_name", "")
    flat["Buying House"] = rec.get("buying_house", "")
    partner = rec.get("partner_id", False)
    flat["Customer"] = partner["display_name"] if partner else ""
    company = rec.get("company_id", False)
    flat["Company"] = company["display_name"] if company else ""
    flat["PI Date"] = rec.get("pi_date", "")
    flat["Order Date"] = rec.get("date_order", "")
    team = rec.get("team_id", False)
    flat["Sales Team"] = team["display_name"] if team else ""
    user = rec.get("user_id", False)
    flat["Salesperson"] = user["display_name"] if user else ""
    flat["Total Qty"] = rec.get("total_product_qty", "")
    flat["Total"] = rec.get("amount_total", "")
    flat["Already Invoiced"] = rec.get("amount_invoiced", "")
    flat["Status"] = rec.get("state", "")
    flat["LC Number"] = rec.get("lc_number", "")
    payment = rec.get("payment_term_id", False)
    flat["Payment Terms"] = payment["display_name"] if payment else ""
    return flat

# --------- Paste to Google Sheet ---------
def paste_to_gsheet(df, sheet_name):
    worksheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    if df.empty:
        print(f"Skip: {sheet_name} DataFrame is empty, not pasting.")
        return
    worksheet.batch_clear(["A:Q"])
    set_with_dataframe(worksheet, df)
    print(f"✅ Data pasted to Google Sheet ({sheet_name}).")

    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update(values=[[f"{local_time}"]], range_name="R2")
    print(f"Timestamp written to R2: {local_time}")

# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    all_flat_records = []
    for company_id, company_name in [(1, "Zipper"), (3, "MetalTrim")]:
        records = fetch_all_data(uid, company_id)
        flat_records = [flatten_record(r) for r in records]
        all_flat_records.extend(flat_records)
        print(f"✅ {company_name}: {len(flat_records)} records collected")

    df = pd.DataFrame(all_flat_records)
    paste_to_gsheet(df, "pi_pending_data_buyer")
