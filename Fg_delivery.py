import requests
import json
import pandas as pd
from datetime import datetime,timedelta
import argparse
import os
import pytz

import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# --------- Read args or default ---------
parser = argparse.ArgumentParser()
parser.add_argument("--from_date", type=str, default=None)
parser.add_argument("--to_date", type=str, default=None)
args = parser.parse_args()

today = datetime.today()
first_day = today.replace(day=1)

if today.day in (1, 1):
    # Previous month range
    last_day_prev_month = first_day - timedelta(days=1)
    prev_month_first = last_day_prev_month.replace(day=1)
    FROM_DATE = args.from_date if args.from_date is not None and args.from_date != '' else prev_month_first.strftime("%Y-%m-%d 00:00:00")
    TO_DATE = args.to_date if args.to_date is not None and args.to_date != '' else last_day_prev_month.strftime("%Y-%m-%d 23:59:59")
else:
    FROM_DATE = args.from_date if args.from_date is not None and args.from_date != '' else first_day.strftime("%Y-%m-%d 00:00:00")
    TO_DATE = args.to_date if args.to_date is not None and args.to_date != '' else today.strftime("%Y-%m-%d 23:59:59")

print(f"📅 Fetching data from {FROM_DATE} to {TO_DATE}")

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
def fetch_all_data(uid, from_date, to_date, company_id, batch_size=1000):
    all_records = []
    offset = 0
    domain = [
        "&", ["next_operation", "=", "Delivery"],
        "&", "&", ["next_operation", "=", "Delivery"], ["state", "!=", "done"], ["state", "!=", "closed"],
        "&", ["action_date", ">=", from_date], ["action_date", "<=", to_date]
    ]
    specification = {
        "action_date": {},
        "qty": {},
        "final_price": {},
        "partner_id": {"fields": {"display_name": {}}},
        "fg_categ_type": {},
        "oa_id": {"fields": {"display_name": {}}},
        "product_template_id": {"fields": {"display_name": {}}},
        "slidercodesfg": {},
        "sale_order_line": {"fields": {
            "invoice_lines": {"fields": {"display_name": {}, "invoice_date": {}}},
            "invoice_status": {}
        }}
    }
    while True:
        url = f"{ODOO_URL}/web/dataset/call_kw/operation.details/web_search_read"
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "operation.details",
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
            "id": 3
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
    flat["Action Date"] = rec.get("action_date", "")
    flat["Qty"] = rec.get("qty", "")
    flat["Final Price"] = rec.get("final_price", "")
    partner = rec.get("partner_id", False)
    flat["Customer"] = partner["display_name"] if partner else ""
    flat["Item"] = rec.get("fg_categ_type", "")
    oa = rec.get("oa_id", False)
    flat["OA"] = oa["display_name"] if oa else ""
    product = rec.get("product_template_id", False)
    flat["Product"] = product["display_name"] if product else ""
    flat["Slider Code"] = rec.get("slidercodesfg", "")
    sol = rec.get("sale_order_line", False)
    if sol:
        invoice_lines = sol.get("invoice_lines", [])
        flat["Sale Order Line/Invoice Lines"] = " / ".join([inv.get("display_name", "") for inv in invoice_lines])
        flat["Sale Order Line/Invoice Status"] = sol.get("invoice_status", "")
        flat["Sale Order Line/Invoice Lines/Invoice/Bill Date"] = " / ".join(
            [str(inv.get("invoice_date", "")) for inv in invoice_lines]
        )
    else:
        flat["Sale Order Line/Invoice Lines"] = ""
        flat["Sale Order Line/Invoice Status"] = ""
        flat["Sale Order Line/Invoice Lines/Invoice/Bill Date"] = ""
    return flat

# --------- Paste to Google Sheet ---------
def paste_to_gsheet(df, sheet_name):
    worksheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    if df.empty:
        print(f"Skip: {sheet_name} DataFrame is empty, not pasting.")
        return
    worksheet.batch_clear(["A:AC"])
    set_with_dataframe(worksheet, df)
    print(f"✅ Data pasted to Google Sheet ({sheet_name}).")

    # Add timestamp
    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("AC2", [[f"{local_time}"]])
    print(f"Timestamp written to AC2: {local_time}")

# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    for company_id, company_name, sheet_name in [
        (1, "Zipper", "Zip Fg pack"),
        (3, "MetalTrim", "MT Fg pack")
    ]:
        records = fetch_all_data(uid, FROM_DATE, TO_DATE, company_id)
        flat_records = [flatten_record(r) for r in records]
        df = pd.DataFrame(flat_records)
        paste_to_gsheet(df, sheet_name)
