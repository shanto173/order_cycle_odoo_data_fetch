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

# Use args if provided, otherwise use default monthly range
if args.from_date and args.to_date:
    FROM_DATE = args.from_date
    TO_DATE = args.to_date
elif today.day == 1:
    # On first day of month, use previous month range
    last_day_prev_month = first_day - timedelta(days=1)
    prev_month_first = last_day_prev_month.replace(day=1)
    FROM_DATE = prev_month_first.strftime("%Y-%m-%d 00:00:00")
    TO_DATE = last_day_prev_month.strftime("%Y-%m-%d 23:59:59")
else:
    # Use current month range
    FROM_DATE = first_day.strftime("%Y-%m-%d 00:00:00")
    TO_DATE = today.strftime("%Y-%m-%d 23:59:59")

print(f"📅 Fetching data from {FROM_DATE} to {TO_DATE}")

# --------- Odoo Config (from env) ---------
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")

# --------- Google Sheet Config ---------
SHEET_ID = "1acV7UrmC8ogC54byMrKRTaD9i1b1Cf9QZ-H1qHU5ZZc"  # or hardcode: "1acV7UrmC8ogC54byMrKRTaD9i1b1Cf9QZ-H1qHU5ZZc"
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
    uid = resp.json()['result']['uid']
    print(f"✅ Logged in! UID: {uid}")
    return uid

# --------- Fetch all combine.invoice data ---------
def fetch_all_data(uid, from_date, to_date, batch_size=1000):
    all_records = []
    offset = 0
    domain = [
        "&", ["state","=","posted"],
        "&", ["invoice_date", ">=", from_date],
             ["invoice_date", "<=", to_date]
    ]
    
    specification = {
        "delivery_date": {},
        "invoice_incoterm_id": {"fields":{"display_name":{}}},
        "invoice_date": {},
        "m_total": {},
        "m_total_q": {},
        "name": {},
        "partner_id": {"fields":{"display_name":{}}},
        "invoice_payment_term_id": {"fields":{"display_name":{}}},
        "qty_total": {},
        "state": {},
        "amount_total": {},
        "z_total": {},
        "z_total_q": {}
    }
    
    while True:
        url = f"{ODOO_URL}/web/dataset/call_kw/combine.invoice/web_search_read"
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "combine.invoice",
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
                        "allowed_company_ids": [1,3],
                        "bin_size": True,
                        "current_company_id": 1
                    },
                    "count_limit": 10001
                }
            },
            "id": 2
        }
        resp = session.post(url, data=json.dumps(payload))
        resp.raise_for_status()
        result = resp.json()['result']
        records = result['records']
        all_records.extend(records)
        print(f"Fetched {len(records)} records, total so far: {len(all_records)}")
        if len(records) < batch_size:
            break
        offset += batch_size
    print(f"✅ Total records fetched: {len(all_records)}")
    return all_records

# --------- Flatten record ---------
def flatten_record(rec):
    flat = {}
    flat['Delivery Date'] = rec.get('delivery_date','')
    incoterm = rec.get('invoice_incoterm_id', False)
    flat['Incoterm'] = incoterm['display_name'] if incoterm else ''
    flat['Invoice/Bill Date'] = rec.get('invoice_date','')
    flat['Metal Total'] = rec.get('m_total','')
    flat['Metal Total Qty'] = rec.get('m_total_q','')
    flat['Number'] = rec.get('name','')
    partner = rec.get('partner_id', False)
    flat['Partner'] = partner['display_name'] if partner else ''
    payment_term = rec.get('invoice_payment_term_id', False)
    flat['Payment Terms'] = payment_term['display_name'] if payment_term else ''
    flat['Qty Total'] = rec.get('qty_total','')
    flat['Status'] = rec.get('state','')
    flat['Total Value'] = rec.get('amount_total','')
    flat['Zipper Total'] = rec.get('z_total','')
    flat['Zipper Total Qty'] = rec.get('z_total_q','')
    return flat

# --------- Paste to Google Sheet ---------
def paste_to_gsheet(df):
    worksheet = client.open_by_key(SHEET_ID).worksheet("Lc recv")
    if df.empty:
        print("Skip: DataFrame is empty, not pasting to sheet.")
        return
    worksheet.batch_clear(['A:AC'])
    set_with_dataframe(worksheet, df)
    print("✅ Data pasted to Google Sheet (Lc recv).")

    # Add timestamp
    local_tz = pytz.timezone('Asia/Dhaka')
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("AC2", [[f"{local_time}"]])
    print(f"Timestamp written to AC2: {local_time}")

# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    records = fetch_all_data(uid, FROM_DATE, TO_DATE)
    flat_records = [flatten_record(r) for r in records]
    df = pd.DataFrame(flat_records)
    paste_to_gsheet(df)
