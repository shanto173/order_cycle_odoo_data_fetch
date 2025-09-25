import pandas as pd
import numpy as np
import gspread
import re
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from datetime import datetime
import pytz

# -------- CONFIG --------
SERVICE_ACCOUNT_FILE = "gcreds.json"  # GitHub secret or repo file
SOURCE_SHEET_ID = "1Rz5ctnSMSh_UGmhYkE_jX6zYGCabz28BEHaNnfbRn0I"
SOURCE_SHEET_NAME = "Sheet1"          # Sheet to fetch data from
TARGET_SHEET_ID = "1acV7UrmC8ogC54byMrKRTaD9i1b1Cf9QZ-H1qHU5ZZc"
TARGET_SHEET_NAME = "SLD_DF"          # Sheet to paste grouped data
BATCH_CLEAR_RANGE = "A:H"
TIMESTAMP_CELL = "I1"
LOCAL_TZ = pytz.timezone("Asia/Dhaka")


# -------- GOOGLE SHEETS AUTH --------
scope = ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=scope
)
client = gspread.authorize(creds)


# -------- CUSTOM READER CLASS --------
class GoogleSheetReader:
    def __init__(self, service_account_file: str, sheet_id: str):
        self.gc = gspread.service_account(filename=service_account_file)
        self.sheet_id = sheet_id

    def read_sheet(self, sheet_name: str, skip_header: bool = False) -> pd.DataFrame:
        sh = self.gc.open_by_key(self.sheet_id)
        worksheet = sh.worksheet(sheet_name)
        all_data = worksheet.get_all_values()

        if not all_data:
            return pd.DataFrame()

        if skip_header:
            return pd.DataFrame(all_data[1:])
        else:
            return pd.DataFrame(all_data[2:], columns=all_data[1])


# -------- FUNCTIONS --------
def paste_to_gsheet(df, sheet_id, sheet_name):
    worksheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    if df.empty:
        print(f"Skip: {sheet_name} DataFrame is empty, not pasting.")
        return
    worksheet.batch_clear([BATCH_CLEAR_RANGE])
    set_with_dataframe(worksheet, df)
    print(f"✅ Data pasted to {sheet_name} ({BATCH_CLEAR_RANGE})")

    # Add timestamp
    local_time = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update(TIMESTAMP_CELL, [[local_time]])
    print(f"Timestamp written to {TIMESTAMP_CELL}: {local_time}")


# -------- MAIN WORKFLOW --------
def main():
    # 1. Read source sheet
    reader = GoogleSheetReader(SERVICE_ACCOUNT_FILE, SOURCE_SHEET_ID)
    df = reader.read_sheet(SOURCE_SHEET_NAME)

    if df.empty:
        print("No data found in source sheet. Exiting.")
        return

    # 2. Clean & transform
    df['Release Date'] = pd.to_datetime(df['Release Date'], errors='coerce')
    df['Month'] = df['Release Date'].values.astype('datetime64[M]')
    df['TZP_Code'] = df['Slider'].apply(
        lambda x: re.search(r'TZP.*$', str(x)).group() if pd.notnull(x) and re.search(r'TZP.*$', str(x)) else "Others"
    ).str.replace('\xa0', '', regex=False)
    df['Quantity (PCS)'] = pd.to_numeric(df['Quantity (PCS)'], errors='coerce')

    std_codes = ["TZP-1862","TZP-2239","TZP-294","TZP-305","TZP-331","TZP-373",
                 "TZP-684","TZP-793","TZP-794","TZP-645","TZP-574"]
    df['TZP_Type'] = df['TZP_Code'].apply(lambda x: "STD" if x in std_codes else "SPEC")

    # 3. Filter July 1 to today
    today = pd.Timestamp.today().normalize()
    start_date = pd.Timestamp("2025-07-01")
    df = df[(df['Release Date'] >= start_date) & (df['Release Date'] <= today)]

    # 4. Group & aggregate
    grouped = df.groupby(
        ["TZP_Type", "Product", "Category", "TZP_Code", "Month", "Release Date"], as_index=False
    ).agg(
        Quantity_PCS_sum=("Quantity (PCS)", "sum"),
        Avg_Unit_Price=("Unit Price", lambda x: np.mean(pd.to_numeric(x, errors='coerce')))
    )

    # 5. Sort
    grouped["TZP_Type"] = pd.Categorical(grouped["TZP_Type"], categories=["STD","SPEC"], ordered=True)
    grouped = grouped.sort_values(by=["TZP_Type", "Quantity_PCS_sum"], ascending=[True, False])

    # 6. Paste to target sheet
    paste_to_gsheet(grouped, TARGET_SHEET_ID, TARGET_SHEET_NAME)


if __name__ == "__main__":
    main()
