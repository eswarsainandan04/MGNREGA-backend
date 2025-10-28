import requests
import pandas as pd
import sqlite3
import time
from io import StringIO

API_BASE_URL = "https://api.data.gov.in/resource/ee03643a-ee4c-48c2-ac30-9f2ff26ab722"
API_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"
DB_NAME = "mgnrega.db"

FIN_YEARS = [
    "2018-2019", "2019-2020", "2020-2021", "2021-2022",
    "2022-2023", "2023-2024", "2024-2025", "2025-2026"
]

STATES = [
    "ANDAMAN AND NICOBAR", "ANDHRA PRADESH", "ARUNACHAL PRADESH", "ASSAM", "BIHAR", "CHHATTISGARH",
    "DN HAVELI AND DD", "GOA", "GUJARAT", "HARYANA", "HIMACHAL PRADESH", "JAMMU AND KASHMIR",
    "JHARKHAND", "KARNATAKA", "KERALA", "LADAKH", "LAKSHADWEEP", "MADHYA PRADESH",
    "MAHARASHTRA", "MANIPUR", "MEGHALAYA", "MIZORAM", "NAGALAND", "ODISHA",
    "PUDUCHERRY", "PUNJAB", "RAJASTHAN", "SIKKIM", "TAMIL NADU", "TELANGANA",
    "TRIPURA", "UTTAR PRADESH", "UTTARAKHAND", "WEST BENGAL"
]

def extract_data(state, fin_year):
    """Fetch data from API for given state and year"""
    url = (
        f"{API_BASE_URL}?api-key={API_KEY}&format=csv"
        f"&filters[state_name]={state.replace(' ', '%20')}"
        f"&filters[fin_year]={fin_year}"
    )
    print(f"📥 Extracting data for {state} ({fin_year}) ...")
    for attempt in range(3):  # retry up to 3 times
        try:
            response = requests.get(url, timeout=90)
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))
                return df
            else:
                print(f"⚠️ Attempt {attempt+1}: Failed ({response.status_code}) for {state} {fin_year}")
                time.sleep(3)
        except Exception as e:
            print(f"⚠️ Attempt {attempt+1} error: {e}")
            time.sleep(3)
    print(f"❌ Skipped {state} ({fin_year}) after retries.")
    return None


def transform_data(df):
    """Clean column names and data"""
    if df is None or df.empty:
        return None
    df.columns = [col.strip().replace(" ", "_").replace("/", "_").replace("-", "_") for col in df.columns]
    df = df.dropna(how='all')
    return df


def load_to_db(df, conn):
    """Insert data into SQLite"""
    try:
        df.to_sql("mgnrega_data", conn, if_exists="append", index=False)
        print(f"✅ Loaded {len(df)} records.")
    except Exception as e:
        print(f"⚠️ DB Insert Error: {e}")


def main():
    conn = sqlite3.connect(DB_NAME)
    total = 0
    for year in FIN_YEARS:
        for state in STATES:
            try:
                df = extract_data(state, year)
                df = transform_data(df)
                if df is not None:
                    load_to_db(df, conn)
                    total += len(df)
                time.sleep(1)
            except Exception as e:
                print(f"⚠️ Error for {state}, {year}: {e}")
    conn.close()
    print(f"\n🎯 ETL Completed — Total {total} records saved to {DB_NAME}")


if __name__ == "__main__":
    main()
