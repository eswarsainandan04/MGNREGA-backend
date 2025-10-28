import pandas as pd
import sqlite3

# Step 1: Load the Excel file
excel_file = "district.xlsx"  # Replace with your actual file path
df = pd.read_excel(excel_file)

# Step 2: Connect to the SQLite database
conn = sqlite3.connect("mgnrega.db")
cursor = conn.cursor()

# Step 3: Create the new table
cursor.execute("""
CREATE TABLE IF NOT EXISTS district_city (
    City TEXT,
    District TEXT
);
""")

# Step 4: Push data into the SQLite table
df.to_sql("district_city", conn, if_exists="replace", index=False)

# Step 5: Commit and close
conn.commit()
conn.close()

print("✅ Data successfully inserted into district_city table in mgnrega.db")
