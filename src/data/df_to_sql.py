import glob
import os.path
import sqlite3
import pandas as pd

file_pattern = 'processed_data/processed_*_titanic.csv'
files = glob.glob(file_pattern)
latest_file = max(files, key=os.path.getctime)

df = pd.read_csv(latest_file)
print(f"Processing file: {latest_file}")
print(df)

conn = sqlite3.connect('titanic_data.db')
df.to_sql('titanic_passengers', conn, if_exists='replace', index=False)
conn.close()
