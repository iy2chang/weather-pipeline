import duckdb
import pandas as pd
import os

DB_PATH = "data/weather.duckdb"

def load_weather(df: pd.DataFrame) -> None:
    """
    Load a cleaned daily weather DataFrame into DuckDB
    """
    os.makedirs("data", exist_ok=True)
    con = duckdb.connect(DB_PATH)

    # Create table if it doesn't exist
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_weather (
            city        VARCHAR,
            date        DATE,
            avg_temp_c  DOUBLE,
            max_temp_c  DOUBLE,
            min_temp_c  DOUBLE,
            avg_temp_f  DOUBLE,
            total_precipitation_mm DOUBLE,
            avg_windspeed_kmh   DOUBLE,
            PRIMARY KEY (city, date)  
        )
    """)

    # Delete exisiting rows for this city+date range so we can re-insert cleanly
    cities = df["city"].unique().tolist()
    for city in cities:
        con.execute("DELETE FROM daily_weather WHERE city = ?", [city])
    
    # Insert the new data
    con.execute("INSERT INTO daily_weather SELECT * FROM df")

    row_count = con.execute("SELECT COUNT(*) FROM daily_weather").fetchone()[0]
    print(f"[load] Inserted {len(df)} rows. Total rows in DB: {row_count}")

    con.close()

if __name__ == "__main__":
    import json
    from transform import transform_weather
    with open("data/raw/tokyo.json") as f:
        raw = json.load(f)
    df = transform_weather(raw)
    load_weather(df)