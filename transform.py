import pandas as pd

def transform_weather(raw: dict) -> pd.DataFrame:
    """
    Transform raw hourly API response into a clean daily summary DataFrame
    """
    city = raw["city"]
    hourly = raw["hourly"]

    print(f"[transform] Processing hourly data for {city}...")

    # Build a DataFrame from the hourly arrays
    df = pd.DataFrame({
        "timestamp": pd.to_datetime(hourly["time"]),
        "temperature_c": hourly["temperature_2m"],
        "precipitation_mm": hourly["precipitation"],
        "windspeed_kmh": hourly["windspeed_10m"]
    })

    # Drop any rows with missing values
    before = len(df)
    df = df.dropna()
    after = len(df)
    if before != after:
        print(f"[transform] Dropped {before - after} rows with null values")

    # Add derived columns
    df["date"] = df["timestamp"].dt.date
    df["city"] = city
    df["temperature_f"] = (df["temperature_c"] * 9 / 5) + 32

    # Aggregate: hourly -> daily
    daily = df.groupby(["city", "date"]).agg(
        avg_temp_c=("temperature_c", "mean"),
        max_temp_c=("temperature_c", "max"),
        min_temp_c=("temperature_c", "min"),
        avg_temp_f=("temperature_f", "mean"),
        total_precipitation_mm=("precipitation_mm", "sum"),
        avg_windspeed_km=("windspeed_kmh", "mean")
    ).reset_index()

    # Round floats for readability
    float_cols = daily.select_dtypes(include="float").columns
    daily[float_cols] = daily[float_cols].round(2)

    print(f"[transform] Produced {len(daily)} daily records")
    return daily

if __name__ == "__main__":
    import json
    with open("data/raw/tokyo.json") as f:
        raw = json.load(f)
    df = transform_weather(raw)
    print(df.to_string(index=False))