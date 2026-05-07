import requests
import json
import os
import random
from datetime import datetime, timedelta

def fetch_weather(city: str, latitude: float, longitude: float, days: int = 7, mock: bool = False) -> dict:
    """
    Fetch hourly weather data from Open-Meteo API
    """
    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "start_date": start_date,
        "end_date": end_date,
    }
    print(f"[extract] Fetching weather data for {city} ({start_date} to {end_date})...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    data["city"] = city

    # Save raw JSON as a checkpoint
    os.makedirs("data/raw", exist_ok=True)
    raw_path = f"data/raw/{city.lower().replace(' ','_')}.json"
    with open(raw_path, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"[extract] Saved raw data to {raw_path}")
    return data

if __name__ == "__main__":
    fetch_weather("Tokyo", latitude=35.6762, longitude=139.6503, mock=False)