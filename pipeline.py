import logging
from extract import fetch_weather
from transform import transform_weather
from load import load_weather
from report import print_report

# Basic logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Citites to track
CITIES = [
    {"name": "Tokyo", "latitude": 35.6762, "longitude": 139.6503}
]

def run_pipeline():
    print("\n Starting weather ELT pipeline...\n")

    for city in CITIES:
        try:
            # Extract
            raw = fetch_weather(city["name"], city["latitude"], city["longitude"], days=7, mock=False)

            # Transform
            df = transform_weather(raw)

            # Load
            load_weather(df)
        
        except Exception as e:
            logging.error(f"Pipeline failed for {city['name']}: {e}")
            raise

    # Report
    print_report()
    print("Pipeline completed!\n")

if __name__ == "__main__":
    run_pipeline()
