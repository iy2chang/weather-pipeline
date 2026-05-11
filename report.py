import duckdb

DB_PATH = "data/weather.duckdb"

def print_report() -> None:
    """
    Query DuckDB and print a weekly weather summary
    """
    con = duckdb.connect(DB_PATH)

    print("\n" + "=" * 50)
    print("  WEEKLY WEATHER REPORT")
    print("=" * 50)

    # Overall summary per city
    summary = con.execute("""
        SELECT
            city,
            MIN(date)                   AS from_date,
            MAX(date)                   AS to_date,
            ROUND(AVG(avg_temp_c), 1)   AS avg_temp_c,
            MAX(max_temp_c)             AS peak_temp_c,
            MIN(min_temp_c)             AS lowest_temp_c,
            ROUND(SUM(total_precipitation_mm), 1)   AS total_rain_mm,
            ROUND(AVG(avg_windspeed_kmh), 1)        AS avg_wind_kmh
        FROM daily_weather
        GROUP BY city
        ORDER BY city
    """).fetchdf()


    for _, row in summary.iterrows():
        print(f"\n {row['city']} ({row['from_date']} -> {row['to_date']})")
        print(f" Avg temp   : {row['avg_temp_c']} °C")
        print(f" Peak temp  : {row['peak_temp_c']} °C")
        print(f" Lowest temp: {row['lowest_temp_c']} °C")
        print(f" Total rain : {row['total_rain_mm']} mm")
        print(f" Avg wind   : {row['avg_wind_kmh']} km/h")

    # Rainest day
    rainiest = con.execute("""
        SELECT city, date, total_precipitation_mm
        FROM daily_weather
        ORDER BY total_precipitation_mm DESC
        LIMIT 1                       
    """).fetchone()

    if rainiest:
        print(f"\n Rainiest day: {rainiest[1]} in {rainiest[0]} {rainiest[2]} mm")

    # Daily breakdown
    print("\n Daily Breakdown")
    print("-" * 50)
    daily = con.execute("""
        SELECT city, date, avg_temp_c, total_precipitation_mm, avg_windspeed_kmh
        FROM daily_weather
        ORDER BY city, date                    
    """).fetchdf()

    for _, row in daily.iterrows():
        rain = f" {row['total_precipitation_mm']}mm" if row['total_precipitation_mm'] > 0 else "dry"
        print(f" {row['city']} | {row['date']} | {row['avg_temp_c']}°C | {rain} | {row['avg_windspeed_kmh']} km/h")

    print("=" * 50 + "\n")
    con.close()

if __name__ == "__main__":
    print_report()