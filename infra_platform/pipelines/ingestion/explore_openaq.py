import os

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    raise ValueError("OPENAQ_API_KEY missing ! Add it in your .env")

headers = {"X-API-Key": API_KEY}
API_BASE = "https://api.openaq.org/v3"


def list_parameters():
    print("/n=== List of polluants (parameters) ===")
    resp = requests.get(f"{API_BASE}/parameters", headers=headers)
    resp.raise_for_status()
    for p in resp.json()["results"]:
        print(f"- {p['id']}: {p['name']} ({p['displayName']}, {p['units']})")


def find_locations(city="Paris", limit=3):
    print(f"/n === Searching stations for city={city} ===")
    resp = requests.get(
        f"{API_BASE}/locations",
        headers=headers,
        params={"country_id": 76, "limit": limit},
    )
    resp.raise_for_status()
    results = resp.json()["results"]
    if not results:
        print("No result for this city.")
    return results


def fetch_hourly(sensor_id, limit=3):
    params = {"limit": limit, "page": 1}

    resp = requests.get(
        f"{API_BASE}/sensors/{sensor_id}/hours", headers=headers, params=params
    )
    resp.raise_for_status()
    return resp.json()


def main():
    # Available parameters
    list_parameters()

    # Locations for a city
    locations = find_locations("London", limit=2)

    for loc in locations:
        print(f"\n--- Location: {loc['name']} (ID {loc['id']}) ---")
        for sensor in loc.get("sensors", []):
            sid = sensor["id"]
            pname = sensor["parameter"]["name"]
            print(f"Sensor ID {sid} - {pname}")

            # Recover hourly data
            data = fetch_hourly(sid, limit=3)
            results = data.get("results", [])
            if not results:
                print("No recent data")
                continue
            for r in results:
                ts = r["period"]["start"]
                val = r["value"]
                unit = r.get("unit")
                print(f"{ts} - {val} {unit}")

            # Verify pagination (if +100 results)
            meta = data.get("meta", {})
            if int(meta.get("found", 0)) > len(results):
                print(f"Pagination : total {meta['found']} results")


if __name__ == "__main__":
    main()
