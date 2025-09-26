import datetime
import json
import os

import requests

API_TOKEN = os.getenv("IDFM_API_TOKEN")
URL = "https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2"

headers = {"apiKey": API_TOKEN}
resp = requests.get(URL, headers=headers)

print(resp.status_code)
data = resp.json()
print(json.dumps(data, indent=2)[:2000])  # Preview first 2000 chars

# Check data freshness
events = data.get("disruptions", [])


def parse_ts(ts):
    return datetime.strptime(ts, "%Y%m%dT%H%M%S")


for ev in events[:10]:
    periods = ev.get("applicationPeriods", [])
    if periods:
        start = parse_ts(periods[0]["begin"])
        end = parse_ts(periods[0].get("end")) if "end" in periods[0] else None
        print(ev["id"], start, end)

# Check field completeness
missing_end = [
    ev
    for ev in events
    if not ev.get("applicationPeriods")
    or "end" not in ev["applicationPeriods"][0]
]
print(f"Events with no end timestamp: {len(missing_end)}")

# Check descriptions
for ev in events[:5]:
    print(ev.get("id"), ev.get("messages", [{}])[0].get("text"))

# Check variety of line values / impacted objects
lines = []
for ev in events:
    for obj in ev.get("impactedObjects", []):
        line = obj.get("ptObject", {}).get("line")
        if line:
            lines.append(line.get("id"))

print(list(set(lines))[:20])

# Check size
print("Total events returned:", len(events))

# Timezones / timestamp formats
if events and events[0].get("applicationPeriods"):
    print(events[0]["applicationPeriods"][0])
