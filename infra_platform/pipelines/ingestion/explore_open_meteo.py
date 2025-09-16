import pandas as pd
import requests

# Paris location
latitude = 48.8566
longitude = 2.3522

# hourly variables
hourly_params = [
    "temperature_2m",
    "precipitation",
    "precipitation_probability",
    "visibility",
    "windspeed_10m",
]

url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": latitude,
    "longitude": longitude,
    "hourly": ",".join(hourly_params),
    "timezone": "Europe/Paris",
}

resp = requests.get(url, params=params)
resp.raise_for_status()
data = resp.json()

# Available Keys
print(data.keys())

# Convert to DataFrame
df = pd.DataFrame(data["hourly"])
print(df.head())
