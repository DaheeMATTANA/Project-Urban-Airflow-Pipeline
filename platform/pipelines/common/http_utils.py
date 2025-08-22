import requests
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def fetch_json_with_retry(url: str) -> dict:
    """
    Fetch JSON data from a URL with retry/backoff.
    Retries up to 5 times with exponential backoff.
    """
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()
