from datetime import datetime,timezone, timedelta
import time
import os
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

# Environment variable configuration
GH_ARCHIVE_URL = os.getenv("GH_ARCHIVE_URL")

if not GH_ARCHIVE_URL:
    raise ValueError("GH_ARCHIVE_URL environment variable is not set.")

# Constants
# The maximum number of hours to look back for data
MAX_LOOK_BACK = 24


# ------------------------
# Helper Functions
# ------------------------

def build_url(ts: datetime) -> str:
    """
    Build the URL for the GitHub archive for the given timestamp.
    """
    return GH_ARCHIVE_URL.format(ts=ts.strftime("%Y-%m-%d-%H"))

def url_exists(url:str) -> bool:
    """
    Check if the URL exists.
    """
    try:
        res = requests.head(url, timeout=10)
        return res.status_code == 200
    except requests.RequestException:
        return False

def last_completed_hour_utc():
    """
    Get the last completed hour in UTC.
    """
    now = datetime.now(timezone.utc)
    return now - timedelta(hours=1)

def find_fresh_hour() -> datetime:
    """
    Find the most recent hour with available data.
    It checks the last completed hour and looks back up to MAX_LOOK_BACK hours.
    """
    ts = last_completed_hour_utc()
    for hrs_back in range(MAX_LOOK_BACK):
        if url_exists(build_url(ts)):
            if hrs_back > 0:
                print(f"Found data {hrs_back} hour(s) back.")
            return ts
        ts -= timedelta(hours=1)
    raise ValueError(f"No available data found in the last {MAX_LOOK_BACK} hours.")

def download_file(ts: datetime, timeout: int = 60) -> bytes:
    """"
    Download the file from the GH archive URL 
    for the given timestamp.
    """
    url = build_url(ts)
    print(f"Downloading {url} ...")
    res = requests.get(url, stream=True, timeout=timeout)
    res.raise_for_status()
    return res.content