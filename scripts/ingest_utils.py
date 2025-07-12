import os
import time
import io
from datetime import datetime, timezone, timedelta
import requests
import boto3
from tqdm import tqdm
from dotenv import load_dotenv
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, EndpointConnectionError
from logger_util import get_logger


# logging configuration
logger = get_logger(__name__)

# Load environment variables
load_dotenv()

# Environment variable configuration
GH_ARCHIVE_URL = os.getenv("GH_ARCHIVE_URL")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")

# Check if required environment variables are set
if not BRONZE_BUCKET:
    raise ValueError("BRONZE_BUCKET environment variable is not set.")

if not GH_ARCHIVE_URL:
    raise ValueError("GH_ARCHIVE_URL environment variable is not set.")

# Constants
# The maximum number of hours to look back for data
MAX_LOOK_BACK = 24


# ------------------------
# Time Helper Functions
# ------------------------

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
                return ts
        ts -= timedelta(hours=1)
    raise ValueError(f"No available data found in the last {MAX_LOOK_BACK} hours.")

# ------------------------------
# Path And Key Helper Functions
# ------------------------------

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
    
def build_s3_key(ts: datetime) -> str:
    """
    Build an S3 key for the given timestamp.
    """
    return ts.strftime("bronze/%Y/%m/%d/%H/events.json.gz")

# ------------------------
# I/O Helper Functions
# ------------------------

def download_file(ts: datetime, timeout: int = 60) -> bytes:
    """"
    Download the file from the GH archive URL 
    for the given timestamp.
    """
    url = build_url(ts)
    try:
        res = requests.get(url, stream=True, timeout=timeout)
        res.raise_for_status()
        data = res.content
        logger.info(f"Downloaded {len(data) / 1_048_576:.2f} MB from {url}")
        return data
    except requests.RequestException as e:
        logger.error(f"Failed to download {url}: {e}")
        raise

def upload_to_s3(data: bytes, s3_key: str):
    """
    Upload raw data to S3.
    """
    try:
        s3 = boto3.client('s3')
        s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=data
        )
        logger.info(f"Uploaded {s3_key} to S3 bucket {BRONZE_BUCKET}")
    except (BotoCoreError, EndpointConnectionError) as e:
        logger.error(f"Failed to upload {s3_key} to S3: {e}")
        raise

    