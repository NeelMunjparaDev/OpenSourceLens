import io
import requests
from datetime import datetime, timedelta
import boto3
from tqdm.auto import tqdm


def build_url(ts: datetime) -> str:
    """
    Build the GH archive URL for the given timestamp.
    Format: https://data.gharchive.org/YYYY-MM-DD-H.json.gz
    """
    return ts.strftime("https://data.gharchive.org/%Y-%m-%d-%-H.json.gz")


def download_file(ts: datetime, timeout: int = 60) -> bytes:
    """
    Download the file from GH Archive for the given timestamp.
    """
    url = build_url(ts)
    response = requests.get(url, stream=True, timeout=timeout)
    response.raise_for_status()
    
    total = int(response.headers.get("Content-Length", 0))
    buf = io.BytesIO()
    bar = tqdm(total=total, unit='B', unit_scale=True, desc=f"Downloading {url}")
    
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            buf.write(chunk)
            bar.update(len(chunk))
    bar.close()
    
    buf.seek(0)
    return buf.getvalue()


def upload_to_s3(content: bytes, ts: datetime, bucket: str, prefix: str = "bronze"):
    """
    Upload given bytes to S3 in the correct hourly folder structure.
    """
    s3_key = f"{prefix}/{ts.strftime('%Y/%m/%d/%H')}/data.json.gz"
    s3 = boto3.client("s3")
    s3.upload_fileobj(io.BytesIO(content), bucket, s3_key)
    print(f"✅ Uploaded to s3://{bucket}/{s3_key}")


def main():
    start = datetime(2025, 7, 1, 17)
    end = datetime(2025, 7, 15, 23)
    bucket = "gh-lake-neel"  # TODO: change to your bucket name

    current = start
    while current <= end:
        try:
            content = download_file(current)
            upload_to_s3(content, current, bucket)
        except Exception as e:
            print(f"❌ Failed at {current}: {e}")
        current += timedelta(hours=1)


if __name__ == "__main__":
    main()
