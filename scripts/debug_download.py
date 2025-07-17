"""
Debug helper – download any hour with a progress bar.
Not used in production cron jobs.
"""
import io
from datetime import datetime
from tqdm.auto import tqdm
import ingest_utils as iu


def download_file(ts: datetime, timeout: int = 60) -> bytes:
    """"
    Download the file from the GH archive URL 
    for the given timestamp.
    """
    url = iu.build_url(ts)
    res = iu.requests.get(url, stream=True, timeout=timeout)
    res.raise_for_status()
    total = int(res.headers.get('Content-Length', 0))
    bar = tqdm(total=total, unit='B', unit_scale=True, desc=f"Downloading {ts.isoformat()}")
    buf = io.BytesIO()
    for chunk in res.iter_content(chunk_size=8192):
        bar.update(len(chunk))
        buf.write(chunk)
    bar.close()
    return buf.getvalue()

if __name__ == "__main__":
    ts = datetime(2025, 7, 10, 12)
    download_file(ts)