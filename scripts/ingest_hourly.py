from datetime import datetime,timezone, timedelta


def last_completed_hour_utc():
    """
    Get the last completed hour in UTC.
    """
    now = datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

def build_s3_key(ts: datetime) -> str:
    """
    Build an S3 key for the given timestamp.
    """
    return ts.strftime("bronze/%Y/%m/%d/%H/events.json.gz")