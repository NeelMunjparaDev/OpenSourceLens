from datetime import datetime,timezone, timedelta


def last_completed_hour_utc():

    now = datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
