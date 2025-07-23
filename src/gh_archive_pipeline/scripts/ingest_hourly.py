from gh_archive_pipeline.utils.ingest_utils import (
    find_fresh_hour,
    download_file,
    build_s3_key,
    build_local_key,
    upload_to_s3,
    store_in_local
)

# ------------------------
# Main Ingestion Script
# ------------------------

def main():
    ts = find_fresh_hour()
    raw_bytes = download_file(ts)
    # s3_key = build_s3_key(ts)
    local_key = build_local_key(ts)
    # upload_to_s3(raw_bytes, s3_key)
    store_in_local(raw_bytes,local_key)