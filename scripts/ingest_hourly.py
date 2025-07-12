from ingest_utils import (
    find_fresh_hour,
    download_file,
    build_s3_key,
    upload_to_s3,
)

# ------------------------
# Main Ingestion Script
# ------------------------

def main():
    ts = find_fresh_hour()
    raw_bytes = download_file(ts)
    s3_key = build_s3_key(ts)
    upload_to_s3(raw_bytes, s3_key)

if __name__ == "__main__":
    main()