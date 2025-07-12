def upload_file_with_streaming(ts, bucket, key, timeout=30):
    """
    Upload a file to S3 using streaming.
    """
    url = build_url(ts)
    print(f"streaming {url} to s3://{bucket}/{key} ...")

    with requests.get(url, stream=True, timeout=timeout) as res:
        res.raise_for_status()
        body = io.BufferedReader(res.raw)
        config = TransferConfig(multipart_chunksize=8*1024*1024,
                                multipart_threshold=8*1024*1024,
                                max_concurrency=5)
        s3 = boto3.client('s3')
        s3.upload_fileobj(
            body,
            bucket,
            key,
            Config=config
        )
        print(f"Uploaded to s3://{bucket}/{key}")

def safe_upload_streaming_files(ts, bucket, key, attempts=3):
    """
    Safely upload a file to S3 with retries.
    """
    for attempt in range(attempts):
        try:
            upload_file_with_streaming(ts, bucket, key)
            return
        except (requests.RequestException, BotoCoreError, EndpointConnectionError) as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            
    raise RuntimeError(f"Failed to upload {key} after {attempts} attempts.")
   