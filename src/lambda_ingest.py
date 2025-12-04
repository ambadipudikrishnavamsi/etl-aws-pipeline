import json
import os
import datetime
import urllib.request
import boto3

# S3 bucket (change this to your actual bucket name)
S3_BUCKET = os.getenv("RAW_BUCKET", "your-raw-bucket-name")
S3_PREFIX = os.getenv("RAW_PREFIX", "etl/raw/")

s3 = boto3.client("s3")

def handler(event, context):
    """
    Example Lambda function that fetches JSON from a public API
    and writes it to S3 as raw data.
    """

    # Example API source (you can replace this with any real API)
    url = os.getenv("SOURCE_URL", "https://jsonplaceholder.typicode.com/todos")

    # Fetch API data
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read())

    # Generate unique filename based on timestamp
    timestamp = datetime.datetime.utcnow().strftime('%Y/%m/%d/%H%M%S')
    key = f"{S3_PREFIX}{timestamp}.json"

    # Upload JSON data to S3
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data).encode("utf-8")
    )

    return {
        "status": "success",
        "message": f"Wrote raw data to s3://{S3_BUCKET}/{key}"
    }
