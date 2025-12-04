import json, os, datetime, urllib.request, boto3

S3_BUCKET = os.getenv("RAW_BUCKET", "your-raw-bucket")
S3_PREFIX = os.getenv("RAW_PREFIX", "etl/raw/")

s3 = boto3.client("s3")

def handler(event, context):
    # Example: fetch sample JSON from a public placeholder API
    url = os.getenv("SOURCE_URL", "https://jsonplaceholder.typicode.com/todos")
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read())

    key = f"{S3_PREFIX}{datetime.datetime.utcnow().strftime('%Y/%m/%d/%H%M%S')}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(data).encode("utf-8"))

    return {"status": "ok", "written": f"s3://{S3_BUCKET}/{key}"}
