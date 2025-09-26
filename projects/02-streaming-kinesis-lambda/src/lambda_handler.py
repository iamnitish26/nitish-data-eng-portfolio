import base64, json, boto3, os, datetime as dt
from collections import defaultdict

S3_BUCKET = os.getenv("S3_BUCKET","<your-bucket>")
S3_PREFIX = os.getenv("S3_PREFIX","stream/aggregates/")
s3 = boto3.client("s3")

def handler(event, context):
    counts = defaultdict(lambda: defaultdict(int))
    amounts = defaultdict(lambda: defaultdict(float))

    for record in event.get("Records", []):
        payload = base64.b64decode(record["kinesis"]["data"])
        rec = json.loads(payload)
        hour = rec["event_time"][:13]  # YYYY-MM-DDTHH
        action = rec.get("action","unknown")
        counts[hour][action] += 1
        amounts[hour][action] += float(rec.get("amount",0.0))

    for hour, actions in counts.items():
        out = {
            "hour": hour,
            "counts": actions,
            "amounts": amounts[hour],
            "ingested_at": dt.datetime.utcnow().isoformat()+"Z"
        }
        key = f"{S3_PREFIX}hour={hour}/aggregates.json"
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(out).encode("utf-8"))
    return {"ok": True, "hours": list(counts.keys())}
