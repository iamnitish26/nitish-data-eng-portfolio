import argparse, json, random, time, uuid, datetime as dt
import boto3

ACTIONS = ["view","click","purchase"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", required=True)
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--region", default="eu-west-2")
    args = ap.parse_args()

    client = boto3.client("kinesis", region_name=args.region)
    for _ in range(args.count):
        rec = {
            "event_time": dt.datetime.utcnow().isoformat() + "Z",
            "user_id": random.randint(1,5),
            "action": random.choice(ACTIONS),
            "amount": round(random.random()*50, 2),
            "event_id": str(uuid.uuid4())
        }
        client.put_record(StreamName=args.stream, Data=json.dumps(rec), PartitionKey=str(rec["user_id"]))
        time.sleep(0.05)

if __name__ == "__main__":
    main()
