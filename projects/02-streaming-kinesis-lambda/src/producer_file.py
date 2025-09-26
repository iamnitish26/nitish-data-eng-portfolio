# Local-only producer that writes JSONL events to a file to simulate a stream.
import argparse, json, random, uuid, datetime as dt, time

ACTIONS = ["view", "click", "purchase"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/events.jsonl")
    ap.add_argument("--count", type=int, default=200)
    args = ap.parse_args()
    with open(args.out, "w", encoding="utf-8") as f:
        for _ in range(args.count):
            rec = {
                "event_time": dt.datetime.utcnow().isoformat() + "Z",
                "user_id": random.randint(1, 5),
                "action": random.choice(ACTIONS),
                "amount": round(random.random() * 50, 2),
                "event_id": str(uuid.uuid4()),
            }
            f.write(json.dumps(rec) + "\n")
            time.sleep(0.01)


if __name__ == "__main__":
    main()
