import argparse
import csv
import random
import datetime as dt

CITIES = [
    "London", "Watford", "Birmingham", "Manchester",
    "Leeds", "Glasgow", "Liverpool", "Bristol"
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="../../data/raw/trips_big.csv")
    ap.add_argument("--rows", type=int, default=100_000)  # default: 100k rows
    ap.add_argument("--start", default="2025-08-25")      # starting date
    ap.add_argument("--days", type=int, default=7)        # number of days
    args = ap.parse_args()

    start = dt.datetime.fromisoformat(args.start)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["trip_id", "ts", "city", "km", "fare"])
        tid = 100000

        for i in range(args.rows):
            # random timestamp within the date range
            d = start + dt.timedelta(minutes=random.randint(0, args.days * 24 * 60))
            city = random.choices(
                CITIES,
                weights=[30, 10, 12, 15, 8, 8, 10, 7],  # bias towards London
                k=1
            )[0]

            # trip distance (skewed, realistic distribution)
            km = round(max(0.4, random.lognormvariate(0.6, 0.6)), 2)

            # base fare + surge multiplier at peak hours
            base = 2.5 + km * 1.6
            surge = (
                random.choice([1.0, 1.1, 1.2, 1.4, 1.8])
                if d.hour in (7, 8, 9, 17, 18, 19)
                else 1.0
            )
            fare = round(base * surge, 2)

            w.writerow([tid + i, d.strftime("%Y-%m-%d %H:%M:%S"), city, km, fare])

if __name__ == "__main__":
    main()
