import argparse, json, os, glob, pandas as pd

def run(parquet_path, expectations_file):
    files = glob.glob(os.path.join(parquet_path, "**/*.parquet"), recursive=True)
    if not files:
        raise SystemExit("No parquet files found; run the batch ETL first.")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    with open(expectations_file, "r") as f:
        exp = json.load(f)["expectations"]
    results = []
    for e in exp:
        t = e["type"]
        k = e["kwargs"]
        if t == "expect_table_row_count_to_be_between":
            ok = (len(df) >= k.get("min_value", 0)) and (k.get("max_value") is None or len(df) <= k["max_value"])
        elif t == "expect_column_values_to_not_be_null":
            ok = df[k["column"]].notna().all()
        elif t == "expect_column_values_to_be_between":
            col = df[k["column"]]
            ok = (col >= k.get("min_value", col.min())).all() and (k.get("max_value") is None or (col <= k["max_value"]).all())
        else:
            ok = True
        results.append({"expectation": t, "passed": bool(ok)})
    print(json.dumps({"results": results}, indent=2))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--expectations", default="expectations/trips_expectations.json")
    args = ap.parse_args()
    run(args.parquet, args.expectations)
