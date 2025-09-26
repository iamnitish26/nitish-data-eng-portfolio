import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def get_spark(app_name="batch-etl-trips-advanced"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

SCHEMA = T.StructType([
    T.StructField("trip_id", T.IntegerType()),
    T.StructField("ts", T.StringType()),      # read as string, cast later
    T.StructField("city", T.StringType()),
    T.StructField("km", T.DoubleType()),
    T.StructField("fare", T.DoubleType()),
])

def transform(df):
    df = df.withColumn("ts", F.to_timestamp("ts"))
    return (
        df
        .withColumn("dt", F.to_date("ts"))
        .withColumn("fare_per_km", F.when(F.col("km") > 0, F.col("fare") / F.col("km")))
        .withColumn("city", F.initcap("city"))
        .withColumn("hour", F.hour("ts"))
        .withColumn("is_peak", F.col("hour").isin([7, 8, 9, 17, 18, 19]).cast("boolean"))
    )

def main(args):
    spark = get_spark()
    df = (
        spark.read
        .schema(SCHEMA)
        .option("header", True)
        .csv(args.input)
    )

    # Optional incremental window
    if args.since:
        df = df.filter(F.col("ts") >= args.since)
    if args.until:
        df = df.filter(F.col("ts") < args.until)

    out = transform(df)

    (out
     .repartition(args.partitions)  # more partitions for bigger data
     .write
     .mode("overwrite" if args.overwrite else "append")
     .partitionBy("dt")
     .parquet(args.output))

    print("Wrote:", args.output)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--since", help='inclusive lower bound timestamp, e.g. "2025-09-01 00:00:00"')
    ap.add_argument("--until", help='exclusive upper bound timestamp, e.g. "2025-09-02 00:00:00"')
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--partitions", type=int, default=8)
    args = ap.parse_args()
    main(args)
