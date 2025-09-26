import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def get_spark(app_name="batch-etl-trips"):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.sources.partitionOverwriteMode","dynamic")
            .getOrCreate())

SCHEMA = T.StructType([
    T.StructField("trip_id", T.IntegerType()),
    T.StructField("ts", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("km", T.DoubleType()),
    T.StructField("fare", T.DoubleType()),
])

def main(args):
    spark = get_spark()
    df = (spark.read
          .schema(SCHEMA)
          .option("header", True)
          .csv(args.input))

    df = (df
          .withColumn("ts", F.to_timestamp("ts"))
          .withColumn("dt", F.to_date("ts"))
          .withColumn("fare_per_km", F.when(F.col("km")>0, F.col("fare")/F.col("km")).otherwise(None))
          .withColumn("city", F.initcap("city"))
         )

    (df
     .repartition(1)  # demo-friendly
     .write
     .mode("overwrite")
     .partitionBy("dt")
     .parquet(args.output))

    print(f"Wrote curated parquet: {args.output}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--dt", required=False, help="Optional partition date; detected from ts if omitted")
    args = parser.parse_args()
    main(args)
