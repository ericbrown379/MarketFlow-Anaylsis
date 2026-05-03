import time
import os
import pathlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, avg, stddev, lag, lit, round, when, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import traceback
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("CryptoMetricsCalculator") \
    .getOrCreate()

def load_dotenv_from_dir(dir_path: str):
    env_path = pathlib.Path(dir_path) / '.env'
    if not env_path.exists():
        return
    print(f"Loading env vars from {env_path}")
    for raw in env_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        k, v = line.split('=', 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        # Do not overwrite existing env vars
        if os.environ.get(k) is None:
            os.environ[k] = v

# Attempt to load a .env in the same directory as this file
HERE = os.path.dirname(__file__)
load_dotenv_from_dir(HERE)
import traceback
import pyspark.sql.functions as F

# Use the SparkSession created above
spark.sparkContext.setLogLevel("ERROR")

# == PostgreSQL Connection Config ==
# PostgreSQL connection comes from environment variables so you can run locally or in CI
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "crypto_metrics")
PG_USER = os.environ.get("PG_USER", "ericbrown")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "")

jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
main_table = "crypto_table"
gainers_table = "top_5_gainers"
losers_table = "top_5_losers"
db_properties = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

print(f"Postgres JDBC URL: {jdbc_url}")
print(f"Postgres DB user: {PG_USER}")

while True:
    try:
        print("\n=== Running Spark Job ===")

        # Load recent parquet files and drop unused columns
        df = spark.read.parquet(
            "/Users/ericbrown/dev/MarketFlow-Analysis/dataframes"
        ).drop(
            "market_cap", "total_volume", "high_24h", "low_24h", "last_updated"
        )

        # Explicit Type Casting
        # Use ingestion_time (created by spark streaming job) as timestamp column
        df = df.withColumn("ingestion_time", to_timestamp("ingestion_time")) \
            .withColumn("price", col("price").cast(DoubleType()))
        
        # Filter data from last 7 minutes
        latest_data = df.filter(col("ingestion_time") >= F.current_timestamp() - F.expr("INTERVAL 7 MINUTES"))
        row_count = latest_data.count()

        if row_count == 0:
            print("No data found in the last 7 minutes. Skipping this cycle")
        else:
            # Define window partition
            coin_window = Window.partitionBy("id").orderBy("ingestion_time")

            # Price Changes (1 min, 5 min) with null guards
            latest_data = latest_data \
                .withColumn("price_1min_ago", lag("price", 1).over(coin_window)) \
                .withColumn("price_5min_ago", lag("price", 5).over(coin_window)) \
                .withColumn(
                    "change_1min",
                    when(
                        col("price_1min_ago").isNull(), None
                    ).otherwise(
                        round((col("price") - col("price_1min_ago")) / col("price_1min_ago") * 100, 2)
                    )
                ) \
                .withColumn(
                    "change_5min",
                    when(
                        col("price_5min_ago").isNull(), None
                    ).otherwise(
                        round((col("price") - col("price_5min_ago")) / col("price_5min_ago") * 100, 2)
                    )
                ) \
                .drop("price_1min_ago", "price_5min_ago")
            
            # SMA and EMA (rolling windows)
            latest_data = latest_data \
                .withColumn("SMA", avg("price").over(coin_window.rowsBetween(-4, 0))) \
                .withColumn("EMA", avg("price").over(coin_window.rowsBetween(-2, 0)))
            
            # Volatility
            latest_data = latest_data \
                .withColumn("volatility", stddev("price").over(coin_window.rowsBetween(-4, 0)))
            
            ## Gainers and Losers with global ranking
            global_rank_gain = Window.orderBy(col("change_5min").desc())
            gain_df = latest_data \
                .withColumn("rank", row_number().over(global_rank_gain)) \
                .filter(col("rank") <= 5) \
                .select("rank", "id", "symbol", "change_5min")

            global_rank_loss = Window.orderBy(col("change_5min").asc())
            loss_df = latest_data \
                .withColumn("rank", row_number().over(global_rank_loss)) \
                .filter(col("rank") <= 5) \
                .select("rank", "id", "symbol", "change_5min")

            # === Show Outputs ===
            print("\n=== All Metrics (Latest Snapshot) ===")
            latest_data.select(
                "ingestion_time", "id", "symbol", "price", "change_1min", "change_5min", "SMA", "EMA", "volatility"
            ).orderBy("ingestion_time").show(truncate=False)

            print("\n=== Top 5 Gainers (5 min change) ===")
            gain_df.show(truncate=False)

            print("\n=== Top 5 Losers (5 min change) ===")
            loss_df.show(truncate=False)

            # === Prepare explicit column ordering and types for JDBC write ===
            # Some DB schemas use different column names (e.g. 'timestamp' instead of 'ingestion_time',
            # and lowercase 'sma'/'ema'). Alias accordingly to match the target DB.
            to_write = (latest_data
                        .withColumn("timestamp", to_timestamp("ingestion_time"))
                        .withColumn("sma", col("SMA").cast(DoubleType()))
                        .withColumn("ema", col("EMA").cast(DoubleType()))
                        .select(
                            "timestamp",
                            "id",
                            "symbol",
                            "price",
                            "change_1min",
                            "change_5min",
                            "sma",
                            "ema",
                            "volatility",
                        ))

            # Write main table (append)
            to_write.write.jdbc(
                url=jdbc_url,
                table=main_table,
                mode="append",
                properties=db_properties
            )

            # For gain/loss tables ensure columns exist and write (overwrite)
            gain_write_cols = ["rank", "id", "symbol", "change_5min"]
            gain_df.select(*gain_write_cols).write.jdbc(
                url=jdbc_url,
                table=gainers_table,
                mode="overwrite",
                properties=db_properties
            )

            loss_write_cols = ["rank", "id", "symbol", "change_5min"]
            loss_df.select(*loss_write_cols).write.jdbc(
                url=jdbc_url,
                table=losers_table,
                mode="overwrite",
                properties=db_properties
            )
            

            print("Data written to PostgreSQL (all 3 tables)")

        spark.catalog.clearCache()
    
    except Exception as e:
        print(f"Error in Spark job: {e}")
        traceback.print_exc()

    # Sleep
    total_seconds = 360
    print("Waiting for next run:")
    for remaining in range(total_seconds, 0, -1):
        mins, secs = divmod(remaining, 60)
        print(f"\rNext run in {mins:02}:{secs:02}", end="")
        time.sleep(1)
    print("\n")


