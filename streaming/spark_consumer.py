from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType
import psycopg2

spark = SparkSession.builder \
    .appName("KafkaCryptoConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

schema = StructType() \
    .add("id", StringType()) \
    .add("symbol", StringType()) \
    .add("name", StringType()) \
    .add("current_price", DoubleType()) \
    .add("market_cap", DoubleType()) \
    .add("total_volume", DoubleType()) \
    .add("last_updated_at", StringType()) \
    .add("ingested_at", DoubleType())

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

def write_to_postgres(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    conn = psycopg2.connect(
        dbname="crypto_staging",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    for row in rows:
        cur.execute("""
            INSERT INTO live_crypto_data (id, symbol, name, current_price, market_cap, total_volume, last_updated_at, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))
            ON CONFLICT (id) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                name = EXCLUDED.name,
                current_price = EXCLUDED.current_price,
                market_cap = EXCLUDED.market_cap,
                total_volume = EXCLUDED.total_volume,
                last_updated_at = EXCLUDED.last_updated_at,
                ingested_at = EXCLUDED.ingested_at
        """, (row.id, row.symbol, row.name, row.current_price, row.market_cap,
              row.total_volume, row.last_updated_at, row.ingested_at))

    conn.commit()
    cur.close()
    conn.close()
    print(f"Upserted {len(rows)} rows into live_crypto_data (batch {batch_id}).")

query = parsed_stream.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

query.awaitTermination()
