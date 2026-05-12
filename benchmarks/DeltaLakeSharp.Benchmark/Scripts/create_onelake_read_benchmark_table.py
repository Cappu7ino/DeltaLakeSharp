# pyright: reportMissingImports=false, reportUndefinedVariable=false

from pyspark.sql import functions as F


TABLE_NAME = "delta_bench_read_1m"
ROW_COUNT = 1_000_000


spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")

df = spark.range(ROW_COUNT).select(
    F.col("id").cast("long").alias("id"),
    ((F.col("id") % F.lit(1000)) + F.lit(1)).cast("int").alias("tenant_id"),
    F.expr(
        "timestamp'2024-01-01 00:00:00' + INTERVAL 1 SECOND * CAST(id % 31536000 AS INT)"
    ).alias("event_ts"),
    F.when((F.col("id") % 5) == 0, F.lit("US-East"))
    .when((F.col("id") % 5) == 1, F.lit("US-West"))
    .when((F.col("id") % 5) == 2, F.lit("EU-West"))
    .when((F.col("id") % 5) == 3, F.lit("APAC"))
    .otherwise(F.lit("SA-East"))
    .alias("region"),
    F.when((F.col("id") % 6) == 0, F.lit("Electronics"))
    .when((F.col("id") % 6) == 1, F.lit("Clothing"))
    .when((F.col("id") % 6) == 2, F.lit("Home"))
    .when((F.col("id") % 6) == 3, F.lit("Books"))
    .when((F.col("id") % 6) == 4, F.lit("Sports"))
    .otherwise(F.lit("Garden"))
    .alias("category"),
    (((F.col("id") * F.lit(13)) % F.lit(100000)).cast("double") / F.lit(100.0)).alias(
        "amount"
    ),
    ((F.col("id") % 10) != 0).cast("boolean").alias("is_active"),
)

(
    df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE_NAME)
)

count = spark.table(TABLE_NAME).count()
print(f"Created table '{TABLE_NAME}' with {count} rows.")
