import dlt
from pyspark.sql import functions as F


@dlt.view(
    name="trips_silver_staging", comment="Transformed trips data ready for CDC upsert"
)
@dlt.expect("valid_date", "year(business_date) >= 2020")
@dlt.expect("valid_driver_rating", "driver_rating BETWEEN 1 AND 10")
@dlt.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")
def trips_silver():
    df_bronze = spark.readStream.table("travel_insights.bronze.trips")
    df_silver = df_bronze.withColumn("passenger_type", F.lower("passenger_type"))

    df_silver = df_bronze.select(
        F.col("trip_id").alias("id"),
        F.col("date").cast("date").alias("business_date"),
        F.col("city_id").alias("city_id"),
        F.col("passenger_type").alias("passenger_category"),
        F.col("distance_travelled_km").alias("distance_kms"),
        F.col("fare_amount").alias("sales_amt"),
        F.col("passenger_rating").alias("passenger_rating"),
        F.col("driver_rating").alias("driver_rating"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp"),
    )

    df_silver = df_silver.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )
    return df_silver


dlt.create_streaming_table(
    name="travel_insights.silver.trips",
    comment="Cleaned and validated orders with CDC upsert capability",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)

dlt.create_auto_cdc_flow(
    target="travel_insights.silver.trips",
    source="trips_silver_staging",
    keys=["id"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)
