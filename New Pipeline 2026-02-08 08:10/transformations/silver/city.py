import dlt
from pyspark.sql.functions import current_timestamp,col
@dlt.table(
    name =  "travel_insights.silver.city",
    comment = 'silver table for cities',
    table_properties = {
        'quality' : "silver",
        'layer' : 'silver',
        'source_format' : 'csv',
        'delta.enableChangeDataFeed' : 'true',
        'delta.autoOptimize.optimizeWrite' : 'true',
        'delta.autoOptimize.autoCompact' : 'true'

    }
)
def silver_processed_city():
    df_silver = spark.table('travel_insights.bronze.city')
    df_silver = df_silver.select(df_silver.city_id, df_silver.city_name, col('ingestion_date').alias('bronze_ingest_time')).withColumn('silver_processed_time',current_timestamp())

    return df_silver