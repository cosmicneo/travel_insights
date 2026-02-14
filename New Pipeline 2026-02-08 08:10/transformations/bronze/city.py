import dlt
from pyspark.sql.functions import current_timestamp,col
@dlt.table(
    name =  "travel_insights.bronze.city",
    comment = 'bronze table for cities',
    table_properties = {
        'quality' : "bronze",
        'layer' : 'bronze',
        'source_format' : 'csv',
        'delta.enableChangeDataFeed' : 'true',
        'delta.autoOptimize.optimizeWrite' : 'true',
        'delta.autoOptimize.autoCompact' : 'true'

    }
)
def city_bronze():
    df = spark.read.format('csv').option('header','true').option('inferSchema','true').option('mergeSchema','true').option('columnNameOfCorruptedRecord','_corrupt_record').load('/Volumes/travel_insights/landing/data/data-store/city/city.csv').withColumn('ingestion_date',current_timestamp()).withColumn('source_file',col("_metadata.file_path"))

    return df