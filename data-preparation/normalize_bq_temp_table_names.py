from pyspark import sql

spark = sql.SparkSession.builder.appName(
    "Normalize BigQuery temporary table names"
).getOrCreate()
