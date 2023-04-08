import sys

from pyspark import sql

"""
===== Main Spark code ==================================================================
"""

# The input CSV file name should be passed as the first argument to spark-submit.
input_file = sys.argv[1]
data_files_path = "/".join(input_file.split("/")[:-2])
# This is the last step of the data preparation pipeline, so the results are persisted
# into the `out` folder.
output_folder = f"{data_files_path}/out"

spark = sql.SparkSession.builder.appName("Dedup network edges").getOrCreate()

df = spark.read.parquet(input_file)

distinct_df = df.distinct()
distinct_df.write.mode("overwrite").option("header", True).csv(output_folder)
