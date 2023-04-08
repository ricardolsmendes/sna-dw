import pathlib
import sys

from pyspark import sql

"""
===== Main Spark code ==================================================================
"""

# The input CSV file name should be passed as the first argument to spark-submit.
input_file = pathlib.Path(sys.argv[1])
# Get the two-levels-up folder.
data_files_path = input_file.parents[1]
# This is the last step of the data preparation pipeline, so the results are persisted
# into the `out` folder.
output_folder = data_files_path.joinpath("out")

spark = sql.SparkSession.builder.appName("Dedup network edges").getOrCreate()

df = spark.read.parquet(str(input_file))

distinct_df = df.distinct()
distinct_df.write.mode("overwrite").option("header", True).csv(str(output_folder))
