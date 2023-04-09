import pathlib
import sys

from pyspark import sql
from pyspark.sql import DataFrame


"""
====== Helper functions ================================================================
"""


def _print_data_frame_stats(data_frame_name: str, data_frame: DataFrame) -> None:
    row = data_frame.count()
    col = len(data_frame.columns)

    print(
        f"Dimension (rows, columns) of the {data_frame_name}"
        f" Data Frame is: {(row, col)}"
    )


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
_print_data_frame_stats("original", df)

deduplicated_df = df.distinct()
_print_data_frame_stats("deduplicated", deduplicated_df)
deduplicated_df.write.mode("overwrite").option("header", True).csv(str(output_folder))
