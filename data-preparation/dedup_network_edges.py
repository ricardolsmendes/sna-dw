import pathlib
import sys

from pyspark import sql
from pyspark.sql import DataFrame


"""
====== Helper functions ================================================================
"""


def _print_data_frame_stats(data_frame_name: str, data_frame: DataFrame) -> None:
    row_count = data_frame.count()
    col_count = len(data_frame.columns)

    print(
        f"Dimension (rows, columns) of the {data_frame_name}"
        f" Data Frame is: {(row_count, col_count)}"
    )


"""
===== Main Spark code ==================================================================
"""

# The input Parquet file or folder should be passed as the first argument to
# spark-submit.
path_arg = pathlib.Path(sys.argv[1])
# Determine whether the script runs as an intermediate step of a pipeline or not.
is_intermediate_step = len(sys.argv) == 3 and sys.argv[2] == "--intermediate-step"
# Get the grandparent folder of the resource represented by `path_arg`.
data_files_path = path_arg.parents[2] if path_arg.is_file() else path_arg.parents[1]
# The results are persisted  into the `staging/no-duplicates` folder if the script runs
# as an intermediate step of the data preparation pipeline. Otherwise, the results are
# persisted into the `out` folder.
output_folder = (
    data_files_path.joinpath("staging").joinpath("no-duplicates")
    if is_intermediate_step
    else data_files_path.joinpath("out")
)

spark = sql.SparkSession.builder.appName("Dedup network edges").getOrCreate()

df = spark.read.parquet(str(path_arg))
_print_data_frame_stats("original", df)

deduplicated_df = df.distinct()
_print_data_frame_stats("deduplicated", deduplicated_df)

if is_intermediate_step:
    deduplicated_df.write.mode("overwrite").parquet(str(output_folder))
else:
    deduplicated_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(output_folder)
    )
