from datetime import datetime
import pathlib
import re
from typing import List
import sys

from pyspark import sql
from pyspark.sql import DataFrame


"""
====== Helper functions ================================================================
"""


def _delete_date_suffixes(lineage_record: List[str]) -> List[str]:
    return [
        _delete_date_suffix(lineage_record[0]),
        _delete_date_suffix(lineage_record[1]),
    ]


def _delete_date_suffix(entity_id: str) -> str:
    split_id = entity_id.split("_")
    suffix = split_id[-1]
    suffix = re.sub("-", "", suffix)
    try:
        datetime.strptime(suffix, "%Y%m%d")
        return "_".join(split_id[:-1])
    except ValueError:
        return entity_id


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
# This is an intermediate step of the data preparation pipeline, so the results are
# persisted into the `staging/no-segments` folder.
output_folder = data_files_path.joinpath("staging").joinpath("no-segments")

spark = sql.SparkSession.builder.appName(
    "Normalize BigQuery segmented table names"
).getOrCreate()

df = spark.read.csv(str(input_file), header=True)
_print_data_frame_stats("original", df)

rdd = df.rdd.map(lambda lineage_record: _delete_date_suffixes(lineage_record))

normalized_df = rdd.toDF(df.schema.names)
_print_data_frame_stats("normalized", normalized_df)
normalized_df.write.mode("overwrite").parquet(str(output_folder))
