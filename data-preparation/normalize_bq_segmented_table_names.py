from datetime import datetime
import pathlib
import re
import sys
from typing import List

from pyspark import sql
from pyspark.sql import DataFrame


"""
====== Helper functions ================================================================
"""


def _delete_date_suffixes(network_connection_record: List[str]) -> List[str]:
    return [
        _delete_date_suffix(network_connection_record[0]),  # from_entity_id
        _delete_date_suffix(network_connection_record[1]),  # to_entity_id
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


def _delete_random_number_suffixes(network_connection_record: List[str]) -> List[str]:
    return [
        _delete_random_number_suffix(network_connection_record[0]),  # from_entity_id
        _delete_random_number_suffix(network_connection_record[1]),  # to_entity_id
    ]


def _delete_random_number_suffix(entity_id: str) -> str:
    split_id = entity_id.split("_")
    suffix = split_id[-1]
    return (
        "_".join(split_id[:-1])
        if len(suffix) >= 10 and re.match("^\\d+$", suffix)
        else entity_id
    )


def _delete_random_suffixes(
    network_connection_record: List[str], prefixes: List[str]
) -> List[str]:
    return [
        _delete_random_suffix(network_connection_record[0], prefixes),  # from_entity_id
        _delete_random_suffix(network_connection_record[1], prefixes),  # to_entity_id
    ]


def _delete_random_suffix(entity_id: str, prefixes: List[str]) -> str:
    split_id = entity_id.split(".")
    table_id = split_id[-1]

    for prefix in prefixes:
        if table_id.startswith(prefix):
            normalized_id_parts = split_id[:-1]
            normalized_id_parts.append(prefix)
            return ".".join(normalized_id_parts)

    return entity_id


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

# The input CSV file or folder should be passed as the first argument to spark-submit.
path_arg = pathlib.Path(sys.argv[1])
# User-known segmented table prefixes.
known_prefixes = []
try:
    arg_index = sys.argv.index("--known-prefixes")
    known_prefixes = sys.argv[arg_index + 1].split(",")
except ValueError:
    known_prefixes = []
# Get the parent folder of the resource represented by `path_arg`.
data_files_path = path_arg.parents[1] if path_arg.is_file() else path_arg.parent
# This is an intermediate step of the data preparation pipeline, so the results are
# persisted into the `staging/no-segments` folder.
output_folder = data_files_path.joinpath("staging").joinpath("no-segments")

spark = sql.SparkSession.builder.appName(
    "Normalize BigQuery segmented table names"
).getOrCreate()

df = spark.read.csv(str(path_arg), header=True)
_print_data_frame_stats("original", df)

rdd = (
    df.rdd.map(lambda row: _delete_date_suffixes(row))
    .map(lambda row: _delete_random_number_suffixes(row))
    .map(lambda row: _delete_random_suffixes(row, known_prefixes))
)

normalized_df = rdd.toDF(df.schema.names)
_print_data_frame_stats("normalized", normalized_df)
normalized_df.write.mode("overwrite").parquet(str(output_folder))
