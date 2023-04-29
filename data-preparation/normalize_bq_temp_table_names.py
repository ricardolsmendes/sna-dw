import pathlib
import re
import sys
from typing import List

from pyspark import sql
from pyspark.sql import DataFrame


"""
====== Helper functions ================================================================
"""


def _normalize_temp_dataset_ids(
    network_connection_record: List[str],
    single_temp_id: str,
) -> List[str]:
    return [
        _normalize_temp_dataset_id(
            network_connection_record[0],  # from_entity_id
            single_temp_id,
        ),
        _normalize_temp_dataset_id(
            network_connection_record[1],  # to_entity_id
            single_temp_id,
        ),
    ]


def _normalize_temp_dataset_id(
    entity_id: str,
    single_temp_id: str,
) -> str:
    dataset_id = entity_id.split(".")[1]
    dataset_replacement_regex = None

    if re.match("^[A-Z0-9]+$", dataset_id):
        dataset_replacement_regex = "\\.[A-Z0-9]+\\."

    if re.match("^_script[a-z0-9]+$", dataset_id):
        dataset_replacement_regex = "\\._script[a-z0-9]+\\."

    if not dataset_replacement_regex:
        return entity_id

    print(f"Replacing dataset id with '{single_temp_id} in '{entity_id}'...")
    return re.sub(dataset_replacement_regex, f".{single_temp_id}.", entity_id)


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
# Get the grandparent folder of the resource represented by `path_arg`.
data_files_path = path_arg.parents[2] if path_arg.is_file() else path_arg.parents[1]
# This is an intermediate step of the data preparation pipeline, so the results are
# persisted into the `staging/no-temps` folder.
output_folder = data_files_path.joinpath("staging").joinpath("no-temps")

spark = sql.SparkSession.builder.appName(
    "Normalize BigQuery temporary table names"
).getOrCreate()

df = spark.read.parquet(str(path_arg))
_print_data_frame_stats("original", df)

rdd = df.rdd.map(
    lambda row: _normalize_temp_dataset_ids(row, "big_query_temporary_dataset")
)

normalized_df = rdd.toDF(df.schema.names)
_print_data_frame_stats("normalized", normalized_df)
normalized_df.write.mode("overwrite").parquet(str(output_folder))
