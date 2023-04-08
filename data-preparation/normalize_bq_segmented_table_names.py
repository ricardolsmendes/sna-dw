from datetime import datetime
import re
from typing import List
import sys

from pyspark import sql


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


"""
===== Main Spark code ==================================================================
"""

# The input CSV file name should be passed as the first argument to spark-submit.
input_file = sys.argv[1]
data_files_path = "/".join(input_file.split("/")[:-2])
# This is an intermediate step of the data preparation pipeline, so the results are
# persisted into the `staging/no-segments` folder.
output_folder = f"{data_files_path}/staging/no-segments"

spark = sql.SparkSession.builder.appName(
    "Normalize BigQuery segmented table names"
).getOrCreate()

df = spark.read.csv(input_file, header=True)

rdd = df.rdd.map(lambda lineage_record: _delete_date_suffixes(lineage_record))

normalized_df = rdd.toDF(df.schema.names)
normalized_df.write.mode("overwrite").parquet(output_folder)
