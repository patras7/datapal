from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def generate_links_metadata(table_fields: DataFrame):
    table_fields = table_fields.select(
        F.col("TABLE_NAME").alias("primary_object_name"),
        F.col("COLUMN_NAME").alias("primary_object_raw_fields"),
        F.col("DATA_TYPE").alias("foreign_object_name"),
        F.col("DATA_TYPE_EXT").alias("foreign_object_raw_fields"),
        F.col("DATA_LENGTH").alias("link_description"),
    )

    return table_fields
