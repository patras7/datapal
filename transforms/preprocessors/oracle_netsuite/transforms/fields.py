from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T


def generate_fields_metadata(columns: DataFrame) -> DataFrame:
    return columns.select(
        F.col("Table").alias("object_name"),
        F.col("Column").alias("field_name"),
        F.col("Column").alias("raw_field_name"),
        F.col("Type").alias("field_type"),
        F.col("Description").alias("field_description"),
        F.lit(None).cast(T.StringType()).alias("domain_name"),
        F.lit(None).cast(T.StringType()).alias("field_literal"),
    ).coalesce(1)
