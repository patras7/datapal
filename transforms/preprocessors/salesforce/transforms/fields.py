from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T


def generate_fields_metadata(sys_tablecolumns: DataFrame) -> DataFrame:
    return sys_tablecolumns.select(
        F.col("TableName").alias("object_name"),
        F.col("ColumnName").alias("field_name"),
        F.col("ColumnName").alias("raw_field_name"),
        F.col("DataTypeName").alias("field_type"),
        F.col("Description").alias("field_description"),
        F.lit(None).cast(T.StringType()).alias("domain_name"),
        F.lit(None).cast(T.StringType()).alias("field_literal"),
    ).coalesce(1)
