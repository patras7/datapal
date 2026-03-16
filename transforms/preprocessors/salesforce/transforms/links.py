from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def generate_links_metadata(sys_foreignkeys: DataFrame):
    return sys_foreignkeys.select(
        F.col("TableName").alias("primary_object_name"),
        F.array(F.col("ColumnName")).alias("primary_object_raw_fields"),
        F.col("ReferencedTableName").alias("foreign_object_name"),
        F.array(F.col("ReferencedColumnName")).alias("foreign_object_raw_fields"),
        F.col("ForeignKeyName").alias("link_description"),
    ).coalesce(1)
