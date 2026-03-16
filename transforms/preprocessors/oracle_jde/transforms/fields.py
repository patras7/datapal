from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
from transforms import verbs as V


def generate_fields_metadata(table_fields: DataFrame) -> DataFrame:
    table_fields = (
        V.group_by(["TABLE_NAME", "COLUMN_NAME"]).max_by("COMMENTS").apply(table_fields)
    )

    table_fields = (
        table_fields.select(
            F.col("TABLE_NAME").alias("object_name"),
            F.col("COMMENTS").alias("field_name"),
            F.col("COLUMN_NAME").alias("raw_field_name"),
            F.col("DATA_TYPE").alias("field_type"),
            F.lit(None).cast(T.StringType()).alias("field_description"),
            F.lit(None).cast(T.StringType()).alias("domain_name"),
            F.lit(None).cast(T.StringType()).alias("field_literal"),
        )
        # TODO: Dropping dupes here since a table could be in multiple schema
        # Not sure what that really means from a DB standpoint
        .dropDuplicates()
    )

    return table_fields
