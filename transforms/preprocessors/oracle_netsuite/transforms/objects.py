from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
from transforms.verbs import columns as C


def generate_objects_metadata(columns: DataFrame):
    return (
        columns.select(
            F.col("Table").alias("object_name"),
            F.array(F.lit("InternalId")).alias("primary_key_raw_fields"),
            F.initcap(
                F.regexp_replace(C.camel_to_snake_case(F.col("Table")), "_", " ")
            ).alias("description"),
            F.array()
            .cast(T.ArrayType(T.StringType(), False))
            .alias("text_table_names"),
        )
        .distinct()
        .coalesce(1)
    )
