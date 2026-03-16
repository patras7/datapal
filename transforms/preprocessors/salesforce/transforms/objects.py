from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
from transforms.verbs import columns as C


def generate_objects_metadata(sys_tablecolumns: DataFrame):
    return (
        sys_tablecolumns.select("TableName")
        .distinct()
        .select(
            F.col("TableName").alias("object_name"),
            F.array(F.lit("Id")).alias("primary_key_raw_fields"),
            F.initcap(
                F.regexp_replace(C.camel_to_snake_case(F.col("TableName")), "_", " ")
            ).alias("description"),
            F.array()
            .cast(T.ArrayType(T.StringType(), False))
            .alias("text_table_names"),
        )
        .coalesce(1)
    )
