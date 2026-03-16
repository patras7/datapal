from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from transforms import verbs as V


def generate_objects_metadata(tables: DataFrame, table_fields: DataFrame):
    tables = tables.select(
        F.col("TABLE_NAME").alias("object_name"),
        F.col("COMMENTS").alias("description"),
        F.col("LAST_MODIFIED"),
    ).dropDuplicates()

    tables = V.group_by(["object_name"]).max_by("LAST_MODIFIED").apply(tables)

    table_fields = (
        V.group_by(["TABLE_NAME", "COLUMN_NAME"]).max_by("COMMENTS").apply(table_fields)
    )

    table_fields = table_fields.filter(F.col("PRIMARY_KEY") == F.lit("PK"))

    table_fields = table_fields.select(
        F.col("TABLE_NAME").alias("object_name"),
        F.col("COLUMN_NAME").alias("raw_field_name"),
        F.col("COMMENTS").alias("description"),
    )

    table_fields = (
        table_fields.groupBy("object_name")
        # TODO: Think we will need to somehow sort these fields before collect_list
        .agg(
            F.collect_list(F.col("raw_field_name")).alias("primary_key_raw_fields")
        ).select(
            F.col("object_name"),
            F.col("primary_key_raw_fields"),
            F.lit(None).cast(T.StringType()).alias("description"),
            F.lit(None).cast(T.StringType()).alias("text_table_names"),
        )
    )

    # TODO: starts with 'table' on the left since there are more tables in the 'tables' datasets than 'table_fields'
    # Not sure if it is right
    tables = tables.join(table_fields, on="object_name", how="left")

    return table_fields
