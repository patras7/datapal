from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T


def generate_objects_metadata(
    tables: DataFrame, indexes: DataFrame, index_columns: DataFrame, columns: DataFrame
):
    columns = columns.select(
        F.col("name").alias("raw_field_name"),
        F.col("object_id"),
        F.col("column_id"),
    )

    index_columns = index_columns.select(
        F.col("object_id"),
        F.col("column_id"),
        F.col("index_id"),
    ).join(
        columns,
        on=["object_id", "column_id"],
        how="inner",
    )

    indexes = indexes.filter(F.col("is_primary_key")).select(
        F.col("object_id"),
        F.col("is_primary_key"),
        F.col("index_id"),
    )

    tables = (
        tables.select(
            F.col("name"),
            F.col("object_id"),
            F.col("schema_id"),
        )
        .join(
            indexes,
            on="object_id",
            how="left",
        )
        .join(index_columns, on=["object_id", "index_id"], how="left")
        # TODO: double check the sortinig is done corrected
        .sort(F.col("schema_id"), F.col("name"), F.col("column_id"))
        .groupBy("name", "object_id", "index_id")
        .agg(F.collect_list(F.col("raw_field_name")).alias("primary_key_raw_fields"))
    )

    return tables.select(
        F.col("name").alias("object_name"),
        F.col("primary_key_raw_fields"),
        F.lit(None).astype("string").alias("description"),
        F.array().cast(T.ArrayType(T.StringType())).alias("text_table_names"),
    )
