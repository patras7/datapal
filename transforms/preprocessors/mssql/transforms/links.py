from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from transforms.verbs import columns as C


def generate_links_metadata(
    tables: DataFrame,
    foreign_keys: DataFrame,
    foreign_keys_columns: DataFrame,
    columns: DataFrame,
):
    tables_foreign = tables.select(
        F.col("name").alias("foreign_table_name"),
        F.col("object_id").alias("tables_foreign_object_id"),
    )

    tables_primary = tables.select(
        F.col("name").alias("primary_table_name"),
        F.col("object_id").alias("tables_primary_object_id"),
    )

    columns_foreign = columns.select(
        F.col("name").alias("foreign_column_name"),
        F.col("object_id").alias("columns_foreign_object_id"),
        F.col("column_id").alias("columns_foreign_column_id"),
    )

    columns_primary = columns.select(
        F.col("name").alias("primary_table_column"),
        F.col("object_id").alias("columns_primary_object_id"),
        F.col("column_id").alias("columns_primary_column_id"),
    )

    foreign_keys = foreign_keys.withColumn(
        "link_description", F.regexp_replace("name", "FK_", "")
    ).select(
        F.col("link_description"),
        F.col("parent_object_id").alias("fk_parent_object_id"),
        F.col("object_id").alias("fk_object_id"),
        F.col("referenced_object_id").alias("fk_referenced_object_id"),
    )

    foreign_keys_columns = foreign_keys_columns.select(
        F.col("constraint_object_id"),
        F.col("parent_object_id").alias("fkc_parent_object_id"),
        F.col("parent_column_id").alias("fkc_parent_column_id"),
        F.col("referenced_object_id").alias("fkc_referenced_object_id"),
        F.col("referenced_column_id").alias("fkc_referenced_column_id"),
    )

    links = tables_foreign.join(
        foreign_keys,
        on=[
            (
                tables_foreign["tables_foreign_object_id"]
                == foreign_keys["fk_parent_object_id"]
            )
        ],
        how="inner",
    )

    links = links.join(
        foreign_keys_columns,
        on=[(links["fk_object_id"] == foreign_keys_columns["constraint_object_id"])],
        how="inner",
    )

    links = links.join(
        columns_foreign,
        on=[
            (
                links["fkc_parent_object_id"]
                == columns_foreign["columns_foreign_object_id"]
            )
            & (
                links["fkc_parent_column_id"]
                == columns_foreign["columns_foreign_column_id"]
            )
        ],
        how="inner",
    )

    links = links.join(
        columns_primary,
        on=[
            (
                links["fkc_referenced_object_id"]
                == columns_primary["columns_primary_object_id"]
            )
            & (
                links["fkc_referenced_column_id"]
                == columns_primary["columns_primary_column_id"]
            )
        ],
        how="inner",
    )

    links = links.join(
        tables_primary,
        on=[
            (
                links["fk_referenced_object_id"]
                == tables_primary["tables_primary_object_id"]
            )
        ],
        how="inner",
    )

    links = links.withColumn(
        "primary_object_name",
        F.regexp_replace(C.camel_to_snake_case(F.col("primary_table_name")), "__", "_"),
    ).withColumn(
        "foreign_object_name", C.camel_to_snake_case(F.col("foreign_table_name"))
    )

    links = (
        links
        # TODO: revisit sorting - primary_object_name = OwnershipDeckInterestTb,
        # the orders for primary_object_raw_fields and foreign_object_raw_fields
        # would be different if we use bothh columns_foreign_column_id and
        # columns_primary_column_id for sorting.
        # Resort to just using columns_foreign_column_id,
        # since it aligns with primary key ordering in object.py
        .sort(F.col("columns_foreign_column_id"))
        .groupBy(
            "primary_table_name",
            "foreign_table_name",
            # TODO: visit the commented out line to understand if it is needed
            # F.lit(SOURCE_SYSTEM).alias('source_system'),
            "link_description",
        )
        .agg(
            F.collect_list("foreign_column_name").alias("foreign_object_raw_fields"),
            F.collect_list("primary_table_column").alias("primary_object_raw_fields"),
        )
    )

    return links.select(
        F.col("foreign_table_name").alias("primary_object_name"),
        F.col("foreign_object_raw_fields").alias("primary_object_raw_fields"),
        F.col("primary_table_name").alias("foreign_object_name"),
        F.col("primary_object_raw_fields").alias("foreign_object_raw_fields"),
        "link_description",
    )
