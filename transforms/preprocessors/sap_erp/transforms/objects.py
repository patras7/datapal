from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from software_defined_integrations.transforms.preprocessors.sap_erp.utils.types import (
    SapFieldNameAlias,
)


# Used to get English tables of names
def generate_objects_metadata(
    dd02l: DataFrame,
    dd02t: DataFrame,
    full_table_relations: DataFrame,
    table_fields_metadata: DataFrame,
    source_config: SourceConfig,
) -> DataFrame:
    partitions = [SapFieldNameAlias.table_name]
    order = [
        SapFieldNameAlias.table_name,
        SapFieldNameAlias.position_of_the_field_in_the_table,
    ]
    w = Window.partitionBy(*partitions).orderBy(*order)
    table_primary_keys = (
        table_fields_metadata.filter(
            F.col(SapFieldNameAlias.identifies_a_key_field_of_a_table) == "X"
        )
        .withColumn(
            "primary_key_raw_fields",
            F.collect_list(F.col(SapFieldNameAlias.field_name)).over(w),
        )
        .groupBy(*partitions)
        .agg(
            F.max("primary_key_raw_fields").alias("primary_key_raw_fields"),
        )
        .select(
            F.col(SapFieldNameAlias.table_name).alias("object_name"),
            F.col("primary_key_raw_fields"),
        )
    )
    dd02t = dd02t.filter(F.col("DDLANGUAGE") == source_config.language_key)

    text_table_rows = (
        full_table_relations.filter(F.col(SapFieldNameAlias.metadata) == F.lit("TEXT"))
        .select(
            F.col(SapFieldNameAlias.check_table_name).alias("object_name"),
            F.col(SapFieldNameAlias.table_name).alias("text_table_names"),
        )
        .dropDuplicates()
    )

    return (
        (
            dd02l.join(dd02t, on="TABNAME", how="left")
            .withColumnRenamed("TABNAME", "object_name")
            .join(table_primary_keys, on="object_name", how="inner")
            .join(text_table_rows, on="object_name", how="left")
            .select(
                F.col("object_name"),
                F.col("primary_key_raw_fields"),
                F.col("DDTEXT").alias("description"),
                F.col("text_table_names"),
            )
            # Edge case when a table has multiple text tables
            .groupby("object_name", "primary_key_raw_fields", "description")
            .agg(F.collect_list("text_table_names").alias("text_table_names"))
        )
        .distinct()
        .coalesce(1)
    )
