from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.window import Window
from software_defined_integrations.transforms.preprocessors.sap_erp.utils.types import (
    SapFieldNameAlias,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig


# Used primarily to generate pkey information and determine what's a non-pkey field
def generate_table_fields_metadata(
    dd03l: DataFrame, dd04t: DataFrame, dd02l: DataFrame, source_config: SourceConfig
) -> DataFrame:
    dd04t = dd04t.filter(F.col("DDLANGUAGE") == source_config.language_key)

    # Certain SAP sources have DATATYPE " ", which we want to filter out
    dd03l = dd03l.filter(
        F.col("DATATYPE").isNotNull() & ~F.trim("DATATYPE").eqNullSafe("")
    )
    table_fields_metadata = (
        dd02l.join(dd03l, "TABNAME", "left")
        .join(dd04t, "ROLLNAME", "left")
        .select(
            F.col("TABNAME").alias(SapFieldNameAlias.table_name),
            F.col("FIELDNAME").alias(SapFieldNameAlias.field_name),
            F.col("ROLLNAME").alias(SapFieldNameAlias.field_data_element),
            F.col("DOMNAME").alias(SapFieldNameAlias.domain_name),
            F.col("KEYFLAG").alias(SapFieldNameAlias.identifies_a_key_field_of_a_table),
            F.col("POSITION").alias(
                SapFieldNameAlias.position_of_the_field_in_the_table
            ),
            F.col("DDLANGUAGE").alias(SapFieldNameAlias.language_key),
            F.col("DATATYPE").alias(SapFieldNameAlias.data_type_in_abap_dictionary),
            F.col("SCRTEXT_S").alias(SapFieldNameAlias.short_field_label),
            F.col("SCRTEXT_L").alias(SapFieldNameAlias.long_field_label),
        )
        .withColumn(SapFieldNameAlias.field_literal, F.lit(None).cast(T.StringType()))
        .withColumn(
            SapFieldNameAlias.position_of_the_field_in_the_table,
            F.col(SapFieldNameAlias.position_of_the_field_in_the_table).cast(
                T.IntegerType()
            ),
        )
    )

    # SPRAS (Language Key) Special Logic:
    # ===================================
    # SPRAS in a primary key = SAP System Language Key
    # SPRAS NOT in a primary key = Row-level Language Key
    # For tables where System-level language keys are missing, we want to create a fake field
    # called `SPRAS` which will contain the literal text of source_system.language_key
    #
    # In this logic, we produce a `SPRAS` on any table that does not have a `SPRAS` field yet
    # and give it a field_data_element and domain_name of `SPRAS` so it can join to real `SPRAS` fields.

    # Compute the next possible position in order to insert SPRAS there
    w = Window.partitionBy(table_fields_metadata[SapFieldNameAlias.table_name])
    table_fields_metadata = table_fields_metadata.withColumn(
        SapFieldNameAlias.next_position,
        F.count(SapFieldNameAlias.table_name).over(w) + 1,
    )

    # Find the tables that contain SPRAS
    tables_with_spras = table_fields_metadata.filter(
        F.col(SapFieldNameAlias.field_name) == "SPRAS"
    )

    # Find the tables where SPRAS does not exist in pkey
    tables_without_spras = (
        table_fields_metadata.select(
            SapFieldNameAlias.table_name,
            SapFieldNameAlias.next_position,
        )
        .join(tables_with_spras, [SapFieldNameAlias.table_name], "left_anti")
        .dropDuplicates()
    )

    # Create the entries for a generated literal SPRAS
    generated_spras_fields = (
        tables_without_spras.withColumn(SapFieldNameAlias.field_name, F.lit("SPRAS"))
        .withColumn(SapFieldNameAlias.field_data_element, F.lit("SPRAS"))
        .withColumn(SapFieldNameAlias.domain_name, F.lit("SPRAS"))
        .withColumn(
            SapFieldNameAlias.identifies_a_key_field_of_a_table,
            F.lit(None).cast(T.StringType()),
        )
        .withColumn(
            SapFieldNameAlias.position_of_the_field_in_the_table,
            F.col(SapFieldNameAlias.next_position),
        )
        .withColumn(SapFieldNameAlias.language_key, F.lit(source_config.language_key))
        .withColumn(SapFieldNameAlias.data_type_in_abap_dictionary, F.lit("CHAR"))
        .withColumn(SapFieldNameAlias.short_field_label, F.lit("SDDI Language Key"))
        .withColumn(SapFieldNameAlias.long_field_label, F.lit("SDDI Language Key"))
        .withColumn(SapFieldNameAlias.field_literal, F.lit(source_config.language_key))
    )

    # Union the base table with the literal SPRAS fields
    table_fields_metadata = table_fields_metadata.unionByName(
        generated_spras_fields
    ).drop(SapFieldNameAlias.next_position)

    return table_fields_metadata
