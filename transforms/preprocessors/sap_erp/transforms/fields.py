from pyspark.sql import DataFrame, functions as F

from software_defined_integrations.transforms.preprocessors.sap_erp.utils.types import (
    SapFieldNameAlias,
)


# Used for renaming columns from table name to display names
def generate_fields_metadata(table_fields_metadata: DataFrame) -> DataFrame:
    return (
        table_fields_metadata.select(
            F.col(SapFieldNameAlias.table_name).alias("object_name"),
            F.col(SapFieldNameAlias.long_field_label).alias("field_name"),
            F.col(SapFieldNameAlias.field_name).alias("raw_field_name"),
            F.col(SapFieldNameAlias.data_type_in_abap_dictionary).alias("field_type"),
            F.col(SapFieldNameAlias.short_field_label).alias("field_description"),
            F.col(SapFieldNameAlias.domain_name).alias("domain_name"),
            F.col(SapFieldNameAlias.field_literal),
            F.col(SapFieldNameAlias.position_of_the_field_in_the_table).alias(
                "field_order"
            ),
        )
        .distinct()
        .coalesce(1)
    )
    # TODO (sbalanovich): Add maybe_add_preceeding_X(field) here
