from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType
from transforms import verbs as V

from software_defined_integrations.transforms.preprocessors.oracle_ebs.utils.types import (
    OracleEbsFieldNameAlias,
    OracleEbsObjectAlias,
)


# Used for renaming columns from table name to display names
def generate_fields_metadata(
    columns_metadata_df: DataFrame,
    source_config: SourceConfig,
) -> DataFrame:
    """
    Prepares the final fields datasets. Oracle EBS does not have a concept of field_literal and domain_name
    Duplicated fieldnames for each table is removed, this is due to duplicated table entry in FND tables.

    """
    fields_df = (
        columns_metadata_df.withColumn(
            OracleEbsObjectAlias.domain_name, F.lit(None).cast(StringType())
        )
        .withColumn("field_literal", F.lit(None).cast(StringType()))
        .select(
            # F.col(OracleEbsFieldNameAlias.table_name).alias("object_name"),
            F.concat(
                F.lit('"'),
                F.col(OracleEbsObjectAlias.owner),
                F.lit('"."'),
                F.col(OracleEbsObjectAlias.object_name),
                F.lit('"'),
            ).alias(OracleEbsObjectAlias.object_name),
            F.col(OracleEbsFieldNameAlias.column_name).alias(
                OracleEbsObjectAlias.field_name
            ),
            F.col(OracleEbsFieldNameAlias.column_name).alias(
                OracleEbsObjectAlias.raw_field_name
            ),
            F.col(OracleEbsFieldNameAlias.column_type).alias(
                OracleEbsObjectAlias.field_type
            ),
            F.col(OracleEbsObjectAlias.field_description),
            F.col(OracleEbsObjectAlias.domain_name),
            F.col(OracleEbsObjectAlias.field_literal),
            # F.col(OracleEbsFieldNameAlias.owner).alias("owner")
        )
        .distinct()
        .coalesce(1)
    )

    fields_df = (
        (
            V.group_by(
                [OracleEbsObjectAlias.object_name, OracleEbsObjectAlias.field_name]
            )
            .max_by(
                [
                    OracleEbsObjectAlias.object_name,
                    OracleEbsObjectAlias.field_name,
                    OracleEbsObjectAlias.field_description,
                ]
            )
            .apply(fields_df)
        )
        .distinct()
        .coalesce(1)
    )

    return fields_df
