from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import types as T

from software_defined_integrations.transforms.preprocessors.oracle_ebs.utils.types import (
    OracleEbsFieldNameAlias,
    OracleEbsObjectAlias,
)


def generate_links_metadata(
    primary_key_df: DataFrame,
    foreign_key_df: DataFrame,
    source_config: SourceConfig,
) -> DataFrame:
    """
    Switching from primary to foreign object, from ORACLE logic to Foundry.
    There might be duplicates in FND_FOREIGN_KEYS table.
    """
    return (
        primary_key_df.join(
            foreign_key_df, on=["primary_key_id", OracleEbsObjectAlias.owner]
        )
        .select(
            # F.col("foreign_object_name").alias("primary_object_name"),
            F.concat(
                F.lit('"'),
                F.col(OracleEbsObjectAlias.owner),
                F.lit('"."'),
                F.col(OracleEbsObjectAlias.foreign_object_name),
                F.lit('"'),
            ).alias(OracleEbsObjectAlias.primary_object_name),
            F.col(OracleEbsObjectAlias.foreign_object_raw_fields).alias(
                OracleEbsObjectAlias.primary_object_raw_fields
            ),
            F.col(OracleEbsObjectAlias.object_name).alias(
                OracleEbsObjectAlias.foreign_object_name
            ),
            F.col(OracleEbsObjectAlias.primary_object_raw_fields).alias(
                OracleEbsObjectAlias.foreign_object_raw_fields
            ),
            F.lit(None)
            .cast(T.StringType())
            .alias(OracleEbsObjectAlias.link_description),
        )
        .distinct()
        .coalesce(1)
    )
