from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, ArrayType
from transforms import verbs as V

from software_defined_integrations.transforms.preprocessors.oracle_ebs.utils.types import (
    OracleEbsFieldNameAlias,
    OracleEbsObjectAlias,
)


def generate_objects_metadata(
    tables_metadata_df: DataFrame,
    primary_keys_df: DataFrame,
    source_config: SourceConfig,
) -> DataFrame:
    """
    Used to get English tables of names
     Duplicated fieldnames for each table is removed, this is due to duplicated table entry in FND tables.
    """

    objects_df = (
        tables_metadata_df.join(
            primary_keys_df,
            on=[OracleEbsObjectAlias.object_name, OracleEbsObjectAlias.owner],
            how="leftouter",
        )
        .withColumn(
            OracleEbsObjectAlias.text_table_names,
            F.lit(None).cast(ArrayType(StringType(), False)),
        )
        .select(
            # F.col("object_name"),
            F.concat(
                F.lit('"'),
                F.col(OracleEbsObjectAlias.owner),
                F.lit('"."'),
                F.col(OracleEbsObjectAlias.object_name),
                F.lit('"'),
            ).alias(OracleEbsObjectAlias.object_name),
            F.col(OracleEbsObjectAlias.primary_object_raw_fields).alias(
                OracleEbsObjectAlias.primary_key_raw_fields
            ),
            F.col(OracleEbsFieldNameAlias.description).alias(
                OracleEbsObjectAlias.description
            ),
            F.col(OracleEbsObjectAlias.text_table_names),
        )
        .filter(
            # (F.col(OracleEbsFieldNameAlias.description).isNotNull()) &
            (F.col(OracleEbsObjectAlias.primary_key_raw_fields).isNotNull())
        )
        .distinct()
        .coalesce(1)
    )

    objects_df = (
        (
            V.group_by(OracleEbsObjectAlias.object_name)
            .max_by(
                OracleEbsObjectAlias.primary_key_raw_fields,
                OracleEbsObjectAlias.description,
            )
            .apply(objects_df)
        )
        .distinct()
        .coalesce(1)
    )

    return objects_df
