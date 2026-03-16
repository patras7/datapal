from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from pyspark.sql import DataFrame, functions as F
from transforms import verbs as V

from software_defined_integrations.transforms.preprocessors.oracle_ebs.utils.types import (
    OracleEbsFieldNameAlias,
    OracleEbsObjectAlias,
)


# used to get all actual tables + tables in data dictionary (FND TABLES)
def generate_tables_metadata(
    fnd_tables: DataFrame,
    all_tables: DataFrame,
    source_config: SourceConfig,
) -> DataFrame:
    return (
        fnd_tables.join(
            all_tables.select(
                OracleEbsFieldNameAlias.owner, OracleEbsFieldNameAlias.table_name
            ).distinct(),
            on=[OracleEbsFieldNameAlias.table_name],
        )
        .withColumnRenamed(OracleEbsFieldNameAlias.owner, OracleEbsObjectAlias.owner)
        .withColumnRenamed(
            OracleEbsFieldNameAlias.table_name, OracleEbsObjectAlias.object_name
        )
        .distinct()
        .coalesce(1)
    )


def generate_primary_keys_metadata(
    tables: DataFrame,
    columns: DataFrame,
    primary_keys: DataFrame,
    primary_key_columns: DataFrame,
    source_config: SourceConfig,
) -> DataFrame:
    """
    This method prepares primary key metadata by joining tables, columns,  primary_keys and primary_key_columns dataset.
    Then aggregates by tablename and primary key id. Primary keys fields are represented as List of columns.

    In the Oracle EBS release R12, there may be more than 1 primary key for a given table in FND_PRIMARY_KEYS.
    This table does not have a column to identify real primary keys. In later releases according to oracle
    documentation, there is a new column for this and based on documentation below, `audit_key_flag` is used as
    filter to filter the primary keys.

    Duplicated PK are removed , any of the primary key is sufficient for uniqueness.

    Doc Reference
    https://docs.oracle.com/en/cloud/saas/applications-common/21d/oedma/fndprimarykeys-11242.html#fndprimarykeys-11242

    UPDATE : There are some tables with primary keys type D only, therefore we added fallback support to primary keys
    if there is no S , it will fallback to D. The aim is to cover as much table as possible from DD tables.
    """

    # actual primary keys
    primary_key_type_s = primary_keys.filter(
        (F.col(OracleEbsFieldNameAlias.audit_key_flag) == "Y")
        & (F.col(OracleEbsFieldNameAlias.primary_key_type) == "S")
    )
    # if there is no S type primary key found, we will fallback to Type D.
    primary_key_type_d = primary_keys.filter(
        (F.col(OracleEbsFieldNameAlias.audit_key_flag) == "Y")
        & (F.col(OracleEbsFieldNameAlias.primary_key_type) == "D")
    )

    # filtered out that are only in type D.
    fallback_primary_keys = primary_key_type_d.join(
        primary_key_type_s,
        on=[OracleEbsFieldNameAlias.application_id, OracleEbsFieldNameAlias.table_id],
        how="leftanti",
    ).select(primary_key_type_s.columns)

    all_primary_keys = primary_key_type_s.unionByName(fallback_primary_keys)

    primary_keys_df = (
        (
            tables.join(
                all_primary_keys.join(
                    primary_key_columns.join(
                        columns.select(
                            F.col(OracleEbsFieldNameAlias.application_id),
                            F.col(OracleEbsFieldNameAlias.table_id),
                            F.col(OracleEbsFieldNameAlias.column_id),
                            F.col(OracleEbsFieldNameAlias.column_name),
                        ),
                        on=[
                            OracleEbsFieldNameAlias.application_id,
                            OracleEbsFieldNameAlias.table_id,
                            OracleEbsFieldNameAlias.column_id,
                        ],
                    ),
                    on=[
                        OracleEbsFieldNameAlias.application_id,
                        OracleEbsFieldNameAlias.table_id,
                        OracleEbsFieldNameAlias.primary_key_id,
                    ],
                ),
                on=[
                    OracleEbsFieldNameAlias.application_id,
                    OracleEbsFieldNameAlias.table_id,
                ],
            ).select(
                F.col(OracleEbsObjectAlias.object_name),
                F.col(OracleEbsFieldNameAlias.primary_key_id).alias(
                    OracleEbsObjectAlias.primary_key_id
                ),
                F.col(OracleEbsFieldNameAlias.column_id).alias(
                    OracleEbsObjectAlias.primary_column_id
                ),
                F.col(OracleEbsFieldNameAlias.column_name).alias(
                    OracleEbsObjectAlias.primary_column_name
                ),
                F.col(OracleEbsObjectAlias.owner),
            )
        )
        .groupBy(
            OracleEbsObjectAlias.object_name,
            OracleEbsObjectAlias.primary_key_id,
            OracleEbsObjectAlias.owner,
        )
        .agg(
            F.sort_array(
                F.collect_list(F.col(OracleEbsObjectAlias.primary_column_name))
            ).alias(OracleEbsObjectAlias.primary_object_raw_fields)
        )
        # .orderBy(OracleEbsObjectAlias.object_name, OracleEbsFieldNameAlias.primary_key_id)
        # .dropDuplicates([OracleEbsObjectAlias.object_name])
        .distinct()
        .coalesce(1)
    )

    primary_keys_df = (
        (
            V.group_by(OracleEbsObjectAlias.object_name)
            .max_by(OracleEbsObjectAlias.primary_key_id)
            .apply(primary_keys_df)
        )
        .distinct()
        .coalesce(1)
    )

    return primary_keys_df


def generate_foreign_keys_metadata(
    tables: DataFrame,
    columns: DataFrame,
    foreign_keys: DataFrame,
    foreign_key_columns: DataFrame,
    source_config: SourceConfig,
) -> DataFrame:
    """
    This method prepares foreign key metadata by joining tables, columns,  foreign_keys and foreign_key_columns dataset.
    Then aggregates by tablename and foreign key id. Foreign keys fields are represented as List of columns.

    Additionally it also contains relations to foreign tables and keys of that tables.
    """

    return (
        (
            tables.join(
                foreign_keys.join(
                    foreign_key_columns.join(
                        columns.select(
                            F.col(OracleEbsFieldNameAlias.application_id),
                            F.col(OracleEbsFieldNameAlias.table_id),
                            F.col(OracleEbsFieldNameAlias.column_id),
                            F.col(OracleEbsFieldNameAlias.column_name),
                        ),
                        on=[
                            OracleEbsFieldNameAlias.application_id,
                            OracleEbsFieldNameAlias.table_id,
                            OracleEbsFieldNameAlias.column_id,
                        ],
                    ),
                    on=[
                        OracleEbsFieldNameAlias.application_id,
                        OracleEbsFieldNameAlias.table_id,
                        OracleEbsFieldNameAlias.foreign_key_id,
                    ],
                ),
                on=OracleEbsFieldNameAlias.table_id,
            ).select(
                F.col(OracleEbsObjectAlias.object_name).alias(
                    OracleEbsObjectAlias.foreign_object_name
                ),
                F.col(OracleEbsFieldNameAlias.foreign_key_id).alias(
                    OracleEbsObjectAlias.foreign_key_id
                ),
                F.col(OracleEbsFieldNameAlias.column_id).alias(
                    OracleEbsObjectAlias.foreign_column_id
                ),
                F.col(OracleEbsFieldNameAlias.column_name).alias(
                    OracleEbsObjectAlias.foreign_column_name
                ),
                F.col(OracleEbsFieldNameAlias.primary_key_table_id).alias(
                    OracleEbsObjectAlias.primary_object_name
                ),
                F.col(OracleEbsFieldNameAlias.primary_key_id).alias(
                    OracleEbsObjectAlias.primary_key_id
                ),
                F.col(OracleEbsObjectAlias.owner),
            )
        )
        .groupBy(
            OracleEbsObjectAlias.foreign_object_name,
            OracleEbsObjectAlias.foreign_key_id,
            OracleEbsObjectAlias.primary_object_name,
            OracleEbsObjectAlias.primary_key_id,
            OracleEbsObjectAlias.owner,
        )
        .agg(
            F.sort_array(
                F.collect_list(F.col(OracleEbsObjectAlias.foreign_column_name))
            ).alias(OracleEbsObjectAlias.foreign_object_raw_fields)
        )
        .distinct()
        .coalesce(1)
    )


def generate_columns_metadata(
    tables_metadata_df: DataFrame, fnd_columns: DataFrame, source_config: SourceConfig
) -> DataFrame:
    """
    Prepares column metadata by joining tables_metada and FND_COLUMNS dataset.
    """
    return (
        tables_metadata_df.join(
            fnd_columns.withColumnRenamed(
                OracleEbsFieldNameAlias.description,
                OracleEbsObjectAlias.field_description,
            ),
            on=[
                OracleEbsFieldNameAlias.application_id,
                OracleEbsFieldNameAlias.table_id,
            ],
        )
        .distinct()
        .coalesce(1)
    )
