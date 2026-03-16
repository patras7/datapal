import functools
import collections
import textwrap
from typing import List, Tuple, Set

from bellhop_authoring_api.bellhop_authoring_api_config_common import ColumnName
from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    TableType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import (
    SourceConfig,
    Table,
)
from bellhop_authoring_api.bellhop_authoring_api_config_transform import BellhopStage
from pyspark import Row
from pyspark.sql import functions as F, Column
from pyspark.sql.dataframe import DataFrame
from transforms.api import configure, incremental, transform
from transforms.verbs.dataframes import union_many

from software_defined_integrations.configs.process_config import (
    get_spark_profile,
    BellhopDirectoryFactory,
    get_customization_tables,
)
from software_defined_integrations.transforms.preprocessors.metadata.utils import (
    find_links,
    extract_fields_metadata,
    extract_literal_fields,
)
from software_defined_integrations.transforms.preprocessors.metadata_schemas import (
    MetadataType,
)
from software_defined_integrations.transforms.types import SpecialFieldName
from software_defined_integrations.transforms.utils import (
    get_primary_object_metadata,
    BELLHOP_COLUMN_PREFIX,
    COLUMN_DELIMITER,
    CUSTOMIZATION_COLUMN_DELIMITER,
    is_dataframe_empty,
    find_table_definition,
    extract_deduplication_comparison_columns_for_table,
    extract_change_mode_column,
)

CUSTOMIZATION_TABLE_ALIAS = "customization_table"


def create_enriched_transform(
    table: Table, pipeline_config: PipelineConfig, source_config: SourceConfig
):
    @configure(
        profile=get_spark_profile(
            table.dataset_transforms_config.spark_profiles, BellhopStage.ENRICHED
        )
    )
    @incremental(
        semantic_version=source_config.deployment_semantic_version,
        snapshot_inputs=["objects_metadata", "fields_metadata", "links_metadata"],
    )
    @transform(
        out=BellhopDirectoryFactory.create_transforms_output(
            pipeline_config,
            source_config,
            BellhopStage.ENRICHED,
            table.dataset_transforms_config.dataset_name,
        ),
        primary_object=BellhopDirectoryFactory.create_transforms_input(
            pipeline_config,
            source_config,
            BellhopStage.CLEANED,
            table.dataset_transforms_config.dataset_name,
        ),
        objects_metadata=BellhopDirectoryFactory.create_transforms_input(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.OBJECTS.value,
        ),
        fields_metadata=BellhopDirectoryFactory.create_transforms_input(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.FIELDS.value,
        ),
        links_metadata=BellhopDirectoryFactory.create_transforms_input(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.LINKS.value,
        ),
        **{
            customization_table.table_name: BellhopDirectoryFactory.create_transforms_input(
                pipeline_config,
                source_config,
                BellhopStage.CLEANED,
                customization_table.dataset_transforms_config.dataset_name,
            )
            for customization_table in get_customization_tables(
                pipeline_config, source_config, table
            )
        },
    )
    def process_function(
        out,
        primary_object,
        objects_metadata,
        fields_metadata,
        links_metadata,
        **enrichments,
    ):
        primary_df = primary_object.dataframe()
        previous_primary_df = primary_object.dataframe("previous")

        # Skip the enrichment work if the table is exclusively a customization.
        if pipeline_config.tables[table.table_name].types == [TableType.CUSTOMIZATION]:
            out.write_dataframe(primary_df)
            return

        # Drop error columns if they exist to avoid noise
        primary_df = primary_df.drop(
            BELLHOP_COLUMN_PREFIX + SpecialFieldName.SCHEMA_MISMATCH_COLUMN_NAME.value
        )
        previous_primary_df = previous_primary_df.drop(
            BELLHOP_COLUMN_PREFIX + SpecialFieldName.SCHEMA_MISMATCH_COLUMN_NAME.value
        )
        links = find_links(links_metadata, pipeline_config, source_config, table)

        # Skip the enrichment work if there's no updates on the primary table and there are no customization tables.

        if is_dataframe_empty(primary_df) and not links:
            if is_dataframe_empty(previous_primary_df):
                out.write_dataframe(primary_df)
            else:
                out.abort()
            return

        # Skip the enrichment work if there are no customization tables.
        # This avoids union-ing the previous views.
        if not links:
            out.write_dataframe(primary_df)
            return

        text_table_names = _get_text_table_names(
            table, objects_metadata.dataframe("current")
        )
        primary_object_metadata = get_primary_object_metadata(
            table.table_name, objects_metadata
        )

        foreign_objects_fields_metadata = extract_fields_metadata(
            list(enrichments.keys()), fields_metadata
        )
        literal_fields = extract_literal_fields(
            table, fields_metadata.dataframe("current")
        )

        for link in links:
            foreign_object_name = link["foreign_object_name"]
            enrichment_df = enrichments[foreign_object_name].dataframe()
            enrichment_table = find_table_definition(foreign_object_name, source_config)

            for prefix, df in [("primary", primary_df), ("foreign", enrichment_df)]:
                assert set(link[prefix + "_object_raw_fields"]).issubset(
                    set(df.columns)
                ), textwrap.dedent(
                    f"""
                    Error in link metadata. Not all {prefix} link fields are present in {prefix} dataframe.
                    Please check the source metadata or preprocessor logic.
                    {prefix} link fields: {link[prefix + "_object_raw_fields"]}
                    {prefix} columns: {df.columns}
                    """
                )

            raw_literal_fields = {
                literal_field["raw_field_name"] for literal_field in literal_fields
            }
            join_columns = list(
                zip(
                    link["primary_object_raw_fields"], link["foreign_object_raw_fields"]
                )
            )

            # Enrich "added" view of the primary object
            primary_df = _enrich_left_join(
                primary_df,
                _rename_enrichment_columns(
                    foreign_object_name,
                    foreign_objects_fields_metadata[foreign_object_name],
                    enrichment_table,
                    source_config,
                    text_table_names,
                    link,
                    raw_literal_fields,
                    enrichments[foreign_object_name].dataframe("current"),
                    primary_object_metadata,
                ),
                join_columns,
            )

            # Update previous output with the new customization tables
            previous_primary_df = _enrich_join(
                previous_primary_df,
                _rename_enrichment_columns(
                    foreign_object_name,
                    foreign_objects_fields_metadata[foreign_object_name],
                    enrichment_table,
                    source_config,
                    text_table_names,
                    link,
                    raw_literal_fields,
                    enrichments[foreign_object_name].dataframe(),
                    primary_object_metadata,
                ),
                join_columns,
            )

        out.write_dataframe(primary_df.unionByName(previous_primary_df))

    process_function.__name__ = (
        f"{source_config.source_name} {BellhopStage.ENRICHED.name} {table.table_name}"
    )
    return process_function


def _enrich_left_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_columns: List[Tuple[str, str]],
) -> DataFrame:
    """
    Enrich left_df with right_df.
    Note that left_df with null join columns are unioned at the end to avoid partition skewing.
    """
    left_with_null_join_columns = left_df.filter(
        functools.reduce(
            lambda a, b: a | b,
            [F.col(left_column).isNull() for left_column, _ in join_columns],
        )
    )
    left_without_null_join_columns = left_df.filter(
        functools.reduce(
            lambda a, b: a & b,
            [F.col(left_column).isNotNull() for left_column, _ in join_columns],
        )
    )
    result = _enrich_join(
        left_without_null_join_columns,
        right_df,
        join_columns,
        "left",
    )

    # we want to guarantee order of result after union_many
    # by construct, left_with_null_join_columns columns are a subset of result columns
    # so just keeping the latter
    column_order = result.columns

    df = union_many(left_with_null_join_columns, result, how="wide")
    return df.select(column_order)


def _enrich_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_columns: List[Tuple[str, str]],
    how: str = "inner",
) -> DataFrame:
    join_conditions = _generate_join_conditions(left_df, right_df, join_columns)
    result = left_df.join(
        right_df.alias(CUSTOMIZATION_TABLE_ALIAS), on=join_conditions, how=how
    )

    # We only want one copy of the join columns
    for _, right_column in join_columns:
        result = result.drop(F.col(f"{CUSTOMIZATION_TABLE_ALIAS}.{right_column}"))

    return result


def _rename_enrichment_columns(
    foreign_object_name: str,
    foreign_object_fields_metadata: List[Row],
    foreign_object_table: Table,
    source_config: SourceConfig,
    text_table_names: List[str],
    link: Row,
    raw_literal_fields: Set[ColumnName],
    enrichment_df: DataFrame,
    primary_object_metadata: Row,
) -> DataFrame:
    foreign_object_change_mode_column = extract_change_mode_column(
        foreign_object_table, source_config
    )
    foreign_object_fields_metadata.sort(key=lambda x: x.field_order)
    customization_columns = _get_renamed_enrichment_customization_columns(
        foreign_object_name,
        link,
        set(
            [row["raw_field_name"] for row in foreign_object_fields_metadata]
            + extract_deduplication_comparison_columns_for_table(
                foreign_object_table, source_config
            )
            + (
                [foreign_object_change_mode_column]
                if foreign_object_change_mode_column
                else []
            )
        )
        - set(link["foreign_object_raw_fields"]),
    )
    title_columns = _get_renamed_enrichment_title_columns(
        foreign_object_name,
        text_table_names,
        link,
        raw_literal_fields,
        primary_object_metadata,
        # Only create title columns from non-foreign-key columns from the fields metadata.
        # This avoids creating titles using additional columns such as SLT's timestamp column.
        [
            row["raw_field_name"]
            for row in foreign_object_fields_metadata
            if row["raw_field_name"] not in set(link["foreign_object_raw_fields"])
            and row["raw_field_name"] in set(enrichment_df.columns)
        ],
    )
    return enrichment_df.select(
        *(link["foreign_object_raw_fields"] + customization_columns + title_columns)
    )


def _get_renamed_enrichment_customization_columns(
    foreign_object_name: str,
    link: Row,
    enrichment_df_non_fkey_columns: Set[str],
) -> List[Column]:
    """
    Get the customization columns for the enrichment.
    """
    return [
        F.col(column_name).alias(
            get_renamed_enrichment_customization_column(
                foreign_object_name, link, column_name
            )
        )
        for column_name in enrichment_df_non_fkey_columns
    ]


def get_renamed_enrichment_customization_column(
    foreign_object_name: str,
    link: Row,
    column_name: ColumnName,
) -> str:
    primary_key_columns_string = "_".join(link["primary_object_raw_fields"])
    return (
        f"{column_name}{COLUMN_DELIMITER}{CUSTOMIZATION_COLUMN_DELIMITER}{foreign_object_name}"
        f"{COLUMN_DELIMITER}{primary_key_columns_string}"
    )


def _get_renamed_enrichment_title_columns(
    foreign_object_name: str,
    text_table_names: List[str],
    link: Row,
    raw_literal_fields: Set[ColumnName],
    primary_object_metadata: Row,
    enrichment_df_non_fkey_columns: List[ColumnName],
) -> List[ColumnName]:
    """
    Get the title columns for the enrichment.
    Only apply the title columns when the join happens on the primary key of the base object.
    """
    if (
        text_table_names
        and foreign_object_name in text_table_names
        and set(primary_object_metadata["primary_key_raw_fields"]).union(
            raw_literal_fields
        )
        == set(link["primary_object_raw_fields"])
    ):
        return [
            F.col(column).alias(f"{SpecialFieldName.TITLE}_{str(index + 1)}")
            for index, column in enumerate(enrichment_df_non_fkey_columns)
        ]

    return []


def _generate_join_conditions(
    left_df: DataFrame,
    right_df: DataFrame,
    join_columns: List[Tuple[str, str]],
) -> List[bool]:
    return [
        left_df[left_column] == right_df[right_column]
        for left_column, right_column in join_columns
    ]


def _get_text_table_names(
    table: Table,
    objects_metadata: DataFrame,
):
    text_table = (
        objects_metadata.filter(  # noqa
            (F.col("object_name") == table.table_name)
            & (F.col("text_table_names").isNotNull())
            & (F.size("text_table_names") > 0)
        )
        .select("text_table_names")
        .collect()
    )
    assert (
        len(text_table) <= 1
    ), f"Object {table.table_name} has more than one Text Table configured. Text Tables: {str(text_table)}."
    return text_table[0][0] if len(text_table) else None
