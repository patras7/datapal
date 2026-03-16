from typing import List, Union

from bellhop_authoring_api.bellhop_authoring_api_config_common import ColumnName
from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    SourceType,
    TableType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import (
    SourceConfig,
    Table,
)
from bellhop_authoring_api.bellhop_authoring_api_config_transform import BellhopStage
from pyspark.sql import DataFrame, functions as F
from transforms import expectations as E
from transforms import verbs as V
from transforms.api import (
    configure,
    transform,
    incremental,
    TransformInput,
    IncrementalTransformInput,
    Check,
)

from software_defined_integrations.configs.process_config import (
    BellhopDirectoryFactory,
    get_spark_profile,
)
from software_defined_integrations.transforms.cleaned.transform import (
    convert_bellhop_column_name,
)
from software_defined_integrations.transforms.enriched.transform import (
    get_renamed_enrichment_customization_column,
)
from software_defined_integrations.transforms.preprocessors.metadata.utils import (
    find_links,
)
from software_defined_integrations.transforms.preprocessors.metadata_schemas import (
    MetadataType,
)
from software_defined_integrations.transforms.renamed_changelog.transform import (
    extract_rename_column_mapping,
)
from software_defined_integrations.transforms.types import SpecialFieldName
from software_defined_integrations.transforms.utils import (
    BELLHOP_COLUMN_PREFIX,
    renamed_name_for_table,
    is_dataframe_empty,
    extract_deduplication_comparison_columns_for_table,
    extract_change_mode_column,
    is_enrichments_stage_enabled,
)
from software_defined_integrations.transforms.types import SpecialFieldName
from software_defined_integrations.transforms.utils import fast_copy_input_to_output


def create_renamed_transform(
    table: Table,
    pipeline_config: PipelineConfig,
    source_config: SourceConfig,
):
    """
    Removes duplications within the same transaction (instead of the entire dataset).
    """

    dataset_name = renamed_name_for_table(
        pipeline_config.tables[table.table_name].display_name, table.table_name
    )

    @configure(
        profile=get_spark_profile(
            table.dataset_transforms_config.spark_profiles, BellhopStage.RENAMED
        )
    )
    @incremental(
        semantic_version=source_config.deployment_semantic_version,
        snapshot_inputs=["fields_metadata", "links_metadata"],
    )
    @transform(
        out=BellhopDirectoryFactory.create_transforms_output(
            pipeline_config,
            source_config,
            BellhopStage.RENAMED,
            dataset_name
            if not pipeline_config.disable_renamed_stage
            else table.dataset_transforms_config.dataset_name,
            checks=Check(
                E.primary_key(SpecialFieldName.PRIMARY_KEY),
                "Check that primary keys are unique",
                on_error="WARN",
            ),
        ),
        primary_object=BellhopDirectoryFactory.create_transforms_input(
            pipeline_config,
            source_config,
            BellhopStage.RENAMED_CHANGELOG
            if not pipeline_config.disable_renamed_stage
            else (
                BellhopStage.ENRICHED
                if is_enrichments_stage_enabled(pipeline_config)
                else BellhopStage.CLEANED
            ),
            dataset_name
            if not pipeline_config.disable_renamed_stage
            else table.dataset_transforms_config.dataset_name,
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
    )
    def process_function(ctx, out, primary_object, fields_metadata, links_metadata):
        primary_df = primary_object.dataframe()

        deduplication_comparison_columns = _extract_deduplication_comparison_columns(
            table, pipeline_config, source_config, fields_metadata, links_metadata
        )

        is_df_modified = False

        # Drop error columns if they exist to avoid noise
        schema_mismatch_column = (
            BELLHOP_COLUMN_PREFIX + SpecialFieldName.SCHEMA_MISMATCH_COLUMN_NAME
        )
        if schema_mismatch_column in primary_df.columns:
            is_df_modified = True
            primary_df = primary_df.drop(schema_mismatch_column)

        lowercase_columns = [column_name.lower() for column_name in primary_df.columns]
        # Generate a primary key column if it does not already exist
        if (
            SpecialFieldName.PRIMARY_KEY not in lowercase_columns
            and BELLHOP_COLUMN_PREFIX + SpecialFieldName.PRIMARY_KEY
            in lowercase_columns
        ):
            is_df_modified = True
            primary_df = primary_df.withColumn(
                SpecialFieldName.PRIMARY_KEY,
                F.col(BELLHOP_COLUMN_PREFIX + SpecialFieldName.PRIMARY_KEY),
            )
        if (
            SpecialFieldName.TITLE not in lowercase_columns
            and BELLHOP_COLUMN_PREFIX + SpecialFieldName.TITLE in lowercase_columns
        ):
            is_df_modified = True
            primary_df = primary_df.withColumn(
                SpecialFieldName.TITLE,
                F.col(BELLHOP_COLUMN_PREFIX + SpecialFieldName.TITLE),
            )

        if not deduplication_comparison_columns:
            # for SAP no PK is added which implies a fast copy can be used
            if not is_df_modified and _fast_copy_is_enabled():
                fast_copy_input_to_output(ctx, out, primary_object)
                return
            out.write_dataframe(primary_df)
            return

        if ctx.is_incremental and is_dataframe_empty(primary_df):
            out.abort()
            return

        in_case_of_tie_pick_randomly = False
        if table.dataset_transforms_config.enforce_unique_primary_keys:
            in_case_of_tie_pick_randomly = (
                table.dataset_transforms_config.enforce_unique_primary_keys
            )

        primary_df = deduplicate_dataframe(
            primary_df,
            convert_bellhop_column_name(
                SpecialFieldName.PRIMARY_KEY, pipeline_config.source_type
            ),
            deduplication_comparison_columns,
            table,
            in_case_of_tie_pick_randomly=in_case_of_tie_pick_randomly,
        )

        if ctx.is_incremental:
            out.set_mode("replace")
            previous_output = out.dataframe(mode="previous", schema=primary_df.schema)
            if not is_dataframe_empty(previous_output):
                primary_df = primary_df.union(
                    previous_output
                    # Remove rows with the same primary keys from the previous output
                    .join(
                        primary_df, on=SpecialFieldName.PRIMARY_KEY, how="left_anti"
                    ).select(primary_df.columns)
                )

        change_mode_column = extract_change_mode_column(table, source_config)
        if change_mode_column:
            primary_df = drop_deleted_records(
                primary_df,
                change_mode_column,
                table,
            )

        out.write_dataframe(primary_df)

    process_function.__name__ = (
        f"{source_config.source_name} {BellhopStage.RENAMED.name} {table.table_name}"
    )
    return process_function


def _extract_deduplication_comparison_columns(
    table: Table,
    pipeline_config: PipelineConfig,
    source_config: SourceConfig,
    fields_metadata: IncrementalTransformInput,
    links_metadata: IncrementalTransformInput,
) -> List[ColumnName]:
    deduplication_comparison_columns = (
        extract_deduplication_comparison_columns_for_table(table, source_config)
    )

    if is_enrichments_stage_enabled(pipeline_config) and pipeline_config.tables[
        table.table_name
    ].types != [TableType.CUSTOMIZATION]:
        links = find_links(links_metadata, pipeline_config, source_config, table)
        tables = {table.table_name: table for table in source_config.tables}
        for link in links:
            customization_table_name = link["foreign_object_name"]
            customization_table_deduplication_comparison_columns = (
                extract_deduplication_comparison_columns_for_table(
                    tables[customization_table_name], source_config
                )
            )
            deduplication_comparison_columns += [
                get_renamed_enrichment_customization_column(
                    customization_table_name,
                    link,
                    column,
                )
                for column in customization_table_deduplication_comparison_columns
            ]

    # Translate columns if renamed
    if not pipeline_config.disable_renamed_stage:
        deduplication_comparison_columns = _translate_deduplication_comparison_columns(
            table, pipeline_config, fields_metadata, deduplication_comparison_columns
        )
    return deduplication_comparison_columns


def _translate_deduplication_comparison_columns(
    table: Table,
    pipeline_config: PipelineConfig,
    fields_metadata: TransformInput,
    deduplication_comparison_columns: List[ColumnName],
) -> List[ColumnName]:
    rename_column_mapping = extract_rename_column_mapping(
        table,
        pipeline_config,
        fields_metadata,
        deduplication_comparison_columns,
    )

    return [
        rename_column_mapping.get(column, column)
        for column in deduplication_comparison_columns
    ]


def deduplicate_dataframe(
    primary_dataframe: DataFrame,
    primary_key_columns: Union[str, List[str]],
    deduplication_fields: List[str],
    primary_table: Table,
    in_case_of_tie_pick_randomly: bool,
):
    assert all(
        column in primary_dataframe.columns for column in deduplication_fields
    ), "Deduplication columns {} don't exist in dataset {}".format(
        deduplication_fields, primary_table.table_name
    )
    return (
        V.group_by(primary_key_columns)
        .max_by(
            deduplication_fields,
            in_case_of_tie_pick_randomly=in_case_of_tie_pick_randomly,
        )
        .apply(primary_dataframe)
    )


def drop_deleted_records(
    primary_dataframe: DataFrame, change_mode_column, primary_table: Table
):
    # depending on pipeline configurations, this may have been lowercased
    if change_mode_column not in primary_dataframe.columns:
        change_mode_column = change_mode_column.lower()

    assert (
        change_mode_column in primary_dataframe.columns
    ), "Change mode column {} doesn't exist in dataset {}".format(
        change_mode_column, primary_table.table_name
    )
    return primary_dataframe.filter(primary_dataframe[change_mode_column] != "D")


def _fast_copy_is_enabled():
    # this is meant to be used as a flag to internally switch fast copy on and off
    return True
