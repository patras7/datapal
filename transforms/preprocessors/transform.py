import os
from typing import Set

from bellhop_authoring_api.bellhop_authoring_api_config_common import (
    TableName,
    ColumnName,
)
from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    TableType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from bellhop_authoring_api.bellhop_authoring_api_config_transform import BellhopStage
from pyspark.sql import DataFrame, types as T, functions as F
from transforms.api import (
    Input,
    TransformContext,
    transform,
    TransformInput,
    TransformOutput,
    incremental,
    configure,
)

from software_defined_integrations.configs.process_config import BellhopDirectoryFactory
from software_defined_integrations.transforms.preprocessors.metadata_schemas import (
    METADATA_FIELDS_SCHEMA,
    METADATA_LINKS_SCHEMA,
    METADATA_OBJECTS_SCHEMA,
    MetadataType,
    METADATA_DIFFS_SCHEMA,
)
from software_defined_integrations.transforms.preprocessors.registry import (
    get_transform,
)

TRANSFORM_SEMANTIC_VERSION = 7


def get_metadata_transform(
    pipeline_config: PipelineConfig, source_config: SourceConfig
):
    metadata_tables = [
        table_name
        for table_name, table_definition in pipeline_config.tables.items()
        if TableType.METADATA in table_definition.types
    ]

    @incremental(
        semantic_version=TRANSFORM_SEMANTIC_VERSION
        + source_config.deployment_semantic_version,
        snapshot_inputs=metadata_tables,
    )
    @configure(profile=(source_config.metadata_spark_profiles))
    @transform(
        objects=BellhopDirectoryFactory.create_transforms_output(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.OBJECTS.value,
        ),
        fields=BellhopDirectoryFactory.create_transforms_output(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.FIELDS.value,
        ),
        links=BellhopDirectoryFactory.create_transforms_output(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.LINKS.value,
        ),
        diffs=BellhopDirectoryFactory.create_transforms_output(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.DIFFS.value,
        ),
        **{
            table_name: Input(
                os.path.join(
                    source_config.raw_folder_structure.data_dictionary,
                    primary_table.dataset_transforms_config.dataset_name,
                )
            )
            for table_name, table_definition in pipeline_config.tables.items()
            if TableType.METADATA in table_definition.types
            for primary_table in source_config.tables
            if primary_table.table_name == table_name
        },
    )
    def metadata_transform(
        ctx: TransformContext,
        objects: TransformOutput,
        fields: TransformOutput,
        links: TransformOutput,
        diffs: TransformOutput,
        **tables: TransformInput,
    ):
        preprocess_transform = get_transform(pipeline_config, source_config)
        objects_df, fields_df, links_df = preprocess_transform(ctx, **tables)

        if "field_order" not in fields_df.columns:
            fields_df = fields_df.select(
                "*", F.lit(None).cast(T.IntegerType()).alias("field_order")
            )
        objects_df = objects_df.select(METADATA_OBJECTS_SCHEMA.fieldNames()).cache()
        fields_df = fields_df.select(METADATA_FIELDS_SCHEMA.fieldNames()).cache()
        links_df = links_df.select(METADATA_LINKS_SCHEMA.fieldNames()).cache()
        diffs_df = _create_diffs_metadata(
            ctx,
            {table.table_name for table in source_config.tables},
            objects_df,
            objects,
            fields_df,
            fields,
            links_df,
            links,
        )

        for metadata_df, metadata_output in [
            (objects_df, objects),
            (fields_df, fields),
            (links_df, links),
            (diffs_df, diffs),
        ]:
            metadata_output.set_mode("replace")
            metadata_output.write_dataframe(metadata_df)

    return metadata_transform


def _create_diffs_metadata(
    ctx: TransformContext,
    all_table_names: Set[TableName],
    objects_df: DataFrame,
    objects_output: TransformOutput,
    fields_df: DataFrame,
    fields_output: TransformOutput,
    links_df: DataFrame,
    links_output: TransformOutput,
) -> DataFrame:
    table_names_from_changed_metadata = (
        _extract_table_names_from_changed_metadata(
            "object_name", objects_df, objects_output
        )
        | _extract_table_names_from_changed_metadata(
            "object_name", fields_df, fields_output
        )
        | _extract_table_names_from_changed_metadata(
            "primary_object_name", links_df, links_output
        )
    )
    return ctx.spark_session.createDataFrame(
        [
            [table_name, table_name in table_names_from_changed_metadata]
            for table_name in all_table_names
        ],
        METADATA_DIFFS_SCHEMA,
    )


def _extract_table_names_from_changed_metadata(
    column_name: ColumnName,
    metadata_df: DataFrame,
    metadata_output: TransformOutput,
) -> Set[TableName]:
    previous_metadata_df = metadata_output.dataframe(
        mode="previous", schema=metadata_df.schema
    )
    return _extract_distinct_column_values_from_left_df(
        column_name, metadata_df, previous_metadata_df
    ) | _extract_distinct_column_values_from_left_df(
        column_name, previous_metadata_df, metadata_df
    )


def _extract_distinct_column_values_from_left_df(
    column_name: ColumnName, left: DataFrame, right: DataFrame
) -> Set[ColumnName]:
    return {
        row[column_name]
        for row in left.subtract(right).select(column_name).distinct().collect()
    }
