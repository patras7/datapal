import itertools
from shutil import copyfileobj
from typing import List

from bellhop_authoring_api.bellhop_authoring_api_config_common import (
    TableName,
    SparkProfile,
)
from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import PipelineConfig
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from bellhop_authoring_api.bellhop_authoring_api_config_transform import BellhopStage
from transforms.api import (
    transform,
    configure,
    TransformInput,
    TransformContext,
    FileSystem,
    TransformOutput,
)
from transforms.verbs.dataframes import union_many

from software_defined_integrations.configs.process_config import (
    BellhopDirectoryFactory,
    get_spark_profile,
)
from software_defined_integrations.transforms.services.schema_service import (
    SchemaService,
)
from software_defined_integrations.transforms.utils import (
    renamed_name_for_table,
    translate_table_name_to_dataset_name,
)


def create_final_transform(
    table_name: TableName,
    pipeline_config: PipelineConfig,
    source_configs: List[SourceConfig],
):
    sources_for_table = [
        source_config
        for source_config in source_configs
        for table in source_config.tables
        if table.table_name == table_name
    ]

    @configure(profile=_extract_spark_profiles(table_name, source_configs))
    @transform(
        out=BellhopDirectoryFactory.create_transforms_output(
            pipeline_config,
            None,
            BellhopStage.FINAL,
            renamed_name_for_table(
                pipeline_config.tables[table_name].display_name, table_name
            )
            if not pipeline_config.disable_renamed_stage
            else translate_table_name_to_dataset_name(table_name),
        ),
        **{
            source_config.source_name: BellhopDirectoryFactory.create_transforms_input(
                pipeline_config,
                source_config,
                BellhopStage.RENAMED,
                renamed_name_for_table(
                    pipeline_config.tables[table_name].display_name,
                    table_name,
                )
                if not pipeline_config.disable_renamed_stage
                else translate_table_name_to_dataset_name(table_name),
            )
            for source_config in sources_for_table
        },
    )
    def process_function(ctx, out, **source_inputs):
        if _are_schemas_same(**source_inputs):
            for source_input in source_inputs.values():
                _copy_input_to_output(out.filesystem(), source_input.filesystem())
            _copy_schema_from_input_to_output(
                ctx, next(iter(source_inputs.values())), out
            )
            return

        # if schemas are not the same, we need to wide-union them.
        # However, we want to preserve column order at best.
        # Therefore, we're creating upfront the superset list of columns to select.
        # Since python 3.6, standard dicts are actually ordered, so using this to guarantee
        # order and uniqueness.
        column_order = {}
        for source_input in source_inputs.values():
            column_order.update({key: None for key in source_input.dataframe().columns})
        unioned_df = union_many(
            *[source_input.dataframe() for source_input in source_inputs.values()],
            how="wide",
        )
        out.write_dataframe(unioned_df.select(list(column_order.keys())))

    process_function.__name__ = f"{BellhopStage.FINAL.name} {table_name}"
    return process_function


def _are_schemas_same(**source_inputs: TransformInput) -> bool:
    schemas = {
        frozenset(source_input.dataframe().schema)
        for source_input in source_inputs.values()
    }
    return len(schemas) == 1


def _copy_input_to_output(
    output_file_system: FileSystem, input_file_system: FileSystem
):
    def copy_file_to_output(file_status):
        file_name = file_status.path
        with input_file_system.open(file_name, "rb") as f_in, output_file_system.open(
            file_name, "wb"
        ) as f_out:
            copyfileobj(f_in, f_out)

    input_file_system.files().rdd.foreach(copy_file_to_output)


def _copy_schema_from_input_to_output(
    ctx: TransformContext,
    transform_input: TransformInput,
    transform_output: TransformOutput,
):
    schema_service = SchemaService(
        ctx._foundry._service_provider._services_config["metadata"]["uris"][0]
    )
    schema = schema_service.get_schema(
        ctx.auth_header, transform_input.rid, transform_input.branch
    )["schema"]
    schema_service.put_schema(
        ctx.auth_header, transform_output.rid, transform_output.branch, schema
    )


def _extract_spark_profiles(
    table_name: TableName, source_configs: List[SourceConfig]
) -> List[SparkProfile]:
    # set is not json serializable
    return list(
        set(
            itertools.chain.from_iterable(
                [
                    get_spark_profile(
                        table.dataset_transforms_config.spark_profiles,
                        BellhopStage.FINAL,
                    )
                    for source_config in source_configs
                    for table in source_config.tables
                    if table.table_name == table_name
                ]
            )
        )
    )
