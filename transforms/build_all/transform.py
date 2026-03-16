import math
import os
from typing import Iterable, List

from bellhop_authoring_api.bellhop_authoring_api_config_common import TableName
from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    TableType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_transform import BellhopStage
from transforms.api import transform, Input, Output

from software_defined_integrations.transforms.utils import (
    renamed_name_for_table,
    translate_table_name_to_dataset_name,
)

# The maximum amount of datasets per BUILD.
# Build2 has a limit of 2,000 datasets per job, and each dataset in Bellhop requires 5 steps
MAXIMUM_DATASETS_PER_BUILD = 390


def generate_build_transforms(
    pipeline_config: PipelineConfig, final_dataset_directory: str
):
    return [
        create_build_all_transform(pipeline_config, final_dataset_directory)
    ] + create_build_subset_transforms(pipeline_config, final_dataset_directory)


def create_build_all_transform(
    pipeline_config: PipelineConfig, final_dataset_directory: str
):
    """
    Create a BUILD transform that contains all Bellhop tables.
    """
    return _create_build_transform(
        pipeline_config,
        final_dataset_directory,
        BellhopStage.BUILD.name,
        _extract_table_names_for_build(pipeline_config),
    )


def create_build_subset_transforms(
    pipeline_config: PipelineConfig, final_dataset_directory: str
):
    """
    Create a list of BUILD transforms each of which contains a subset of Bellhop tables.
    """
    table_names = _extract_table_names_for_build(pipeline_config)
    num_builds = math.ceil(len(table_names) / MAXIMUM_DATASETS_PER_BUILD)

    return [
        _create_build_transform(
            pipeline_config,
            final_dataset_directory,
            f"{BellhopStage.BUILD.name}_{build_id}",
            table_names[
                (build_id - 1)
                * MAXIMUM_DATASETS_PER_BUILD : build_id
                * MAXIMUM_DATASETS_PER_BUILD
            ],
        )
        for build_id in range(1, num_builds + 1)
    ]


def _create_build_transform(
    pipeline_config: PipelineConfig,
    final_dataset_directory: str,
    transform_name: str,
    table_names: Iterable[TableName],
):
    @transform(
        out=Output(os.path.join(final_dataset_directory, transform_name)),
        **{
            table_name: Input(
                os.path.join(
                    final_dataset_directory,
                    renamed_name_for_table(
                        pipeline_config.tables[table_name].display_name, table_name
                    )
                    if not pipeline_config.disable_renamed_stage
                    else translate_table_name_to_dataset_name(table_name),
                )
            )
            for table_name in table_names
        },
    )
    def process_function(out, **_inputs):
        with out.filesystem().open("status.txt", mode="w") as f:
            f.write("SUCCESS")

    process_function.__name__ = transform_name
    return process_function


def _extract_table_names_for_build(pipeline_config: PipelineConfig) -> List[TableName]:
    return [
        table_name
        for table_name in pipeline_config.tables
        if pipeline_config.tables[table_name].types != [TableType.METADATA]
    ]
