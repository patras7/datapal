from typing import List

from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    TableType,
    PostProcessStageType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_transform import BellhopStage
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig

from software_defined_integrations.configs.process_config import (
    BellhopDirectoryFactory,
    extract_configs,
    has_single_source_system,
)
from software_defined_integrations.transforms.cleaned.transform import (
    create_cleaned_transform,
)
from software_defined_integrations.transforms.enriched.transform import (
    create_enriched_transform,
)
from software_defined_integrations.transforms.preprocessors.transform import (
    get_metadata_transform,
)
from software_defined_integrations.transforms.renamed_changelog.transform import (
    create_renamed_changelog_transform,
)
from software_defined_integrations.transforms.renamed.transform import (
    create_renamed_transform,
)
from software_defined_integrations.transforms.final.transform import (
    create_final_transform,
)
from software_defined_integrations.transforms.build_all.transform import (
    generate_build_transforms,
)
from software_defined_integrations.transforms.utils import is_enrichments_stage_enabled
from software_defined_integrations.derived.generate import generate_workflow_transforms
from software_defined_integrations.transforms.post_process.transform import (
    generate_post_process_transforms,
)


"""
Pipeline Builder
This transform reads the configs and kicks off all the transforms steps.
"""


def generate_metadata_and_transforms(
    pipeline_config: PipelineConfig,
    source_configs: List[SourceConfig],
):
    # Generate empty build graph if there are no tables configured
    if not pipeline_config.tables:
        return []

    # Generate metadata dataset transform
    transforms = [
        get_metadata_transform(pipeline_config, source_config)
        for source_config in source_configs
    ]

    enabled_stages = [create_cleaned_transform]
    if is_enrichments_stage_enabled(pipeline_config):
        enabled_stages.append(create_enriched_transform)
    if not pipeline_config.disable_renamed_stage:
        enabled_stages.append(create_renamed_changelog_transform)
    enabled_stages.append(create_renamed_transform)

    transforms += [
        transform_function(table, pipeline_config, source_config)
        for source_config in source_configs
        for table in source_config.tables
        if pipeline_config.tables[table.table_name].types != [TableType.METADATA]
        for transform_function in enabled_stages
    ]

    # Final dataset transforms, only for multiple source systems
    if not has_single_source_system(pipeline_config):
        source_table_names = {
            table.table_name
            for source_config in source_configs
            for table in source_config.tables
        }
        transforms += [
            create_final_transform(table_name, pipeline_config, source_configs)
            for table_name in pipeline_config.tables.keys() and source_table_names
            if pipeline_config.tables[table_name].types != [TableType.METADATA]
        ]

    final_dataset_directory = _get_final_dataset_directory(pipeline_config)

    # This transform creates a BUILD dataset for each X datasets for better scheduling
    # It also creates a BUILD dataset which takes in every dataset and returns nothing
    transforms += generate_build_transforms(pipeline_config, final_dataset_directory)

    if pipeline_config.workflows:
        transforms += generate_workflow_transforms(
            final_dataset_directory, pipeline_config
        )

    # Generate post process stages if the config specifies any
    if len(pipeline_config.post_process_stages) > 0:
        transforms += generate_post_process_transforms(
            pipeline_config, final_dataset_directory
        )

    return transforms


# Get final directory as either "final" or "renamed"
def _get_final_dataset_directory(pipeline_config: PipelineConfig) -> str:
    bellhop_stage = (
        BellhopStage.RENAMED
        if has_single_source_system(pipeline_config)
        else BellhopStage.FINAL
    )
    return BellhopDirectoryFactory.create_directory_path(
        pipeline_config.output_folder, bellhop_stage=bellhop_stage
    )


configs = extract_configs()
TRANSFORMS = generate_metadata_and_transforms(*configs)
