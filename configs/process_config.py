import os
from typing import List, AnyStr, Union

from bellhop_authoring_api.bellhop_authoring_api_config_common import SparkProfile
from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    TableType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import (
    SourceConfig,
    Table,
)
from bellhop_authoring_api.bellhop_authoring_api_config_transform import (
    SparkProfiles,
    BellhopStage,
)
from transforms.api import Output, Input, Check

from software_defined_integrations.configs.config_parser import ConfigParser
from software_defined_integrations.configs.conjure_config_parser import (
    ConjureConfigParser,
)

CONFIG_PACKAGE = "config"


def extract_configs() -> (PipelineConfig, List[SourceConfig]):
    config_parser = _create_config_parser(CONFIG_PACKAGE)

    if not config_parser:
        raise ValueError("No valid configs can be found for any parsers.")

    return config_parser.parse_config()


def _create_config_parser(package: str) -> ConfigParser:
    if ConjureConfigParser.has_configs(package):
        return ConjureConfigParser(package)


class BellhopDirectoryFactory:
    @staticmethod
    def create_directory_path(*path: AnyStr, bellhop_stage: BellhopStage) -> str:
        return os.path.join(*path, bellhop_stage.value.lower())

    @staticmethod
    def create_transforms_output(
        pipeline_config: PipelineConfig,
        source_config: SourceConfig,
        bellhop_stage: BellhopStage,
        dataset_name: str,
        checks: Union[List[Check], Check] = None,
    ) -> Output:
        return Output(
            BellhopDirectoryFactory._create_compass_path(
                pipeline_config, source_config, bellhop_stage, dataset_name
            ),
            checks=checks,
        )

    @staticmethod
    def create_transforms_input(
        pipeline_config: PipelineConfig,
        source_config: SourceConfig,
        bellhop_stage: BellhopStage,
        dataset_name: str,
    ) -> Input:
        return Input(
            BellhopDirectoryFactory._create_compass_path(
                pipeline_config, source_config, bellhop_stage, dataset_name
            )
        )

    @staticmethod
    def _create_compass_path(
        pipeline_config: PipelineConfig,
        source_config: SourceConfig,
        bellhop_stage: BellhopStage,
        dataset_name: str,
    ) -> str:
        if has_single_source_system(pipeline_config) or not source_config:
            return os.path.join(
                pipeline_config.output_folder,
                bellhop_stage.value.lower(),
                dataset_name,
            )
        return os.path.join(
            pipeline_config.output_folder,
            source_config.source_name,
            bellhop_stage.value.lower(),
            dataset_name,
        )


def get_spark_profile(
    spark_profiles: SparkProfiles, bellhop_stage: BellhopStage
) -> List[SparkProfile]:
    if not spark_profiles:
        return []
    if spark_profiles.stages and bellhop_stage not in spark_profiles.stages:
        return []
    return spark_profiles.profiles


def has_deduplication(source_config: SourceConfig, table: Table):
    return (
        source_config.deduplication_config
        or table.dataset_transforms_config.deduplication_comparison_columns
    )


def has_single_source_system(pipeline_config: PipelineConfig):
    return len(pipeline_config.source_config_file_names) == 1


def get_customization_tables(
    pipeline_config: PipelineConfig, source_config: SourceConfig, primary_table: Table
) -> List[Table]:
    return [
        customization_table
        for customization_table in source_config.tables
        if TableType.CUSTOMIZATION
        in pipeline_config.tables[customization_table.table_name].types
        and customization_table.table_name != primary_table.table_name
    ]
