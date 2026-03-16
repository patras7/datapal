import ast
from datetime import datetime, date
from distutils import util
from typing import List, Any, Union

from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    SourceType,
    SourceConfigFileName,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from bellhop_authoring_api.bellhop_authoring_api_config_workflow_element import (
    ElementVariable,
    ObjectElement,
    JoinTableElement,
)
from conjure_python_client import ConjureDecoder

from software_defined_integrations.configs.config_parser import ConfigParser
from software_defined_integrations.derived.generate import (
    extract_available_workflows,
    extract_available_elements,
    extract_buildable_elements,
    DEFAULT_WORKFLOW_VERSION,
)
from software_defined_integrations.transforms.cleaned.function_libraries import (
    common,
    deployment,
)
from software_defined_integrations.transforms.utils import load_yaml


class ConjureConfigParser(ConfigParser):
    PIPELINE_CONFIG_FILE_NAME = "PipelineConfig.yaml"

    def __init__(self, package: str):
        super().__init__(package)

    def parse_config(self) -> (PipelineConfig, List[SourceConfig]):
        pipeline_config = self._load_pipeline_config()
        source_configs = self._load_source_configs(pipeline_config)
        return pipeline_config, source_configs

    def _load_pipeline_config(self) -> PipelineConfig:
        pipeline_config = ConjureDecoder().decode(
            load_yaml(self.package, ConjureConfigParser.PIPELINE_CONFIG_FILE_NAME),
            PipelineConfig,
        )
        ConjureConfigParser.convert_workflow_variables(pipeline_config)
        pipeline_config._disable_enriched_stage = (
            pipeline_config._disable_enriched_stage or False
        )
        pipeline_config._disable_renamed_stage = (
            pipeline_config._disable_renamed_stage or False
        )
        pipeline_config._disable_foreign_key_generation = (
            pipeline_config._disable_foreign_key_generation or False
        )
        pipeline_config._disable_translation = (
            pipeline_config._disable_translation or False
        )
        pipeline_config._post_process_stages = (
            pipeline_config._post_process_stages or {}
        )

        # check that sourceType is not OTHER or UNKNOWN
        VALID_SOURCE_TYPES = [
            sourceType.name
            for sourceType in SourceType
            if sourceType not in [SourceType.OTHER.name, SourceType.UNKNOWN.name]
        ]
        assert (
            pipeline_config.source_type != "OTHER"
        ), f"""Error in  {ConjureConfigParser.PIPELINE_CONFIG_FILE_NAME}: sourceType OTHER is not supported. "
            "Please use a sourceType from {VALID_SOURCE_TYPES}"""

        return pipeline_config

    def _load_source_configs(
        self, pipeline_config: PipelineConfig
    ) -> List[SourceConfig]:
        return [
            self._load_source_config(source_config_file_name)
            for source_config_file_name in pipeline_config.source_config_file_names
        ]

    def _load_source_config(
        self, source_config_file_name: SourceConfigFileName
    ) -> SourceConfig:
        source_config = ConjureDecoder().decode(
            load_yaml(self.package, source_config_file_name),
            SourceConfig,
        )
        if not source_config.raw_folder_structure.data_dictionary:
            source_config.raw_folder_structure._data_dictionary = (
                source_config.raw_folder_structure.raw
            )
        if not source_config.deployment_semantic_version:
            source_config._deployment_semantic_version = 0
        if not source_config.language_key:
            source_config._language_key = "E"
        if not source_config.metadata_spark_profiles:
            source_config._metadata_spark_profiles = []
        # check that we actually have all the cleaning libraries that are specified
        function_libraries = [common, deployment]
        all_cleaning_libraries = set()
        for library in source_config.cleaning_libraries:
            all_cleaning_libraries.add(library)
        for table in source_config.tables:
            for library in table.dataset_transforms_config.table_cleaning_libraries:
                all_cleaning_libraries.add(library)
        for library in list(all_cleaning_libraries):
            assert any(
                library in dir(function_library)
                for function_library in function_libraries
            ), f"Can't find a cleaning library named '{library}'"
        return source_config

    @staticmethod
    def convert_workflow_variables(pipeline_config: PipelineConfig):
        available_workflows = extract_available_workflows()
        available_elements = extract_available_elements()
        buildable_elements = extract_buildable_elements(
            available_elements, pipeline_config._disable_translation
        )

        for workflow_name, workflow_config in pipeline_config.workflows.items():
            workflow_config._variables = (
                ConjureConfigParser.convert_workflow_variable_value_types(
                    workflow_config.variables or [],
                    [
                        buildable_elements[element_name]
                        for element_name in available_workflows[workflow_name][
                            DEFAULT_WORKFLOW_VERSION
                        ].elements
                        if element_name in buildable_elements
                    ],
                )
            )

    @staticmethod
    def convert_workflow_variable_value_types(
        variable_configs: List[ElementVariable],
        enabled_elements: List[Union[ObjectElement, JoinTableElement]],
    ) -> List[ElementVariable]:
        default_variables = {
            default_variable.name: default_variable.value
            for element in enabled_elements
            for default_variable in element.contract.variables_with_default_values or []
        }
        return [
            ElementVariable(
                name=variable.name,
                value=ConjureConfigParser.convert_workflow_variable_value_type(
                    variable.value, default_variables[variable.name]
                ),
            )
            for variable in variable_configs
            if variable.name in default_variables
        ]

    @staticmethod
    def convert_workflow_variable_value_type(
        conjure_variable_value: any, default_variable_value: Any
    ):
        if default_variable_value is None:
            return conjure_variable_value

        conjure_variable_value = str(conjure_variable_value)

        default_variable_type = type(default_variable_value)
        if default_variable_type is list:
            return ast.literal_eval(conjure_variable_value)
        if default_variable_type is bool:
            return bool(util.strtobool(conjure_variable_value))
        if default_variable_type is date:
            return datetime.strptime(conjure_variable_value, "%Y-%M-%d").date()
        return default_variable_type(conjure_variable_value)

    @staticmethod
    def has_configs(package: str) -> bool:
        return ConfigParser._has_configs(
            package, ConjureConfigParser.PIPELINE_CONFIG_FILE_NAME
        )
