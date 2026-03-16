from typing import Tuple

from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    SourceType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from pyspark.sql.dataframe import DataFrame
from transforms.api import TransformContext, TransformInput

from software_defined_integrations.transforms.preprocessors.preprocessor import (
    Preprocessor,
)
from software_defined_integrations.transforms.preprocessors.registry import register
from software_defined_integrations.transforms.preprocessors.oracle_jde.transforms.fields import (
    generate_fields_metadata,
)
from software_defined_integrations.transforms.preprocessors.oracle_jde.transforms.links import (
    generate_links_metadata,
)
from software_defined_integrations.transforms.preprocessors.oracle_jde.transforms.objects import (
    generate_objects_metadata,
)


@register(SourceType.ORACLE_JDE)
class OracleJDE(Preprocessor):
    def __init__(self, pipeline_config: PipelineConfig, source_config: SourceConfig):
        super().__init__(pipeline_config, source_config)

    def transform(
        self, _ctx: TransformContext, **dd_tables: TransformInput
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        fields_metadata_df = generate_fields_metadata(
            dd_tables["table_fields"].dataframe(),
        )
        links_metadata_df = generate_links_metadata(
            dd_tables["table_fields"].dataframe(),
        )
        objects_metadata_df = generate_objects_metadata(
            dd_tables["tables"].dataframe(), dd_tables["table_fields"].dataframe()
        )

        return objects_metadata_df, fields_metadata_df, links_metadata_df
