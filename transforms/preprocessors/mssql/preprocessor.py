from typing import Tuple

from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    SourceType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from pyspark.sql.dataframe import DataFrame
from transforms.api import TransformContext, TransformInput

from software_defined_integrations.transforms.preprocessors.mssql.transforms.fields import (
    generate_fields_metadata,
)
from software_defined_integrations.transforms.preprocessors.mssql.transforms.links import (
    generate_links_metadata,
)
from software_defined_integrations.transforms.preprocessors.mssql.transforms.objects import (
    generate_objects_metadata,
)
from software_defined_integrations.transforms.preprocessors.preprocessor import (
    Preprocessor,
)
from software_defined_integrations.transforms.preprocessors.registry import register


@register(SourceType.MSSQL)
class MsSql(Preprocessor):
    def __init__(self, pipeline_config: PipelineConfig, source_config: SourceConfig):
        super().__init__(pipeline_config, source_config)

    def transform(
        self, _ctx: TransformContext, **dd_tables: TransformInput
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        fields_metadata_df = generate_fields_metadata(
            dd_tables["sys.tables"].dataframe(),
            dd_tables["sys.columns"].dataframe(),
            dd_tables["sys.types"].dataframe(),
            dd_tables["sys.default_constraints"].dataframe(),
            dd_tables["sys.index_columns"].dataframe(),
            dd_tables["sys.indexes"].dataframe(),
            dd_tables["sys.foreign_keys"].dataframe(),
            dd_tables["sys.foreign_key_columns"].dataframe(),
            dd_tables["sys.check_constraints"].dataframe(),
            dd_tables["sys.extended_properties"].dataframe(),
            dd_tables["sys.computed_columns"].dataframe(),
        )
        links_metadata_df = generate_links_metadata(
            dd_tables["sys.tables"].dataframe(),
            dd_tables["sys.foreign_keys"].dataframe(),
            dd_tables["sys.foreign_key_columns"].dataframe(),
            dd_tables["sys.columns"].dataframe(),
        )
        objects_metadata_df = generate_objects_metadata(
            dd_tables["sys.tables"].dataframe(),
            dd_tables["sys.indexes"].dataframe(),
            dd_tables["sys.index_columns"].dataframe(),
            dd_tables["sys.columns"].dataframe(),
        )

        return objects_metadata_df, fields_metadata_df, links_metadata_df
