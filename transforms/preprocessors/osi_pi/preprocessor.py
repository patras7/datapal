from typing import Tuple

from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    SourceType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from pyspark.sql.dataframe import DataFrame
from transforms.api import TransformContext, TransformInput

from software_defined_integrations.transforms.preprocessors.osi_pi.transforms.fields import (
    generate_fields_metadata,
)
from software_defined_integrations.transforms.preprocessors.osi_pi.transforms.links import (
    generate_links_metadata,
)
from software_defined_integrations.transforms.preprocessors.osi_pi.transforms.objects import (
    generate_objects_metadata,
)
from software_defined_integrations.transforms.preprocessors.preprocessor import (
    Preprocessor,
)
from software_defined_integrations.transforms.preprocessors.registry import register


@register(SourceType.OSI_PI)
class OsiPi(Preprocessor):
    OSI_PI_METADATA_OBJECT = "OsiPiMetadataSync"

    def __init__(self, pipeline_config: PipelineConfig, source_config: SourceConfig):
        super().__init__(pipeline_config, source_config)

    def transform(
        self, ctx: TransformContext, **_dd_tables: TransformInput
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        objects_metadata_df = generate_objects_metadata(
            ctx.spark_session, self.OSI_PI_METADATA_OBJECT
        )
        fields_metadata_df = generate_fields_metadata(
            ctx.spark_session, self.OSI_PI_METADATA_OBJECT
        )
        links_metadata_df = generate_links_metadata(ctx.spark_session)

        return objects_metadata_df, fields_metadata_df, links_metadata_df
