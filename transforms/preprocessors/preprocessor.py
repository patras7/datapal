import abc
from typing import Tuple

from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import PipelineConfig
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from pyspark.sql import DataFrame
from transforms.api import TransformContext, TransformInput


class Preprocessor(metaclass=abc.ABCMeta):
    def __init__(self, pipeline_config: PipelineConfig, source_config: SourceConfig):
        self._pipeline_config = pipeline_config
        self._source_config = source_config

    @abc.abstractmethod
    def transform(
        self, _ctx: TransformContext, **dd_tables: TransformInput
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        raise NotImplementedError()
