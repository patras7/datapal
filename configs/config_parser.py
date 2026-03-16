import abc
import os
from typing import List

import pkg_resources
from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import PipelineConfig
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig


class ConfigParser(metaclass=abc.ABCMeta):
    """
    Contains functions to parse different types of config files.
    """

    @abc.abstractmethod
    def __init__(self, package: str):
        self.package = package

    @abc.abstractmethod
    def parse_config(self) -> (PipelineConfig, List[SourceConfig]):
        """
        Reads valid config files and returns the objects for configs.
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def has_configs(package: str) -> bool:
        """
        Checks if there are files in reasonable formats for the ConfigParser to start processing.
        """
        pass

    @staticmethod
    def _has_configs(package: str, config_file_name: str) -> bool:
        return os.path.exists(
            pkg_resources.resource_filename(package, config_file_name)
        )
