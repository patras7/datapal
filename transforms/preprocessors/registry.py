import pkgutil
from typing import Union

from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    SourceType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig

from software_defined_integrations.transforms import preprocessors
from software_defined_integrations.transforms.preprocessors.preprocessor import (
    Preprocessor,
)

_registry = {}


def register(source_type: Union[SourceType, str]):
    """Registers a class as a source-type preprocessor.
    A `register` annotated class defines the logic required to take raw input datasets from a source
    and generate the object, field, and link metadata tables. The class it annotates must be a subclass
    of Preprocessor.

    Users must provide the identifier for the type of pre-processor as a string argument to this
    decorator.

    All valid pre-processor classes decorated with `@register` will be available by calling
    :meth:`get_transform` with a configuration file whose `source_type` value equals the
    `source_type` value supplied.

    Example:
        >>> @register(SourceType.SALESFORCE)
        ... class Salesforce(Preprocessor):
        ...     def __init__(self, pipeline_config: PipelineConfig, source_config: SourceConfig):
        ...         super().__init__(pipeline_config, source_config)
        ...
        ...     def transform(
        ...         self, _ctx: TransformContext, **tables: TransformInput
        ...     ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        ...         pass

    Args:
        source_type: A string identifying the type of source system this class processes. Sources
                          whose `source_type` configuration value match this will be pre-processed
                          using the decorated class.
    """

    def preprocessor_wrapper(preprocessor_class):
        assert issubclass(
            preprocessor_class, Preprocessor
        ), "Only subclasses of 'Preprocessor' can be registered."

        _registry[
            source_type.name if isinstance(source_type, SourceType) else source_type
        ] = preprocessor_class
        return preprocessor_class

    if not source_type:
        raise ValueError("source_type was not provided")

    return preprocessor_wrapper


def get_transform(pipeline_config: PipelineConfig, source_config: SourceConfig):
    # Load all subclasses of Preprocessor
    for loader, module_name, _ in pkgutil.walk_packages(preprocessors.__path__):
        loader.find_module(module_name).load_module(module_name)

    assert (
        pipeline_config.source_type in _registry
    ), f"Preprocessor type {pipeline_config.source_type} is not found. The supported types are {list(_registry.keys())}"

    return _registry[pipeline_config.source_type](
        pipeline_config, source_config
    ).transform
