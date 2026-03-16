import json
import pkgutil
import re
from shutil import copyfileobj
from typing import List, Mapping

import yaml
from pyspark.sql import functions as F
from pyspark.sql import Column, Row
from pyspark.sql.dataframe import DataFrame
from transforms.api import (
    FileSystem,
    TransformContext,
    TransformInput,
    TransformOutput,
)
from transforms.api import TransformInput
from bellhop_authoring_api.bellhop_authoring_api_config_common import (
    TableName,
    ColumnName,
)
from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    SourceType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import (
    SourceConfig,
    Table,
)
from software_defined_integrations.transforms.services.schema_service import (
    SchemaService,
)

# Global column and key delimiter for all fields
BELLHOP_COLUMN_PREFIX = "palantir_"

COLUMN_DELIMITER = "_|_"

CUSTOMIZATION_COLUMN_DELIMITER = "customization_field_"

FOREIGN_KEY_COLUMN_DELIMITER = "foreign_key_"

# Pattern for converting camel case to snake case
SNAKE_CASE_PATTERN = re.compile(r"(?<!^)(?=[A-Z])")


def is_enrichments_stage_enabled(pipeline_config: PipelineConfig):
    return not pipeline_config.disable_enriched_stage


def camel_case_to_snake_case(original: str) -> str:
    return SNAKE_CASE_PATTERN.sub("_", original).lower()


def convert_camel_case_to_snake_case(value):
    # Special case for SAP technical table names (e.g. MARA)
    if value.isupper():
        return value
    return camel_case_to_snake_case(value)


# Generate a renamed name in the format "Display Name (Table Name)"
def renamed_name_for_table(display_name: str, table_name: str):
    return f"{display_name.replace('/', '-')} ({renamed_table_name(table_name)})"


def renamed_table_name(table_name: TableName) -> str:
    """
    Replace characters that are not supported in Foundry dataset names.
    """
    return table_name.replace("/", "_")


def translate_table_name_to_dataset_name(table_name: TableName) -> str:
    """
    Reproduces datasetName for a given table name.
    Should ideally be done by renamed_table_name function, but modifying it now would break existing installations
    """
    return table_name.strip("/").replace("/", "-")


# Check uniqueness of foreign keys being joined in
def are_related_table_keys_unique(
    related_object_primary_key_raw_fields: List[str],
    related_df: DataFrame,
):
    return (
        related_df.count()
        == related_df.select(*related_object_primary_key_raw_fields).distinct().count()
    )


def get_primary_object_metadata(
    table_name: str,
    objects_metadata: TransformInput,
) -> Row:
    object_metadata_collected = (
        objects_metadata.dataframe()
        .filter((F.col("object_name") == table_name))
        .collect()
    )

    assert len(object_metadata_collected) == 1, (
        f"{len(object_metadata_collected)} instances of {table_name} found in 'objects' metadata table. "
        "Please ensure that there is only one instance in the objects metadata."
    )

    return object_metadata_collected[0]


# Convert all fields with blank values to nulls
def blank_as_null(col):
    return F.when(F.trim(col) != "", col).otherwise(None)


def convert_dictionary_keys_from_camel_case_to_snake_case(dictionary):
    if isinstance(dictionary, Mapping):
        return {
            convert_camel_case_to_snake_case(
                key
            ): convert_dictionary_keys_from_camel_case_to_snake_case(value)
            for key, value in dictionary.items()
        }
    return dictionary


def concat_ws_with_nulls(delimiter: str = COLUMN_DELIMITER, *columns: Column):
    return F.when(F.concat(*columns).isNull(), F.lit(None)).otherwise(
        F.concat_ws(delimiter, *columns)
    )


# Load json as python dict
def load_config_json(package: str, filename: str):
    return json.loads(pkgutil.get_data(package, filename))


def load_yaml(package: str, filename: str):
    return yaml.load(pkgutil.get_data(package, filename), yaml.SafeLoader) or {}


# Deep merge dict1 and dict2 and overwrite dict1 values with dict2 values at every level
def deep_merge_dictionaries(dict1, dict2):
    for key in dict2:
        if (
            key in dict1
            and isinstance(dict1[key], dict)
            and isinstance(dict2[key], dict)
        ):
            deep_merge_dictionaries(dict1[key], dict2[key])
        else:
            dict1[key] = dict2[key]
    return dict1


def is_dataframe_empty(dataframe):
    # Workaround for the following two spark issues
    # https://issues.apache.org/jira/browse/SPARK-18381
    # https://issues.apache.org/jira/browse/SPARK-35324
    # Select lit(1) to avoid issues due to data weirdness such as invalid dates,
    # before checking if at least one row exists
    return not (bool(dataframe.select(F.lit(1)).head(1)))


def find_table_definition(table_name: str, source_config: SourceConfig) -> Table:
    table_definitions = [
        table for table in source_config.tables if table.table_name == table_name
    ]
    assert (
        len(table_definitions) == 1
    ), f"{table_name} must appear 1 time only in Source Config. It appears {len(table_definitions)} times"

    return table_definitions[0]


def extract_deduplication_comparison_columns_for_table(
    table: Table,
    source_config: SourceConfig,
) -> List[ColumnName]:
    deduplication_comparison_columns = []
    if source_config.deduplication_config:
        deduplication_comparison_columns += (
            source_config.deduplication_config.comparison_columns
        )
    deduplication_comparison_columns += (
        table.dataset_transforms_config.deduplication_comparison_columns
    )
    return deduplication_comparison_columns


def extract_change_mode_column(table: Table, source_config: SourceConfig) -> ColumnName:
    if table.dataset_transforms_config.change_mode_column:
        return table.dataset_transforms_config.change_mode_column

    if (
        source_config.deduplication_config
        and source_config.deduplication_config.change_mode_column
    ):
        return source_config.deduplication_config.change_mode_column


def has_clashing_column(dataframe: DataFrame, column_name: str) -> bool:
    return (
        len(
            [
                existing_column
                for existing_column in dataframe.columns
                if existing_column.lower() == column_name.lower()
            ]
        )
        != 0
    )


def fast_copy_input_to_output(
    ctx: TransformContext,
    transform_output: TransformOutput,
    transform_input: TransformInput,
):
    # Copy all files from input to output
    input_file_system = transform_input.filesystem()
    output_file_system = transform_output.filesystem()

    def _copy_file_to_output(file_status):
        file_name = file_status.path
        with input_file_system.open(file_name, "rb") as f_in, output_file_system.open(
            file_name, "wb"
        ) as f_out:
            copyfileobj(f_in, f_out)

    input_file_system.files().rdd.foreach(_copy_file_to_output)

    # Copy schema from input to output
    schema_service = SchemaService(
        ctx._foundry._service_provider._services_config["metadata"]["uris"][0]
    )
    schema = schema_service.get_schema(
        ctx.auth_header, transform_input.rid, transform_input.branch
    )["schema"]
    schema_service.put_schema(
        ctx.auth_header, transform_output.rid, transform_output.branch, schema
    )
