import re
from typing import Dict, List

from bellhop_authoring_api.bellhop_authoring_api_config_common import ColumnName
from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    SourceType,
    TableType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import (
    SourceConfig,
    Table,
)
from bellhop_authoring_api.bellhop_authoring_api_config_transform import BellhopStage
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from transforms.api import configure, incremental, transform, TransformInput

from software_defined_integrations.configs.process_config import (
    get_spark_profile,
    BellhopDirectoryFactory,
)
from software_defined_integrations.transforms.preprocessors.metadata.utils import (
    extract_fields_metadata,
)
from software_defined_integrations.transforms.preprocessors.metadata_schemas import (
    MetadataType,
)
from software_defined_integrations.transforms.utils import (
    renamed_name_for_table,
    COLUMN_DELIMITER,
    CUSTOMIZATION_COLUMN_DELIMITER,
    FOREIGN_KEY_COLUMN_DELIMITER,
)
from software_defined_integrations.transforms.types import SpecialFieldName


def create_renamed_changelog_transform(
    table: Table,
    pipeline_config: PipelineConfig,
    source_config: SourceConfig,
):
    @configure(
        profile=get_spark_profile(
            table.dataset_transforms_config.spark_profiles,
            BellhopStage.RENAMED_CHANGELOG,
        )
    )
    @incremental(
        semantic_version=source_config.deployment_semantic_version,
        snapshot_inputs=["fields_metadata"],
    )
    @transform(
        out=BellhopDirectoryFactory.create_transforms_output(
            pipeline_config,
            source_config,
            BellhopStage.RENAMED_CHANGELOG,
            renamed_name_for_table(
                pipeline_config.tables[table.table_name].display_name, table.table_name
            ),
        ),
        primary_object=BellhopDirectoryFactory.create_transforms_input(
            pipeline_config,
            source_config,
            BellhopStage.ENRICHED
            if not pipeline_config.disable_enriched_stage
            else BellhopStage.CLEANED,
            table.dataset_transforms_config.dataset_name,
        ),
        fields_metadata=BellhopDirectoryFactory.create_transforms_input(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.FIELDS.value,
        ),
    )
    def process_function(out, primary_object, fields_metadata):
        dataframe = _rename_columns(
            table, pipeline_config, fields_metadata, primary_object.dataframe()
        )
        if pipeline_config.source_type == SourceType.SAP_ERP.name:
            dataframe = _update_bellhop_title_column(dataframe)
        out.write_dataframe(dataframe)

    process_function.__name__ = f"{source_config.source_name} {BellhopStage.RENAMED_CHANGELOG.name} {table.table_name}"
    return process_function


def _rename_columns(
    table: Table,
    pipeline_config: PipelineConfig,
    fields_metadata: TransformInput,
    dataframe: DataFrame,
) -> DataFrame:
    rename_column_mapping = extract_rename_column_mapping(
        table,
        pipeline_config,
        fields_metadata,
        dataframe.columns,
    )
    return dataframe.select(
        *[
            F.col(column_name).alias(renamed_column_name)
            for column_name, renamed_column_name in rename_column_mapping.items()
        ]
    )


def _update_bellhop_title_column(dataframe: DataFrame) -> DataFrame:
    if f"{SpecialFieldName.TITLE}_1".upper() in dataframe.columns:
        return dataframe.withColumn(
            SpecialFieldName.TITLE, F.col(f"{SpecialFieldName.TITLE}_1".upper())
        )
    if f"{SpecialFieldName.TITLE}_1".lower() in dataframe.columns:
        return dataframe.withColumn(
            SpecialFieldName.TITLE, F.col(f"{SpecialFieldName.TITLE}_1".lower())
        )
    return dataframe


def extract_rename_column_mapping(
    table: Table,
    pipeline_config: PipelineConfig,
    fields_metadata: TransformInput,
    column_names: List[ColumnName],
) -> Dict[ColumnName, ColumnName]:
    # Get human names and primary key info for columns in all tables
    customization_table_names = [
        table_name
        for table_name, table_definition in pipeline_config.tables.items()
        if TableType.CUSTOMIZATION in table_definition.types
        and table_name != table.table_name
    ]
    all_fields_metadata = extract_fields_metadata(
        [table.table_name] + customization_table_names, fields_metadata
    )
    # Get human names and primary key information for columns in enriched table
    primary_object_fields_metadata = all_fields_metadata.pop(table.table_name)

    # Create rename mapping for enriched table. Remove TITLE since that's a special column we don't want to rename.
    primary_rename_mapping = {
        raw_field_name: _get_new_column_name(raw_field_name, renamed_field_name)
        for (raw_field_name, renamed_field_name, _, _) in primary_object_fields_metadata
    }
    primary_rename_mapping.pop(SpecialFieldName.TITLE, None)

    # Create rename mappings for customization tables and store in dict. Remove TITLE since that's a special column.
    customization_rename_mapping = {}
    for customization in all_fields_metadata:
        customization_rename_mapping[customization] = {
            raw_field_name: _get_new_column_name(raw_field_name, renamed_field_name)
            for (raw_field_name, renamed_field_name, _, _) in all_fields_metadata[
                customization
            ]
        }
        customization_rename_mapping[customization].pop(SpecialFieldName.TITLE, None)

    # Rename columns to human readable names
    return {
        column: _get_renamed_selector_for_column(
            column, primary_rename_mapping, customization_rename_mapping
        )
        for column in column_names
    }


# Rename the column appropriately and return the selector for the column with its new alias
def _get_renamed_selector_for_column(
    column: ColumnName,
    primary_rename_mapping: Dict[ColumnName, ColumnName],
    customization_rename_mapping: Dict[str, Dict[ColumnName, ColumnName]],
) -> ColumnName:
    # Customization field is structured as '{c}_|_customization_field_{table}_|_{fields}'
    # Example: 'TBEZ_|_customization_field_T134T_|_MANDT_SPRAS_MTART'
    customization_match = re.match(
        rf"(\S+){re.escape(COLUMN_DELIMITER)}{re.escape(CUSTOMIZATION_COLUMN_DELIMITER)}"
        rf"(\S+){re.escape(COLUMN_DELIMITER)}(\S+)",
        column,
    )
    foreign_key_match = re.match(
        rf"(\S+){re.escape(COLUMN_DELIMITER)}{re.escape(FOREIGN_KEY_COLUMN_DELIMITER)}(\S+)",
        column,
    )
    # Don't do anything to foreign keys
    if foreign_key_match:
        return column
    elif customization_match:
        (
            source_column,
            customization_table,
            customized_columns,
        ) = customization_match.groups()
        renamed_column = customization_rename_mapping[customization_table].get(
            source_column, source_column.lower()
        )
        return (
            f"{renamed_column}{COLUMN_DELIMITER}{CUSTOMIZATION_COLUMN_DELIMITER}"
            f"{customization_table}{COLUMN_DELIMITER}{customized_columns}"
        )
    return primary_rename_mapping.get(column, column.lower())


def _get_new_column_name(raw_field_name: str, renamed_field_name: str):
    """
    The 'is None' is deliberate here.
    If the translated field is "", keep it as _|_technical_name instead of technical_name_|_technical_name.
    """
    if renamed_field_name is None:
        return raw_field_name

    new_name = f"{renamed_field_name}{COLUMN_DELIMITER}{raw_field_name}"
    new_name = _normalize(_remove_invalid_chars(new_name))
    return new_name.lower()


def _normalize(column_name):
    """
    Replace white spaces with underscores
    """
    return column_name.title().replace(" ", "_")


def _remove_invalid_chars(column_name):
    """
    Remove invalid characters
    """
    replacement_map = {
        ".": "",
        "(": "",
        ")": "",
        "/": "",
        "-": "",
        ",": "",
        ":": "",
        "=": "",
        "?": "",
        "+": "",
        "'": "",
        ";": "",
        "US$": "USD",
    }
    for invalid_char in replacement_map:
        column_name = column_name.replace(invalid_char, replacement_map[invalid_char])
    return column_name
