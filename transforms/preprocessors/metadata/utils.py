from collections import defaultdict
from typing import Dict, List

from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import PipelineConfig
from bellhop_authoring_api.bellhop_authoring_api_config_source import (
    SourceConfig,
    Table,
)
from pyspark import Row
from pyspark.sql import functions as F, DataFrame
from transforms.api import IncrementalTransformInput, TransformInput

from software_defined_integrations.configs.process_config import (
    get_customization_tables,
)


def find_links(
    links_metadata: IncrementalTransformInput,
    pipeline_config: PipelineConfig,
    source_config: SourceConfig,
    primary_table: Table,
):
    customization_table_names = [
        table.table_name
        for table in get_customization_tables(
            pipeline_config, source_config, primary_table
        )
    ]
    return (
        links_metadata.dataframe()
        .filter(
            (F.col("primary_object_name") == primary_table.table_name)
            & (F.col("foreign_object_name").isin(customization_table_names))
        )
        .collect()
    )


def extract_fields_metadata(
    table_names: List[str],
    fields_metadata: TransformInput,
) -> Dict[str, List[Row]]:
    """
    Return a dictionary of fields metadata
    with object names as keys and fields metadata rows for these object names as values.
    """
    collected_fields_metadata = (
        fields_metadata.dataframe()
        .filter(F.col("object_name").isin(table_names))
        .select(
            F.col("raw_field_name"),
            F.col("field_name"),
            F.col("object_name"),
            F.col("field_order"),
        )
        .collect()
    )
    fields_metadata_dict = defaultdict(list)
    for row in collected_fields_metadata:
        fields_metadata_dict[row["object_name"]].append(row)
    return fields_metadata_dict


def extract_literal_fields(table: Table, fields_metadata: DataFrame) -> List[Row]:
    """
    Extract literal fields for an SAP table.
    """
    return (
        fields_metadata.filter(
            (F.col("object_name") == table.table_name)
            & (F.col("field_literal").isNotNull())
        )
        .select("raw_field_name", "field_literal")
        .collect()
    )
