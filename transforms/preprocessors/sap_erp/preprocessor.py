from typing import Dict, Tuple
from pyspark.sql import functions as F

from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    TableType,
    SourceType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import (
    SourceConfig,
    Table,
)
from pyspark.sql.dataframe import DataFrame
from transforms.api import TransformContext, TransformInput

from software_defined_integrations.transforms.preprocessors.registry import register
from software_defined_integrations.transforms.preprocessors.sap_erp.transforms.full_table_relations import (
    generate_full_table_relations,
)
from software_defined_integrations.transforms.preprocessors.sap_erp.transforms.table_fields_metadata import (
    generate_table_fields_metadata,
)
from software_defined_integrations.transforms.preprocessors.preprocessor import (
    Preprocessor,
)
from software_defined_integrations.transforms.preprocessors.sap_erp.transforms.fields import (
    generate_fields_metadata,
)
from software_defined_integrations.transforms.preprocessors.sap_erp.transforms.links import (
    generate_links_metadata,
)
from software_defined_integrations.transforms.preprocessors.sap_erp.transforms.objects import (
    generate_objects_metadata,
)
from software_defined_integrations.transforms.preprocessors.sap_erp.utils.types import (
    SapDataDictionaryTableName,
)
from software_defined_integrations.transforms.renamed.transform import (
    deduplicate_dataframe,
    drop_deleted_records,
)
from software_defined_integrations.transforms.utils import (
    find_table_definition,
    extract_deduplication_comparison_columns_for_table,
    extract_change_mode_column,
)

PRIMARY_KEY_FIELDS = {
    SapDataDictionaryTableName.DD02L: ["TABNAME", "AS4LOCAL", "AS4VERS"],
    SapDataDictionaryTableName.DD02T: ["TABNAME", "DDLANGUAGE", "AS4LOCAL", "AS4VERS"],
    SapDataDictionaryTableName.DD03L: [
        "TABNAME",
        "FIELDNAME",
        "AS4LOCAL",
        "AS4VERS",
        "POSITION",
    ],
    SapDataDictionaryTableName.DD04T: ["ROLLNAME", "DDLANGUAGE", "AS4LOCAL", "AS4VERS"],
    SapDataDictionaryTableName.DD05S: [
        "TABNAME",
        "FIELDNAME",
        "PRIMPOS",
        "AS4LOCAL",
        "AS4VERS",
    ],
    SapDataDictionaryTableName.DD08L: ["TABNAME", "FIELDNAME", "AS4LOCAL", "AS4VERS"],
}


@register(SourceType.SAP_ERP)
class SapErp(Preprocessor):
    def __init__(self, pipeline_config: PipelineConfig, source_config: SourceConfig):
        super().__init__(pipeline_config, source_config)

    def transform(
        self,
        _ctx: TransformContext,
        **tables: TransformInput,
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        metadata_tables = self._extract_metadata_dataframes(tables)

        full_table_relations = (
            generate_full_table_relations(
                metadata_tables[SapDataDictionaryTableName.DD08L],
                metadata_tables[SapDataDictionaryTableName.DD05S],
            )
            .coalesce(1)
            .cache()
        )
        table_fields_metadata = (
            generate_table_fields_metadata(
                metadata_tables[SapDataDictionaryTableName.DD03L],
                metadata_tables[SapDataDictionaryTableName.DD04T],
                metadata_tables[SapDataDictionaryTableName.DD02L],
                self._source_config,
            )
            .coalesce(1)
            .cache()
        )
        fields_metadata_df = generate_fields_metadata(table_fields_metadata)
        links_metadata_df = generate_links_metadata(
            full_table_relations,
            table_fields_metadata,
        )
        objects_metadata_df = generate_objects_metadata(
            metadata_tables[SapDataDictionaryTableName.DD02L],
            metadata_tables[SapDataDictionaryTableName.DD02T],
            full_table_relations,
            table_fields_metadata,
            self._source_config,
        )

        return objects_metadata_df, fields_metadata_df, links_metadata_df

    def _extract_metadata_dataframes(
        self, tables: Dict[str, TransformInput]
    ) -> Dict[str, DataFrame]:
        dd_tables = {
            table_name: transform_input.dataframe().distinct().cache()
            for table_name, transform_input in tables.items()
            if TableType.METADATA in self._pipeline_config.tables[table_name].types
        }

        all_table_names = {table.table_name for table in self._source_config.tables}

        for table_name in SapDataDictionaryTableName:
            assert table_name in dd_tables, f"{table_name} not found."
            table_definition = find_table_definition(table_name, self._source_config)

            if "TABNAME" in dd_tables[table_name].columns:
                dd_tables[table_name] = dd_tables[table_name].filter(
                    F.col("TABNAME").isin(all_table_names)
                )
            deduplication_comparison_columns = (
                extract_deduplication_comparison_columns_for_table(
                    table_definition, self._source_config
                )
            )
            in_case_of_tie_pick_randomly = False
            if table_definition.dataset_transforms_config.enforce_unique_primary_keys:
                in_case_of_tie_pick_randomly = (
                    table_definition.dataset_transforms_config.enforce_unique_primary_keys
                )
            if deduplication_comparison_columns:
                dd_tables[table_name] = deduplicate_dataframe(
                    dd_tables[table_name],
                    PRIMARY_KEY_FIELDS[table_name],
                    deduplication_comparison_columns,
                    table_definition,
                    in_case_of_tie_pick_randomly=in_case_of_tie_pick_randomly,
                )
                change_mode_column = extract_change_mode_column(
                    table_definition, self._source_config
                )
                if change_mode_column:
                    dd_tables[table_name] = drop_deleted_records(
                        dd_tables[table_name],
                        change_mode_column,
                        table_definition,
                    )
        return dd_tables
