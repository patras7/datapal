from typing import Dict, Tuple
from pyspark.sql import DataFrame, functions as F

from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    TableType,
    SourceType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import SourceConfig
from pyspark.sql.dataframe import DataFrame

from transforms.api import TransformContext, TransformInput

from software_defined_integrations.transforms.preprocessors.registry import register
from software_defined_integrations.transforms.preprocessors.oracle_ebs.utils.types import (
    OracleEbsDataDictionaryTableName,
)

from software_defined_integrations.transforms.preprocessors.preprocessor import (
    Preprocessor,
)

from software_defined_integrations.transforms.preprocessors.oracle_ebs.transforms.table_fields_metadata import (
    generate_tables_metadata,
    generate_primary_keys_metadata,
    generate_foreign_keys_metadata,
    generate_columns_metadata,
)
from software_defined_integrations.transforms.preprocessors.oracle_ebs.transforms.objects import (
    generate_objects_metadata,
)
from software_defined_integrations.transforms.preprocessors.oracle_ebs.transforms.fields import (
    generate_fields_metadata,
)
from software_defined_integrations.transforms.preprocessors.oracle_ebs.transforms.links import (
    generate_links_metadata,
)


@register("ORACLE_EBS")
class OracleEbs(Preprocessor):
    def __init__(self, pipeline_config: PipelineConfig, source_config: SourceConfig):
        super().__init__(pipeline_config, source_config)

    def transform(
        self,
        _ctx: TransformContext,
        **tables: TransformInput,
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        metadata_tables = self._extract_metadata_dataframes(tables)

        tables_metadata_df = (
            generate_tables_metadata(
                metadata_tables[OracleEbsDataDictionaryTableName.FND_TABLES],
                metadata_tables[OracleEbsDataDictionaryTableName.ALL_TABLES],
                self._source_config,
            )
            .coalesce(1)
            .cache()
        )

        primary_keys_df = (
            generate_primary_keys_metadata(
                tables_metadata_df,
                metadata_tables[OracleEbsDataDictionaryTableName.FND_COLUMNS],
                metadata_tables[OracleEbsDataDictionaryTableName.FND_PRIMARY_KEYS],
                metadata_tables[
                    OracleEbsDataDictionaryTableName.FND_PRIMARY_KEY_COLUMNS
                ],
                self._source_config,
            )
            .coalesce(1)
            .cache()
        )

        foreign_keys_df = (
            generate_foreign_keys_metadata(
                tables_metadata_df,
                metadata_tables[OracleEbsDataDictionaryTableName.FND_COLUMNS],
                metadata_tables[OracleEbsDataDictionaryTableName.FND_FOREIGN_KEYS],
                metadata_tables[
                    OracleEbsDataDictionaryTableName.FND_FOREIGN_KEY_COLUMNS
                ],
                self._source_config,
            )
            .coalesce(1)
            .cache()
        )

        columns_metadata_df = (
            generate_columns_metadata(
                tables_metadata_df,
                metadata_tables[OracleEbsDataDictionaryTableName.FND_COLUMNS],
                self._source_config,
            )
            .coalesce(1)
            .cache()
        )

        objects_metadata_df = generate_objects_metadata(
            tables_metadata_df,
            primary_keys_df,
            self._source_config,
        )

        fields_metadata_df = (
            generate_fields_metadata(
                columns_metadata_df,
                self._source_config,
            )
            .coalesce(1)
            .cache()
        )

        links_metadata_df = generate_links_metadata(
            primary_keys_df,
            foreign_keys_df,
            self._source_config,
        )

        return objects_metadata_df, fields_metadata_df, links_metadata_df

    def _extract_metadata_dataframes(
        self, tables: Dict[str, TransformInput]
    ) -> Dict[str, DataFrame]:
        """
        Extracting only METADATA tables from source config.
        """
        dd_tables = {
            table_name: transform_input.dataframe().distinct().cache()
            for table_name, transform_input in tables.items()
            if TableType.METADATA in self._pipeline_config.tables[table_name].types
        }

        non_metadata_table_names = {
            table.table_name for table in self._source_config.tables
        } - dd_tables.keys()

        for table_name in OracleEbsDataDictionaryTableName:
            assert table_name in dd_tables, f"{table_name} not found."

            if "TABNAME" in dd_tables[table_name].columns:
                dd_tables[table_name] = dd_tables[table_name].filter(
                    F.col("TABNAME").isin(non_metadata_table_names)
                )

        return dd_tables
