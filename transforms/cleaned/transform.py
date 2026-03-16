import logging
import os
from datetime import datetime
from functools import reduce
from typing import List

from bellhop_authoring_api.bellhop_authoring_api_config_common import (
    TableName,
    ColumnName,
)
from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    SourceType,
)
from bellhop_authoring_api.bellhop_authoring_api_config_source import (
    SourceName,
    SourceConfig,
    Table,
)
from bellhop_authoring_api.bellhop_authoring_api_config_transform import BellhopStage
from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
from pyspark.sql.types import BinaryType
from transforms.api import (
    Input,
    configure,
    incremental,
    transform,
    TransformContext,
    TransformInput,
    TransformOutput,
)

from software_defined_integrations.configs.process_config import (
    get_spark_profile,
    BellhopDirectoryFactory,
)
from software_defined_integrations.transforms.cleaned.function_libraries import (
    common,
    deployment,
)
from software_defined_integrations.transforms.preprocessors.metadata.utils import (
    extract_fields_metadata,
    extract_literal_fields,
)
from software_defined_integrations.transforms.preprocessors.metadata_schemas import (
    MetadataType,
)
from transforms import expectations as E
from transforms.api import Check
from software_defined_integrations.transforms.utils import (
    blank_as_null,
    get_primary_object_metadata,
    COLUMN_DELIMITER,
    concat_ws_with_nulls,
    BELLHOP_COLUMN_PREFIX,
    extract_deduplication_comparison_columns_for_table,
    extract_change_mode_column,
    has_clashing_column,
)
from software_defined_integrations.transforms.types import SpecialFieldName
from software_defined_integrations.transforms.preprocessors.transform import (
    TRANSFORM_SEMANTIC_VERSION,
)
from software_defined_integrations.transforms.preprocessors.sap_erp.utils.types import (
    ConnectorFieldPrefixes,
)


def create_cleaned_transform(
    primary_table: Table,
    pipeline_config: PipelineConfig,
    source_config: SourceConfig,
):
    @configure(
        profile=get_spark_profile(
            primary_table.dataset_transforms_config.spark_profiles, BellhopStage.CLEANED
        )
    )
    @incremental(
        semantic_version=TRANSFORM_SEMANTIC_VERSION
        + source_config.deployment_semantic_version,
        snapshot_inputs=[
            "objects_metadata",
            "fields_metadata",
            "links_metadata",
            "diffs_metadata",
        ],
    )
    @transform(
        out=BellhopDirectoryFactory.create_transforms_output(
            pipeline_config,
            source_config,
            BellhopStage.CLEANED,
            primary_table.dataset_transforms_config.dataset_name,
            checks=Check(
                ~E.col(
                    BELLHOP_COLUMN_PREFIX + SpecialFieldName.SCHEMA_MISMATCH_COLUMN_NAME
                ).non_null(),
                f"Check that schema matches Data Dictionary - \
                see {BELLHOP_COLUMN_PREFIX + SpecialFieldName.SCHEMA_MISMATCH_COLUMN_NAME} column for details",
                on_error="WARN",
            ),
        ),
        objects_metadata=BellhopDirectoryFactory.create_transforms_input(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.OBJECTS.value,
        ),
        fields_metadata=BellhopDirectoryFactory.create_transforms_input(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.FIELDS.value,
        ),
        links_metadata=BellhopDirectoryFactory.create_transforms_input(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.LINKS.value,
        ),
        diffs_metadata=BellhopDirectoryFactory.create_transforms_input(
            pipeline_config,
            source_config,
            BellhopStage.METADATA,
            MetadataType.DIFFS.value,
        ),
        **_get_primary_input_or_batch_union_inputs(primary_table, source_config),
    )
    def process_function(
        ctx,
        out,
        objects_metadata,
        fields_metadata,
        links_metadata,
        diffs_metadata,
        **primary_or_batch_union_inputs,
    ):
        # Case 1: If the input is a snapshot (i.e. ctx.is_incremental is False) -> snapshot
        # Case 2: If the input is incremental, and
        # the diffs_metadata is created after the last run of the cleaned dataset, and
        # it has changed -> snapshot
        run_as_snapshot = not ctx.is_incremental or (
            _is_diffs_metadata_new(ctx, diffs_metadata, out)
            and diffs_metadata.dataframe()
            .filter(F.col("table_name") == primary_table.table_name)
            .collect()[0]["has_changed"]
        )

        primary_dataframe = _generate_output_df(
            out,
            objects_metadata,
            fields_metadata,
            links_metadata,
            run_as_snapshot,
            source_config,
            pipeline_config,
            primary_table,
            primary_or_batch_union_inputs,
        )

        # if schema is changed, force snapshot build
        if not run_as_snapshot and set(primary_dataframe.columns).symmetric_difference(
            set(out.dataframe().columns)
        ):
            run_as_snapshot = True

            primary_dataframe = _generate_output_df(
                out,
                objects_metadata,
                fields_metadata,
                links_metadata,
                run_as_snapshot,
                source_config,
                pipeline_config,
                primary_table,
                primary_or_batch_union_inputs,
            )

        if run_as_snapshot:
            out.set_mode("replace")

        out.write_dataframe(primary_dataframe)

    process_function.__name__ = f"{source_config.source_name} {BellhopStage.CLEANED.name} {primary_table.table_name}"
    return process_function


def _generate_output_df(
    out,
    objects_metadata,
    fields_metadata,
    links_metadata,
    run_as_snapshot: bool,
    source_config: SourceConfig,
    pipeline_config: PipelineConfig,
    primary_table: Table,
    primary_or_batch_union_inputs,
):
    primary_dataframe = _union_batch_components(
        pipeline_config.source_type,
        primary_or_batch_union_inputs,
        "current" if run_as_snapshot else "added",
    )

    if run_as_snapshot or (
        BELLHOP_COLUMN_PREFIX + SpecialFieldName.SCHEMA_MISMATCH_COLUMN_NAME
        in out.dataframe().columns
    ):
        primary_dataframe = _maybe_add_schema_mismatch_column(
            primary_table,
            fields_metadata,
            primary_dataframe,
            source_config,
            pipeline_config.source_type,
        )

    cleaning_metadata_dataframe = fields_metadata.dataframe()

    function_libraries = [common, deployment]
    for library in (
        source_config.cleaning_libraries
        + primary_table.dataset_transforms_config.table_cleaning_libraries
    ):
        # expect that this library exists in one (and only one) of the cleaning libraries
        assert any(
            library in dir(function_library) for function_library in function_libraries
        ), f"Can't find a distinct cleaning library named '{library}'"

        # First apply common function libraries, then deployment-specific ones
        for function_library in function_libraries:
            if library in dir(function_library):
                primary_dataframe = getattr(function_library, library)(
                    primary_dataframe,
                    cleaning_metadata_dataframe,
                    primary_table,
                    source_config,
                )

    primary_dataframe = _convert_blank_fields_to_null(primary_dataframe)
    primary_dataframe = _drop_binary_columns(primary_dataframe)
    primary_dataframe = _add_literal_fields(
        primary_table, primary_dataframe, fields_metadata
    )
    primary_dataframe = _add_bellhop_columns(
        source_type=pipeline_config.source_type,
        source_name=source_config.source_name,
        table_name=primary_table.table_name,
        dataframe=primary_dataframe,
        objects_metadata=objects_metadata,
        links_metadata=links_metadata,
        all_table_names=list(pipeline_config.tables.keys()),
        disable_foreign_key_generation=pipeline_config.disable_foreign_key_generation,
    )

    return primary_dataframe


def _maybe_add_schema_mismatch_column(
    primary_table: Table,
    fields_metadata: DataFrame,
    primary_dataframe: DataFrame,
    source_config: SourceConfig,
    source_type: str,
):
    """
    Checks schema against fields metadata and adds a schema mismatch column if there is a difference
    """
    all_metadata_fields = set(
        [
            row["raw_field_name"]
            for row in extract_fields_metadata(
                [primary_table.table_name], fields_metadata
            )[primary_table.table_name]
        ]
    )
    all_dataframe_fields = set(primary_dataframe.columns) - {
        convert_bellhop_column_name(
            SpecialFieldName.SOURCE_DATASET_NAME.value, source_type
        )
    }
    fields_diff = all_dataframe_fields.difference(all_metadata_fields)
    primary_change_mode_column = extract_change_mode_column(
        primary_table, source_config
    )
    EXCLUDED_COLUMNS = ["ODQ_ENTITYCNTR"]
    fields_diff = fields_diff - set(
        extract_deduplication_comparison_columns_for_table(primary_table, source_config)
        + ([primary_change_mode_column] if primary_change_mode_column else [])
        + [
            field
            for prefix in ConnectorFieldPrefixes
            for field in all_dataframe_fields
            if field.startswith(prefix)
        ]
        + [col for col in EXCLUDED_COLUMNS]
    )
    if fields_diff:
        return primary_dataframe.withColumn(
            BELLHOP_COLUMN_PREFIX + SpecialFieldName.SCHEMA_MISMATCH_COLUMN_NAME,
            F.lit(f"Fields in schema but missing in metadata: {fields_diff}"),
        )
    return primary_dataframe


def _is_diffs_metadata_new(
    ctx: TransformContext,
    diffs_metadata: TransformInput,
    transform_output: TransformOutput,
):
    """
    Diffs metadata is new if the new view is created after the last time the cleaned dataset was built.
    """
    output_txn_time = _get_last_committed_transaction_time(ctx, transform_output)
    if not output_txn_time:
        # The output hasn't been created before.  The diffs_metadata is always new in this case
        return True
    return output_txn_time < _get_last_committed_transaction_time(ctx, diffs_metadata)


def _get_last_committed_transaction_time(
    ctx: TransformContext, transform_input: TransformInput
):
    reverse_transactions_in_view = (
        ctx._foundry._service_provider.catalog().get_reverse_transactions_in_view(
            ctx.auth_header,
            transform_input.rid,
            transform_input.end_transaction_rid,
            transform_input.end_transaction_rid,
        )
    )
    timestamps = [
        transaction["transaction"]["closeTime"]
        for transaction in reverse_transactions_in_view
    ]
    if len(timestamps) == 0:
        # The dataset has never been built
        return None
    # Ignore the nanoseconds
    timestamp = timestamps[0].split(".")[0]
    # Ignore the timezone
    timestamp = timestamp.split("Z")[0]
    # workaround for the issue #842
    if timestamp.count(":") == 1:
        timestamp = f"{timestamp}:00"

    logging.info(
        f"""get_last_committed_transaction_time:
         dataset_rid={transform_input.rid}, transaction_rid={transform_input.end_transaction_rid}, timestamp={timestamp}
         """
    )

    return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")


def _get_primary_input_or_batch_union_inputs(
    primary_table: Table, source_config: SourceConfig
):
    # If there are no batch union components, return an array with the primary input
    batch_union_components = (
        primary_table.dataset_transforms_config.batch_union_components
        or [primary_table.dataset_transforms_config.dataset_name]
    )
    return {
        batch_component: Input(
            os.path.join(source_config.raw_folder_structure.raw, batch_component)
        )
        for batch_component in batch_union_components
    }


def _union_batch_components(source_type: str, primary_or_batch_union_inputs, mode: str):
    primary_or_batch_union_dfs = [
        source_dataset.dataframe(mode).withColumn(
            convert_bellhop_column_name(
                SpecialFieldName.SOURCE_DATASET_NAME, source_type
            ),
            F.lit(source_dataset_name),
        )
        for source_dataset_name, source_dataset in primary_or_batch_union_inputs.items()
    ]
    return reduce(DataFrame.unionByName, primary_or_batch_union_dfs)


def _convert_blank_fields_to_null(dataframe: DataFrame) -> DataFrame:
    """Converts all-blank fields in a dataframe to all-null"""
    return dataframe.select(
        *[blank_as_null(F.col(column)).alias(column) for column in dataframe.columns]
    )


def _drop_binary_columns(dataframe: DataFrame) -> DataFrame:
    binary_columns = filter(
        lambda column: dataframe.schema[column].dataType == BinaryType,
        dataframe.columns,
    )
    return dataframe.drop(*binary_columns)


def _add_literal_fields(
    table: Table, dataframe: DataFrame, fields_metadata: TransformInput
):
    """
    Create or override any literal fields that have been specified in the metadata
    """
    literal_fields = extract_literal_fields(table, fields_metadata.dataframe())
    columns = [F.col(column) for column in dataframe.columns]
    for raw_field_name, field_literal in literal_fields:
        columns.append(F.lit(field_literal).alias(raw_field_name))
    return dataframe.select(*columns)


def _add_bellhop_columns(
    source_type: str,
    source_name: SourceName,
    table_name: TableName,
    dataframe: DataFrame,
    objects_metadata: TransformInput,
    links_metadata: TransformInput,
    all_table_names: List[str],
    disable_foreign_key_generation: bool,
) -> DataFrame:
    # Add primary_key
    dataframe = _add_primary_key_column(
        source_type, source_name, table_name, dataframe, objects_metadata
    )
    dataframe = dataframe.dropna(
        subset=[convert_bellhop_column_name(SpecialFieldName.PRIMARY_KEY, source_type)]
    )

    # Add source_name
    dataframe = _add_source_column(source_name, source_type, dataframe)

    # Add title
    dataframe = _add_title_column(source_type, dataframe)

    # Add foreign keys, unless disabled
    if not disable_foreign_key_generation:
        dataframe = _add_foreign_key_columns(
            source_name, table_name, dataframe, links_metadata, all_table_names
        )

    return dataframe


def convert_bellhop_column_name(column_name: ColumnName, source_type: str):
    return (
        column_name
        if source_type == SourceType.SAP_ERP.name
        else f"{BELLHOP_COLUMN_PREFIX}{column_name}"
    )


def _add_source_column(
    source_name: SourceName,
    source_type: str,
    dataframe: DataFrame,
):
    source_column_name = convert_bellhop_column_name(
        SpecialFieldName.SOURCE, source_type
    )

    if source_type == SourceType.SAP_ERP.name and has_clashing_column(
        dataframe, source_column_name
    ):
        source_column_name = f"{BELLHOP_COLUMN_PREFIX}{source_column_name}"

    return dataframe.withColumn(source_column_name, F.lit(source_name))


def _add_primary_key_column(
    source_type: str,
    source_name: SourceName,
    table_name: TableName,
    dataframe: DataFrame,
    objects_metadata: TransformInput,
) -> DataFrame:
    primary_key_column = convert_bellhop_column_name(
        SpecialFieldName.PRIMARY_KEY, source_type
    )
    if primary_key_column.upper() in dataframe.columns:
        # Lowercase PRIMARY_KEY if it already exists
        return dataframe.withColumnRenamed(
            primary_key_column.upper(),
            primary_key_column,
        )
    if primary_key_column in dataframe.columns:
        # Do nothing if a correct primary key already exists
        return dataframe

    primary_object_metadata = get_primary_object_metadata(table_name, objects_metadata)

    primary_key_columns = ([F.lit(source_name)] if source_name.strip() else []) + [
        F.col(column) for column in primary_object_metadata["primary_key_raw_fields"]
    ]
    return dataframe.withColumn(
        primary_key_column,
        F.concat_ws(COLUMN_DELIMITER, *primary_key_columns),
    )


def _add_title_column(source_type: str, dataframe: DataFrame) -> DataFrame:
    if SpecialFieldName.TITLE.lower() in dataframe.columns:
        title_column = SpecialFieldName.TITLE.lower()
    elif SpecialFieldName.TITLE.upper() in dataframe.columns:
        dataframe = dataframe.withColumnRenamed(
            SpecialFieldName.TITLE.upper(), SpecialFieldName.TITLE.lower()
        )
        title_column = SpecialFieldName.TITLE.lower()
    else:
        title_column = convert_bellhop_column_name(
            SpecialFieldName.PRIMARY_KEY, source_type
        )

    return dataframe.withColumn(
        convert_bellhop_column_name(SpecialFieldName.TITLE, source_type),
        F.col(title_column),
    )


def _add_foreign_key_columns(
    source_name: SourceName,
    table_name: TableName,
    dataframe: DataFrame,
    links_metadata: TransformInput,
    all_table_names: List[str],
) -> DataFrame:
    # Get the foreign and primary raw dataset name for the foreign keys
    # Filter down to only the links where the foreign_object_name is this object's name
    links_from_dataset = (
        links_metadata.dataframe()  # noqa
        .filter(F.col("primary_object_name") == table_name)
        .filter(F.col("foreign_object_name").isin(all_table_names))
        .select("foreign_object_name", "primary_object_raw_fields")
        .collect()
    )

    # Create foreign keys for each link. Since the link's foreign object is the current object,
    # Use the "foreign object fields" (the fields of this object)
    # And the "primary object name" (the name of the related table from the source system)
    for link in links_from_dataset:
        if not set(link["primary_object_raw_fields"]).issubset(set(dataframe.columns)):
            # Skip foreign key creation when the required columns don't exist in the dataframe.
            continue
        dataframe = _add_foreign_key_column(
            dataframe,
            source_name,
            link["foreign_object_name"],
            link["primary_object_raw_fields"],
        )

    return dataframe


def _add_foreign_key_column(
    dataframe: DataFrame,
    source_name: SourceName,
    foreign_table_name: str,
    field_names: List[str],
) -> DataFrame:
    # Field refers to each property in each object; column is each column in the raw source-system tables

    columns = [F.lit(source_name)] + [F.col(field_name) for field_name in field_names]

    return dataframe.withColumn(
        _generate_foreign_key_column_name_for_ontology(foreign_table_name, field_names),
        concat_ws_with_nulls(COLUMN_DELIMITER, *columns),
    )


def _generate_foreign_key_column_name_for_ontology(
    foreign_table_name: str,
    field_names: List[str],
) -> str:
    return f"{'_'.join(field_names).lower()}{COLUMN_DELIMITER}foreign_key_{foreign_table_name}"
