from typing import List
import os
from transforms.api import transform, Input, Output, TransformContext, configure
from transforms.verbs.dataframes import union_many
from bellhop_authoring_api.bellhop_authoring_api_config_pipeline import (
    PipelineConfig,
    TableType,
    PostProcessStageType,
)
from software_defined_integrations.transforms.utils import (
    renamed_name_for_table,
    translate_table_name_to_dataset_name,
)
from pyspark.sql import functions as F


def generate_post_process_transforms(
    pipeline_config: PipelineConfig, final_dataset_directory: str
):
    transforms = []
    if PostProcessStageType.ROW_COUNT in pipeline_config.post_process_stages:
        transforms.append(
            _generate_row_count_transform(pipeline_config, final_dataset_directory)
        )

    return transforms


def _generate_row_count_transform(
    pipeline_config: PipelineConfig, final_dataset_directory: str
):
    @configure(
        profile=pipeline_config.post_process_stages.get(PostProcessStageType.ROW_COUNT)
    )
    @transform(
        out=Output(
            str(pipeline_config.output_folder)
            + "/row_count_"
            + str(pipeline_config.project_name)
        ),
        **{
            table_name: Input(
                os.path.join(
                    final_dataset_directory,
                    renamed_name_for_table(
                        pipeline_config.tables[table_name].display_name, table_name
                    )
                    if not pipeline_config.disable_renamed_stage
                    else translate_table_name_to_dataset_name(table_name),
                )
            )
            for table_name, table_values in pipeline_config.tables.items()
            if table_values.types != [TableType.METADATA]
        }
    )
    def compute(ctx: TransformContext, out: Output, **kwargs):
        tables = [
            table.dataframe().select("source", F.lit(table_name).alias("table"))
            for table_name, table in kwargs.items()
        ]
        result = union_many(*tables).groupBy("source", "table").count()
        out.write_dataframe(result)

    return compute
