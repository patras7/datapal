from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
from software_defined_integrations.transforms.preprocessors.infor.transforms.utils import (
    create_infor_tables_df,
)


def generate_objects_metadata(ctx, raw_json: DataFrame):
    objects = create_infor_tables_df(ctx, raw_json)
    return objects.select(
        "object_name",
        "primary_key_raw_fields",
        "description",
        F.lit(None).cast(T.StringType()).alias("text_table_names"),
    )
