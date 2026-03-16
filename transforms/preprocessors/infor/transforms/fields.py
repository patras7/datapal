from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
from software_defined_integrations.transforms.preprocessors.infor.transforms.utils import (
    create_infor_tables_df,
)


def generate_fields_metadata(ctx, raw_json: DataFrame) -> DataFrame:
    tables_df = create_infor_tables_df(ctx, raw_json)
    columns_df = tables_df.select(
        F.col("object_name"),
        F.explode(tables_df["object_properties"]).alias("exploded"),
    )

    return columns_df.select(
        "object_name",
        F.col("exploded")["column_desc"].alias("field_name"),
        F.col("exploded")["column_name"].alias("raw_field_name"),
        F.col("exploded")["column_type"].alias("field_type"),
        F.col("exploded")["column_desc"].alias("field_description"),
        F.lit(None).cast(T.StringType()).alias("domain_name"),
        F.lit(None).cast(T.StringType()).alias("field_literal"),
    )
