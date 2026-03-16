from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
from software_defined_integrations.transforms.preprocessors.infor.transforms.utils import (
    create_infor_tables_df,
)
import re

reference_column_identifier = "ref"


def generate_links_metadata(objects_df: DataFrame, fields_df: DataFrame):
    df = fields_df.filter(F.col("raw_field_name").contains(reference_column_identifier))
    df = df.withColumn(
        "ref_table", extract_ref_table(F.col("field_description"), F.col("object_name"))
    )

    references_df = (
        df.join(objects_df, df["ref_table"] == objects_df["object_name"], "inner")
        .select(
            df["ref_table"], df["object_name"], objects_df["primary_key_raw_fields"]
        )
        .withColumnRenamed("primary_key_raw_fields", "ref_pks")
    )

    main_df = (
        df.select("object_name")
        .distinct()
        .join(objects_df, "object_name", "inner")
        .select(df["object_name"], objects_df["primary_key_raw_fields"])
        .withColumnRenamed("primary_key_raw_fields", "object_pks")
    )

    links_df = main_df.join(references_df, "object_name", "left")

    return links_df.select(
        F.col("object_name").alias("primary_object_name"),
        F.col("object_pks").alias("primary_object_raw_fields"),
        F.col("ref_table").alias("foreign_object_name"),
        F.col("ref_pks").alias("foreign_object_raw_fields"),
        F.lit(None).cast(T.StringType()).alias("link_description"),
    )


@F.udf
def extract_ref_table(field_description, root_object):
    pattern = re.match(r"(\w+)_.*", root_object)
    object_src = pattern.group(1) if pattern else None

    pattern = re.match(r".* table (\w+) .*", field_description)
    ref_object = pattern.group(1) if pattern else None

    return "{object_src}_{ref_object}".format(
        object_src=object_src, ref_object=ref_object
    )
