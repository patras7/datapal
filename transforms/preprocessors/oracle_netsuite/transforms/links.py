from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def generate_links_metadata(foreign_keys: DataFrame):
    if "Table" in foreign_keys.columns:
        return foreign_keys.select(
            F.col("Table").alias("primary_object_name"),
            F.array(F.col("Fk_Column")).alias("primary_object_raw_fields"),
            F.col("Pk_Table").alias("foreign_object_name"),
            F.array(F.col("Pk_Column")).alias("foreign_object_raw_fields"),
            F.concat_ws(" to ", F.col("Table"), F.col("Pk_Table")).alias(
                "link_description"
            ),
        ).coalesce(1)
    else:
        return foreign_keys.select(
            F.col("Fk_Table").alias("primary_object_name"),
            F.col("Fk_Column").alias("primary_object_raw_fields"),
            F.col("Pk_Table").alias("foreign_object_name"),
            F.col("Pk_Column").alias("foreign_object_raw_fields"),
            F.concat_ws(" to ", F.col("Fk_Table"), F.col("Pk_Table")).alias(
                "link_description"
            ),
        ).coalesce(1)
