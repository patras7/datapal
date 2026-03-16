from pyspark.sql import DataFrame, functions as F
from software_defined_integrations.transforms.preprocessors.sap_erp.utils.types import (
    SapFieldNameAlias,
)


# Used to determine relations between tables (for customizations and foreign keys)
def generate_full_table_relations(dd08l: DataFrame, dd05s: DataFrame) -> DataFrame:
    dd05s = dd05s.filter(
        (F.col("TABNAME") == F.col("FORTABLE"))
        | (
            F.col("FORKEY") == "MANDT"
        )  # TODO (sbalanovich): Resolve this more elegantly
    ).select("TABNAME", "FIELDNAME", "PRIMPOS", "FORKEY")
    return dd08l.join(dd05s, ["TABNAME", "FIELDNAME"]).select(
        F.col("TABNAME").alias(SapFieldNameAlias.table_name),
        F.col("FORKEY").alias(SapFieldNameAlias.column_name),
        F.col("CHECKTABLE").alias(SapFieldNameAlias.check_table_name),
        F.col("PRIMPOS").alias(SapFieldNameAlias.position),
        F.col("FIELDNAME").alias(SapFieldNameAlias.segment_by),
        F.col("FRKART").alias(SapFieldNameAlias.metadata),
    )
