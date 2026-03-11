# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-27T10:50:37.452882700Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


from pyspark.sql import functions as F
# from transforms.api import incremental
from myproject.utils import (
    schema_with_statistics
)

# _SEMANTIC_VERSION = 1
# _REQUIRE_INCREMENTAL = True


# @configure(profile=_SPARK_OPTS) # Removed: Not supported in Databricks
# @incremental(
#     require_incremental=_REQUIRE_INCREMENTAL,
#     semantic_version=_SEMANTIC_VERSION,
# )

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
customer_df = spark.read.table("ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b")
material_df=spark.read.table("ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b")
customer_material_pair_df=spark.read.table("ri.foundry.main.dataset.f17c2ffd-f444-4691-a43c-ca87b008b644")

def compute(customer_df, material_df, customer_material_pair_df):
    """Converted from Palantir Transform
    
    Inputs:
      - customer_df: ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b
      - material_df: ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b
      - customer_material_pair_df: ri.foundry.main.dataset.f17c2ffd-f444-4691-a43c-ca87b008b644
    Output: ri.foundry.main.dataset.83b52f9d-3f93-4264-bb68-bcbbb771cfeb
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - null (WARN)
      - null (FAIL)
    """
    
    
    # Transform logic
    customer_df_meta = schema_with_statistics(customer_df).withColumn('dataset_name', F.lit('customer'))
    material_df_meta = schema_with_statistics(material_df).withColumn('dataset_name', F.lit('material'))
    df = customer_df_meta.unionByName(material_df_meta)
    return df

result = compute(customer_df, material_df, customer_material_pair_df)

# Write result to Delta table

result.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.b1405cce-3ce8-47d4-a5d0-4b02661ff794")