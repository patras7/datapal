from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, Check, configure
# from transforms.api import incremental
from transforms import expectations as E
from myproject.utils import (
    schema_with_statistics
)

_SPARK_OPTS = [
    "DRIVER_MEMORY_LARGE",
    "NUM_EXECUTORS_16",
    "EXECUTOR_MEMORY_LARGE"
]
# _SEMANTIC_VERSION = 1
# _REQUIRE_INCREMENTAL = True


@configure(profile=_SPARK_OPTS)
# @incremental(
#     require_incremental=_REQUIRE_INCREMENTAL,
#     semantic_version=_SEMANTIC_VERSION,
# )
@transform_df(
    Output("ri.foundry.main.dataset.83b52f9d-3f93-4264-bb68-bcbbb771cfeb"),
    customer_df=Input("ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b"),
    material_df=Input("ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b"),
    customer_material_pair_df=Input("ri.foundry.main.dataset.f17c2ffd-f444-4691-a43c-ca87b008b644")
)
def compute(customer_df, material_df, customer_material_pair_df):
    customer_df_meta = schema_with_statistics(customer_df).withColumn('dataset_name', F.lit('customer'))
    material_df_meta = schema_with_statistics(material_df).withColumn('dataset_name', F.lit('material'))
    df = customer_df_meta.unionByName(material_df_meta)
    return df