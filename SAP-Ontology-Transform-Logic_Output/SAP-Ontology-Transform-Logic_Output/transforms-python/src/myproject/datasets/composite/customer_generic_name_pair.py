# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-27T10:50:36.055726300Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


from pyspark.sql import functions as F
# from transforms.api import incremental
from myproject.utils import (
    remove_null_cols,
    get_cols_sorted_by_null_count,
    cast_decimal_cols_to_double,
    enrich_with_billing_document_metrics,
    enrich_with_billing_document_features,
    add_time_series_metric_id_cols
)


_OUTPUT_NAME = 'customer_generic_name_pair'
_PK_COLS = ['primary_key']

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
customer = spark.read.table("ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b")
generic_name=spark.read.table("ri.foundry.main.dataset.6051decb-362e-4c58-9318-bb61c14fd4e2")
billing_document=spark.read.table("ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72")
    
def compute(customer,generic_name,billing_document):
    """Converted from Palantir Transform
    
    Inputs:
      - customer: ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b
      - billing_document: ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72
      - generic_name: ri.foundry.main.dataset.6051decb-362e-4c58-9318-bb61c14fd4e2
    Output: ri.foundry.main.dataset.b1405cce-3ce8-47d4-a5d0-4b02661ff794
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - null (WARN)
      - null (FAIL)
    """
    
    # Transform logic
    customer_generic_name_pair = (
        billing_document.dataframe()
        .select(
            F.col('mandt_kunag_|_foreign_key_KNA1'),
            F.col('mandt_kunrg_|_foreign_key_KNA1'),
            F.col('mandt_kunnr_shipto_|_foreign_key_KNA1'),
            F.col("generic_name_|_yygenn")
        )
        .withColumn(
            'mandt_kunnr_|_foreign_key_KNA1',
            F.explode(
                F.array_distinct(
                    F.array(
                        F.col('mandt_kunag_|_foreign_key_KNA1'),
                        F.col('mandt_kunrg_|_foreign_key_KNA1'),
                        F.col('mandt_kunnr_shipto_|_foreign_key_KNA1')
                    )
                )
            )
        )
        .select(
            F.col('mandt_kunnr_|_foreign_key_KNA1'),
            F.col("generic_name_|_yygenn")
        )
        .filter(
            (
                (F.col('mandt_kunnr_|_foreign_key_KNA1').isNotNull())
                & (F.length(F.col('mandt_kunnr_|_foreign_key_KNA1')) > 0)
                & (F.col("generic_name_|_yygenn").isNotNull())
                & (F.length(F.col('generic_name_|_yygenn')) > 0)
            )
        )
        .drop_duplicates()
        .withColumn(
            'primary_key',
            F.concat_ws('_||_', F.col("mandt_kunnr_|_foreign_key_KNA1"), F.col("generic_name_|_yygenn")))
        )
        .withColumn(
            '_group_type', F.lit('customer_generic_name_pair')
        )
        .withColumn(
            '_filter_id', F.lit('all')
        )
        .filter(
            F.col('primary_key').isNotNull()
        )
    )

    # get billing document metrics:
    # 1. calculating metrics on the sold-to level:
    customer_generic_name_pair = (
        enrich_with_billing_document_metrics(
            customer_generic_name_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunag_|_foreign_key_KNA1'), F.col('generic_name_|_yygenn')],
            'customer_soldto_generic_name_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # 2. calculating metrics on the bill-to level:
    customer_generic_name_pair = (
        enrich_with_billing_document_metrics(
            customer_generic_name_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunrg_|_foreign_key_KNA1'), F.col('generic_name_|_yygenn')],
            'customer_billto_generic_name_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # 3. calculating metrics on the ship-to level:
    customer_generic_name_pair = (
        enrich_with_billing_document_metrics(
            customer_generic_name_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunnr_shipto_|_foreign_key_KNA1'), F.col('generic_name_|_yygenn')],
            'customer_shipto_generic_name_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # left-join customer cols:
    customer = (
        customer.dataframe()
        .withColumnRenamed('primary_key', 'mandt_kunnr_|_foreign_key_KNA1')
    )
    intersecting_cols = (
        set.intersection(set(customer_generic_name_pair.columns), set(customer.columns))
        - set(['mandt_kunnr_|_foreign_key_KNA1'])
    )
    customer = customer.drop(*intersecting_cols)
    customer_generic_name_pair = (
        customer_generic_name_pair
        .join(customer, on='mandt_kunnr_|_foreign_key_KNA1', how='left')
    )

    # left-join generic_name cols:
    generic_name = (
        generic_name.dataframe()
        .withColumnRenamed('primary_key', "generic_name_|_yygenn")
    )

    intersecting_cols = (
        set.intersection(set(customer_generic_name_pair.columns), set(generic_name.columns))
        - set(['generic_name_|_yygenn'])
    )
    generic_name = generic_name.drop(*intersecting_cols)
    customer_generic_name_pair = (
        customer_generic_name_pair
        .join(generic_name, on='generic_name_|_yygenn', how='left')
    )

    # adding time series metrics:
    customer_generic_name_pair = add_time_series_metric_id_cols(
        customer_generic_name_pair,
        'primary_key',
        group_type_id=_OUTPUT_NAME,
        filter_ids=['all']
    )

    # customer_generic_name_pair = remove_null_cols(customer_generic_name_pair)
    # customer_generic_name_pair = customer_generic_name_pair.select(*get_cols_sorted_by_null_count(customer_generic_name_pair))
    # customer_generic_name_pair = cast_decimal_cols_to_double(customer_generic_name_pair)
    return customer_generic_name_pair
    
customer_generic_name_pair_output = compute(customer,generic_name,billing_document)
# Run data quality checks
result = validate_transform(customer_generic_name_pair_output)

# Write result to Delta table

result.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.b1405cce-3ce8-47d4-a5d0-4b02661ff794")


# =============================================================================
# DATA QUALITY VALIDATION FUNCTIONS
# Converted from Palantir Checks
# =============================================================================

# Original Palantir Checks:
# Check(
#                 expectation=E.primary_key(*_PK_COLS),
#                 name=f'{_OUTPUT_NAME} primary key uniqueness check',
#                 on_error='WARN',
#                 description=f'warns when {_OUTPUT_NAME} has duplicates'
#             )
# Check(
#                 expectation=E.count().gt(0),
#                 name=f'{_OUTPUT_NAME} not empty check',
#                 on_error='FAIL',
#                 description=f'fails when {_OUTPUT_NAME} is empty '
#             )

def validate_transform(df):
    """
    Data quality validation - converted from Palantir Checks
    
    Checks:
      - null: null
      - null: null
    """
    from pyspark.sql import functions as F

    # Check 1: null
    # Check 2: null
    # Row count check
    row_count = df.count()
    if not (row_count > 0):
        message = f"Count check failed: {row_count} does not satisfy 'gt 0'"
        raise ValueError(f"❌ {message}")
    else:
        print(f"✅ null passed: {row_count:,} rows")

    return df  