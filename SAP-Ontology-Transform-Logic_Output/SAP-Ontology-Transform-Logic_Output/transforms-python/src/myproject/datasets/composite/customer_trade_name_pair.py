# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-27T10:50:36.760447600Z (UTC)
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


_OUTPUT_NAME = 'customer_trade_name_pair'
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
trade_name=spark.read.table("ri.foundry.main.dataset.a924b066-b68a-434b-a539-5494a4f72f18")
billing_document=spark.read.table("ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72")
    
def compute(customer,trade_name,billing_document):
    """Converted from Palantir Transform
    
    Inputs:
      - customer: ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b
      - billing_document: ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72
      - trade_name: ri.foundry.main.dataset.a924b066-b68a-434b-a539-5494a4f72f18
    Output: ri.foundry.main.dataset.2e63a301-4736-403d-a3ae-d001b01d5052
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - null (WARN)
      - null (FAIL)
    """
    
    # Transform logic
    customer_trade_name_pair = (
        billing_document.dataframe()
        .select(
            F.col('mandt_kunag_|_foreign_key_KNA1'),
            F.col('mandt_kunrg_|_foreign_key_KNA1'),
            F.col('mandt_kunnr_shipto_|_foreign_key_KNA1'),
            F.col("trade_name_|_yytradname")
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
            F.col("trade_name_|_yytradname")
        )
        .filter(
            (
                (F.col('mandt_kunnr_|_foreign_key_KNA1').isNotNull())
                & (F.length(F.col('mandt_kunnr_|_foreign_key_KNA1')) > 0)
                & (F.col("trade_name_|_yytradname").isNotNull())
                & (F.length(F.col('trade_name_|_yytradname')) > 0)
            )
        )
        .drop_duplicates()
        .withColumn(
            'primary_key',
            F.concat_ws('_||_', F.col("mandt_kunnr_|_foreign_key_KNA1"), F.col("trade_name_|_yytradname")))
        )
        .withColumn(
            '_group_type', F.lit('customer_trade_name_pair')
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
    customer_trade_name_pair = (
        enrich_with_billing_document_metrics(
            customer_trade_name_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunag_|_foreign_key_KNA1'), F.col('trade_name_|_yytradname')],
            'customer_soldto_trade_name_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # 2. calculating metrics on the bill-to level:
    customer_trade_name_pair = (
        enrich_with_billing_document_metrics(
            customer_trade_name_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunrg_|_foreign_key_KNA1'), F.col('trade_name_|_yytradname')],
            'customer_billto_trade_name_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # 3. calculating metrics on the ship-to level:
    customer_trade_name_pair = (
        enrich_with_billing_document_metrics(
            customer_trade_name_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunnr_shipto_|_foreign_key_KNA1'), F.col('trade_name_|_yytradname')],
            'customer_shipto_trade_name_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # left-join customer cols:
    customer = (
        customer.dataframe()
        .withColumnRenamed('primary_key', 'mandt_kunnr_|_foreign_key_KNA1')
    )
    intersecting_cols = (
        set.intersection(set(customer_trade_name_pair.columns), set(customer.columns))
        - set(['mandt_kunnr_|_foreign_key_KNA1'])
    )
    customer = customer.drop(*intersecting_cols)
    customer_trade_name_pair = (
        customer_trade_name_pair
        .join(customer, on='mandt_kunnr_|_foreign_key_KNA1', how='left')
    )

    # left-join trade_name cols:
    trade_name = (
        trade_name.dataframe()
        .withColumnRenamed('primary_key', "trade_name_|_yytradname")
    )

    intersecting_cols = (
        set.intersection(set(customer_trade_name_pair.columns), set(trade_name.columns))
        - set(['trade_name_|_yytradname'])
    )
    trade_name = trade_name.drop(*intersecting_cols)
    customer_trade_name_pair = (
        customer_trade_name_pair
        .join(trade_name, on='trade_name_|_yytradname', how='left')
    )

    # adding time series metrics:
    customer_trade_name_pair = add_time_series_metric_id_cols(
        customer_trade_name_pair, 
        'primary_key',
        group_type_id=_OUTPUT_NAME, 
        filter_ids=['all']
    )

    # customer_trade_name_pair = remove_null_cols(customer_trade_name_pair)
    # customer_trade_name_pair = customer_trade_name_pair.select(*get_cols_sorted_by_null_count(customer_trade_name_pair))
    # customer_trade_name_pair = cast_decimal_cols_to_double(customer_trade_name_pair)
    
    return customer_trade_name_pair
    
customer_trade_name_pair_output = compute(customer,trade_name,billing_document)
# Run data quality checks
result = validate_transform(customer_trade_name_pair_output)

# Write result to Delta table

result.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.2e63a301-4736-403d-a3ae-d001b01d5052")


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