# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.721176300Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from myproject.utils import enrich_with_billing_document_metrics, add_time_series_metric_id_cols


_OUTPUT_NAME = 'vendor'

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
lfa1 = spark.read.table("ri.foundry.main.dataset.fc385255-6db6-4567-9052-5a8dea6881d7")
billing_document = spark.read.table("ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72")

def compute(lfa1, billing_document):
    """Converted from Palantir Transform
    
    Inputs:
      - lfa1: ri.foundry.main.dataset.fc385255-6db6-4567-9052-5a8dea6881d7
      - billing_document: ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72
    Output: ri.foundry.main.dataset.0c6ae379-81f7-4385-b251-aa56bf65e126
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - null (WARN)
      - null (FAIL)
    """


    # Transform logic

    vendor = (
    lfa1
    .select(
    F.col("primary_key"),
    F.col("vendor_|_lifnr"),
    F.col("name_|_name1"),
    F.col("name_2_|_name2"),
    F.col("name_3_|_name3"),
    F.col("name_4_|_name4"),
    F.col("search_term_|_sortl"),
    F.col("repackager_|_yyrepackager"),
    F.col("exclusive_distributor_|_yyexcldist"),
    F.col("wholesale_government_distributor_|_yywholgov"),
    F.col("manufacturer_|_yymanuf"),
    F.col("tax_jurisdiction_|_txjcd"),
    F.col("customer_|_kunnr"),
    F.col("account_group_|_ktokk"),
    F.col("vendor_|_lifnr").cast("int").alias("vendor_nbr")
    )
    .withColumn(
    "title",
    F.concat("name_|_name1", F.lit(" ("), "vendor_nbr", F.lit(")"))
    )
    )

    # add billing document metrics:
    # note: we join with billing document on only manufacturer here (not yet on the supplier)
    vendor = enrich_with_billing_document_metrics(
    vendor,
    # billing_document.dataframe().repartition('mandt_mfrnr_|_foreign_key_LFA1'),
    billing_document,
    grouping_cols=[F.col('mandt_mfrnr_|_foreign_key_LFA1')],
    grouping_name='manufacturer'
    )

    # adding time series metrics:
    vendor = add_time_series_metric_id_cols(
    vendor,
    'primary_key',
    group_type_id=_OUTPUT_NAME,
    filter_ids=['all', 'spd_only', 'pd_only']
    )
    return vendor

result = compute(lfa1, billing_document)

# Run data quality checks
result = validate_transform(result)

# Write result to Delta table
result.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.0c6ae379-81f7-4385-b251-aa56bf65e126")


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