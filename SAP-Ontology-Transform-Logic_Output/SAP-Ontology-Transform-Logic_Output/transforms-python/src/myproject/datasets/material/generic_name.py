# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.249843100Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from myproject.utils import remove_null_cols, get_cols_sorted_by_null_count
from myproject.utils import (
    enrich_with_billing_document_metrics, enrich_with_billing_document_features, add_time_series_metric_id_cols
)
from myproject.utils import cast_decimal_cols_to_double


_OUTPUT_NAME = 'generic_name'

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
material = spark.read.table("ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b")
billing_document = spark.read.table("ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72")
    
def compute(material, billing_document):
    """Converted from Palantir Transform
    
    Inputs:
      - material: ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b
      - billing_document: ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72
    Output: ri.foundry.main.dataset.6051decb-362e-4c58-9318-bb61c14fd4e2
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - null (WARN)
      - null (FAIL)
    """

    # Transform logic

    generic_name = (
    material
    .filter(
    ~(
    F.col("generic_name_|_yygenn").isNull()
    | (F.col("generic_name_|_yygenn") == "")
    )
    )
    .groupby(
    "generic_name_|_yygenn"
    )
    .agg(
    F.first("client_|_mandt").alias("client_|_mandt"),
    F.collect_set('primary_key').alias('mandt_matnr_|_foreign_key_MARA_|_list'),
    F.collect_set('trade_name_|_yytradname').alias('trade_name_|_yytradname_|_list'),
    F.collect_set("fdb_ndc_|_yyndcfdb").alias("fdb_ndc_|_yyndcfdb_|_list"),
    F.collect_set("gcn_|_yycgcn").alias("gcn_|_yycgcn_|_list"),
    F.collect_set("mandt_mfrnr_|_foreign_key_LFA1").alias("mandt_mfrnr_|_foreign_key_LFA1_|_list"),
    F.collect_set("is_exclusive").alias("exclusive_labels"),
    F.collect_list("spd_labels").alias("spd_labels"),
    F.collect_list("source_labels").alias("source_labels"),
    )
    .withColumn(
    'mandt_matnr_|_foreign_key_MARA_|_count', F.size('mandt_matnr_|_foreign_key_MARA_|_list')
    )
    .withColumn(
    'trade_name_|_yytradname_|_count', F.size('trade_name_|_yytradname_|_list')
    )
    .withColumn(
    'fdb_ndc_|_yyndcfdb_|_count', F.size('fdb_ndc_|_yyndcfdb_|_list')
    )
    .withColumn(
    "mandt_mfrnr_|_foreign_key_LFA1_|_count", F.size("mandt_mfrnr_|_foreign_key_LFA1_|_list")
    )
    .withColumn(
    'spd_labels', F.array_distinct(F.flatten('spd_labels'))
    )
    .withColumn(
    'source_labels', F.array_distinct(F.flatten('source_labels'))
    )
    .withColumn(
    'exclusive_labels_count', F.size('exclusive_labels')
    )
    .withColumn(
    'spd_labels_count', F.size('spd_labels')
    )
    .withColumn(
    'source_labels_count', F.size('source_labels')
    )
    .withColumnRenamed("generic_name_|_yygenn", "primary_key")
    )

    generic_name = enrich_with_billing_document_metrics(
    generic_name,
    billing_document,
    [F.col("generic_name_|_yygenn")],
    _OUTPUT_NAME
    )

    # adding time series metrics:
    generic_name = add_time_series_metric_id_cols(
    generic_name, 
    'primary_key',
    group_type_id=_OUTPUT_NAME,
    filter_ids=['all', 'spd_only', 'pd_only']
    )

    # generic_name = remove_null_cols(generic_name)
    # generic_name = generic_name.select(*get_cols_sorted_by_null_count(generic_name))
    generic_name = cast_decimal_cols_to_double(generic_name)
    return generic_name

result = compute(material, billing_document)

# Run data quality checks
result = validate_transform(result)

# Write result to Delta table
result.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.6051decb-362e-4c58-9318-bb61c14fd4e2")


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
