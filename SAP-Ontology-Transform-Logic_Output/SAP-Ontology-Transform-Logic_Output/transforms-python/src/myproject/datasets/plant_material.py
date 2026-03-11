# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.687272100Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from myproject.utils import remove_null_cols, get_cols_sorted_by_null_count

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
source_df = spark.read.table("ri.foundry.main.dataset.02f73f93-36e2-49ae-9071-592ebb139573")
    
def compute(source_df):
    """Converted from Palantir Transform
    
    Inputs:
      - source_df: ri.foundry.main.dataset.02f73f93-36e2-49ae-9071-592ebb139573
    Output: ri.foundry.main.dataset.965b7a77-29fe-4071-97aa-582f2d183df8
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - null (WARN)
      - null (FAIL)
    """


    # Transform logic

    source_df = remove_null_cols(source_df)
    source_df = (
    source_df
    .select(
    *get_cols_sorted_by_null_count(source_df)
    ).withColumn(
    "spd_flag",
    F.when(
    F.col(
    'mandt_werks_|_foreign_key_T001W'
    ).isin([
    'QE9_test_|_100_|_P080',
    'QE9_test_|_100_|_P090',
    'QE9_test_|_100_|_P101',
    'QE9_test_|_100_|_P102',
    'QE9_test_|_100_|_P104',
    'QE9_test_|_100_|_P105']),
    True
    ).otherwise(False)
    )
    )

    return source_df

result = compute(source_df)

# Run data quality checks
result = validate_transform(result)

# Write result to Delta table
result.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.965b7a77-29fe-4071-97aa-582f2d183df8")


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