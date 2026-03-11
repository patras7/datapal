# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.696238200Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from myproject.utils import remove_null_cols, get_cols_sorted_by_null_count

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
raw_orders = spark.read.table("ri.foundry.main.dataset.c9e4cf3b-7422-481e-b0fd-39c76bb1b8b8")
    
def compute(raw_orders):
    """Converted from Palantir Transform
    
    Inputs:
      - raw_orders: ri.foundry.main.dataset.c9e4cf3b-7422-481e-b0fd-39c76bb1b8b8
    Output: ri.foundry.main.dataset.068dd3b5-6835-4595-ae74-e2fc0b833d8a
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - null (WARN)
      - null (FAIL)
    """

    # Transform logic

    output = remove_null_cols(raw_orders)
    output = output.select(
    *get_cols_sorted_by_null_count(output)
    )
    return output

result = compute(raw_orders)

# Run data quality checks
result = validate_transform(result)

# Write result to Delta table
result.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.068dd3b5-6835-4595-ae74-e2fc0b833d8a")
    


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
