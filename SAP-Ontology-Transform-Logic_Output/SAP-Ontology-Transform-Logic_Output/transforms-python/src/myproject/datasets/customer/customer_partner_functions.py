# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.034419200Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

_PK_COLS = [
    # 'mandt_kunnr_vkorg_vtweg_spart_|_foreign_key_KNVV',
    'mandt_kunnr_|_foreign_key_KNA1',
    'mandt_kunn2_|_foreign_key_KNA1',
    'partner_function_|_parvw',
]

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
knvp = spark.read.table("ri.foundry.main.dataset.d25c665a-eefa-4ee9-8d9f-1efb3158ee76")
    
def compute(knvp):
    """Converted from Palantir Transform
    
    Inputs:
      - knvp: ri.foundry.main.dataset.d25c665a-eefa-4ee9-8d9f-1efb3158ee76
    Output: ri.foundry.main.dataset.c961e7a0-5e7c-4668-a556-8f8befc21b8d
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - customer_a1_partner_func primary key uniqueness check (WARN)
      - customer_a1_partner_func not empty check (FAIL)
    """

    # Transform logic

    knvp = (
    knvp
    .select(
    *_PK_COLS
    )
    )
    
    a1 = knvp
    .filter(F.col('partner_function_|_parvw') == F.lit('A1'))
    .drop_duplicates()
    a1 = validate_transform(a1)
    a1.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.c961e7a0-5e7c-4668-a556-8f8befc21b8d")
    
    a2 = knvp
    .filter(F.col('partner_function_|_parvw') == F.lit('A2'))
    .drop_duplicates()
    a2 = validate_transform(a2)
    a2.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.f9fc0c0d-a98f-4cee-b5af-5b9c29110118")
    
    a3 = knvp
    .filter(F.col('partner_function_|_parvw') == F.lit('A3'))
    .drop_duplicates()
    a3 = validate_transform(a3)
    a3.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.ac2cd031-f630-4a51-b9ea-4baecc4278a1")

    al = knvp
    .filter(F.col('partner_function_|_parvw') == F.lit('AL'))
    .drop_duplicates()
    al = validate_transform(al)
    al.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.0bd653f4-fab4-447e-9b4d-d39ddf8d1f6b")

result = compute(knvp)
# =============================================================================
# DATA QUALITY VALIDATION FUNCTIONS
# Converted from Palantir Checks
# =============================================================================

# Original Palantir Checks:
# Check(
#                 expectation=E.primary_key(*_PK_COLS),
#                 name='customer_a1_partner_func primary key uniqueness check',
#                 on_error='WARN',
#                 description='warns when customer_a1_partner_func has duplicates'
#             )
# Check(
#                 expectation=E.count().gt(0),
#                 name='customer_a1_partner_func not empty check',
#                 on_error='FAIL',
#                 description='fails when customer_a1_partner_func is empty '
#             )

def validate_transform(df):
    """
    Data quality validation - converted from Palantir Checks
    
    Checks:
      - customer_a1_partner_func primary key uniqueness check: warns when customer_a1_partner_func has duplicates
      - customer_a1_partner_func not empty check: fails when customer_a1_partner_func is empty 
    """
    from pyspark.sql import functions as F

    # Check 1: customer_a1_partner_func primary key uniqueness check
    # Check 2: customer_a1_partner_func not empty check
    # Row count check
    row_count = df.count()
    if not (row_count > 0):
        message = f"Count check failed: {row_count} does not satisfy 'gt 0'"
        raise ValueError(f"❌ {message}")
    else:
        print(f"✅ customer_a1_partner_func not empty check passed: {row_count:,} rows")

    return df
