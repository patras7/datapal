# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.259816600Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import Window
from myproject.utils import (
    cast_decimal_cols_to_double,
    enrich_with_billing_document_metrics,
    enrich_with_billing_document_features,
    add_time_series_metric_id_cols,
    drop_cols_where_all_values_are_the_same
)

_OUTPUT_NAME = 'material'

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
material = spark.read.table("ri.foundry.main.dataset.f66a2392-be40-483b-977a-e048b660413a")
mara = spark.read.table("ri.foundry.main.dataset.34e811fd-c8f6-44fd-b4b9-8b2b9bb069e4")
billing_document = spark.read.table("ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72")
    
def get_material_enriched_df(material, mara, billing_document):
    """Converted from Palantir Transform
    
    Inputs:
      - material: ri.foundry.main.dataset.f66a2392-be40-483b-977a-e048b660413a
      - mara: ri.foundry.main.dataset.34e811fd-c8f6-44fd-b4b9-8b2b9bb069e4
      - billing_document: ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72
    Output: ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - null (WARN)
      - null (FAIL)
    """

    # Transform logic

    # enrich with missing mara columns:
    intersecting_cols = set.intersection(set(material.columns), set(mara.columns)) - set(['primary_key'])
    mara = mara.drop(*intersecting_cols)
    material = (
    material
    .join(mara, on=['primary_key'], how='left')
    )

    # clean up cols:
    material = drop_cols_where_all_values_are_the_same(material)
    material = cast_decimal_cols_to_double(material)
    material = add_corrected_pack_qty(material)

    # trim leading 0s:
    numeric_id_map = {
    "material_|_matnr": "cin",
    "fdb_ndc_|_yyndcfdb": "ndc",
    "supplier_|_yysupnr": "supplier_number",
    "manufacturer_|_mfrnr": "manufacturer_number"
    }
    material = clean_numeric_id_strings(material, numeric_id_map)

    # add cardinal key
    material = material.withColumn(
    'cardinal_key',
    F.concat('gcn_seq_no_|_yygcnsqn', 'cah_packaging_key_|_yypkgkeyc')
    )

    # enrich with billing doc features (spd and source flags):
    material = enrich_with_billing_document_features(
    material,
    billing_document,
    [F.col('mandt_matnr_|_foreign_key_MARA')]
    )

    material = (
    material
    .withColumn(
    'spd_labels_count', F.size('spd_labels')
    )
    .withColumn(
    'source_labels_count', F.size('source_labels')
    )
    )

    # enrich with billing doc metrics:
    material = enrich_with_billing_document_metrics(
    material,
    billing_document,
    [F.col('mandt_matnr_|_foreign_key_MARA')],
    'material'
    )

    # adding time series metrics:
    material = add_time_series_metric_id_cols(
    material,
    'primary_key',
    group_type_id=_OUTPUT_NAME,
    filter_ids=['all']
    )

    # add is_exclusive flag:
    exclusives_df = (
    spark_session.createDataFrame(
    [
    ('RETACRIT', 'PFIZER'),
    ('SIRTURO', 'JOHNSON AND JONHSON'),
    ('VISTOGARD', 'WELLSTAT THERAPEUTICS'),
    ('LIVTENCITY', 'TAKEDA'),
    ('ONCASPAR', 'SERVIER'),
    ('TAVNEOS', 'CHEMOCENTRYX'),
    ('JELMYTO', 'UROGEN'),
    ('KORSUVA', 'VIFOR'),
    ('ARCALYST', 'KINIKSA'),
    ('ASPARLAS', 'SERVIER'),
    ('MIRCERA', 'VIFOR')
    ],
    ["exclusive_trade_name", "exclusive_manufacturer"]
    )
    )

    material = (
    material.alias("material")
    .join(
    exclusives_df.alias("exclusives"),
    F.upper(material["trade_name_|_yytradname"]).contains(F.upper(exclusives_df["exclusive_trade_name"])),
    how='left'
    )
    )
    material = (
    material
    .withColumn(
    "is_exclusive",
    F.when(
    F.isnull(F.col("exclusive_trade_name")), False
    ).otherwise(True)
    )
    .drop("exclusive_trade_name", "exclusive_manufacturer")
    )

    return material

result = get_material_enriched_df(material, mara, billing_document)

# Run data quality checks
result = validate_transform(result)

# Write result to Delta table
result.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b")


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
#                 description=f'fails when {_OUTPUT_NAME} is empty'
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


def clean_numeric_id_strings(df, from_to_col_names: dict):
    for orig_col, new_col in from_to_col_names.items():
        df = (
            df
            .withColumn(        # Remove leading 0s from string
                new_col,
                F.trim(F.regexp_replace(orig_col, r'^[0]*', ''))
            )
            .withColumn(        # Replace empty string with null
                new_col,
                F.when(
                    F.col(new_col) == '',
                    F.lit(None).cast("string")
                ).otherwise(F.col(new_col))
            )
        )

    return df


def add_corrected_pack_qty(material):
    return material.withColumn(
        'corrected_pack_qty',
        correct_pack_qty(
            F.col('sizedimensions_|_groes'),
            F.col('yypkqtym'),
            F.col('pack_size_|_yypksze'),
            F.col('form_|_yyform'),
            F.col('material_group_|_matkl')
        ).cast('double')
    )


@udf
def correct_pack_qty(sizedimensions, pack_qty, pack_size, form, material_group):
    if pack_size == 0 or material_group == 'ZSERVICE':
        return 1

    pack_qty = pack_qty if pack_qty and pack_qty > 0 else 1
    pack_size = pack_size if pack_size and pack_size > 0 else 1

    if sizedimensions:
        if "EA" in sizedimensions:
            return get_pack_size_for_solids(sizedimensions, pack_size, "EA")

        if any(unit in sizedimensions for unit in ["ML", "OZ", "GM"]):
            return get_pack_qty_for_non_solids(sizedimensions, pack_size, pack_qty)

        if "PC" in sizedimensions:
            return get_pack_size_for_solids(sizedimensions, pack_size, "PC")

        if form and (form == 'TB' or form == 'CP'):
            return get_pack_size_for_solids(sizedimensions, pack_size)

        if "." in sizedimensions:
            return get_pack_qty_for_non_solids(sizedimensions, pack_size, pack_qty)

    return pack_size


def get_pack_size_for_solids(sizedimensions, pack_size, unit=None):
    if "X" in sizedimensions:
        pattern = r'(\d+)X(\d+)(?:X(\d+))?'  # match digits (group 1), X, digits (group 2), optional X + digits (group 3)
        match = re.search(pattern, sizedimensions)
        if match:
            num1 = int(match.group(1))
            num2 = int(match.group(2))
            num3 = match.group(3)
            if num3:
                return num1 * num2 * int(num3)  # ex. {sizedimension: 4X6X8 EA} => return 192; {sizedimension: 4X6X8, form: TB} => return 192
            else:
                return num1 * num2  # ex. {sizedimension: 10X10 EA} => return 100; {sizedimension: 4X6, form: TB} => return 24
    if "+" in sizedimensions:
        return pack_size  # ex. {sizedimension: 100+10 EA, pack_size: 110} => return 110
    if unit and unit in sizedimensions:
        if ' ' in sizedimensions:
            return sizedimensions.split(' ')[0] # ex. 60 EA => return 60
        else:
            return sizedimensions.split(unit)[0]  # ex. 60EA => return 60
    return pack_size  # ex. {sizedimension: '   100', pack_size: 100, pack_qty: null} => return 100


def get_pack_qty_for_non_solids(sizedimensions, pack_size, pack_qty):
    if "X" in sizedimensions:
        pattern = r'((\d+)X){2}(\d+(\.\d+)?)'  # match (digits, X) twice, then digits, (optional decimcal + digits)
        double_multiplication = re.search(pattern, sizedimensions)
        if double_multiplication:
            split = sizedimensions.split('X')
            return int(split[0]) * int(split[1])  # ex. {sizedimension: 6X3X15ML, pack_size: 15} => return 18
        else:
            return sizedimensions.split('X')[0]  # ex. {sizedimension: 6X1000ML, pack_size: 1000} => return 6
    else:
        return pack_qty  # ex. {sizedimension: 473 ML, pack_size: 473, pack_qty: 1} => return 1