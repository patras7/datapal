# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.706211Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from myproject.utils import remove_null_cols, get_cols_sorted_by_null_count

_OUTPUT_NAME = 'sales_document_item'

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
sales_order = spark.read.table("ri.foundry.main.dataset.743f1ee3-2655-4683-a619-8554d64355dd")
material = spark.read.table("ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b")
customer= spark.read.table("ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b"),
customer_account= spark.read.table("ri.foundry.main.dataset.1559cda9-175c-4853-afe8-6b087f46f39e")
    
def compute(sales_order, material, customer, customer_account):
    """Converted from Palantir Transform
    
    Inputs:
      - sales_order: ri.foundry.main.dataset.743f1ee3-2655-4683-a619-8554d64355dd
      - material: ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b
    Output: ri.foundry.main.dataset.fe51af4c-3c10-492d-950b-249b35abd3b8
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - null (WARN)
      - null (FAIL)
    """

    # Transform logic

    sales_order = sales_order.drop('title')

    material = material.select(
    'primary_key',
    'fdb_ndc_|_yyndcfdb',
    'ndc',
    F.col('gcn_seq_no_|_yygcnsqn').alias('gsn_|_yygcnsqn'),
    'gcn_|_yycgcn',
    'cah_packaging_key_|_yypkgkeyc',
    'pack_size_|_yypksze',
    'trade_name_|_yytradname',
    'generic_name_|_yygenn',
    'sizedimensions_|_groes',
    F.col('yypkqtym').alias('pack_qty_|_yypkqtym'),
    'strength_|_yystrn',
    'form_|_yyform'
    ).withColumn(
    'cardinal_key',
    F.concat('gsn_|_yygcnsqn', 'cah_packaging_key_|_yypkgkeyc')
    )

    customer = customer.select(
    'primary_key',
    F.col('name_|_name1').alias('customer_name_|_name1'),
    F.col('customer_|_kunnr').alias('sap_customer_number_|_kunnr'),
    F.col('mandt_kunn2_|_name_|_name1_|_parvw__A1').alias('affiliation_1_name'),
    F.col('mandt_kunn2_|_foreign_key_KNA1_|_parvw__A1').alias('customer_fk_affiliation_1'),
    F.col('mandt_kunn2_|_name_|_name1_|_parvw__A2').alias('affiliation_2_name'),
    F.col('mandt_kunn2_|_foreign_key_KNA1_|_parvw__A2').alias('customer_fk_affiliation_2'),
    F.col('mandt_kunn2_|_name_|_name1_|_parvw__A3').alias('affiliation_3_name'),
    F.col('mandt_kunn2_|_foreign_key_KNA1_|_parvw__A3').alias('customer_fk_affiliation_3'),
    F.col('mandt_kunn2_|_name_|_name1_|_parvw__PG').alias('affiliation_pg_name'),
    F.col('mandt_kunn2_|_foreign_key_KNA1_|_parvw__PG').alias('customer_fk_affiliation_pg'),
    F.col('description_|_vtext').alias('class_of_trade_|_vtext'),
    'type_of_business_|_j_1kftbus'
    )

    customer_account = customer_account.select(
    'primary_key',
    'kvgr1_description_|_bezei'
    )

    df = sales_order.join(
    material,
    on=material['primary_key'] == sales_order['mandt_matnr_|_foreign_key_MARA'],
    how='left'
    ).drop(
    material['primary_key']
    ).join(
    customer,
    on=customer['primary_key'] == sales_order['mandt_kunnr_|_foreign_key_KNA1'],
    how='left'
    ).drop(
    customer['primary_key']
    ).join(
    customer_account,
    on=sales_order['mandt_kunnr_vkorg_vtweg_spart_|_foreign_key_KNVV'] == customer_account['primary_key'],
    how='left'
    ).drop(
    customer_account['primary_key']
    ).withColumn(
    'cin',
    F.col("material_|_matnr").cast('int').cast('string')
    )

    final_df = df.select(
    "primary_key",
    F.when(
    F.col("customer_name_|_name1").isNotNull(),
    F.concat(F.lit('Sales Document '),
    "sales_document_|_vbeln",
    F.lit(", Item "),
    "sales_document_item_|_posnr",
    F.lit(" (CIN: "),
    'cin',
    F.lit(") for "),
    "customer_name_|_name1",
    F.lit (" on "),
    F.col("document_date_|_audat").cast('string')
    )
    ).otherwise(
    F.concat(F.lit('Sales Document '),
    "sales_document_|_vbeln",
    F.lit(", Item "),
    "sales_document_item_|_posnr",
    F.lit(" (CIN: "),
    'cin',
    F.lit (") on "),
    F.col("document_date_|_audat").cast('string')
    )
    ).alias('title'),
    "document_date_|_audat",
    "delivery_date_|_edatu",
    "sales_document_|_vbeln",
    "sales_document_item_|_posnr",
    "order_quantity_|_kwmeng",
    "order_quantity_base_unit_of_measure",
    "net_price_|_netpr",
    "subtotal_1_|_kzwi1",
    "subtotal_2_|_kzwi2",
    "subtotal_3_|_kzwi3",
    "subtotal_4_|_kzwi4",
    "subtotal_5_|_kzwi5",
    "subtotal_6_|_kzwi6",
    "manual_price_|_mprok",
    "tax_amount_|_mwsbp",
    "document_currency_|_waerk",
    "net_weight_|_ntgew",
    "conversion_factor_|_umziz",
    "delivery_priority_|_lprio",
    "serial_no_profile_|_serail",
    "reason_for_rejection_|_abgru",
    "is_open",
    "schedule_line_number_|_etenr",
    "division_|_spart",
    "business_area_|_gsber",
    # Product Info
    "material_description_|_maktx_|_customization_field_MAKT_|_MANDT_MATNR_SPRAS",
    "description_|_arktx",
    "cin",
    "material_|_matnr",
    "ndc",
    "fdb_ndc_|_yyndcfdb",
    "trade_name_|_yytradname",
    "generic_name_|_yygenn",
    "strength_|_yystrn",
    "form_|_yyform",
    "gsn_|_yygcnsqn",
    "gcn_|_yycgcn",
    "cah_packaging_key_|_yypkgkeyc",
    "cardinal_key",
    F.col("pack_size_|_yypksze").cast('double'),
    F.col("pack_qty_|_yypkqtym").cast('double'),
    "sizedimensions_|_groes",
    "sales_unit_|_vrkme",
    "base_unit_of_measure_|_meins",
    "material_group_|_matkl",
    "item_category_|_pstyv",
    # Customer Info
    "customer_name_|_name1",
    "sap_customer_number_|_kunnr",
    "affiliation_1_name",
    "affiliation_2_name",
    "affiliation_3_name",
    "affiliation_pg_name",
    "class_of_trade_|_vtext",
    "type_of_business_|_j_1kftbus",
    "kvgr1_description_|_bezei",
    # Foreign Keys
    "mandt_upmat_|_foreign_key_MARA",  # Material/Product
    "mandt_matwa_|_foreign_key_MARA",  # Material/Product
    "mandt_matnr_|_foreign_key_MARA",  # Material/Product
    "mandt_pmatn_|_foreign_key_MARA",  # Material/Product
    "mandt_kunnr_|_foreign_key_KNA1",  # Customer Master
    "mandt_knkli_|_foreign_key_KNA1",  # Customer Master
    "mandt_irm_gpo_cd_|_foreign_key_KNA1",  # Customer Master
    "customer_fk_affiliation_1",  # KNA1/Customer Master
    "customer_fk_affiliation_2",  # KNA1/Customer Master
    "customer_fk_affiliation_3",  # KNA1/Customer Master
    "customer_fk_affiliation_pg",  # KNA1/Customer Master
    "mandt_kunnr_vkorg_vtweg_spart_|_foreign_key_KNVV",  # Customer Account
    "mandt_vbeln_|_foreign_key_VBRK",  # Billing Document Header
    "mandt_vbeln_posnr_|_foreign_key_VBRP",  # Billing Document Item
    "mandt_vbeln_|_foreign_key_VBAK",  # Sales Document Header
    "mandt_vbeln_posnr_|_foreign_key_VBAP",  # Sales Document Item
    "mandt_irm_mfrnr_|_foreign_key_LFA1",  # Vendor
    "mandt_spras_kvgr1_|_foreign_key_TVV1T",  # Level 3 Class of Trade
    "mandt_matnr_spras_|_foreign_key_MAKT",  # Material Descriptions
    "mandt_knuma_ag_|_foreign_key_KONA",  # Agreements
    "mandt_spras_fkara_|_foreign_key_TVFKT",  # Billing Document Types Text
    "mandt_vbeln_|_foreign_key_LIKP",  # SD Document Delivery Header Data
    "mandt_vbeln_posnr_|_foreign_key_LIPS",  # SD Document Delivery Item Data
    "mandt_matnr_werks_|_foreign_key_MARC",  # Plant Data for Material
    "mandt_werks_|_foreign_key_T001W",  # Plants Branches
    "mandt_vpwrk_|_foreign_key_T001W",  # Plants Branches
    "mandt_bukrs_vf_|_foreign_key_T001",  # Company Codes
    "mandt_pctyp_spras_|_foreign_key_/IRM/TPCTYPT",  # PC Contact Type
    ).sort(F.col('document_date_|_audat').desc())
    return final_df

result = compute(sales_order, material, customer, customer_account)
    
# Run data quality checks
result = validate_transform(result)

# Write result to Delta table
result.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.fe51af4c-3c10-492d-950b-249b35abd3b8")


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
