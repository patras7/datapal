# =============================================================
# Converted from Palantir Transforms â†’ Databricks PySpark
# Generated on 2026-02-23T11:54:03.665615987Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import datetime, timedelta
 
# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
billing_document = spark.read.table("ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72")
customer = spark.read.table("ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b")
customer_account = spark.read.table("ri.foundry.main.dataset.1559cda9-175c-4853-afe8-6b087f46f39e")
vendor = spark.read.table("ri.foundry.main.dataset.0c6ae379-81f7-4385-b251-aa56bf65e126")
warehouse = spark.read.table("ri.foundry.main.dataset.f639e8e1-f6f3-4e4a-9cb1-998692be2e9e")
material = spark.read.table("ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b")
def compute(billing_document,customer,customer_account,vendor,warehouse,material):
    """Converted from Palantir Transform
    
    Inputs:
      - billing_document: ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72
      - customer: ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b
      - customer_account: ri.foundry.main.dataset.1559cda9-175c-4853-afe8-6b087f46f39e
      - vendor: ri.foundry.main.dataset.0c6ae379-81f7-4385-b251-aa56bf65e126
      - warehouse: ri.foundry.main.dataset.f639e8e1-f6f3-4e4a-9cb1-998692be2e9e
      - material: ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b
    Output: ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - null (WARN)
      - null (FAIL)
    """

    # Transform logic

    today = datetime.date.today()
    rolling_25_months = today - relativedelta(months=25)
    billing_document = billing_document.filter(billing_document["billing_date_|_fkdat"] >= rolling_25_months)
    billing_document = enrich_with_ship_to_customer_attributes(billing_document, customer)
    billing_document = enrich_with_sold_to_customer_account_attributes(billing_document, customer_account)
    billing_document = enrich_with_vendor_attributes(billing_document, vendor)
    billing_document = enrich_with_warehouse_attributes(billing_document, warehouse)
    billing_document = enrich_with_material_attributes(billing_document, material)
    billing_document = add_unit_price(billing_document)
    billing_document = add_foreign_keys(billing_document)
    billing_document = update_title(billing_document)
    result = select_cols(billing_document)
    return result

result = compute(billing_document,customer,customer_account,vendor,warehouse,material)

# Run data quality checks
result = validate_transform(result)

# Write result to Delta table
result.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03")
    
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
        raise ValueError(f"â�Œ {message}")
    else:
        print(f"âœ… null passed: {row_count:,} rows")

    return df


def update_title(billing_doc):
    return billing_doc.withColumn(
        'title',
        F.concat(
            F.lit('Invoice: '),
            F.col('billing_document_|_vbeln'),
            F.lit(', Item: '),
            F.col('item_|_posnr'),
            F.lit(', Ship To: '),
            F.col('customer_|_kunnr'),
            F.lit(' on '),
            F.col('billing_date_|_fkdat').cast('string'),
        )
    )


def enrich_with_ship_to_customer_attributes(billing_document, customer):
    # enrich billing documents with various general customer attributes
    # note: this join is being done on the ship-to customer number,
    #       so the below-listed properties on billing documents will refer to the ship-to customer
    return billing_document.join(
        select_customer_cols(customer),
        on=['mandt_kunnr_shipto_|_foreign_key_KNA1'],
        how="left"
    )


def select_customer_cols(customer):
    return customer.select(
        F.col('primary_key').alias('mandt_kunnr_shipto_|_foreign_key_KNA1'),
        'region_|_regio',
        'account_group_|_ktokd',
        'country_|_land1',
        'name_|_name1',  # TODO: replace dependencies with ship_to_customer_name, then remove
        'name_2_|_name2',
        'city_|_ort01',
        'postal_code_|_pstlz',  # used to connect to zip code object type
        'district_|_ort02',
        'industry_|_brsch',  # TODO: replace dependencies with ship_to_account_type, then remove
        'customer_classific_|_kukla',
        'description_|_vtext',
        'customer_|_kunnr'  # TODO: replace dependencies with ship_to_customer_number, then remove
    ).withColumn(
        'ship_to_customer_number',
        F.col('customer_|_kunnr')
    ).withColumn(
        'ship_to_customer_name',
        F.col('name_|_name1')
    ).withColumn(
        'ship_to_account_type',
        F.col('industry_|_brsch')
    )


def enrich_with_sold_to_customer_account_attributes(billing_document_enriched, customer_account):
    # enrich billing documents with sales-specific customer attributes
    # note: this join is being done on the sold-to customer number (kunag),
    #       so the below-listed properties on billing documents will refer to the sold-to customer
    return billing_document_enriched.join(
        select_customer_account_cols(customer_account),
        on=['mandt_kunag_vkorg_vtweg_spart_|_foreign_key_KNVV'],
        how="left"
    )


def select_customer_account_cols(customer_account):
    return customer_account.select(
        F.col('primary_key').alias('mandt_kunag_vkorg_vtweg_spart_|_foreign_key_KNVV'),
        'billing_block_for_sales_area_|_faksd',
        'central_billing_block_|_faksd',
        'customer_group_1_|_kvgr1',  # used for SPD - Level 3 Class of Trade
        'kvgr1_description_|_bezei',  # Level 3 Class of Trade Desription
        'customer_group_5_|_kvgr5',  # used to determine market segment
        'pva_prime_number_|_yypvapri',
        'customer_primary_bu_|_yyprime',
        "mandt_kunn2_|_foreign_key_KNA1_|_parvw__A1",
        "mandt_kunn2_|_name_|_name1_|_parvw__A1",
        "mandt_kunn2_|_foreign_key_KNA1_|_parvw__A2",
        "mandt_kunn2_|_name_|_name1_|_parvw__A2",
        "mandt_kunn2_|_foreign_key_KNA1_|_parvw__A3",
        "mandt_kunn2_|_name_|_name1_|_parvw__A3",
        "mandt_kunn2_|_foreign_key_KNA1_|_parvw__PG",
        "mandt_kunn2_|_name_|_name1_|_parvw__PG",
        "ISS_Territory_Name",
        "ISS__Name",
        "ISS_TerritoryAbbr",
        "SalesRepName",
        'mandt_lifnr_|_foreign_key_LFA1',  # TODO: review dependencies, then remove
        'vendor_|_lifnr'  # TODO: review dependencies, then remove
    )


def enrich_with_vendor_attributes(billing_document, vendor):
    return billing_document.join(
        select_vendor_cols(vendor),
        on="mandt_mfrnr_|_foreign_key_LFA1",
        how="left"
    )


def select_vendor_cols(vendor):
    return vendor.select(
        F.col('primary_key').alias('mandt_mfrnr_|_foreign_key_LFA1'),
        F.col('name_|_name1').alias('manufacturer_|_name_|_name1'),
        F.col('name_2_|_name2').alias('manufacturer_|_name_2_|_name2')
    )


def enrich_with_warehouse_attributes(billing_document, warehouse):
    return billing_document.join(
        F.broadcast(select_warehouse_cols(warehouse)),
        on="mandt_werks_|_foreign_key_T001W",
        how="left"
    )


def select_warehouse_cols(warehouse):
    return warehouse.select(
        F.col('primary_key').alias('mandt_werks_|_foreign_key_T001W'),
        F.col('city_|_ort01').alias('warehouse_|_city_|_ort01'),
        F.col('plant_|_werks').alias('warehouse_|_plant_|_werks'),
        F.col('region_|_regio').alias('warehouse_|_region_|_regio'),
        F.col('name_1_|_name1').alias('warehouse_|_name_1_|_name1')
    )


def enrich_with_material_attributes(billing_document, material):
    return billing_document.join(
        select_material_cols(material),
        on='mandt_matnr_|_foreign_key_MARA',
        how='left'
    )


def select_material_cols(material):
    return material.select(
        F.col('primary_key').alias('mandt_matnr_|_foreign_key_MARA'),
        'cin',
        'gcn_|_yycgcn',
        'gcn_seq_no_|_yygcnsqn',
        'cardinal_key',
        'sizedimensions_|_groes',
        'strength_|_yystrn',
        'pack_size_|_yypksze',
        'yypkqtym',
        'form_|_yyform',
        'corrected_pack_qty'
    )


def add_unit_price(billing_document):
    return billing_document.withColumn(
            'unit_price', F.col('sales_value') / F.col('billed_quantity_|_fkimg')
        )


def add_foreign_keys(billing_document):
    return (
        billing_document
        .withColumn(
            'customer_soldto_material_pair_foreign_key',
            F.concat_ws('_||_', *['mandt_kunag_|_foreign_key_KNA1', 'mandt_matnr_|_foreign_key_MARA'])
        )
        .withColumn(
            'customer_billto_material_pair_foreign_key',
            F.concat_ws('_||_', *['mandt_kunrg_|_foreign_key_KNA1', 'mandt_matnr_|_foreign_key_MARA'])
        )
        .withColumn(
            'customer_shipto_material_pair_foreign_key',
            F.concat_ws('_||_', *['mandt_kunnr_shipto_|_foreign_key_KNA1', 'mandt_matnr_|_foreign_key_MARA'])
        )
        .withColumn(
            'customer_soldto_trade_name_pair_foreign_key',
            F.concat_ws('_||_', *['mandt_kunag_|_foreign_key_KNA1', 'trade_name_|_yytradname'])
        )
        .withColumn(
            'customer_billto_trade_name_pair_foreign_key',
            F.concat_ws('_||_', *['mandt_kunrg_|_foreign_key_KNA1', 'trade_name_|_yytradname'])
        )
        .withColumn(
            'customer_shipto_trade_name_pair_foreign_key',
            F.concat_ws('_||_', *['mandt_kunnr_shipto_|_foreign_key_KNA1', 'trade_name_|_yytradname'])
        )
    )


def select_cols(billing_doc):
    return billing_doc.select(
        # Billing Document Data
        "primary_key",
        "title",
        "billing_document_|_vbeln",
        "item_|_posnr",
        "billing_date_|_fkdat",
        "reference_document_|_vgbel",
        "billing_type_|_fkart",
        "sd_document_categ_|_vbtyp",
        # Sales Document Data
        "sales_document_|_aubel",
        "sales_document_item_|_aupos",
        # Quantity
        "billed_quantity_|_fkimg",
        "total_unit_strength",
        "speciality_strength_value_|_yystngsp",
        # Item Info
        "cin",
        "material_|_matnr",
        "fdb_ndc_|_yyndcfdb",
        "eanupc_|_ean11",
        "material_description_|_maktg_|_customization_field_MAKT_|_MANDT_MATNR_SPRAS",
        "trade_name_|_yytradname",
        "generic_name_|_yygenn",
        "strength_|_yystrn",
        "form_|_yyform",
        "gcn_seq_no_|_yygcnsqn",
        "gcn_|_yycgcn",
        "cardinal_key",
        'corrected_pack_qty',
        "sizedimensions_|_groes",
        "pack_size_|_yypksze",
        "yypkqtym",
        "material_group_|_matkl",
        "material_pricing_grp_|_kondm",
        "material_pricing_group_names",
        "base_unit_of_measure_|_meins",
        "item_category_|_pstyv",
        "spd_flag",
        "source_flag",
        # Vendor/Manufacturer Info
        "manufacturer_|_name_|_name1",
        "manufacturer_|_name_2_|_name2",
        # Pricing Info
        "unit_price",
        "subtotal_2_|_kzwi2",
        "subtotal_11_|_yykzwi11",
        "net_value_|_netwr",
        "sales_value",
        "document_currency_|_waerk",
        # Distribution, Segment, Division, etc.
        "distribution_channel_|_vtweg",
        "sales_organization_|_vkorg",
        "division_|_spart",
        "industry_sector_|_mbrsh",
        "contract_type_|_pctyp",
        # Ship To Customer Info
        'ship_to_customer_number',
        'ship_to_customer_name',
        'ship_to_account_type',
        "customer_|_kunnr",
        "name_|_name1",
        "name_2_|_name2",
        "industry_|_brsch",
        "description_|_vtext",
        "account_group_|_ktokd",
        "customer_classific_|_kukla",
        # Ship To Location Info
        "region_|_regio",
        "country_|_land1",
        "city_|_ort01",
        "postal_code_|_pstlz",
        "district_|_ort02",
        # Warehouse Info
        "warehouse_|_plant_|_werks",
        "warehouse_|_name_1_|_name1",
        "warehouse_|_city_|_ort01",
        "warehouse_|_region_|_regio",
        # Sold To Customer Info
        "customer_group_1_|_kvgr1",
        "kvgr1_description_|_bezei",
        "customer_group_5_|_kvgr5",
        "customer_primary_bu_|_yyprime",
        "pva_prime_number_|_yypvapri",
        "billing_block_for_sales_area_|_faksd",
        "central_billing_block_|_faksd",
        "vendor_|_lifnr",
        # Affiliations
        "mandt_kunn2_|_name_|_name1_|_parvw__A1",
        "mandt_kunn2_|_name_|_name1_|_parvw__A2",
        "mandt_kunn2_|_name_|_name1_|_parvw__A3",
        "mandt_kunn2_|_name_|_name1_|_parvw__PG",
        # Inside Sales Info
        "ISS_Territory_Name",
        "ISS__Name",
        "ISS_TerritoryAbbr",
        "SalesRepName",
        # Billing Details
        "weight_unit_|_gewei",
        "net_weight_|_ntgew",
        "transportation_group_|_tragr",
        "cancelled_|_fksto",
        "ds_vendor_invoice_|_yyvndrinv",
        # Foreign Keys
        "mandt_aubel_aupos_|_foreign_key_VBAP",
        "mandt_vbeln_posnr_|_foreign_key_VBAP",
        "mandt_aubel_|_foreign_key_VBAK",
        "mandt_vbeln_|_foreign_key_VBAK",
        "mandt_vbeln_|_foreign_key_VBRK",
        "mandt_knkli_|_foreign_key_KNA1",
        "mandt_kunag_|_foreign_key_KNA1",
        "mandt_kunrg_|_foreign_key_KNA1",
        "mandt_kunnr_shipto_|_foreign_key_KNA1",
        "mandt_irm_gpo_cd_|_foreign_key_KNA1",
        "mandt_kunn2_|_foreign_key_KNA1_|_parvw__A1",
        "mandt_kunn2_|_foreign_key_KNA1_|_parvw__A2",
        "mandt_kunn2_|_foreign_key_KNA1_|_parvw__A3",
        "mandt_kunn2_|_foreign_key_KNA1_|_parvw__PG",
        "mandt_kunag_vkorg_vtweg_spart_|_foreign_key_KNVV",
        "mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV",
        "mandt_irm_gpo_cd_vkorg_auft_vtweg_auft_spart_|_foreign_key_KNVV",
        "mandt_pmatn_|_foreign_key_MARA",
        "mandt_matwa_|_foreign_key_MARA",
        "mandt_matnr_|_foreign_key_MARA",
        "mandt_upmat_|_foreign_key_MARA",
        "mandt_lifnr_|_foreign_key_LFA1",
        "mandt_mfrnr_|_foreign_key_LFA1",
        "mandt_vbeln_|_foreign_key_LIKP",
        "mandt_vbeln_posnr_|_foreign_key_LIPS",
        "mandt_matnr_spras_|_foreign_key_MAKT",
        "mandt_matnr_werks_|_foreign_key_MARC",
        "mandt_knuma_ag_|_foreign_key_KONA",
        "mandt_bukrs_|_foreign_key_T001",
        "mandt_werks_|_foreign_key_T001W",
        "mandt_spras_kvgr1_|_foreign_key_TVV1T",
        "mandt_spras_fkart_|_foreign_key_TVFKT",
        "customer_soldto_material_pair_foreign_key",
        "customer_billto_material_pair_foreign_key",
        "customer_shipto_material_pair_foreign_key",
        "customer_soldto_trade_name_pair_foreign_key",
        "customer_billto_trade_name_pair_foreign_key",
        "customer_shipto_trade_name_pair_foreign_key",
        "mandt_pctyp_spras_|_foreign_key_/IRM/TPCTYPT",
    )  