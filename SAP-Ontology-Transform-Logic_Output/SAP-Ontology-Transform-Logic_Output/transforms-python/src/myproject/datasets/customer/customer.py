# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.017465600Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from myproject.utils import remove_null_cols, get_cols_sorted_by_null_count
from myproject.utils import enrich_with_billing_document_metrics, add_time_series_metric_id_cols

_PARTNER_FUNCTION_IDS = ['A1', 'A2', 'A3', 'PG']

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
kna1 = spark.read.table("ri.foundry.main.dataset.ef45e3c5-4321-4bcd-9199-281cb1d13b06")
knvv = spark.read.table("ri.foundry.main.dataset.8d7d861f-1c62-4802-97fa-0c96b90d3bc3")
knvp = spark.read.table("ri.foundry.main.dataset.d25c665a-eefa-4ee9-8d9f-1efb3158ee76")
tkukt = spark.read.table("ri.foundry.main.dataset.991c0bd0-7287-4de1-8791-b35ad579d224")
billing_document = spark.read.table("ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72")
usa_state_metadata = spark.read.table("ri.foundry.main.dataset.9f2a148c-62dc-45eb-ac67-9cc5c0c0a353")
usa_zip_code_metadata = spark.read.table("ri.foundry.main.dataset.1d6e6fde-1ae9-4f3b-b9d0-25785157aebf")
kna1_territories_mapping = spark.read.table("ri.foundry.main.dataset.77906277-99d0-42cb-b1f3-fcbb0eba6415")
tvv1t = spark.read.table("ri.foundry.main.dataset.3c735c21-498b-4e63-91a5-b652868d35dd")
    
    
def compute(kna1,
    knvv,
    knvp,
    tkukt,
    billing_document,
    usa_state_metadata,
    usa_zip_code_metadata,
    kna1_territories_mapping,
    tvv1t):
    """Converted from Palantir Transform
    
    Inputs:
      - kna1: ri.foundry.main.dataset.ef45e3c5-4321-4bcd-9199-281cb1d13b06
      - knvv: ri.foundry.main.dataset.8d7d861f-1c62-4802-97fa-0c96b90d3bc3
      - knvp: ri.foundry.main.dataset.d25c665a-eefa-4ee9-8d9f-1efb3158ee76
      - tkukt: ri.foundry.main.dataset.991c0bd0-7287-4de1-8791-b35ad579d224
      - billing_document: ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72
      - usa_state_metadata: ri.foundry.main.dataset.9f2a148c-62dc-45eb-ac67-9cc5c0c0a353
      - usa_zip_code_metadata: ri.foundry.main.dataset.1d6e6fde-1ae9-4f3b-b9d0-25785157aebf
      - kna1_territories_mapping: ri.foundry.main.dataset.77906277-99d0-42cb-b1f3-fcbb0eba6415
      - tvv1t: ri.foundry.main.dataset.3c735c21-498b-4e63-91a5-b652868d35dd
    Output: ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    Data Quality Checks: 2 check(s)
      - customer kna1 primary key uniqueness check (WARN)
      - customer kna1 not empty check (FAIL)
    """
    
    # enrich Customer:
    kna1_enriched = get_kna1_enriched_df(
    kna1,
    tkukt,
    usa_state_metadata,
    usa_zip_code_metadata,
    knvp,
    kna1_territories_mapping,
    billing_document
    )
    customer.write_dataframe(kna1_enriched)

    # enrich Customer Account:
    knvv_enriched = get_knvv_enriched_df(
    kna1_enriched,
    knvv,
    knvp,
    billing_document,
    tvv1t
    )
    customer_account = knvv_enriched
    return customer_account

result = compute(kna1,
    knvv,
    knvp,
    tkukt,
    billing_document,
    usa_state_metadata,
    usa_zip_code_metadata,
    kna1_territories_mapping,
    tvv1t)

# Run data quality checks
result = validate_transform(result)

# Write result to Delta table
result.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b")


# =============================================================================
# DATA QUALITY VALIDATION FUNCTIONS
# Converted from Palantir Checks
# =============================================================================

# Original Palantir Checks:
# Check(
#                 expectation=E.primary_key(*_PK_COLS),
#                 name='customer kna1 primary key uniqueness check',
#                 on_error='WARN',
#                 description='warns when customer kna1 has duplicates'
#             )
# Check(
#                 expectation=E.count().gt(0),
#                 name='customer kna1 not empty check',
#                 on_error='FAIL',
#                 description='fails when customer kna1 is empty '
#             )

def validate_transform(df):
    """
    Data quality validation - converted from Palantir Checks
    
    Checks:
      - customer kna1 primary key uniqueness check: warns when customer kna1 has duplicates
      - customer kna1 not empty check: fails when customer kna1 is empty 
    """
    from pyspark.sql import functions as F

    # Check 1: customer kna1 primary key uniqueness check
    # Check 2: customer kna1 not empty check
    # Row count check
    row_count = df.count()
    if not (row_count > 0):
        message = f"Count check failed: {row_count} does not satisfy 'gt 0'"
        raise ValueError(f"❌ {message}")
    else:
        print(f"✅ customer kna1 not empty check passed: {row_count:,} rows")

    return df


def get_kna1_enriched_df(kna1, tkukt, usa_state_metadata, usa_zip_code_metadata, knvp, kna1_territories_mapping, billing_document):
    kna1_enriched = (
        kna1.dataframe()
        # remove deleted customers
        .filter(
            F.col("central_deletion_flag_|_loevm").isNull()
        )
        # get customer descriptions from tkukt:
        .join(
            (
                tkukt.dataframe()
                .select('primary_key', 'description_|_vtext')
                .withColumnRenamed("primary_key", "mandt_spras_kukla_|_foreign_key_TKUKT")
            ),
            on=['mandt_spras_kukla_|_foreign_key_TKUKT'],
            how="left"
        )
        # add state metadata:
        .withColumn('state_code', F.col('region_|_regio'))
        .withColumn('zip_code', F.regexp_replace(F.col('postal_code_|_pstlz'), r'[-].*', ''))
        .join(
            usa_state_metadata.dataframe(),
            on=['state_code'],
            how='left'
        )
        # add zip code metadata:
        .withColumn('zip_code_geo_id', F.concat_ws('_||_', F.col('state_geo_id'), F.col('zip_code')))
        .join(
            (
                usa_zip_code_metadata.dataframe()
                .select(
                    'zip_code_geo_id',
                    'zip_code_geometry_centroid_ontology_geo_point',
                    'zip_code_geometry_to_h3_r5_outer_cover',
                    'zip_code_geometry_to_h3_r5_outer_cover_neighbors',
                    'zip_code_geometry_to_h3_r6_outer_cover',
                    'zip_code_geometry_to_h3_r6_outer_cover_neighbors'
                )
            ),
            on=['zip_code_geo_id'],
            how='left'
        )
        # map salesforce territories and sales reps to customers:
        .join(
            kna1_territories_mapping.dataframe(),
            on=['primary_key'],
            how='left'
        )
    )

    # add partner fks and names:
    for pf_id in _PARTNER_FUNCTION_IDS:
        kna1_enriched = enrich_df_with_customer_partner_fks_and_names(
            kna1_enriched,
            kna1.dataframe(),
            knvp.dataframe(),
            partner_function_id=pf_id,
            customer_entity_key='mandt_kunnr_|_foreign_key_KNA1'
        )
    # kna1_enriched = kna1_enriched.drop_duplicates(subset=['primary_key'])  # hotfix to ensure no duplicates

    # add billing document metrics based on sold-to party (default for kna1_enriched):
    kna1_enriched = enrich_with_billing_document_metrics(
        kna1_enriched,
        billing_document.dataframe(),
        grouping_cols=[F.col('mandt_kunag_|_foreign_key_KNA1')],
        grouping_name='customer'  # this should later be changed to 'customer_soldto' but requires ontology property col remapping
    )

    # add time series metrics based on sold-to party (kunag) (default for kna1_enriched):
    kna1_enriched = add_time_series_metric_id_cols(
        kna1_enriched,
        'primary_key',
        group_type_id='customer',  # this should later be changed to 'customer_soldto' but requires ontology property col remapping
        filter_ids=['all', 'spd_only', 'pd_only']
    )

    # add billing document metrics based on ship-to party:
    kna1_enriched = enrich_with_billing_document_metrics(
        kna1_enriched,
        billing_document.dataframe(),
        grouping_cols=[F.col('mandt_kunnr_shipto_|_foreign_key_KNA1')],
        grouping_name='customer_shipto'
    )

    # add time series metrics based on ship-to party:
    kna1_enriched = add_time_series_metric_id_cols(
        kna1_enriched,
        'primary_key',
        group_type_id='customer_shipto',
        filter_ids=['all', 'spd_only', 'pd_only']
    )

    # add billing document metrics based on bill-to party (kunrg):
    kna1_enriched = enrich_with_billing_document_metrics(
        kna1_enriched,
        billing_document.dataframe(),
        grouping_cols=[F.col('mandt_kunrg_|_foreign_key_KNA1')],
        grouping_name='customer_billto'
    )

    # add time series metrics based on bill-to party (kunrg):
    kna1_enriched = add_time_series_metric_id_cols(
        kna1_enriched,
        'primary_key',
        group_type_id='customer_billto',
        filter_ids=['all', 'spd_only', 'pd_only']
    )

    kna1_enriched = kna1_enriched.drop_duplicates(subset=['primary_key'])
    # clean up:
    # kna1_enriched = remove_null_cols(kna1_enriched)
    # kna1_enriched = kna1_enriched.select(
    #     *get_cols_sorted_by_null_count(kna1_enriched)
    # )
    return kna1_enriched


def get_knvv_enriched_df(kna1, knvv, knvp, billing_document, tvv1t):
    knvv = knvv.dataframe()

    # enrich knvv with partner fks and names (note: this should be done first to avoid col collisions later on)
    for pf_id in _PARTNER_FUNCTION_IDS:
        knvv_enriched = enrich_df_with_customer_partner_fks_and_names(
            knvv,
            kna1,
            knvp.dataframe(),
            partner_function_id=pf_id,
            customer_entity_key="mandt_kunnr_vkorg_vtweg_spart_|_foreign_key_KNVV"
        )

    # knvv_enriched = knvv_enriched.drop_duplicates(subset=['primary_key'])  # hotfix to ensure no duplicates

    # create knvv_enriched:
    kna1_uniq_cols = list(set(kna1.columns) - set(knvv.columns))
    knvv_enriched = (
        knvv
        # remove deleted customer accounts:
        .filter(
            F.col("del_indicator_for_sales_area_|_loevm").isNull()
        )
        # join knvv with kna1_enriched and pre-select cols to avoid collisions:
        .join(
            kna1.select(
                F.col('primary_key').alias('mandt_kunnr_|_foreign_key_KNA1'),
                *kna1_uniq_cols
            ),
            on=['mandt_kunnr_|_foreign_key_KNA1'],
            how='left'
        )
        # Get Level 3 Class of Trade descriptions from TVV1T:
        .join(
            (
                tvv1t.dataframe()
                .select('primary_key', 'description_|_bezei', "customer_group_1_|_kvgr1")
                .withColumnRenamed("primary_key", "foreign_key_TVV1T")
                .withColumnRenamed("description_|_bezei", "kvgr1_description_|_bezei")
            ),
            on=["customer_group_1_|_kvgr1"],
            how="left"
        )
    )

    # add billing document metrics based on sold-to party (kunag) (default for knvv_enriched)
    knvv_enriched = enrich_with_billing_document_metrics(
        knvv_enriched,
        billing_document.dataframe(),
        grouping_cols=[F.col('mandt_kunag_vkorg_vtweg_spart_|_foreign_key_KNVV')],
        grouping_name='customer_account'  # this should later be changed to 'customer_account_soldto' but requires ontology property col remapping
    )

    # add time series metric id cols based on sold-to party (kunag) (default for knvv_enriched)
    knvv_enriched = add_time_series_metric_id_cols(
        knvv_enriched,
        'primary_key',
        group_type_id='customer_account',  # this should later be changed to 'customer_account_soldto' but requires ontology property col remapping
        filter_ids=['all', 'spd_only', 'pd_only']
    )

    # add billing document metrics based on bill-to party (kunrg)
    knvv_enriched = enrich_with_billing_document_metrics(
        knvv_enriched,
        billing_document.dataframe(),
        grouping_cols=[F.col('mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')],
        grouping_name='customer_account_billto'
    )

    # add time series metric id cols based on bill-to party (kunrg)
    knvv_enriched = add_time_series_metric_id_cols(
        knvv_enriched,
        'primary_key',
        group_type_id='customer_account_billto',
        filter_ids=['all', 'spd_only', 'pd_only']
    )

    # clean up
    # knvv_enriched = remove_null_cols(knvv_enriched)
    # knvv_enriched = knvv_enriched.select(
    #     *get_cols_sorted_by_null_count(knvv_enriched)
    # )

    knvv_enriched = knvv_enriched.drop_duplicates(subset=['primary_key'])

    return knvv_enriched


def enrich_df_with_customer_partner_fks_and_names(
    df, kna1, knvp, partner_function_id,
    customer_entity_key,
    partner_col_suffix=''
):
    # note: customer_entity_key is the primary key col for either the customer or the customer account

    # note: partner_col_suffix is needed when you are enriching a df that has multiple customer-reference cols
    #       (like billing documents, for example, which has both kunag [sold-to-party] and kunrg [bill-to-party])

    partner_fk_col_name = f'mandt_kunn2_|_foreign_key_KNA1_|_parvw__{partner_function_id}{partner_col_suffix}'
    partner_name1_col_name = f'mandt_kunn2_|_name_|_name1_|_parvw__{partner_function_id}{partner_col_suffix}'

    knvp_filtered = (
        knvp
        .filter(
            # remove NO AFFILIATION partners
            F.col('mandt_kunn2_|_foreign_key_KNA1') != F.lit("QE9_test_|_100_|_7007999999")
        )
        .filter(
            # filter knvp for partner function of interest
            F.col('partner_function_|_parvw') == F.lit(partner_function_id)
        )
        .filter(
            # remove reflexive partnerships
            F.col('mandt_kunnr_|_foreign_key_KNA1') != F.col('mandt_kunn2_|_foreign_key_KNA1')
        )
        .select(customer_entity_key, 'mandt_kunn2_|_foreign_key_KNA1')
        .withColumnRenamed('mandt_kunn2_|_foreign_key_KNA1', partner_fk_col_name)  # add partner function id to partner foreign key
        .drop_duplicates(subset=[customer_entity_key])
    )

    knvp_filtered_with_partner_names = (
        knvp_filtered
        .join(
            # get partner names from kna1:
            (
                kna1.select(
                    F.col('primary_key').alias(partner_fk_col_name),
                    F.col('name_|_name1').alias(partner_name1_col_name)
                )
            ),
            on=[partner_fk_col_name],
            how='left'
        )
    )

    df_enriched = (
        df
        .join(
            knvp_filtered_with_partner_names.withColumnRenamed(customer_entity_key, 'primary_key'),
            on=['primary_key'],
            how='left'
        )
    )
    return df_enriched.drop_duplicates()

def enrich_with_chc_info(kna1_enriched, chc_pharmacy_list):
    chc_pharmacy_list_renamed = chc_pharmacy_list.dataframe().withColumnRenamed("customer_|_kunnr", "chc_customer_|_kunnr")
    
    kna1_enriched_with_chc = kna1_enriched.join(
        chc_pharmacy_list_renamed,
        kna1_enriched["customer_|_kunnr"] == chc_pharmacy_list_renamed["chc_customer_|_kunnr"],
        "left"
    )
    
    kna1_enriched_with_chc = kna1_enriched_with_chc.withColumn(
        "is_chc",
        F.when(F.col("chc_customer_|_kunnr").isNotNull(), True).otherwise(False)
    ).drop("chc_customer_|_kunnr")
    
    return kna1_enriched_with_chc