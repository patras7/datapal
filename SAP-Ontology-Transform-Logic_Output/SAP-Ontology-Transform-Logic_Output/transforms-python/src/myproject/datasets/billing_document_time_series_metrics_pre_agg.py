# =============================================================
# Converted from Palantir Transforms â†’ Databricks PySpark
# Generated on 2026-02-23T11:54:06.411584507Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import datetime, timedelta
from myproject.utils import remove_null_cols, cast_decimal_cols_to_double
from dateutil.relativedelta import relativedelta

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
billing_document = spark.read.table("ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03")
    
def compute(billing_document):
    """Converted from Palantir Transform
    
    Inputs:
      - billing_document: ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03
    Output: ri.foundry.main.dataset.d1c9e450-487f-4c26-8717-379311b4bbe3
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    """

    # Transform logic

    six_months_ago = datetime.now() - relativedelta(months=6)
    _DATE_COL = 'billing_date_|_fkdat'
    _GROUPING_COLS = [
    # product-based properties
    F.col('mandt_matnr_|_foreign_key_MARA'),
    F.col('trade_name_|_yytradname'),
    F.col('eanupc_|_ean11'),
    F.col('fdb_ndc_|_yyndcfdb'),

    # customer-based properties:
    F.col("mandt_kunag_|_foreign_key_KNA1"),
    F.col("mandt_kunrg_|_foreign_key_KNA1"),
    F.col("mandt_kunnr_shipto_|_foreign_key_KNA1"),
    F.col("mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV"),
    F.col("mandt_kunn2_|_foreign_key_KNA1_|_parvw__A1"),
    F.col("mandt_kunn2_|_foreign_key_KNA1_|_parvw__A2"),
    F.col("mandt_kunn2_|_foreign_key_KNA1_|_parvw__A3"),

    # vendor-based properties:
    F.col('mandt_mfrnr_|_foreign_key_LFA1'),

    # billing-document based properties:
    F.col("spd_flag"),
    F.col("source_flag"),

    # date-based properties:
    F.year('billing_date_|_fkdat').alias('billing_year_|_fkdat'),
    F.month('billing_date_|_fkdat').alias('billing_month_|_fkdat'),
    F.weekofyear('billing_date_|_fkdat').alias('billing_weekofyear_|_fkdat'),
    F.dayofyear('billing_date_|_fkdat').alias('billing_dayofyear_|_fkdat')
    ]
    _FILTERS = [
    (F.col('billing_date_|_fkdat') >= six_months_ago)
    ]

    pre_aggd_df =     remove_null_cols(
    cast_decimal_cols_to_double(
    get_billing_document_ts_metrics_df(
    billing_document,
    _GROUPING_COLS,
    _DATE_COL,
    filters=_FILTERS
    )
    )
    )

    # Write result to Delta table
    pre_aggd_df.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.d1c9e450-487f-4c26-8717-379311b4bbe3")


def get_billing_document_ts_metrics_df(billing_document, grouping_cols, date_col, filters=[]):
    billing_document_ts_metrics_df = (
        billing_document
        .withColumn(date_col, F.col(date_col).cast("timestamp"))
        .withColumn('_group_id', F.concat_ws('_||_', *grouping_cols))
    )

    for df_filter in filters:
        billing_document_ts_metrics_df = billing_document_ts_metrics_df.filter(df_filter)

    billing_document_ts_metrics_df = (
        billing_document_ts_metrics_df
        .groupby('_group_id', date_col, *grouping_cols)
        .agg(
            F.count(
                F.col('primary_key')
            ).alias('count_of_billing_document_items'),

            F.collect_set(
                F.col('mandt_matnr_|_foreign_key_MARA')
            ).alias('set_of_mandt_matnr_|_foreign_key_MARA'),

            F.collect_set(
                F.col("mandt_kunag_|_foreign_key_KNA1")
            ).alias('set_of_mandt_kunag_|_foreign_key_KNA1'),

            F.collect_set(
                F.col("mandt_kunrg_|_foreign_key_KNA1")
            ).alias('set_of_mandt_kunrg_|_foreign_key_KNA1'),

            F.collect_set(
                F.col("mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV")
            ).alias('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV'),

            F.sum(
                F.col('billed_quantity_|_fkimg')
            )
            .alias('sum_of_billed_quantity_|_fkimg'),

            F.sum(
                F.col("sales_value")
            ).alias('sum_of_sales_value')
        )
        .withColumn(
            'count_of_mandt_matnr_|_foreign_key_MARA',
            F.size('set_of_mandt_matnr_|_foreign_key_MARA')
        )
        .withColumn(
            'count_of_mandt_kunag_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunag_|_foreign_key_KNA1')
        )
        .withColumn(
            'count_of_mandt_kunrg_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunrg_|_foreign_key_KNA1')
        )
        .withColumn(
            'count_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV',
            F.size('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')
        )
        .withColumn(
            'avg_unit_price',
            F.col('sum_of_sales_value') / F.col('sum_of_billed_quantity_|_fkimg')
        )
    )
    return billing_document_ts_metrics_df
    
result = compute(billing_document)