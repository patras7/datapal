# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.055363300Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

PARTITION_COLS = [
    # 'mandt_kunag_|_foreign_key_KNA1',
    # 'mandt_matnr_|_foreign_key_MARA',
    'trade_name_|_yytradname',
    'base_unit_of_measure_|_meins'
]
DATE_COL = 'billing_date_|_fkdat'
TXN_BLOCK_RELATIVE_SIZE = 0.01
TXN_BLOCK_ABSOLUTE_SIZE = 10000000

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
billing_document_df = spark.read.table("ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03")
    

def get_billing_document_txn_blocks(#relative_size_txn_block_output, absolute_size_txn_block_output
billing_document_df):
    """Converted from Palantir Transform
    
    Inputs:
      - billing_document_df: ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03
    Output: ri.foundry.main.dataset.3fdd6cbc-305b-446b-8873-a8e2db8c5655
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    """

    # Transform logic

    total_sales_value = (
    billing_document_df
    .groupby(*PARTITION_COLS)
    .agg(
    F.sum('sales_value').alias('total_sales_value')
    )
    )

    sorted_date_col_window_asc = (
    Window
    .partitionBy(*PARTITION_COLS)
    .orderBy(
    F.col(DATE_COL).asc(),
    F.col('primary_key').asc()
    )
    )

    sorted_date_col_window_asc_for_cumulative_ops = (
    sorted_date_col_window_asc
    .rangeBetween(
    Window.unboundedPreceding,
    0
    )
    )

    billing_document_df = (
    billing_document_df
    .withColumn(
    'row_rank', F.row_number().over(sorted_date_col_window_asc)
    )
    .withColumn(
    'cumulative_total_sales_value',
    F.sum('sales_value').over(sorted_date_col_window_asc_for_cumulative_ops)
    )
    .join(
    total_sales_value,
    on=PARTITION_COLS if PARTITION_COLS else None,
    how='left' if PARTITION_COLS else 'cross'
    )
    .withColumn(
    'cumulative_total_sales_value_pcntg',
    F.col('cumulative_total_sales_value') / F.col('total_sales_value')
    )
    .withColumn(
    'relative_size_transaction_block_label',
    F.floor(
    F.col('cumulative_total_sales_value_pcntg') / TXN_BLOCK_RELATIVE_SIZE
    )
    )
    .withColumn(
    'relative_size_transaction_block_label',
    F.when(F.col('relative_size_transaction_block_label') < 0, 0).otherwise(F.col('relative_size_transaction_block_label'))
    )
    .withColumn(
    'relative_size_transaction_block_id',
    F.concat_ws(
    '_||_',
    *PARTITION_COLS,
    F.col('relative_size_transaction_block_label').cast('string')
    )
    )
    .withColumn(
    'absolute_size_transaction_block_label',
    F.floor(
    F.col('cumulative_total_sales_value') / TXN_BLOCK_ABSOLUTE_SIZE
    )
    )
    .withColumn(
    'absolute_size_transaction_block_label',
    F.when(F.col('absolute_size_transaction_block_label') < 0, 0).otherwise(F.col('absolute_size_transaction_block_label'))
    )
    .withColumn(
    'absolute_size_transaction_block_id',
    F.concat_ws(
    '_||_',
    *PARTITION_COLS,
    F.col('absolute_size_transaction_block_label').cast('string')
    )
    )
    # .withColumn(
    #     'cumulative_avg_sales_value',
    #     F.avg('sales_value').over(sorted_date_col_window_asc_for_cumulative_ops)
    # )
    # .withColumn(
    #     'cumulative_billing_doc_ids',
    #     F.collect_set('primary_key').over(sorted_date_col_window_asc)
    # )
    )

    relative_txn_block_df = (
    billing_document_df
    .groupby(
    'relative_size_transaction_block_id',
    *PARTITION_COLS
    )
    .agg(
    F.min(DATE_COL).alias('earliest_txn_date'),
    F.max(DATE_COL).alias('latest_txn_date'),
    F.datediff(F.max(DATE_COL), F.min(DATE_COL)).alias('txn_block_duration_in_days'),
    F.approx_count_distinct(F.col('primary_key')).alias('uniq_txn_count'),
    F.approx_count_distinct(F.col('mandt_kunag_|_foreign_key_KNA1')).alias('uniq_customer_count'),
    F.approx_count_distinct(F.col('mandt_matnr_|_foreign_key_MARA')).alias('uniq_product_count'),
    F.sum('sales_value').alias('total_sales_value'),
    F.avg('sales_value').alias('avg_sales_value'),
    F.min('unit_price').alias('min_unit_price'),
    F.percentile_approx(F.col('unit_price'), 0.5).alias('median_unit_price'),
    F.avg('unit_price').alias('avg_unit_price'),
    F.max('unit_price').alias('max_unit_price'),
    )
    )
    relative_size_txn_block_output.write_dataframe(relative_txn_block_df)

    absolute_txn_block_df = (
    billing_document_df
    .groupby(
    'absolute_size_transaction_block_id',
    *PARTITION_COLS
    )
    .agg(
    F.min(DATE_COL).alias('earliest_txn_date'),
    F.max(DATE_COL).alias('latest_txn_date'),
    F.datediff(F.max(DATE_COL), F.min(DATE_COL)).alias('txn_block_duration_in_days'),
    F.approx_count_distinct(F.col('primary_key')).alias('uniq_txn_count'),
    F.approx_count_distinct(F.col('mandt_kunag_|_foreign_key_KNA1')).alias('uniq_customer_count'),
    F.approx_count_distinct(F.col('mandt_matnr_|_foreign_key_MARA')).alias('uniq_product_count'),
    F.sum('sales_value').alias('total_sales_value'),
    F.avg('sales_value').alias('avg_sales_value'),
    F.min('unit_price').alias('min_unit_price'),
    F.percentile_approx(F.col('unit_price'), 0.5).alias('median_unit_price'),
    F.avg('unit_price').alias('avg_unit_price'),
    F.max('unit_price').alias('max_unit_price'),
    )
    )
    absolute_size_txn_block_output = absolute_txn_block_df
    absolute_size_txn_block_output.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.3fdd6cbc-305b-446b-8873-a8e2db8c5655")

result = get_billing_document_txn_blocks(billing_document_df)