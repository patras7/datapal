from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, Check, configure
# from transforms.api import incremental
from transforms import expectations as E
from myproject.utils import enrich_with_billing_document_metrics, add_time_series_metric_id_cols

_OUTPUT_NAME = 'vendor'
_PK_COLS = ['primary_key']
_SPARK_OPTS = ["DRIVER_MEMORY_EXTRA_LARGE", "NUM_EXECUTORS_32", "EXECUTOR_MEMORY_LARGE"]
_METRIC_COLS = [
    "count_of_billing_document_items",
    "count_of_mandt_kunag_|_foreign_key_KNA1",
    "count_of_mandt_kunrg_|_foreign_key_KNA1",
    "count_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV",
    "sum_of_billed_quantity_|_fkimg",
    "sum_of_sales_value",
    "avg_unit_price",
    'size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
    'lagged_size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA'
]
# _SEMANTIC_VERSION = 1
# _REQUIRE_INCREMENTAL = True


@configure(profile=_SPARK_OPTS)
# @incremental(
#     require_incremental=_REQUIRE_INCREMENTAL,
#     semantic_version=_SEMANTIC_VERSION,
# )
@transform_df(
    Output(
        "ri.foundry.main.dataset.0c6ae379-81f7-4385-b251-aa56bf65e126",
        checks=[
            Check(
                expectation=E.primary_key(*_PK_COLS),
                name=f'{_OUTPUT_NAME} primary key uniqueness check',
                on_error='WARN',
                description=f'warns when {_OUTPUT_NAME} has duplicates'
            ),
            Check(
                expectation=E.count().gt(0),
                name=f'{_OUTPUT_NAME} not empty check',
                on_error='FAIL',
                description=f'fails when {_OUTPUT_NAME} is empty '
            ),
        ]
    ),
    lfa1=Input("ri.foundry.main.dataset.fc385255-6db6-4567-9052-5a8dea6881d7"),
    billing_document=Input("ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72"),
)
def compute(lfa1, billing_document):
    vendor = (
        lfa1
        .select(
            F.col("primary_key"),
            F.col("vendor_|_lifnr"),
            F.col("name_|_name1"),
            F.col("name_2_|_name2"),
            F.col("name_3_|_name3"),
            F.col("name_4_|_name4"),
            F.col("search_term_|_sortl"),
            F.col("repackager_|_yyrepackager"),
            F.col("exclusive_distributor_|_yyexcldist"),
            F.col("wholesale_government_distributor_|_yywholgov"),
            F.col("manufacturer_|_yymanuf"),
            F.col("tax_jurisdiction_|_txjcd"),
            F.col("customer_|_kunnr"),
            F.col("account_group_|_ktokk"),
            F.col("vendor_|_lifnr").cast("int").alias("vendor_nbr")
        )
        .withColumn(
            "title",
            F.concat("name_|_name1", F.lit(" ("), "vendor_nbr", F.lit(")"))
        )
    )

    # add billing document metrics:
    # note: we join with billing document on only manufacturer here (not yet on the supplier)
    vendor = enrich_with_billing_document_metrics(
        vendor,
        # billing_document.dataframe().repartition('mandt_mfrnr_|_foreign_key_LFA1'),
        billing_document,
        grouping_cols=[F.col('mandt_mfrnr_|_foreign_key_LFA1')],
        grouping_name='manufacturer'
    )

    # adding time series metrics:
    vendor = add_time_series_metric_id_cols(
        vendor,
        'primary_key',
        group_type_id=_OUTPUT_NAME,
        filter_ids=['all', 'spd_only', 'pd_only']
    )
    return vendor
