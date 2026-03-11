from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, Check, configure
# from transforms.api import incremental
from transforms import expectations as E
from myproject.utils import (
    remove_null_cols,
    get_cols_sorted_by_null_count,
    cast_decimal_cols_to_double,
    enrich_with_billing_document_metrics,
    enrich_with_billing_document_features,
    add_time_series_metric_id_cols
)


_OUTPUT_NAME = 'customer_trade_name_pair'
_PK_COLS = ['primary_key']
_SPARK_OPTS = [
    "DRIVER_MEMORY_EXTRA_LARGE",
    "DRIVER_MEMORY_OVERHEAD_EXTRA_LARGE",
    "NUM_EXECUTORS_32",
    "EXECUTOR_MEMORY_LARGE",
    "EXECUTOR_MEMORY_OVERHEAD_LARGE"
]
# _SEMANTIC_VERSION = 1
# _REQUIRE_INCREMENTAL = True


@configure(profile=_SPARK_OPTS)
# @incremental(
#     require_incremental=_REQUIRE_INCREMENTAL,
#     semantic_version=_SEMANTIC_VERSION,
# )
@transform(
    customer_trade_name_pair_output=Output(
        "ri.foundry.main.dataset.2e63a301-4736-403d-a3ae-d001b01d5052",
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
    customer=Input("ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b"),
    trade_name=Input("ri.foundry.main.dataset.a924b066-b68a-434b-a539-5494a4f72f18"),
    billing_document=Input("ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72"),
)
def compute(customer_trade_name_pair_output, customer, trade_name, billing_document):
    # this is done in place of a cross join to save computation time:
    customer_trade_name_pair = (
        billing_document.dataframe()
        .select(
            F.col('mandt_kunag_|_foreign_key_KNA1'),
            F.col('mandt_kunrg_|_foreign_key_KNA1'),
            F.col('mandt_kunnr_shipto_|_foreign_key_KNA1'),
            F.col("trade_name_|_yytradname")
        )
        .withColumn(
            'mandt_kunnr_|_foreign_key_KNA1',
            F.explode(
                F.array_distinct(
                    F.array(
                        F.col('mandt_kunag_|_foreign_key_KNA1'),
                        F.col('mandt_kunrg_|_foreign_key_KNA1'),
                        F.col('mandt_kunnr_shipto_|_foreign_key_KNA1')
                    )
                )
            )
        )
        .select(
            F.col('mandt_kunnr_|_foreign_key_KNA1'),
            F.col("trade_name_|_yytradname")
        )
        .filter(
            (
                (F.col('mandt_kunnr_|_foreign_key_KNA1').isNotNull())
                & (F.length(F.col('mandt_kunnr_|_foreign_key_KNA1')) > 0)
                & (F.col("trade_name_|_yytradname").isNotNull())
                & (F.length(F.col('trade_name_|_yytradname')) > 0)
            )
        )
        .drop_duplicates()
        .withColumn(
            'primary_key',
            F.concat_ws('_||_', *['mandt_kunnr_|_foreign_key_KNA1', 'trade_name_|_yytradname'])
        )
        .withColumn(
            '_group_type', F.lit('customer_trade_name_pair')
        )
        .withColumn(
            '_filter_id', F.lit('all')
        )
        .filter(
            F.col('primary_key').isNotNull()
        )
    )

    # get billing document metrics:
    # 1. calculating metrics on the sold-to level:
    customer_trade_name_pair = (
        enrich_with_billing_document_metrics(
            customer_trade_name_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunag_|_foreign_key_KNA1'), F.col('trade_name_|_yytradname')],
            'customer_soldto_trade_name_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # 2. calculating metrics on the bill-to level:
    customer_trade_name_pair = (
        enrich_with_billing_document_metrics(
            customer_trade_name_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunrg_|_foreign_key_KNA1'), F.col('trade_name_|_yytradname')],
            'customer_billto_trade_name_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # 3. calculating metrics on the ship-to level:
    customer_trade_name_pair = (
        enrich_with_billing_document_metrics(
            customer_trade_name_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunnr_shipto_|_foreign_key_KNA1'), F.col('trade_name_|_yytradname')],
            'customer_shipto_trade_name_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # left-join customer cols:
    customer = (
        customer.dataframe()
        .withColumnRenamed('primary_key', 'mandt_kunnr_|_foreign_key_KNA1')
    )
    intersecting_cols = (
        set.intersection(set(customer_trade_name_pair.columns), set(customer.columns))
        - set(['mandt_kunnr_|_foreign_key_KNA1'])
    )
    customer = customer.drop(*intersecting_cols)
    customer_trade_name_pair = (
        customer_trade_name_pair
        .join(customer, on='mandt_kunnr_|_foreign_key_KNA1', how='left')
    )

    # left-join trade_name cols:
    trade_name = (
        trade_name.dataframe()
        .withColumnRenamed('primary_key', "trade_name_|_yytradname")
    )

    intersecting_cols = (
        set.intersection(set(customer_trade_name_pair.columns), set(trade_name.columns))
        - set(['trade_name_|_yytradname'])
    )
    trade_name = trade_name.drop(*intersecting_cols)
    customer_trade_name_pair = (
        customer_trade_name_pair
        .join(trade_name, on='trade_name_|_yytradname', how='left')
    )

    # adding time series metrics:
    customer_trade_name_pair = add_time_series_metric_id_cols(
        customer_trade_name_pair, 
        'primary_key',
        group_type_id=_OUTPUT_NAME, 
        filter_ids=['all']
    )

    # customer_trade_name_pair = remove_null_cols(customer_trade_name_pair)
    # customer_trade_name_pair = customer_trade_name_pair.select(*get_cols_sorted_by_null_count(customer_trade_name_pair))
    # customer_trade_name_pair = cast_decimal_cols_to_double(customer_trade_name_pair)
    customer_trade_name_pair_output.write_dataframe(customer_trade_name_pair)