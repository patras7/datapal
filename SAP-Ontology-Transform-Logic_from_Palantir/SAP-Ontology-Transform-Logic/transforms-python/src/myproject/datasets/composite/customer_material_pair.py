from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, Check, configure
# from transforms.api import incremental
from transforms import expectations as E
from myproject.utils import remove_null_cols, get_cols_sorted_by_null_count
from myproject.utils import (
    enrich_with_billing_document_metrics, enrich_with_billing_document_features, add_time_series_metric_id_cols
)
from myproject.utils import cast_decimal_cols_to_double


_OUTPUT_NAME = 'customer_material_pair'
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
    customer_material_pair_output=Output(
        "ri.foundry.main.dataset.f17c2ffd-f444-4691-a43c-ca87b008b644",
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
    material=Input("ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b"),
    billing_document=Input("ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72"),
)
def compute(customer_material_pair_output, customer, material, billing_document):
    # this is done in place of a cross join to save computation time:
    customer_material_pair = (
        billing_document.dataframe()
        .select(
            F.col('mandt_kunag_|_foreign_key_KNA1'),
            F.col('mandt_kunrg_|_foreign_key_KNA1'),
            F.col('mandt_kunnr_shipto_|_foreign_key_KNA1'),
            F.col('mandt_matnr_|_foreign_key_MARA')
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
            F.col('mandt_matnr_|_foreign_key_MARA')
        )
        .filter(
            (
                (F.col('mandt_kunnr_|_foreign_key_KNA1').isNotNull())
                & (F.length(F.col('mandt_kunnr_|_foreign_key_KNA1')) > 0)
                & (F.col("mandt_matnr_|_foreign_key_MARA").isNotNull())
                & (F.length(F.col('mandt_matnr_|_foreign_key_MARA')) > 0)
            )
        )
        .drop_duplicates()
        .withColumn(
            'primary_key',
            F.concat_ws('_||_', *['mandt_kunnr_|_foreign_key_KNA1', 'mandt_matnr_|_foreign_key_MARA'])
        )
        .withColumn(
            '_group_type', F.lit('customer_material_pair')
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
    customer_material_pair = (
        enrich_with_billing_document_metrics(
            customer_material_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunag_|_foreign_key_KNA1'), F.col('mandt_matnr_|_foreign_key_MARA')],
            'customer_soldto_material_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # 2. calculating metrics on the bill-to level:
    customer_material_pair = (
        enrich_with_billing_document_metrics(
            customer_material_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunrg_|_foreign_key_KNA1'), F.col('mandt_matnr_|_foreign_key_MARA')],
            'customer_billto_material_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # 3. calculating metrics on the ship-to level:
    customer_material_pair = (
        enrich_with_billing_document_metrics(
            customer_material_pair,
            billing_document.dataframe(),
            [F.col('mandt_kunnr_shipto_|_foreign_key_KNA1'), F.col('mandt_matnr_|_foreign_key_MARA')],
            'customer_shipto_material_pair',
            enrich_with_ranks_pcntgs_and_cumsums=False
        )
    )

    # left-join customer cols:
    customer = (
        customer.dataframe()
        .withColumnRenamed('primary_key', 'mandt_kunnr_|_foreign_key_KNA1')
    )
    intersecting_cols = (
        set.intersection(set(customer_material_pair.columns), set(customer.columns))
        - set(['mandt_kunnr_|_foreign_key_KNA1'])
    )
    customer = customer.drop(*intersecting_cols)
    customer_material_pair = (
        customer_material_pair
        .join(customer, on='mandt_kunnr_|_foreign_key_KNA1', how='left')
    )


    # left-join material cols:
    material = (
        material.dataframe()
        .withColumnRenamed('primary_key', 'mandt_matnr_|_foreign_key_MARA')
    )
    intersecting_cols = (
        set.intersection(set(customer_material_pair.columns), set(material.columns))
        - set(['mandt_matnr_|_foreign_key_MARA'])
    )
    material = material.drop(*intersecting_cols)
    customer_material_pair = (
        customer_material_pair
        .join(material, on='mandt_matnr_|_foreign_key_MARA', how='left')
    )

    # adding time series metrics:
    customer_material_pair = add_time_series_metric_id_cols(
        customer_material_pair,
        'primary_key',
        group_type_id=_OUTPUT_NAME,
        filter_ids=['all']
    )

    # customer_material_pair = remove_null_cols(customer_material_pair)
    # customer_material_pair = customer_material_pair.select(*get_cols_sorted_by_null_count(customer_material_pair))
    # customer_material_pair = cast_decimal_cols_to_double(customer_material_pair)
    customer_material_pair_output.write_dataframe(customer_material_pair)