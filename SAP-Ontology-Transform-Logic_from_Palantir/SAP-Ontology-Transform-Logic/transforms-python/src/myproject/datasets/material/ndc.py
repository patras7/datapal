from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, Check, configure
# from transforms.api import incremental
from transforms import expectations as E
from myproject.utils import remove_null_cols, get_cols_sorted_by_null_count
from myproject.utils import (
    enrich_with_billing_document_metrics, enrich_with_billing_document_features, add_time_series_metric_id_cols
)
from myproject.utils import cast_decimal_cols_to_double
from pyspark.sql import SparkSession


_OUTPUT_NAME = 'ndc'
_PK_COLS = ['primary_key']
_SPARK_OPTS = ["DRIVER_MEMORY_EXTRA_LARGE", "NUM_EXECUTORS_32", "EXECUTOR_MEMORY_LARGE"]
# _SEMANTIC_VERSION = 1
# _REQUIRE_INCREMENTAL = True


@configure(profile=_SPARK_OPTS)
# @incremental(
#     require_incremental=_REQUIRE_INCREMENTAL,
#     semantic_version=_SEMANTIC_VERSION,
# )
@transform_df(
    Output(
        "/Cardinal Health/ph-core-ontology-sap/data/ontology/ndc",
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
    material=Input("ri.foundry.main.dataset.9d75b88e-db81-44e3-9772-51d73994b54b"),
    billing_document=Input("ri.foundry.main.dataset.1b3a46f2-8c30-4c50-a15d-ebb6945a1e72"),
)
def compute(material, billing_document):
    ndc = (
        material
        .filter(
            ~(
                F.col("fdb_ndc_|_yyndcfdb").isNull()
                | (F.col("fdb_ndc_|_yyndcfdb") == "")
            )
        )
        .groupby(
            "fdb_ndc_|_yyndcfdb"
        )
        .agg(
            F.first("client_|_mandt").alias("client_|_mandt"),
            F.collect_set('primary_key').alias('mandt_matnr_|_foreign_key_MARA_|_list'),
            F.collect_set('generic_name_|_yygenn').alias('generic_name_|_yygenn_|_list'),
            F.collect_set("trade_name_|_yytradname").alias("trade_name_|_yytradname_|_list"),
            F.collect_set("gcn_|_yycgcn").alias("gcn_|_yycgcn_|_list"),
            F.collect_set("mandt_mfrnr_|_foreign_key_LFA1").alias("mandt_mfrnr_|_foreign_key_LFA1_|_list"),
            F.collect_set("is_exclusive").alias("exclusive_labels"),
            F.collect_list("spd_labels").alias("spd_labels"),
            F.collect_list("source_labels").alias("source_labels")
        )
        .withColumn(
            'mandt_matnr_|_foreign_key_MARA_|_count', F.size('mandt_matnr_|_foreign_key_MARA_|_list')
        )
        .withColumn(
            'generic_name_|_yygenn_|_count', F.size('generic_name_|_yygenn_|_list')
        )
        .withColumn(
            'trade_name_|_yytradname_|_count', F.size('trade_name_|_yytradname_|_list')
        )
        .withColumn(
            "mandt_mfrnr_|_foreign_key_LFA1_|_count", F.size("mandt_mfrnr_|_foreign_key_LFA1_|_list")
        )
        .withColumn(
            'spd_labels', F.array_distinct(F.flatten('spd_labels'))
        )
        .withColumn(
            'source_labels', F.array_distinct(F.flatten('source_labels'))
        )
        .withColumn(
            'exclusive_labels_count', F.size('exclusive_labels')
        )
        .withColumn(
            'spd_labels_count', F.size('spd_labels')
        )
        .withColumn(
            'source_labels_count', F.size('source_labels')
        )
        .withColumnRenamed("fdb_ndc_|_yyndcfdb", "primary_key")
    )

    ndc = enrich_with_billing_document_metrics(
        ndc,
        billing_document,
        [F.col("fdb_ndc_|_yyndcfdb")],
        'ndc'
    )


    # adding time series metrics:
    ndc = add_time_series_metric_id_cols(
        ndc, 
        'primary_key',
        group_type_id=_OUTPUT_NAME, 
        filter_ids=['all']
    )
    
    # ndc = remove_null_cols(ndc)
    # ndc = ndc.select(*get_cols_sorted_by_null_count(ndc))
    ndc = cast_decimal_cols_to_double(ndc)
    return ndc