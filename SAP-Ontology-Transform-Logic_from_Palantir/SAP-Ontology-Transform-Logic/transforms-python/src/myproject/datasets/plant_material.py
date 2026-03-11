from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, Check, configure
# from transforms.api import incremental
from transforms import expectations as E
from myproject.utils import remove_null_cols, get_cols_sorted_by_null_count

_OUTPUT_NAME = 'plant_material'
_PK_COLS = ['primary_key']
#_SPARK_OPTS = ["DRIVER_MEMORY_MEDIUM", "NUM_EXECUTORS_16"]
_SPARK_OPTS = ["KUBERNETES_NO_EXECUTORS"]
# _SEMANTIC_VERSION = 1
# _REQUIRE_INCREMENTAL = True


@configure(profile=_SPARK_OPTS)
# @incremental(
#     require_incremental=_REQUIRE_INCREMENTAL,
#     semantic_version=_SEMANTIC_VERSION,
# )
@transform_df(
    Output(
        "ri.foundry.main.dataset.965b7a77-29fe-4071-97aa-582f2d183df8",
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
    source_df=Input("ri.foundry.main.dataset.02f73f93-36e2-49ae-9071-592ebb139573"),
)
def compute(source_df):
    source_df = remove_null_cols(source_df)
    source_df = (
        source_df
        .select(
            *get_cols_sorted_by_null_count(source_df)
        ).withColumn(
            "spd_flag",
            F.when(
                F.col(
                    'mandt_werks_|_foreign_key_T001W'
                ).isin([
                    'QE9_test_|_100_|_P080',
                    'QE9_test_|_100_|_P090',
                    'QE9_test_|_100_|_P101',
                    'QE9_test_|_100_|_P102',
                    'QE9_test_|_100_|_P104',
                    'QE9_test_|_100_|_P105']),
                True
                ).otherwise(False)
        )
    )

    return source_df
