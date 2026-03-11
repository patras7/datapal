# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, Check, configure
# from transforms.api import incremental
from transforms import expectations as E
from myproject.utils import remove_null_cols, get_cols_sorted_by_null_count

_OUTPUT_NAME = 'plant'
_PK_COLS = ['primary_key']
_SPARK_OPTS = ["DRIVER_MEMORY_MEDIUM", "NUM_EXECUTORS_16"]
# _SEMANTIC_VERSION = 1
# _REQUIRE_INCREMENTAL = True


@configure(profile=_SPARK_OPTS)
# @incremental(
#     require_incremental=_REQUIRE_INCREMENTAL,
#     semantic_version=_SEMANTIC_VERSION,
# )
@transform_df(
    Output(
        "ri.foundry.main.dataset.f639e8e1-f6f3-4e4a-9cb1-998692be2e9e",
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
    source_df=Input("ri.foundry.main.dataset.315f272c-1d47-49f2-ba00-27474ae7b700"),
)
def compute(source_df):
    source_df = remove_null_cols(source_df)
    source_df = source_df.select(
        *get_cols_sorted_by_null_count(source_df)
    )
    return source_df
