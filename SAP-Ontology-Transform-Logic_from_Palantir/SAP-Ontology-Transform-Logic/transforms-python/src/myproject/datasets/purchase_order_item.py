from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, Check, configure
# from transforms.api import incremental
from transforms import expectations as E
from myproject.utils import remove_null_cols, get_cols_sorted_by_null_count


_PK_COLS = ['primary_key']
_OUTPUT_NAME = "purchase_order_item"
_SPARK_OPTS = ["DRIVER_MEMORY_MEDIUM", "NUM_EXECUTORS_64"]
# _SEMANTIC_VERSION = 1
# _REQUIRE_INCREMENTAL = True


@configure(profile=_SPARK_OPTS)
# @incremental(
#     require_incremental=_REQUIRE_INCREMENTAL,
#     semantic_version=_SEMANTIC_VERSION,
# )
@transform_df(
    Output(
        "ri.foundry.main.dataset.068dd3b5-6835-4595-ae74-e2fc0b833d8a",
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
    raw_orders=Input("ri.foundry.main.dataset.c9e4cf3b-7422-481e-b0fd-39c76bb1b8b8"),
)
def compute(raw_orders):
    output = remove_null_cols(raw_orders)
    output = output.select(
        *get_cols_sorted_by_null_count(output)
    )
    return output
