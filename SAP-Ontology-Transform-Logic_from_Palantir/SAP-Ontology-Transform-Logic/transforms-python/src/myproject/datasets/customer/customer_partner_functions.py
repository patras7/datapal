from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, Check, configure
from transforms import expectations as E

_PK_COLS = [
    # 'mandt_kunnr_vkorg_vtweg_spart_|_foreign_key_KNVV',
    'mandt_kunnr_|_foreign_key_KNA1',
    'mandt_kunn2_|_foreign_key_KNA1',
    'partner_function_|_parvw',
]
_SPARK_OPTS = ["DRIVER_MEMORY_LARGE", "NUM_EXECUTORS_16"]
# _SEMANTIC_VERSION = 1
# _REQUIRE_INCREMENTAL = True

PARTNER_FUNCTION_NAME_MAP = {
    "1D": "Hierarchy Node",
    "A1": "partner_func level1",
    "A2": "partner_func level2",
    "A3": "partner_func level3",
    "AL": "Dynamic Allocation (Common Location)",
    "CM": "Campus",
    "PG": "Primary GPO",
    "PY": "Payer",
    "SG": "Alternate GPO",
    "SH": "Ship-to party",
    "SP": "Sold-to party",
    "VG": "Volume Group (Old Common Owner)",
    "WG": "WAPD Group",
}


@configure(profile=_SPARK_OPTS)
# @incremental(
#     require_incremental=_REQUIRE_INCREMENTAL,
#     semantic_version=_SEMANTIC_VERSION,
# )
@transform(
    a1=Output(
        "ri.foundry.main.dataset.c961e7a0-5e7c-4668-a556-8f8befc21b8d",
        checks=[
            Check(
                expectation=E.primary_key(*_PK_COLS),
                name='customer_a1_partner_func primary key uniqueness check',
                on_error='WARN',
                description='warns when customer_a1_partner_func has duplicates'
            ),
            Check(
                expectation=E.count().gt(0),
                name='customer_a1_partner_func not empty check',
                on_error='FAIL',
                description='fails when customer_a1_partner_func is empty '
            ),
        ]
    ),
    a2=Output(
        "ri.foundry.main.dataset.f9fc0c0d-a98f-4cee-b5af-5b9c29110118",
        checks=[
            Check(
                expectation=E.primary_key(*_PK_COLS),
                name='customer_a2_partner_func primary key uniqueness check',
                on_error='WARN',
                description='warns when customer_a2_partner_func has duplicates'
            ),
            Check(
                expectation=E.count().gt(0),
                name='customer_a2_partner_func not empty check',
                on_error='FAIL',
                description='fails when customer_a2_partner_func is empty '
            ),
        ]
    ),
    a3=Output(
        "ri.foundry.main.dataset.ac2cd031-f630-4a51-b9ea-4baecc4278a1",
        checks=[
            Check(
                expectation=E.primary_key(*_PK_COLS),
                name='customer_a3_partner_func primary key uniqueness check',
                on_error='WARN',
                description='warns when customer_a3_partner_func has duplicates'
            ),
            Check(
                expectation=E.count().gt(0),
                name='customer_a3_partner_func not empty check',
                on_error='FAIL',
                description='fails when customer_a3_partner_func is empty '
            ),
        ]
    ),
    al=Output(
        "ri.foundry.main.dataset.0bd653f4-fab4-447e-9b4d-d39ddf8d1f6b",
        checks=[
            Check(
                expectation=E.primary_key(*_PK_COLS),
                name='customer_al_partner_func primary key uniqueness check',
                on_error='WARN',
                description='warns when customer_al_partner_func has duplicates'
            ),
            Check(
                expectation=E.count().gt(0),
                name='customer_al_partner_func not empty check',
                on_error='FAIL',
                description='fails when customer_al_partner_func is empty '
            ),
        ]
    ),
    knvp=Input("ri.foundry.main.dataset.d25c665a-eefa-4ee9-8d9f-1efb3158ee76"),
)
def compute(a1, a2, a3, al, knvp):
    knvp = knvp.dataframe()
    knvp = (
        knvp
        .select(
            *_PK_COLS
        )
    )
    a1.write_dataframe(
        knvp
        .filter(F.col('partner_function_|_parvw') == F.lit('A1'))
        .drop_duplicates()
    )
    a2.write_dataframe(
        knvp
        .filter(F.col('partner_function_|_parvw') == F.lit('A2'))
        .drop_duplicates()
    )
    a3.write_dataframe(
        knvp
        .filter(F.col('partner_function_|_parvw') == F.lit('A3'))
        .drop_duplicates()
    )
    al.write_dataframe(
        knvp
        .filter(F.col('partner_function_|_parvw') == F.lit('AL'))
        .drop_duplicates()
    )
