from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, configure
# from transforms.api import Check
# from transforms import expectations as E
from myproject.utils import remove_null_cols, enrich_with_billing_document_metrics, add_time_series_metric_id_cols

_PK_COLS = [
    'mandt_kunnr_vkorg_vtweg_spart_|_foreign_key_KNVV',
    'mandt_kunnr_|_foreign_key_KNA1',
    'mandt_kunn2_|_foreign_key_KNA1',
    'partner_function_|_parvw',
]

_SPARK_OPTS = [
    "EXECUTOR_MEMORY_LARGE",
    "DRIVER_MEMORY_MEDIUM",
    "EXECUTOR_MEMORY_OVERHEAD_LARGE",
    "NUM_EXECUTORS_64",
    "DRIVER_MEMORY_OVERHEAD_EXTRA_LARGE",
    "EXECUTOR_CORES_LARGE",
    "DYNAMIC_ALLOCATION_ENABLED"
]
# _SEMANTIC_VERSION = 1
# _REQUIRE_INCREMENTAL = True

_PARTNER_FUNCTION_IDS = [
    'A1',
    'A2',
    'A3',
    'AL',
    'CM',
    'PG',
    # 'PY',
    'SG',
    # 'SH',
    # 'SP',
    'VG',
    'WG'
]

_PARTNER_FUNCTION_NAME_MAP = {
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


def get_customer_affiliation_df(customer, billing_document, partner_function_id):
    customer_affiliation_df = enrich_with_billing_document_metrics(
        customer.dataframe(),
        billing_document.dataframe(),
        grouping_cols=[F.col(f'mandt_kunn2_|_foreign_key_KNA1_|_parvw__{partner_function_id}')],
        grouping_name=f'customer_affiliation__{partner_function_id}'
    )
    customer_affiliation_df = (
        customer_affiliation_df
        .filter(
            F.col(f"customer_affiliation__{partner_function_id}_has_billing_documents_since_2022_01_01").isNotNull()
        )
    )
    customer_affiliation_df = remove_null_cols(customer_affiliation_df)
    customer_affiliation_df = add_time_series_metric_id_cols(
        customer_affiliation_df,
        'primary_key',
        group_type_id=f'customer_affiliation__{partner_function_id}',
        filter_ids=['all', 'spd_only', 'pd_only']
    )
    return customer_affiliation_df


@configure(profile=_SPARK_OPTS)
# @incremental(
#     require_incremental=_REQUIRE_INCREMENTAL,
#     semantic_version=_SEMANTIC_VERSION,
# )
@transform(
    customer=Input("ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b"),
    billing_document=Input("ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03"),
    customer_affiliation_a3=Output("ri.foundry.main.dataset.b1600f93-a9e4-4ef1-b2c2-e63d2207edee")
    # customer_affiliation_a1=Output("ri.foundry.main.dataset.3e1a53ec-1880-4a36-9ef1-3e3314e21594"),
    # customer_affiliation_a2=Output("ri.foundry.main.dataset.79cdec15-62f8-4710-9b46-83cb2758d2b8"),
    # customer_affiliation_pg=Output("ri.foundry.main.dataset.b5669e30-e712-4179-9464-ff4ac2ec60a9")
)
def output_customer_affiliation_dfs(
    customer, billing_document,  # inputs
    customer_affiliation_a3  # outputs
    # customer_affiliation_a1, customer_affiliation_a2, customer_affiliation_pg  # outputs
):
    # customer_affiliation_a1.write_dataframe(
    #     get_customer_affiliation_df(customer, billing_document, partner_function_id='A1')
    # )
    # customer_affiliation_a2.write_dataframe(
    #     get_customer_affiliation_df(customer, billing_document, partner_function_id='A2')
    # )
    customer_affiliation_a3.write_dataframe(
        get_customer_affiliation_df(customer, billing_document, partner_function_id='A3')
    )
    # customer_affiliation_pg.write_dataframe(
    #     get_customer_affiliation_df(customer, billing_document, partner_function_id='PG')
    # )
