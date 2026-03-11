from pyspark.sql import functions as F
from transforms.api import transform_df, configure, Check, Input, Output
from transforms import expectations as E

_OUTPUT_NAME = 'billing_document_contract_filters'
_PK_COLS = ['billing_document_fk']
_SPARK_OPTS = [
    "EXECUTOR_MEMORY_LARGE",
    "EXECUTOR_MEMORY_OVERHEAD_LARGE",
    "NUM_EXECUTORS_64",
    "DRIVER_MEMORY_OVERHEAD_EXTRA_LARGE",
    "ADAPTIVE_ENABLED"
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
        "ri.foundry.main.dataset.4b1945c3-b9d6-4073-b15d-676b97260b8c",
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
                description=f'fails when {_OUTPUT_NAME} is empty'
            ),
        ]
    ),
    billing_document=Input("ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03"),
    material=Input("ri.foundry.main.dataset.f66a2392-be40-483b-977a-e048b660413a"),
)
def compute(billing_document, material):
    billing_document = (
        billing_document
        .select(
            F.col("primary_key").alias("billing_document_fk"),
            F.col("mandt_matnr_|_foreign_key_MARA").alias("product_fk"),
            F.col("mandt_kunrg_|_foreign_key_KNA1").alias("customer_payer_fk"),
            F.col("mandt_kunag_|_foreign_key_KNA1").alias("customer_sold_to_fk"),
            F.col("mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV").alias("customer_master_sales_fk"),


            "sales_value",
            F.col("billing_date_|_fkdat").alias("billing_date"),
            F.col("billed_quantity_|_fkimg").alias("billed_quantity"),
            F.col("material_pricing_group_names").alias("material_pricing_group_name"),
            "total_unit_strength",
            "spd_flag",
            "source_flag",

            # Inherited customer columns
            F.col("industry_|_brsch").alias("industry"),
            F.col("description_|_vtext").alias("customer_classification_name"),
            F.col("name_|_name1").alias("customer_name1"),
            F.col("name_2_|_name2").alias("customer_name2"),
            F.col("mandt_kunn2_|_name_|_name1_|_parvw__A1").alias("affiliation1_name1"),
            F.col("mandt_kunn2_|_name_|_name1_|_parvw__A2").alias("affiliation2_name1"),
            F.col("mandt_kunn2_|_name_|_name1_|_parvw__A3").alias("affiliation3_name1"),

            # Temporary Columns
            F.col("trade_name_|_yytradname").alias("trade_name_billing_order")
        )
    )

    material = (
        material
        .select(
            F.col("primary_key").alias("product_fk"),
            F.col("mandt_yysupnr_|_foreign_key_LFA1").alias("supplier_vendor_fk"),
            F.col("mandt_mfrnr_|_foreign_key_LFA1").alias("manufacturer_vendor_fk"),
            F.col("material_|_matnr").alias("product_cin"),

            F.col("speciality_strength_value_|_yystngsp").alias("specialty_units"),
            F.col("strength_|_yystrn").alias("product_strength"),
            F.col("material_description_|_maktx_|_customization_field_MAKT_|_MANDT_MATNR_SPRAS").alias("product_description"),
            F.col("material_group_|_matkl").alias("product_group"),

            # Temporary Columns
            F.col("trade_name_|_yytradname").alias("trade_name_product")
        )
    )

    joined = (
        billing_document
        .join(
            material,
            on="product_fk",
            how="left"
        )
    )

    derived_columns = (
        joined
        .withColumn(
            "trade_name",
            F.coalesce("trade_name_product", "trade_name_billing_order")
        )
    )

    output = (
        derived_columns
        .drop(
            "trade_name_product",
            "trade_name_billing_order"
        )
    )

    return output
