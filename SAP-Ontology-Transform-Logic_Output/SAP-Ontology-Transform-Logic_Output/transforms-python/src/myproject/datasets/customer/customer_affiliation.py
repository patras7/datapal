# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.026440800Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from myproject.utils import remove_null_cols, enrich_with_billing_document_metrics, add_time_series_metric_id_cols

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
customer = spark.read.table("ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b")
billing_document = spark.read.table("ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03")

def get_customer_affiliation_df(customer, billing_document, partner_function_id):
    customer_affiliation_df = enrich_with_billing_document_metrics(
        customer,
        billing_document,
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
    
def output_customer_affiliation_dfs(customer, billing_document  # inputs
    #,customer_affiliation_a3  # outputs
    # customer_affiliation_a1, customer_affiliation_a2, customer_affiliation_pg  # outputs
    ):
    """Converted from Palantir Transform
    
    Inputs:
      - customer: ri.foundry.main.dataset.b9b31e14-4754-476d-9a3e-20a6b5c9962b
      - billing_document: ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03
    Output: ri.foundry.main.dataset.b1600f93-a9e4-4ef1-b2c2-e63d2207edee
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    """

    # Transform logic

    # customer_affiliation_a1.write_dataframe(
    #     get_customer_affiliation_df(customer, billing_document, partner_function_id='A1')
    # )
    # customer_affiliation_a2.write_dataframe(
    #     get_customer_affiliation_df(customer, billing_document, partner_function_id='A2')
    # )

    customer_affiliation_a3 = get_customer_affiliation_df(customer, billing_document, partner_function_id='A3')
    customer_affiliation_a3.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.b1600f93-a9e4-4ef1-b2c2-e63d2207edee")

    
    # customer_affiliation_pg.write_dataframe(
    #     get_customer_affiliation_df(customer, billing_document, partner_function_id='PG')
    # )

result = output_customer_affiliation_dfs(customer, billing_document)