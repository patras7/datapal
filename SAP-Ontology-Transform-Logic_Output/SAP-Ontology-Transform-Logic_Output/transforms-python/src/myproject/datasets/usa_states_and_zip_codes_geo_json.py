# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.712194800Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from myproject.utils import remove_null_cols
from myproject.utils import enrich_with_billing_document_metrics, add_time_series_metric_id_cols

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
source_df = spark.read.table("ri.foundry.main.dataset.73749345-3f8e-493a-8f04-01c6ca5f0851")
billing_document = spark.read.table("ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03")
    
def compute1(source_df, billing_document):
    """Converted from Palantir Transform
    
    Inputs:
      - source_df: ri.foundry.main.dataset.73749345-3f8e-493a-8f04-01c6ca5f0851
      - billing_document: ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03
    Output: ri.foundry.main.dataset.49f2aeb5-1143-4620-8459-0eabc4b83692
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    """

    # Transform logic

    df = source_df.withColumnRenamed('state_code', 'primary_key')

    state_df_enriched = (
    enrich_with_billing_document_metrics(
    df,
    billing_document.withColumnRenamed('region_|_regio', 'state_code'),
    grouping_cols=[F.col('state_code')],
    grouping_name='state'
    )
    .withColumnRenamed('primary_key', 'state_code')
    .withColumnRenamed('state_geo_id', 'primary_key')
    )

    state_df_enriched = remove_null_cols(state_df_enriched)

    # adding time series metrics:
    state_df_enriched = add_time_series_metric_id_cols(
    state_df_enriched,
    'primary_key',
    group_type_id='state', 
    filter_ids=['all', 'spd_only', 'pd_only']
    )
    # Write result to Delta table
    state_df_enriched.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.49f2aeb5-1143-4620-8459-0eabc4b83692")
    
    return state_df_enriched

result = compute1(source_df, billing_document)

# Load input datasets
source_df = spark.read.table("ri.foundry.main.dataset.1d6e6fde-1ae9-4f3b-b9d0-25785157aebf")
billing_document = spark.read.table("ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03")
    
def compute2(source_df, billing_document):
    """Converted from Palantir Transform
    
    Inputs:
      - source_df: ri.foundry.main.dataset.73749345-3f8e-493a-8f04-01c6ca5f0851
      - billing_document: ri.foundry.main.dataset.78905dc3-ec76-4ca1-b497-cb2f8a02ba03
    Output: ri.foundry.main.dataset.49f2aeb5-1143-4620-8459-0eabc4b83692
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    """

    # Transform logic

    df = source_df.withColumnRenamed('zip_code', 'primary_key')

    billing_document_df = billing_document.withColumn(
    'zip_code',
    F.regexp_replace(F.col('postal_code_|_pstlz'), r'[-].*', '')

    zip_code_df_enriched = (
        enrich_with_billing_document_metrics(
            df,
            billing_document_df,
            grouping_cols=[F.col('zip_code')],
            grouping_name='zip_code'
        )
        .withColumnRenamed('primary_key', 'zip_code')
        .withColumnRenamed('zip_code_geo_id', 'primary_key')
    )

    zip_code_df_enriched = remove_null_cols(zip_code_df_enriched)
    # adding time series metrics:
    zip_code_df_enriched = add_time_series_metric_id_cols(
        zip_code_df_enriched,
        'primary_key',
        group_type_id='zip_code', 
        filter_ids=['all', 'spd_only', 'pd_only']
    )
    # Write result to Delta table
    zip_code_df_enriched.write.format("delta").mode("overwrite").saveAsTable("ri.foundry.main.dataset.f462b9db-a440-4bcc-9fd1-23e355d5106e")    
    
    return zip_code_df_enriched
    
result = compute2(source_df, billing_document)