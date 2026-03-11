# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-27T10:50:36.984908500Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


import csv
from pyspark.sql import Row
from myproject.utils import to_snake_case

FILE_NAMES = [
    "ActionTypes_Lookup.txt",
    "ApplicationDocs.txt",
    "Applications.txt",
    "ApplicationsDocsType_Lookup.txt",
    "MarketingStatus.txt",
    "MarketingStatus_Lookup.txt",
    "Products.txt",
    "SubmissionClass_Lookup.txt",
    "SubmissionPropertyType.txt",
    "Submissions.txt",
    "TE.txt"
ROOT_OUTPUT_PATH = '/Cardinal Health/ph-core-ontology-sap/data/transform'
OUTPUT_DATASET_NAMES = list(sorted(['fda_drug_'+to_snake_case(y.split('.txt')[0]) for y in FILE_NAMES]))
OUTPUT_DATASET_OBJS = [Output(f'{ROOT_OUTPUT_PATH}/{x}') for x in OUTPUT_DATASET_NAMES]
OUTPUT_MAP = dict(zip(OUTPUT_DATASET_NAMES, OUTPUT_DATASET_OBJS))
OUTPUT_MAP2 = dict(zip(OUTPUT_DATASET_NAMES, FILE_NAMES))


# @configure(
#    profile=[
#        "NUM_EXECUTORS_4"
#) # Removed: Not supported in Databricks
    
# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load input datasets
fda_upload_df = spark.read.table("ri.foundry.main.dataset.4c423eb4-f7bc-4e9b-84d1-c9f1a19d5d09")

def parse_fda_datasets(fda_upload_df,OUTPUT_MAP):
    """Converted from Palantir Transform
    
    Inputs:
      - fda_upload_df: ri.foundry.main.dataset.4c423eb4-f7bc-4e9b-84d1-c9f1a19d5d09
    
     NOTE: Output uses Foundry RID format
    Consider replacing with: catalog.schema.table_name
    
    """
    
    def process_file(file_status):
        with fda_upload_df.filesystem().open(file_status.path, 'r', encoding='utf-8', errors='ignore') as f:
            r = csv.reader(f, delimiter='\t')
            header = next(r)
            MyRow = Row(*(to_snake_case(x) for x in header))
            for row in r:
                if len(row) != len(header):
                    row = row[0:len(header)]
                yield MyRow(*row)

    # file_regex = f"({'|'.join(OUTPUT_MAP2.values())})"
    # files_df = fda_upload_df.filesystem().files(regex=file_regex)
    # processed_df = files_df.rdd.flatMapValues(process_file)
    # processed_df2 = processed_df.flatMap(lambda x: x.toDF())
    # for ind, v in enumerate(OUTPUT_MAP.values()):
    #     rdd_vals = processed_df2[ind]
    #     v.write_dataframe(rdd_vals.toDF())
    # # # temp = 2
    for k, v in OUTPUT_MAP.items():
        files_df = fda_upload_df.filesystem().files(OUTPUT_MAP2.get(k))
        processed_df = files_df.rdd.flatMap(process_file).toDF()
        v.write_dataframe(processed_df)

result = parse_fda_datasets(fda_upload_df,OUTPUT_MAP)
# =============================================================================
# FILESYSTEM OPERATIONS GUIDE
# =============================================================================
# Original Palantir code used .filesystem() for file operations.
# 
# Databricks equivalents:
#   1. List files:
#      files = dbutils.fs.ls('/path/to/files')
# 
#   2. Read binary files:
#      df = spark.read.format('binaryFile').load('/path')
# 
#   3. Read text files:
#      df = spark.read.text('/path')
# 
#   4. Process multiple files:
#      paths = [f.path for f in dbutils.fs.ls('/path')]
#      for path in paths:
#          df = spark.read.parquet(path)
# =============================================================================
