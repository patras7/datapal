# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.730148300Z (UTC)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


from transforms.api import Pipeline

from myproject import datasets


my_pipeline = Pipeline()
my_pipeline.discover_transforms(datasets)
