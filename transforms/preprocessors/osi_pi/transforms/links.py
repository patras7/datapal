from pyspark.sql.session import SparkSession

from software_defined_integrations.transforms.preprocessors.metadata_schemas import (
    METADATA_LINKS_SCHEMA,
)


def generate_links_metadata(spark_session: SparkSession):
    return spark_session.createDataFrame([], METADATA_LINKS_SCHEMA)
