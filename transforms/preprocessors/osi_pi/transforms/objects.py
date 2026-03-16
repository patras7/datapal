from pyspark.sql.session import SparkSession

from software_defined_integrations.transforms.preprocessors.metadata_schemas import (
    METADATA_OBJECTS_SCHEMA,
)


def generate_objects_metadata(spark_session: SparkSession, object_name: str):
    return spark_session.createDataFrame(
        [
            {
                "object_name": object_name,
                "primary_key_raw_fields": ["path"],
                "description": "The unioned metadata dataset for all root elements",
                "text_table_names": [],
            }
        ],
        METADATA_OBJECTS_SCHEMA,
    )
