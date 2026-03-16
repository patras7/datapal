from pyspark.sql.session import SparkSession

from software_defined_integrations.transforms.preprocessors.metadata_schemas import (
    METADATA_FIELDS_SCHEMA,
)


def generate_fields_metadata(spark_session: SparkSession, object_name: str):
    field_names = [
        "parentWebId",
        "webId",
        "id",
        "name",
        "description",
        "path",
        "hasChildren",
        "templateName",
        "type",
        "entityType",
    ]

    fields = [
        {
            "object_name": object_name,
            "field_name": field_name,
            "raw_field_name": field_name,
            "field_type": "string",
        }
        for field_name in field_names
    ]

    return spark_session.createDataFrame(fields, METADATA_FIELDS_SCHEMA)
