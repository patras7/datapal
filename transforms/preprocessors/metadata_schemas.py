from enum import Enum

from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    BooleanType,
)


class MetadataType(str, Enum):
    OBJECTS = "objects"
    FIELDS = "fields"
    LINKS = "links"
    DIFFS = "diffs"


METADATA_OBJECTS_SCHEMA = StructType(
    [
        StructField("object_name", StringType(), False),
        StructField("primary_key_raw_fields", ArrayType(StringType(), False), False),
        StructField("description", StringType(), True),
        # Some source systems (like SAP) store text translation of IDs in a text table. If this field is non-null,
        # it can be used to identify the text table that needs to be joined in to translate a primary key.
        StructField("text_table_names", ArrayType(StringType(), False), True),
    ]
)

METADATA_FIELDS_SCHEMA = StructType(
    [
        StructField("object_name", StringType(), False),
        StructField("field_name", StringType(), False),
        StructField("raw_field_name", StringType(), False),
        StructField("field_type", StringType(), False),
        StructField("field_description", StringType(), True),
        # SAP-specific field, indicating the SAP domain of the field. Used for cleaning.
        StructField("domain_name", StringType(), True),
        # If there is a value in the field_literal, this is not a real field in the table.
        # Instead, it is a fabricated field on the object that must contain the literal string provided.
        StructField("field_literal", StringType(), True),
        # Indicates order of field in table
        StructField("field_order", IntegerType(), True),
    ]
)

METADATA_LINKS_SCHEMA = StructType(
    [
        StructField("primary_object_name", StringType(), False),
        StructField("primary_object_raw_fields", ArrayType(StringType(), False), False),
        StructField("foreign_object_name", StringType(), False),
        StructField("foreign_object_raw_fields", ArrayType(StringType(), False), False),
        StructField("link_description", StringType(), True),
    ]
)

METADATA_DIFFS_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("has_changed", BooleanType(), False),
    ]
)
