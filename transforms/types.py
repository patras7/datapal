from enum import Enum


# Enumerate SpecialFieldName
class SpecialFieldName(str, Enum):
    PRIMARY_KEY = "primary_key"
    TITLE = "title"
    SOURCE = "source"
    SOURCE_DATASET_NAME = "source_dataset_name"
    SCHEMA_MISMATCH_COLUMN_NAME = "schema_mismatch"
    SERIES_ID = "series_id"
